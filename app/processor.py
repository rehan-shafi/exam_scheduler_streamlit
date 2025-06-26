import pandas as pd
from app.mapping_utils import generate_course_and_student_mappings
import xml.etree.ElementTree as ET
from typing import List, Dict, Tuple
from io import BytesIO
from sqlalchemy.orm import Session
from db.models import XMLFile
from db.models import Course, IgnoredCourse,Student, CourseStudent
from db.session import SessionLocal

def extract_student_course_data(xml_files: List[BytesIO]) -> Tuple[Dict[str, List[str]], Dict[str, List[str]]]:
    student_to_courses = {}
    course_to_students = {}

    for file in xml_files:
        tree = ET.parse(file)
        root = tree.getroot()

        for g_semester in root.findall(".//G_SEMESTER"):
            course_code = g_semester.findtext("COURSE_CODE", "").strip().replace(" ", "")
            student_list = g_semester.find("LIST_G_STUDENT_ID")
            if not course_code or student_list is None:
                continue

            for g_student in student_list.findall("G_STUDENT_ID"):
                student_id = g_student.findtext("STUDENT_ID1", "").strip()
                if not student_id:
                    continue

                student_to_courses.setdefault(student_id, []).append(course_code)
                course_to_students.setdefault(course_code, []).append(student_id)

    return student_to_courses, course_to_students


def insert_xml_data(xml_file: BytesIO, gender: str, filename: str, db: Session):
    # ✅ Step 1: Insert XML record
    xml_record = XMLFile(filename=filename, gender_group=gender)
    db.add(xml_record)
    db.commit()
    db.refresh(xml_record)
    xml_file_id = xml_record.id

    # ✅ Step 2: Parse XML
    tree = ET.parse(xml_file)
    root = tree.getroot()

    # ✅ Step 3: Load ignored course codes from DB
    ignored_course_rows = db.query(IgnoredCourse).all()
    ignored_codes = {row.course_code.strip().replace(" ", "") for row in ignored_course_rows}

    # ✅ Step 4: Loop and insert valid courses
    unique_courses = {}

    for g_semester in root.findall(".//G_SEMESTER"):
        course_code = g_semester.findtext("COURSE_CODE", "").strip().replace(" ", "")
        if not course_code or course_code in ignored_codes:
            continue

        # Only add course once per XML
        if course_code not in unique_courses:
            course_name = g_semester.findtext("COURSE_NAME", "").strip()
            section = g_semester.findtext("SECTION", "").strip()

            course = Course(
                course_code=course_code,
                course_name=course_name,
                section=section,
                xml_file_id=xml_file_id
            )
            db.add(course)
            db.flush()  # get ID immediately
            unique_courses[course_code] = course.id

   
    db.commit()
    # Step 5: Reload inserted courses for lookup (course_code → id)
    course_code_to_id = unique_courses

    # Step 6: Insert students and their course mappings
    student_id_map = {}  # to avoid duplicates

    for g_semester in root.findall(".//G_SEMESTER"):
        course_code = g_semester.findtext("COURSE_CODE", "").strip().replace(" ", "")
        if course_code not in course_code_to_id:
            continue  # was ignored or invalid

        course_id = course_code_to_id[course_code]
        student_list = g_semester.find("LIST_G_STUDENT_ID")
        if student_list is None:
            continue

        for g_student in student_list.findall("G_STUDENT_ID"):
            student_id1 = g_student.findtext("STUDENT_ID1", "").strip()
            name = g_student.findtext("STUDENT_NAME_S", "").strip()
            major = g_student.findtext("MAJOR_DESC", "").strip()

            if not student_id1:
                continue

            # Add student once per XML
            if student_id1 not in student_id_map:
                student = Student(
                    student_id1=student_id1,
                    name=name,
                    major=major,
                    xml_file_id=xml_file_id
                )
                db.add(student)
                db.flush()  # get auto ID without full commit
                student_id_map[student_id1] = student.id
            else:
                student = student_id_map[student_id1]

            # Add course-student mapping
            mapping = CourseStudent(
                course_id=course_id,
                student_id=student_id_map[student_id1]
            )
            db.add(mapping)

    db.commit()  # Bulk commit after inserting all courses
    print(f"\n✅ Finished inserting XML ID {xml_file_id}")
    print(f"   Total courses: {len(course_code_to_id)}")
    print(f"   Total students: {len(student_id_map)}")
    print(f"   Total mappings: {db.query(CourseStudent).filter(CourseStudent.course_id.in_(course_code_to_id.values())).count()}")

    return xml_file_id, root


def process_uploaded_file(file, gender):
    if file is None:
        return None

    db = SessionLocal()
    xml_id, _ = insert_xml_data(file, gender=gender, filename=file.name, db=db)
    db.close()
    return xml_id
