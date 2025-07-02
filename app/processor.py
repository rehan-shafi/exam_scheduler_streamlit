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


def insert_xml_data(xml_file: BytesIO, gender: str, filename: str, db: Session, first_file_id):
    import xml.etree.ElementTree as ET
    from db.models import Course, Student, CourseStudent, XMLFile, IgnoredCourse

    existing_mappings = set()

    # ✅ Step 1: Insert XML file record
    xml_record = XMLFile(filename=filename, gender_group=gender)
    db.add(xml_record)
    db.commit()
    db.refresh(xml_record)
    xml_file_id = xml_record.id

    # ✅ Step 2: Parse XML
    tree = ET.parse(xml_file)
    root = tree.getroot()

    # ✅ Step 3: Load ignored course codes
    ignored_rows = db.query(IgnoredCourse).all()
    ignored_codes = {r.course_code.strip().replace(" ", "") for r in ignored_rows}

    # ✅ Step 4: Prepare mappings
    unique_courses = {}
    student_id_map = {}

    # ✅ Male Campus Parsing
    if gender == "regular":
        for g_semester in root.findall(".//G_SEMESTER"):
            course_code = g_semester.findtext("COURSE_CODE", "").strip().replace(" ", "")
            if not course_code or course_code in ignored_codes:
                continue

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
                db.flush()
                unique_courses[course_code] = course.id

            course_id = unique_courses[course_code]
            student_list = g_semester.find("LIST_G_STUDENT_ID")
            if student_list is not None:
                for g_student in student_list.findall("G_STUDENT_ID"):
                    student_id = g_student.findtext("STUDENT_ID1", "").strip()
                    name = g_student.findtext("STUDENT_NAME_S", "").strip()
                    major = g_student.findtext("MAJOR_DESC", "").strip()

                    if not student_id:
                        continue

                    if student_id not in student_id_map:
                        student = Student(
                            student_id1=student_id,
                            name=name,
                            major=major,
                            xml_file_id=xml_file_id
                        )
                        db.add(student)
                        db.flush()
                        student_id_map[student_id] = student.id

                    if (course_id, student_id_map[student_id]) not in existing_mappings:
                        mapping = CourseStudent(
                            course_id=course_id,
                            student_id=student_id_map[student_id]
                        )
                        db.add(mapping)
                        existing_mappings.add((course_id, student_id_map[student_id]))

    # ✅ Female Campus Parsing
    elif gender == "visitor":
        for record in root.findall(".//ACADEMIC_RECORDS"):
            student_id = record.findtext("STUDENT_ID", "").strip()
            student_name = record.findtext("STUDENT_NAME", "").strip()
            major = record.findtext("MAJOR_NAME", "").strip()

            if not student_id:
                continue

            if student_id not in student_id_map:
                student = Student(
                    student_id1=student_id,
                    name=student_name,
                    major=major,
                    xml_file_id=xml_file_id
                )
                db.add(student)
                db.flush()
                student_id_map[student_id] = student.id

            for g_course in record.findall(".//G_STUDENT_ID1"):
                course_code = g_course.findtext("COURSE_CODE", "").strip().replace(" ", "")
                if "(" in course_code:
                    course_code = course_code.split("(")[0].strip()
                if not course_code or course_code in ignored_codes:
                    continue

                course_name = g_course.findtext("COURSE_NAME", "").strip()
                section = g_course.findtext("SECTION", "").strip()

                if course_code not in unique_courses:
                    existing = db.query(Course).filter(
                        Course.course_code == course_code,
                        Course.xml_file_id.in_([xml_file_id, first_file_id])
                    ).first()

                    if existing:
                        unique_courses[course_code] = existing.id
                    else:
                        course = Course(
                            course_code=course_code,
                            course_name=course_name,
                            section=section,
                            xml_file_id=xml_file_id
                        )
                        db.add(course)
                        db.flush()
                        unique_courses[course_code] = course.id

                course_id = unique_courses[course_code]
                if (course_id, student_id_map[student_id]) not in existing_mappings:
                    mapping = CourseStudent(
                        course_id=course_id,
                        student_id=student_id_map[student_id]
                    )
                    db.add(mapping)
                    existing_mappings.add((course_id, student_id_map[student_id]))

    # ✅ Final Commit
    db.commit()

    print(f"\n✅ Finished inserting XML ID {xml_file_id}")
    print(f"   Total courses: {len(unique_courses)}")
    print(f"   Total students: {len(student_id_map)}")
    print(f"   Total mappings: {db.query(CourseStudent).filter(CourseStudent.course_id.in_(unique_courses.values())).count()}")

    return xml_file_id, root

def process_uploaded_file(file, gender, first_file_id):
    if file is None:
        return None

    db = SessionLocal()
    xml_id, _ = insert_xml_data(file, gender=gender, filename=file.name, db=db,first_file_id=first_file_id)
    db.close()
    return xml_id
