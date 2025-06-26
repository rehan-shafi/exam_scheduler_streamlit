from db.session import SessionLocal
from db.models import Course, Student, CourseStudent
from collections import defaultdict
import pandas as pd
from datetime import datetime, timedelta

def get_day_and_time(slot, start_date):
    day_offset = slot // 2
    exam_date = start_date + timedelta(days=day_offset)
    time = "AM" if slot % 2 == 0 else "PM"
    return exam_date.strftime("%Y-%m-%d"), time

def get_student_course_mappings(xml_file_ids):
    db = SessionLocal()
    courses = db.query(Course).filter(Course.xml_file_id.in_(xml_file_ids)).all()
    students = db.query(Student).filter(Student.xml_file_id.in_(xml_file_ids)).all()

    course_map = {c.id: (c.course_code, c.course_name) for c in courses}
    course_code_to_id = {v: k for k, v in course_map.items()}
    student_map = {s.id: s.student_id1 for s in students}

    course_to_students = defaultdict(set)
    student_to_courses = defaultdict(set)

    mappings = db.query(CourseStudent).all()
    for m in mappings:
        if m.course_id not in course_map or m.student_id not in student_map:
            continue
        course_code = course_map[m.course_id]
        student_id = student_map[m.student_id]
        course_to_students[course_code].add(student_id)
        student_to_courses[student_id].add(course_code)

    db.close()
    return course_to_students, student_to_courses, course_map

def schedule_exams_from_db(xml_file_ids, start_date, num_days):
    total_slots = num_days * 2

    course_to_students, student_to_courses, course_map = get_student_course_mappings(xml_file_ids)
    sorted_courses = sorted(course_to_students.items(), key=lambda x: len(x[1]), reverse=True)
    course_list = [course for course, _ in sorted_courses]

    am_slots = list(range(0, total_slots, 2))
    pm_slots = list(range(1, total_slots, 2))
    slot_order = [s for pair in zip(am_slots, pm_slots) for s in pair]

    student_slot_map = {}
    course_slot_map = {}

    for course in course_list:
        students = course_to_students[course]
        assigned = False

        for slot in slot_order:
            conflict = any(
                slot in student_slot_map.get(student, []) or
                any(s // 2 == slot // 2 for s in student_slot_map.get(student, []))
                for student in students
            )
            if not conflict:
                course_slot_map[course] = slot
                for student in students:
                    student_slot_map.setdefault(student, []).append(slot)
                assigned = True
                break

        if not assigned:
            print(f"⚠️ Could not assign slot to: {course}")

    rows = []
    for course, students in course_to_students.items():
        slot = course_slot_map.get(course, None)
        day, time = get_day_and_time(slot, start_date) if slot is not None else ("Unscheduled", "")
        course_code, course_name = course
        for student in students:
            rows.append({
                "Student ID": student,
                "Course Code": course_code,
                "Course Name": course_name,
                "Day": day,
                "Time": time,
                "Slot #": slot if slot is not None else "N/A"
            })


    final_schedule_df = pd.DataFrame(rows)
    return final_schedule_df, student_to_courses, course_to_students
