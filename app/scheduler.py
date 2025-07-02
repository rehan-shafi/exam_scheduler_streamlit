from db.session import SessionLocal
from db.models import Course, Student, CourseStudent
from collections import defaultdict
import pandas as pd
from datetime import datetime, timedelta

# ✅ New: Static fixed slots for specified courses
FIXED_COURSE_SLOTS = {
    "ARAB.202": 0,   # Arabic Editing
    "IC.408": 4      # Islamic Political System
}

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

def apply_merged_course_mapping(course_to_students, student_to_courses):
    from db.models import MergedCourse
    db = SessionLocal()
    merged_groups = db.query(MergedCourse).all()
    db.close()

    group_map = defaultdict(set)
    course_to_group = {}

    for item in merged_groups:
        group_map[item.group_id].add(item.course_code)
        course_to_group[item.course_code] = item.group_id

    merged_course_to_students = defaultdict(set)
    for course, students in course_to_students.items():
        course_code = course[0]
        group_id = course_to_group.get(course_code, course_code)
        merged_course_to_students[group_id].update(students)

    merged_student_to_courses = defaultdict(set)
    for student, courses in student_to_courses.items():
        for course in courses:
            course_code = course[0]
            group_id = course_to_group.get(course_code, course_code)
            merged_student_to_courses[student].add(group_id)

    return merged_course_to_students, merged_student_to_courses, group_map, course_to_group

def build_conflict_map(student_to_courses):
    conflict_map = defaultdict(set)
    for courses in student_to_courses.values():
        courses = list(courses)
        for i in range(len(courses)):
            for j in range(i + 1, len(courses)):
                c1 = courses[i]
                c2 = courses[j]
                conflict_map[c1].add(c2)
                conflict_map[c2].add(c1)
    return conflict_map

def backtrack_schedule(course_list, conflict_map, slot_list, fixed_slot_assignment):
    slot_assignment = fixed_slot_assignment.copy()

    def is_valid(course, slot):
        for neighbor in conflict_map.get(course, []):
            if slot_assignment.get(neighbor) == slot:
                return False
        return True

    def backtrack(index):
        if index == len(course_list):
            return True
        course = course_list[index]
        if course in slot_assignment:
            return backtrack(index + 1)
        for slot in slot_list:
            if is_valid(course, slot):
                slot_assignment[course] = slot
                if backtrack(index + 1):
                    return True
                del slot_assignment[course]
        return False

    success = backtrack(0)
    return slot_assignment if success else None

def expand_grouped_course_slots(course_slot_map, group_map, course_map):
    expanded_map = {}
    code_to_tuple = {code: (code, name) for _, (code, name) in course_map.items()}

    for group_or_code, slot in course_slot_map.items():
        if group_or_code in group_map:
            for course_code in group_map[group_or_code]:
                if course_code in code_to_tuple:
                    expanded_map[code_to_tuple[course_code]] = slot
                else:
                    expanded_map[(course_code, "Unknown Course")] = slot
        else:
            if group_or_code in code_to_tuple:
                expanded_map[code_to_tuple[group_or_code]] = slot
            else:
                expanded_map[(group_or_code, "Unknown Course")] = slot

    return expanded_map

def rebuild_course_to_students_with_names(course_to_students, course_map, course_to_group):
    from db.session import SessionLocal
    from db.models import CourseStudent, Course, Student

    db = SessionLocal()

    code_to_name = {code: name for _, (code, name) in course_map.items()}
    group_to_courses = defaultdict(list)
    for course_code, group_id in course_to_group.items():
        group_to_courses[group_id].append(course_code)

    all_mappings = db.query(CourseStudent).all()
    all_students = {s.id: s.student_id1 for s in db.query(Student).all()}

    rebuilt = defaultdict(set)

    for m in all_mappings:
        course = db.query(Course).get(m.course_id)
        student_id = all_students.get(m.student_id)
        if not course or not student_id:
            continue

        course_code = course.course_code
        group_id = course_to_group.get(course_code)
        course_name = code_to_name.get(course_code, "Unknown Course")

        if group_id:
            rebuilt[(course_code, course_name)].add(student_id)
        else:
            rebuilt[(course_code, course_name)].add(student_id)

    db.close()
    return rebuilt

def schedule_exams_from_db(xml_file_ids, start_date, num_days):
    total_slots = num_days * 2
    course_to_students, student_to_courses, course_map = get_student_course_mappings(xml_file_ids)
    course_to_students, student_to_courses, group_map, course_to_group = apply_merged_course_mapping(course_to_students, student_to_courses)
    conflict_map = build_conflict_map(student_to_courses)

    sorted_courses = sorted(course_to_students.items(), key=lambda x: len(x[1]), reverse=True)
    course_list = [course for course, _ in sorted_courses]

    fixed_slot_assignment = {}
    fixed_courses_set = set()
    for course_code, slot in FIXED_COURSE_SLOTS.items():
        fixed_slot_assignment[course_code] = slot
        fixed_courses_set.add(course_code)

    backtrack_courses = [c for c in course_list if c not in fixed_courses_set]

    final_slot_assignment = None
    am_slots = list(range(0, total_slots, 2))
    pm_slots = list(range(1, total_slots, 2))
    preferred_slots = am_slots + pm_slots

    for slot_limit in range(3, total_slots + 1):
        current_slot_list = preferred_slots[:slot_limit]
        print(f"🧪 Trying with first {slot_limit} preferred slots: {current_slot_list}")
        tentative_assignment = backtrack_schedule(backtrack_courses, conflict_map, current_slot_list, fixed_slot_assignment)
        if tentative_assignment:
            final_slot_assignment = tentative_assignment
            break

    if final_slot_assignment is None:
        raise Exception("❌ Could not find a valid schedule using backtracking.")

    course_slot_map = final_slot_assignment
    course_slot_map = expand_grouped_course_slots(course_slot_map, group_map, course_map)
    course_to_students = rebuild_course_to_students_with_names(course_to_students, course_map, course_to_group)

    db = SessionLocal()
    student_names = {s.student_id1: s.name for s in db.query(Student).all()}
    db.close()

    rows = []
    for course, students in course_to_students.items():
        slot = course_slot_map.get(course, None)
        day, time = get_day_and_time(slot, start_date) if slot is not None else ("Unscheduled", "")
        course_code, course_name = course
        for student in students:
            rows.append({
                "Student ID": student,
                "Student Name": student_names.get(student, "Unknown"),
                "Course Code": course_code,
                "Course Name": course_name,
                "Day": day,
                "Time": time,
                "Slot #": slot if slot is not None else "N/A"
            })

    final_schedule_df = pd.DataFrame(rows)
    return final_schedule_df, student_to_courses, course_to_students
