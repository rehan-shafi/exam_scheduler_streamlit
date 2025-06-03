from collections import defaultdict

def generate_course_and_student_mappings(df):
    course_to_students = defaultdict(set)
    student_to_courses = defaultdict(set)

    for _, row in df.iterrows():
        course = str(row["Course Info"]).strip()
        student_id = str(row["Student ID"]).strip()

        # Skip if either is missing
        if not course or not student_id:
            continue

        course_to_students[course].add(student_id)
        student_to_courses[student_id].add(course)

    return dict(course_to_students), dict(student_to_courses)
