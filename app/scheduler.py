from app.processor import extract_student_course_data
import pandas as pd

TOTAL_SLOTS = 20  # 20 regular + 2 overflow (Day 11 AM, PM) Removed overflow. for overfolw changed the value 22

def get_day_and_time(slot):
    day = slot // 2 + 1
    time = "AM" if slot % 2 == 0 else "PM"
    return f"Day {day}", time

def schedule_exams(file_path):
    # Step 1: Data cleanup + mapping
    df, output_path, course_to_students, student_to_courses = extract_student_course_data(file_path)

    # Step 2: Sort courses by number of students
    sorted_courses = sorted(course_to_students.items(), key=lambda x: len(x[1]), reverse=True)
    course_list = [course for course, _ in sorted_courses]

    # Step 3: Interleaved AM/PM slot order
    am_slots = list(range(0, 20, 2))    # 0, 2, 4, ..., 18
    pm_slots = list(range(1, 20, 2))    # 1, 3, 5, ..., 19
    slot_order = [s for pair in zip(am_slots, pm_slots) for s in pair]   # Interleaved + overflow + [20, 21] 

    student_slot_map = {}  # student_id → list of slots
    course_slot_map = {}   # course → assigned slot

    for course in course_list:
        students = course_to_students[course]
        assigned = False

        for slot in slot_order:
            conflict = False
            for student in students:
                existing_slots = student_slot_map.get(student, [])

                # ❌ Same slot conflict
                if slot in existing_slots:
                    conflict = True
                    break

                # ❌ Same day conflict (AM and PM of same day)
                if any(s // 2 == slot // 2 for s in existing_slots):
                    conflict = True
                    break
                #Removing the Consective day soft conflict. Admin can manage it by drag and drop
                # ❌ Consecutive day soft conflict
                #if any(abs(s // 2 - slot // 2) == 1 for s in existing_slots):
                #    conflict = True
                #    break

            if not conflict:
                course_slot_map[course] = slot
                for student in students:
                    student_slot_map.setdefault(student, []).append(slot)
                assigned = True
                break

        if not assigned:
            print(f"⚠️ Could not assign any slot to: {course}")

    # Step 4: Generate final schedule
    rows = []
    for course, students in course_to_students.items():
        slot = course_slot_map.get(course, None)
        day, time = get_day_and_time(slot) if slot is not None else ("Unscheduled", "")
        for student in students:
            rows.append({
                "Student ID": student,
                "Course": course,
                "Day": day,
                "Time": time,
                "Slot #": slot if slot is not None else "N/A"
            })

    final_schedule_df = pd.DataFrame(rows)

    # Step 5: Save to CSV
    schedule_path = "output/final_exam_schedule.csv"
    final_schedule_df.to_csv(schedule_path, index=False)

    return final_schedule_df, schedule_path,student_to_courses,course_to_students
