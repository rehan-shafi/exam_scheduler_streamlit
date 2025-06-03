from collections import defaultdict
import random

def fix_am_pm_distribution(course_slot_map, course_to_students, student_slot_map):
    TOTAL_SLOTS = 20
    day_am_courses = defaultdict(list)
    slot_course_map = {v: k for k, v in course_slot_map.items()}  # slot → course

    # Step 1: Build AM slot list correctly
    for course, slot in course_slot_map.items():
        if slot is not None and slot % 2 == 0:  # AM slot
            day = slot // 2
            day_am_courses[day].append((slot, course))

    # Step 2: Redistribute 40% of AM courses to PM slots
    for day, am_list in day_am_courses.items():
        pm_slot = day * 2 + 1
        if pm_slot >= TOTAL_SLOTS or pm_slot in slot_course_map:
            continue  # skip invalid or already used PM slot

        if len(am_list) == 0:
            continue

        num_to_move = max(1, round(len(am_list) * 0.4))
        to_move = random.sample(am_list, min(num_to_move, len(am_list)))

        for am_slot, course in to_move:
            # Move course to PM slot
            course_slot_map[course] = pm_slot
            slot_course_map[pm_slot] = course
            if am_slot in slot_course_map:
                del slot_course_map[am_slot]

            # Update student_slot_map
            students = course_to_students[course]
            for student in students:
                if am_slot in student_slot_map.get(student, []):
                    student_slot_map[student].remove(am_slot)
                    student_slot_map[student].append(pm_slot)

    return course_slot_map, student_slot_map
