from db.session import SessionLocal
from db.models import Course, Student, CourseStudent
from collections import defaultdict
import pandas as pd
from datetime import timedelta
import time

# ✅ Static fixed slots for specified courses (slot = day index; 0-based; AM only)
# Example: "ARAB.202": 0 means Day 1 AM
FIXED_COURSE_SLOTS = {
    # "ARAB.202": 0,
    # "IC.408": 4
}

# -------------------------
# Instrumentation helpers
# -------------------------
def _now_ms():
    return int(time.time() * 1000)

def _fmt_ms(ms):
    return f"{ms/1000:.2f}s"


def get_day_and_time(slot, start_date):
    """
    With AM-only scheduling, slot == day index (0-based).
    """
    exam_date = start_date + timedelta(days=slot)
    return exam_date.strftime("%Y-%m-%d"), "AM"


def get_student_course_mappings(xml_file_ids):
    t0 = _now_ms()
    print("📥 [get_student_course_mappings] start", flush=True)
    db = SessionLocal()

    courses = db.query(Course).filter(Course.xml_file_id.in_(xml_file_ids)).all()
    students = db.query(Student).filter(Student.xml_file_id.in_(xml_file_ids)).all()
    print(f"  • Courses fetched: {len(courses)}  • Students fetched: {len(students)}", flush=True)

    course_map = {c.id: (c.course_code, c.course_name) for c in courses}
    student_map = {s.id: s.student_id1 for s in students}

    course_to_students = defaultdict(set)
    student_to_courses = defaultdict(set)

    # NOTE: full scan — consider filtering via join in future for speed
    mappings = db.query(CourseStudent).all()
    print(f"  • CourseStudent mappings fetched (unfiltered): {len(mappings)}", flush=True)

    miss_course, miss_student = 0, 0
    for m in mappings:
        if m.course_id not in course_map:
            miss_course += 1
            continue
        if m.student_id not in student_map:
            miss_student += 1
            continue
        course_code = course_map[m.course_id]
        student_id = student_map[m.student_id]
        course_to_students[course_code].add(student_id)
        student_to_courses[student_id].add(course_code)

    if miss_course or miss_student:
        print(f"  • Skipped mappings   missing_course={miss_course} missing_student={miss_student}", flush=True)

    db.close()
    print("📤 [get_student_course_mappings] done in", _fmt_ms(_now_ms() - t0), flush=True)
    return course_to_students, student_to_courses, course_map


def apply_merged_course_mapping(course_to_students, student_to_courses):
    print("🔗 [apply_merged_course_mapping] start", flush=True)
    from db.models import MergedCourse
    db = SessionLocal()
    merged_groups = db.query(MergedCourse).all()
    db.close()
    print(f"  • Merged groups rows: {len(merged_groups)}", flush=True)

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

    print("✅ [apply_merged_course_mapping] done", flush=True)
    return merged_course_to_students, merged_student_to_courses, group_map, course_to_group


def build_conflict_map(student_to_courses):
    print("🧮 [build_conflict_map] start", flush=True)
    conflict_map = defaultdict(set)
    for courses in student_to_courses.values():
        courses = list(courses)
        for i in range(len(courses)):
            for j in range(i + 1, len(courses)):
                c1 = courses[i]
                c2 = courses[j]
                conflict_map[c1].add(c2)
                conflict_map[c2].add(c1)
    edges = sum(len(v) for v in conflict_map.values())
    print(f"  • Conflict edges (directed count): {edges}", flush=True)
    print(f"  • Nodes in conflict_map: {len(conflict_map)}", flush=True)
    print("✅ [build_conflict_map] done", flush=True)
    return conflict_map


# -------------------------
# Fast pass: DSATUR greedy
# -------------------------
def _dsatur_color(course_list, conflict_map, max_colors, preferred_slots, fixed_slot_assignment):
    """
    DSATUR greedy coloring (using only provided preferred_slots).
    Returns dict(course -> slot) if it fits within max_colors, else None.
    """
    print("⚡ [dsatur] start (AM-only)", flush=True)
    # assignment uses actual slot ids from preferred_slots (0..num_days-1)
    assignment = dict(fixed_slot_assignment)  # course -> slot (day index)

    neighbors = {c: set(conflict_map.get(c, set())) for c in course_list}
    degrees = {c: len(neighbors[c]) for c in course_list}

    uncolored = [c for c in course_list if c not in assignment]

    sat_deg = {c: 0 for c in uncolored}
    neighbor_slots = {c: set() for c in uncolored}

    # Seed saturation with fixed slots in neighbors
    for c in uncolored:
        seen = set()
        for n in neighbors[c]:
            if n in assignment:
                seen.add(assignment[n])
        neighbor_slots[c] = seen
        sat_deg[c] = len(seen)

    def pick_next():
        return max(uncolored, key=lambda x: (sat_deg[x], degrees[x]))

    cap = min(max_colors, len(preferred_slots))

    while uncolored:
        v = pick_next()

        forbidden = neighbor_slots[v]
        chosen_slot = None
        # Try AM slots in the given order
        for k in range(cap):
            slot_id = preferred_slots[k]
            if slot_id not in forbidden:
                chosen_slot = slot_id
                break

        if chosen_slot is None:
            print("⚡ [dsatur] needs more than", cap, "slots. fallback required.", flush=True)
            return None

        assignment[v] = chosen_slot
        uncolored.remove(v)

        for n in neighbors[v]:
            if n in neighbor_slots and chosen_slot not in neighbor_slots[n]:
                neighbor_slots[n].add(chosen_slot)
                sat_deg[n] = len(neighbor_slots[n])

    print("✅ [dsatur] success within available AM slots", flush=True)
    return assignment


# ------------------------------------
# Capped backtracking (AM-only)
# ------------------------------------
def backtrack_schedule(course_list, conflict_map, slot_list, fixed_slot_assignment,
                       max_ms_per_attempt=10000, max_calls_per_attempt=2_000_000):
    print(f"🧠 [backtrack_schedule] Start: courses={len(course_list)} slots={len(slot_list)} fixed={len(fixed_slot_assignment)} (AM-only)", flush=True)
    slot_assignment = fixed_slot_assignment.copy()
    neighbors = conflict_map  # alias

    def is_valid(course, slot):
        for neighbor in neighbors.get(course, []):
            if slot_assignment.get(neighbor) == slot:
                return False
        return True

    calls = 0
    last_report = _now_ms()
    t0 = _now_ms()

    # Order courses by degree (high first)
    order = sorted([c for c in course_list if c not in slot_assignment],
                   key=lambda c: len(neighbors.get(c, [])),
                   reverse=True)

    def backtrack(index):
        nonlocal calls, last_report
        calls += 1

        # Caps
        if calls > max_calls_per_attempt:
            return False
        if _now_ms() - t0 > max_ms_per_attempt:
            return False

        # Heartbeat
        now = _now_ms()
        if now - last_report > 2000:
            assigned = len(slot_assignment)
            print(f"    ⏳ backtrack heartbeat: idx={index}/{len(order)} assigned={assigned} calls={calls}", flush=True)
            last_report = now

        if index == len(order):
            return True

        course = order[index]
        for slot in slot_list:
            if is_valid(course, slot):
                slot_assignment[course] = slot
                if backtrack(index + 1):
                    return True
                del slot_assignment[course]
        return False

    success = backtrack(0)
    print(f"🧠 [backtrack_schedule] {'SUCCESS' if success else 'FAIL'} in {_fmt_ms(_now_ms() - t0)} with {calls} calls", flush=True)
    return slot_assignment if success else None


def expand_grouped_course_slots(course_slot_map, group_map, course_map):
    print("📦 [expand_grouped_course_slots] start", flush=True)
    expanded_map = {}
    code_to_tuple = {code: (code, name) for _, (code, name) in course_map.items()}

    expanded_count = 0
    for group_or_code, slot in course_slot_map.items():
        if group_or_code in group_map:
            for course_code in group_map[group_or_code]:
                if course_code in code_to_tuple:
                    expanded_map[code_to_tuple[course_code]] = slot
                else:
                    expanded_map[(course_code, "Unknown Course")] = slot
                expanded_count += 1
        else:
            if group_or_code in code_to_tuple:
                expanded_map[code_to_tuple[group_or_code]] = slot
            else:
                expanded_map[(group_or_code, "Unknown Course")] = slot
            expanded_count += 1

    print(f"✅ [expand_grouped_course_slots] expanded items: {expanded_count}", flush=True)
    return expanded_map


def rebuild_course_to_students_with_names(course_to_students, course_map, course_to_group):
    print("🔁 [rebuild_course_to_students_with_names] start", flush=True)
    from db.session import SessionLocal as _SessionLocal
    from db.models import CourseStudent as _CourseStudent, Course as _Course, Student as _Student

    db = _SessionLocal()

    code_to_name = {code: name for _, (code, name) in course_map.items()}

    all_mappings = db.query(_CourseStudent).all()
    all_students = {s.id: s.student_id1 for s in db.query(_Student).all()}
    all_courses = {c.id: c for c in db.query(_Course).all()}

    print(f"  • CourseStudent rows: {len(all_mappings)}  • Students cached: {len(all_students)}  • Courses cached: {len(all_courses)}", flush=True)

    rebuilt = defaultdict(set)

    for idx, m in enumerate(all_mappings, 1):
        course = all_courses.get(m.course_id)
        student_id = all_students.get(m.student_id)
        if not course or not student_id:
            continue

        course_code = course.course_code
        course_name = code_to_name.get(course_code, "Unknown Course")

        rebuilt[(course_code, course_name)].add(student_id)

        if idx % 10000 == 0:
            print(f"    … processed {idx} mappings", flush=True)

    db.close()
    print("✅ [rebuild_course_to_students_with_names] done", flush=True)
    return rebuilt


def schedule_exams_from_db(xml_file_ids, start_date, num_days):
    t_all = _now_ms()
    print("🚀 [schedule_exams_from_db] START (AM-only)", flush=True)

    # AM-only ⇒ total_slots == num_days
    total_slots = num_days
    print(f"  • num_days={num_days} total_slots(AM-only)={total_slots}", flush=True)

    course_to_students, student_to_courses, course_map = get_student_course_mappings(xml_file_ids)
    print(f"  • After mapping: courses={len(course_to_students)} students={len(student_to_courses)}", flush=True)

    course_to_students, student_to_courses, group_map, course_to_group = apply_merged_course_mapping(course_to_students, student_to_courses)
    print(f"  • After merge: merged_courses={len(course_to_students)}", flush=True)

    conflict_map = build_conflict_map(student_to_courses)

    # Order courses by popularity (more students → earlier)
    sorted_courses = sorted(course_to_students.items(), key=lambda x: len(x[1]), reverse=True)
    course_list = [course for course, _ in sorted_courses]
    print(f"  • Target list size: {len(course_list)}", flush=True)

    # Fixed slots (day indices)
    fixed_slot_assignment = {}
    fixed_courses_set = set()
    for course_code, slot in FIXED_COURSE_SLOTS.items():
        fixed_slot_assignment[course_code] = slot
        fixed_courses_set.add(course_code)
    if fixed_slot_assignment:
        print(f"  • Fixed slots preset: {fixed_slot_assignment}", flush=True)

    # Preferred slots are AM day indices only: [0..num_days-1]
    preferred_slots = list(range(total_slots))
    print(f"  • Preferred AM slots (day indices): {preferred_slots}", flush=True)

    # ----------------------------
    # 1) Fast pass: DSATUR greedy
    # ----------------------------
    degrees = {c: len(conflict_map.get(c, set())) for c in course_list}
    max_deg = max(degrees.values()) if degrees else 0
    lower_bound = min(total_slots, max_deg + 1)
    print(f"  • Degree stats: max_degree={max_deg}  → lower_bound_slots={lower_bound}", flush=True)

    dsatur_assignment = _dsatur_color(
        course_list,
        conflict_map,
        max_colors=total_slots,                # 10 max (AM-only)
        preferred_slots=preferred_slots,
        fixed_slot_assignment=fixed_slot_assignment
    )

    if dsatur_assignment is not None:
        print("✅ Using DSATUR assignment (no backtracking needed).", flush=True)
        course_slot_map = dsatur_assignment
    else:
        # ---------------------------------------
        # 2) Fallback: capped backtracking search
        # ---------------------------------------
        backtrack_courses = [c for c in course_list if c not in fixed_courses_set]
        print(f"  • Courses to backtrack (excl. fixed): {len(backtrack_courses)}", flush=True)

        final_slot_assignment = None

        # Start from realistic bound (Δ+1), but not less than 1
        start_limit = max(lower_bound, 1)
        print(f"  • Backtracking start_limit={start_limit} (AM-only)", flush=True)

        for slot_limit in range(start_limit, total_slots + 1):
            current_slot_list = preferred_slots[:slot_limit]
            print(f"🧪 Trying with first {slot_limit} AM slots (days): {current_slot_list}", flush=True)
            t_try = _now_ms()
            tentative_assignment = backtrack_schedule(
                backtrack_courses, conflict_map, current_slot_list, fixed_slot_assignment,
                max_ms_per_attempt=10000,  # hard cap per attempt
                max_calls_per_attempt=2_000_000
            )
            print(f"   ↪ attempt finished in {_fmt_ms(_now_ms() - t_try)}", flush=True)
            if tentative_assignment:
                final_slot_assignment = tentative_assignment
                print("   ✅ Found valid assignment at slot_limit =", slot_limit, flush=True)
                break

        if final_slot_assignment is None:
            print("❌ Could not find a valid AM-only schedule within the available days.", flush=True)
            raise Exception("❌ Could not find a valid AM-only schedule using DSATUR or capped backtracking.")

        course_slot_map = final_slot_assignment

    # Expand grouped codes back to individual courses
    course_slot_map = expand_grouped_course_slots(course_slot_map, group_map, course_map)

    # Rebuild student mappings with names
    course_to_students = rebuild_course_to_students_with_names(course_to_students, course_map, course_to_group)

    db = SessionLocal()
    student_names = {s.student_id1: s.name for s in db.query(Student).all()}
    db.close()
    print(f"  • Student names fetched: {len(student_names)}", flush=True)

    print("🧾 Building final dataframe…", flush=True)
    rows = []
    missing_slots = 0
    for course, students in course_to_students.items():
        slot = course_slot_map.get(course, None)
        if slot is None:
            missing_slots += 1
        day, time_label = get_day_and_time(slot, start_date) if slot is not None else ("Unscheduled", "")
        course_code, course_name = course
        for student in students:
            rows.append({
                "Student ID": student,
                "Student Name": student_names.get(student, "Unknown"),
                "Course Code": course_code,
                "Course Name": course_name,
                "Day": day,
                "Time": time_label,  # always "AM"
                "Slot #": slot if slot is not None else "N/A"
            })

    if missing_slots:
        print(f"  ⚠️ Courses without slots after expansion: {missing_slots}", flush=True)

    final_schedule_df = pd.DataFrame(rows)
    print(f"✅ [schedule_exams_from_db] DONE (AM-only) in {_fmt_ms(_now_ms() - t_all)}  • rows={len(final_schedule_df)}", flush=True)
    return final_schedule_df, student_to_courses, course_to_students
