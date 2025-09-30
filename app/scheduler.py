from db.session import SessionLocal
from db.models import Course, Student, CourseStudent
from collections import defaultdict
import pandas as pd
from datetime import timedelta, date
import time
from sqlalchemy import text  # ✅ for lightweight bulk inserts

# ✅ Static fixed slots for specified courses (slot = even index; 0=Day1 AM, 2=Day2 AM, ..., 18=Day10 AM)
FIXED_COURSE_SLOTS = {
    # "ARAB.202": 0,
    # "IC.408": 8
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
    Slots are global indices where even = AM, odd = PM.
    Day offset = slot // 2.
    """
    exam_date = start_date + timedelta(days=slot // 2)
    return exam_date.strftime("%Y-%m-%d"), ("AM" if (slot % 2 == 0) else "PM")


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
    DSATUR greedy coloring confined to 'preferred_slots' (even indices for AM).
    Returns dict(course -> slot) if fits within max_colors, else None.
    """
    print("⚡ [dsatur] start (AM-only, even slot IDs)", flush=True)
    assignment = dict(fixed_slot_assignment)  # course -> slot (even index)

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

    cap = min(max_colors, len(preferred_slots))

    def pick_next():
        return max(uncolored, key=lambda x: (sat_deg[x], degrees[x]))

    while uncolored:
        v = pick_next()

        forbidden = neighbor_slots[v]
        chosen_slot = None
        for k in range(cap):
            slot_id = preferred_slots[k]
            if slot_id not in forbidden:
                chosen_slot = slot_id
                break

        if chosen_slot is None:
            print("⚡ [dsatur] needs more than", cap, "AM slots. fallback required.", flush=True)
            return None

        assignment[v] = chosen_slot
        uncolored.remove(v)

        for n in neighbors[v]:
            if n in neighbor_slots and chosen_slot not in neighbor_slots[n]:
                neighbor_slots[n].add(chosen_slot)
                sat_deg[n] = len(neighbor_slots[n])

    print("✅ [dsatur] success within available AM (even) slots", flush=True)
    return assignment


# ------------------------------------
# Capped backtracking (AM-only, even)
# ------------------------------------
def backtrack_schedule(course_list, conflict_map, slot_list, fixed_slot_assignment,
                       max_ms_per_attempt=10000, max_calls_per_attempt=2_000_000):
    print(f"🧠 [backtrack_schedule] Start: courses={len(course_list)} slots={len(slot_list)} fixed={len(fixed_slot_assignment)} (AM-only even)", flush=True)
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


# -------------------------
# NEW: Persist schedule to DB (one run + rows)
# -------------------------
def _pydate_from_str(yyyy_mm_dd: str) -> date:
    y, m, d = yyyy_mm_dd.split("-")
    return date(int(y), int(m), int(d))

def save_schedule_to_db(course_slot_map, rows, start_date, num_days, xml_file_ids):
    """
    course_slot_map: dict[(course_code, course_name)] -> slot
    rows: list of dicts with Student-level rows (already built below)
    start_date: datetime.date
    num_days: int
    xml_file_ids: list[int] or list[str] (stored as CSV for traceability)
    """
    print("🗄️ [save_schedule_to_db] start", flush=True)
    db = SessionLocal()
    try:
        # 1) Insert run header and get run_id
        csv_ids = ",".join([str(x) for x in (xml_file_ids or [])])
        run_id = db.execute(
            text("""
                INSERT INTO public.exam_schedule_runs (start_date, num_days, xml_file_ids)
                VALUES (:start_date, :num_days, :xml_file_ids)
                RETURNING id;
            """),
            {"start_date": start_date, "num_days": num_days, "xml_file_ids": csv_ids}
        ).scalar_one()

        # 2) Insert course-level slots
        course_rows = []
        for (course_code, course_name), slot in course_slot_map.items():
            day_idx = slot // 2
            exam_date_str, time_label = get_day_and_time(slot, start_date)
            course_rows.append({
                "run_id": run_id,
                "group_or_code": course_code,  # after expansion, this is the normalized code
                "course_code": course_code,
                "course_name": course_name,
                "day_index": day_idx,
                "slot": int(slot),
                "exam_date": _pydate_from_str(exam_date_str),
                "time_label": time_label
            })

        if course_rows:
            db.execute(text("""
                INSERT INTO public.exam_slots
                    (run_id, group_or_code, course_code, course_name, day_index, slot, exam_date, time_label)
                VALUES
                    (:run_id, :group_or_code, :course_code, :course_name, :day_index, :slot, :exam_date, :time_label)
                ON CONFLICT (run_id, course_code) DO UPDATE
                    SET day_index = EXCLUDED.day_index,
                        slot = EXCLUDED.slot,
                        exam_date = EXCLUDED.exam_date,
                        time_label = EXCLUDED.time_label,
                        course_name = EXCLUDED.course_name;
            """), course_rows)

        # 3) Insert student-level rows
        # rows already contain: Student ID, Student Name, Course Code, Course Name, Day, Time, Slot #
        student_rows = []
        for r in rows:
            # skip unscheduled
            if r.get("Slot #") == "N/A":
                continue
            student_rows.append({
                "run_id": run_id,
                "student_id": r["Student ID"],
                "student_name": r.get("Student Name"),
                "course_code": r["Course Code"],
                "course_name": r.get("Course Name"),
                "day_index": int(r["Slot #"]) // 2,
                "slot": int(r["Slot #"]),
                "exam_date": _pydate_from_str(r["Day"]) if isinstance(r["Day"], str) else r["Day"],
                "time_label": r.get("Time", "AM")
            })

        if student_rows:
            db.execute(text("""
                INSERT INTO public.student_exams
                    (run_id, student_id, student_name, course_code, course_name, day_index, slot, exam_date, time_label)
                VALUES
                    (:run_id, :student_id, :student_name, :course_code, :course_name, :day_index, :slot, :exam_date, :time_label)
                ON CONFLICT (run_id, student_id, course_code) DO NOTHING;
            """), student_rows)

        db.commit()
        print(f"🗄️ [save_schedule_to_db] committed run_id={run_id}  • courses={len(course_rows)}  • student_rows={len(student_rows)}", flush=True)
        return run_id
    except Exception as e:
        db.rollback()
        print(f"❌ [save_schedule_to_db] error: {e}", flush=True)
        raise
    finally:
        db.close()


def schedule_exams_from_db(xml_file_ids, start_date, num_days):
    t_all = _now_ms()
    print("🚀 [schedule_exams_from_db] START (AM-only, even indices + shrink)", flush=True)

    # We schedule ONLY AM, but with even-numbered global slot IDs: [0,2,4,...,2*(num_days-1)]
    total_days = num_days
    print(f"  • num_days={num_days} total_days(AM-only)={total_days}", flush=True)

    course_to_students, student_to_courses, course_map = get_student_course_mappings(xml_file_ids)
    print(f"  • After mapping: courses={len(course_to_students)} students={len(student_to_courses)}", flush=True)

    course_to_students, student_to_courses, group_map, course_to_group = apply_merged_course_mapping(course_to_students, student_to_courses)
    print(f"  • After merge: merged_courses={len(course_to_students)}", flush=True)

    conflict_map = build_conflict_map(student_to_courses)

    # Order courses by popularity (more students → earlier)
    sorted_courses = sorted(course_to_students.items(), key=lambda x: len(x[1]), reverse=True)
    course_list = [course for course, _ in sorted_courses]
    print(f"  • Target list size: {len(course_list)}", flush=True)

    # Fixed slots (even indices)
    fixed_slot_assignment = {}
    fixed_courses_set = set()
    for course_code, slot in FIXED_COURSE_SLOTS.items():
        fixed_slot_assignment[course_code] = slot
        fixed_courses_set.add(course_code)
    if fixed_slot_assignment:
        print(f"  • Fixed slots preset: {fixed_slot_assignment}", flush=True)

    # Preferred AM slots as even indices: [0,2,4,...,2*(num_days-1)]
    preferred_slots = [2 * i for i in range(total_days)]
    print(f"  • Preferred AM (even) slots: {preferred_slots}", flush=True)

    # Degree lower bound
    degrees = {c: len(conflict_map.get(c, set())) for c in course_list}
    max_deg = max(degrees.values()) if degrees else 0
    lower_bound = min(total_days, max_deg + 1)
    print(f"  • Degree stats: max_degree={max_deg}  → lower_bound_days={lower_bound}", flush=True)

    # -------- helper to try a given number of days (slots subset) --------
    def try_with_days(day_limit: int):
        slots = preferred_slots[:day_limit]  # first N even slots
        # 1) DSATUR
        ds = _dsatur_color(course_list, conflict_map, max_colors=day_limit,
                           preferred_slots=slots, fixed_slot_assignment=fixed_slot_assignment)
        if ds is not None:
            return ds
        # 2) capped backtracking fallback
        return backtrack_schedule(
            [c for c in course_list if c not in fixed_courses_set],
            conflict_map, slots, fixed_slot_assignment,
            max_ms_per_attempt=10000, max_calls_per_attempt=2_000_000
        )

    # -------- ensure it fits within the requested days --------
    course_slot_map = try_with_days(total_days)
    if course_slot_map is None:
        print("❌ Could not fit within requested days.", flush=True)
        raise Exception("❌ Could not find a valid AM-only schedule within available days.")

    # -------- NEW: shrink pass to find minimum number of days --------
    best_days = total_days
    best_assignment = course_slot_map
    for day_limit in range(total_days - 1, 0, -1):
        print(f"🔎 Trying to shrink to {day_limit} days…", flush=True)
        candidate = try_with_days(day_limit)
        if candidate is not None:
            best_days = day_limit
            best_assignment = candidate
            print(f"   ✅ Works with {day_limit} days; continuing to shrink…", flush=True)
        else:
            print(f"   ❌ {day_limit} days not feasible; keeping {best_days}.", flush=True)
            break

    course_slot_map = best_assignment
    print(f"🏁 Final days used: {best_days}", flush=True)

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
                "Time": time_label,  # "AM"
                "Slot #": slot if slot is not None else "N/A"
            })

    if missing_slots:
        print(f"  ⚠️ Courses without slots after expansion: {missing_slots}", flush=True)

    # ✅ Persist schedule BEFORE returning (without changing the function’s return signature)
    try:
        run_id = save_schedule_to_db(course_slot_map, rows, start_date, best_days, xml_file_ids)
        print(f"🗂️ Schedule saved with run_id={run_id}", flush=True)
    except Exception as e:
        print(f"⚠️ Schedule persistence failed, continuing to return DataFrame. Error: {e}", flush=True)

    final_schedule_df = pd.DataFrame(rows)
    print(f"✅ [schedule_exams_from_db] DONE (AM-only, even indices + shrink) in {_fmt_ms(_now_ms() - t_all)}  • rows={len(final_schedule_df)}", flush=True)
    return final_schedule_df, student_to_courses, course_to_students
