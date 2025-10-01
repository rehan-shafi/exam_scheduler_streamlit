from db.session import SessionLocal
from db.models import Course, Student, CourseStudent
from collections import defaultdict, Counter
import pandas as pd
from datetime import timedelta, date
import time
from sqlalchemy import text  # for lightweight bulk inserts
import random  # 🔹 for seeded restarts/tie-breaks

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
# ORDER-AWARE triple tools (works with any day order)
# -------------------------
def _build_slot_day_maps(day_slots):
    """
    day_slots: list of even slot ids in the exact day order being used (e.g., [0,10,2,12,...])
    Returns:
      slot_to_day: dict[slot] -> day_index
      day_to_slot: dict[day_index] -> slot
    """
    slot_to_day = {s: d for d, s in enumerate(day_slots)}
    day_to_slot = {d: s for d, s in enumerate(day_slots)}
    return slot_to_day, day_to_slot

def _triples_from_slots_order_aware(slots_set, slot_to_day, num_days):
    """
    Convert a set of slots -> day indices using slot_to_day, then return all (d,d+1,d+2) windows present.
    """
    day_idxs = sorted({slot_to_day[s] for s in slots_set if s in slot_to_day})
    dayset = set(day_idxs)
    triples = []
    for d in range(num_days - 2):
        if d in dayset and (d+1) in dayset and (d+2) in dayset:
            triples.append((d, d+1, d+2))
    return triples

def _triple_would_be_created_order_aware(current_slots_set, candidate_slot, slot_to_day, num_days, old_slot=None):
    sset = set(current_slots_set)
    if old_slot is not None and old_slot in sset:
        sset.remove(old_slot)
    sset.add(candidate_slot)
    return len(_triples_from_slots_order_aware(sset, slot_to_day, num_days)) > 0


# -------------------------
# Fast pass: DSATUR greedy (with order-aware triple tie-break)
# -------------------------
def _dsatur_color(course_list, conflict_map, max_colors, preferred_slots, fixed_slot_assignment,
                  course_to_students, seed=0):
    """
    DSATUR greedy confined to 'preferred_slots' (AM-only).
    Tie-break prefers slots that do NOT create 3-in-3 (order-aware by day index).
    """
    print("⚡ [dsatur] start (AM-only, even slot IDs)  • seed=", seed, flush=True)
    rnd = random.Random(seed)
    assignment = dict(fixed_slot_assignment)

    neighbors = {c: set(conflict_map.get(c, set())) for c in course_list}
    degrees = {c: len(neighbors[c]) for c in course_list}

    # Build day maps for THIS order
    day_slots = preferred_slots[:max_colors]
    slot_to_day, _ = _build_slot_day_maps(day_slots)
    num_days = len(day_slots)

    # Dynamic per-student slots (from fixed)
    student_slots_dyn = defaultdict(set)
    for c_fixed, sl in fixed_slot_assignment.items():
        if sl in slot_to_day:
            for stu in course_to_students.get(c_fixed, set()):
                student_slots_dyn[stu].add(sl)

    uncolored = [c for c in course_list if c not in assignment]
    rand_rank = {c: rnd.random() for c in uncolored}

    sat_deg = {c: 0 for c in uncolored}
    neighbor_slots = {c: set() for c in uncolored}

    for c in uncolored:
        seen = set()
        for n in neighbors[c]:
            if n in assignment:
                seen.add(assignment[n])
        neighbor_slots[c] = seen
        sat_deg[c] = len(seen)

    cap = min(max_colors, len(preferred_slots))

    def pick_next():
        return max(uncolored, key=lambda x: (sat_deg[x], degrees[x], rand_rank[x]))

    while uncolored:
        v = pick_next()
        forbidden = neighbor_slots[v]

        candidates = [preferred_slots[k] for k in range(cap) if preferred_slots[k] not in forbidden]

        if not candidates:
            print("⚡ [dsatur] needs more than", cap, "AM slots. fallback required.", flush=True)
            return None

        enrolled = course_to_students.get(v, set())

        def triple_penalty(slot):
            # penalty 1 if any enrolled student forms a triple under order-aware check
            for stu in enrolled:
                if _triple_would_be_created_order_aware(student_slots_dyn.get(stu, set()), slot, slot_to_day, num_days):
                    return 1
            return 0

        candidates_ranked = sorted(candidates, key=lambda s: (triple_penalty(s), preferred_slots.index(s)))

        chosen_slot = candidates_ranked[0]
        assignment[v] = chosen_slot
        uncolored.remove(v)

        for n in neighbors[v]:
            if n in neighbor_slots and chosen_slot not in neighbor_slots[n]:
                neighbor_slots[n].add(chosen_slot)
                sat_deg[n] = len(neighbor_slots[n])

        for stu in enrolled:
            student_slots_dyn[stu].add(chosen_slot)

    print("✅ [dsatur] success within available AM (even) slots", flush=True)
    return assignment


# ------------------------------------
# Capped backtracking (AM-only, even)
# ------------------------------------
def backtrack_schedule(course_list, conflict_map, slot_list, fixed_slot_assignment,
                       max_ms_per_attempt=10000, max_calls_per_attempt=2_000_000):
    print(f"🧠 [backtrack_schedule] Start: courses={len(course_list)} slots={len(slot_list)} fixed={len(fixed_slot_assignment)} (AM-only even)", flush=True)
    slot_assignment = fixed_slot_assignment.copy()
    neighbors = conflict_map

    def is_valid(course, slot):
        for neighbor in neighbors.get(course, []):
            if slot_assignment.get(neighbor) == slot:
                return False
        return True

    calls = 0
    last_report = _now_ms()
    t0 = _now_ms()

    order = sorted([c for c in course_list if c not in slot_assignment],
                   key=lambda c: len(neighbors.get(c, [])),
                   reverse=True)

    def backtrack(index):
        nonlocal calls, last_report
        calls += 1

        if calls > max_calls_per_attempt:
            return False
        if _now_ms() - t0 > max_ms_per_attempt:
            return False

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
# IMPROVED: 3-in-3 detector & repair (order-aware)
# -------------------------
def _compute_student_slots_map(course_slot_map, student_to_courses):
    student_slots = defaultdict(set)
    student_courses_by_slot = defaultdict(lambda: defaultdict(list))
    for student, courses in student_to_courses.items():
        for c in courses:
            if c in course_slot_map:
                sl = course_slot_map[c]
                student_slots[student].add(sl)
                student_courses_by_slot[student][sl].append(c)
    return student_slots, student_courses_by_slot

def _detect_violations_order_aware(student_slots, day_slots):
    slot_to_day, _ = _build_slot_day_maps(day_slots)
    num_days = len(day_slots)
    violations = []
    for stu, sset in student_slots.items():
        triples = _triples_from_slots_order_aware(sset, slot_to_day, num_days)
        for t in triples:
            violations.append((stu, t))
    return violations

def _slot_load(course_slot_map):
    return Counter(course_slot_map.values())

def _course_violation_weight(course, student_slots, student_courses_by_slot, day_slots):
    slot_to_day, _ = _build_slot_day_maps(day_slots)
    num_days = len(day_slots)
    count = 0
    for stu, slots in student_slots.items():
        triples = _triples_from_slots_order_aware(slots, slot_to_day, num_days)
        if not triples:
            continue
        for (d0, d1, d2) in triples:
            # translate back to slots for membership check
            s0 = day_slots[d0]; s1 = day_slots[d1]; s2 = day_slots[d2]
            if course in student_courses_by_slot.get(stu, {}).get(s0, []): 
                count += 1; continue
            if course in student_courses_by_slot.get(stu, {}).get(s1, []): 
                count += 1; continue
            if course in student_courses_by_slot.get(stu, {}).get(s2, []): 
                count += 1; continue
    return count

def _candidate_slots_rank(preferred, loads, current_slot, avoid_soft=None):
    avoid_soft = avoid_soft or set()
    def key(s):
        penalty = 1 if s in avoid_soft else 0
        return (penalty, loads.get(s, 0), -abs(s - current_slot))
    return sorted(preferred, key=key)

def _try_move_course(course, course_slot_map, conflict_map, course_to_students,
                     preferred_slots, student_slots, current_slot,
                     target_student=None, target_triplet=None):
    neighbors = conflict_map.get(course, set())
    enrolled_students = course_to_students.get(course, set())
    loads = _slot_load(course_slot_map)

    neighbor_slots = {course_slot_map[n] for n in neighbors if n in course_slot_map}

    # Build order-aware maps
    day_slots = preferred_slots[:]  # the active day set
    slot_to_day, _ = _build_slot_day_maps(day_slots)
    num_days = len(day_slots)

    avoid_soft = set()
    if target_student is not None and target_triplet is not None:
        # target_triplet is in day-index space; map the "others" to slots and avoid neighbors around them (soft)
        d0, d1, d2 = target_triplet
        others = [d for d in (d0, d1, d2) if day_slots[d] != current_slot]
        if len(others) == 2:
            a, b = sorted(others)
            for x in (a-1, a, a+1, b-1, b, b+1):
                if 0 <= x < num_days:
                    avoid_soft.add(day_slots[x])

    candidates = [s for s in day_slots if s != current_slot and s not in neighbor_slots]
    candidates = _candidate_slots_rank(candidates, loads, current_slot, avoid_soft)

    per_student_sets = {stu: set(student_slots.get(stu, set())) for stu in enrolled_students}

    for cand in candidates:
        ok_for_all = True
        for stu in enrolled_students:
            if _triple_would_be_created_order_aware(per_student_sets[stu], cand, slot_to_day, num_days, old_slot=current_slot):
                ok_for_all = False
                break
        if not ok_for_all:
            continue
        return cand

    return None

def _swap_would_be_valid(courseA, slotA, courseB, slotB, course_slot_map, conflict_map,
                         course_to_students, student_slots, day_slots):
    slot_to_day, _ = _build_slot_day_maps(day_slots)
    num_days = len(day_slots)

    neighA = conflict_map.get(courseA, set())
    neighB = conflict_map.get(courseB, set())

    for n in neighA:
        if n in course_slot_map and course_slot_map[n] == slotB:
            return False
    for n in neighB:
        if n in course_slot_map and course_slot_map[n] == slotA:
            return False

    for stu in course_to_students.get(courseA, set()):
        sset = set(student_slots.get(stu, set()))
        if slotA in sset:
            sset.remove(slotA)
        sset.add(slotB)
        if len(_triples_from_slots_order_aware(sset, slot_to_day, num_days)) > 0:
            return False

    for stu in course_to_students.get(courseB, set()):
        sset = set(student_slots.get(stu, set()))
        if slotB in sset:
            sset.remove(slotB)
        sset.add(slotA)
        if len(_triples_from_slots_order_aware(sset, slot_to_day, num_days)) > 0:
            return False

    return True

def _try_swap_course(course, current_slot, course_slot_map, conflict_map,
                     course_to_students, student_slots, preferred_slots,
                     target_student=None, target_triplet=None):
    day_slots = preferred_slots[:]
    loads = _slot_load(course_slot_map)

    avoid_soft = set()
    if target_student is not None and target_triplet is not None:
        d0, d1, d2 = target_triplet
        others = [d for d in (d0, d1, d2) if day_slots[d] != current_slot]
        if len(others) == 2:
            a, b = sorted(others)
            for x in (a-1, a, a+1, b-1, b, b+1):
                if 0 <= x < len(day_slots):
                    avoid_soft.add(day_slots[x])

    occupied_targets = [s for s in day_slots if s != current_slot and s in loads]
    occupied_targets = _candidate_slots_rank(occupied_targets, loads, current_slot, avoid_soft)

    slot_to_courses = defaultdict(list)
    for c, sl in course_slot_map.items():
        slot_to_courses[sl].append(c)

    def partner_weight(cc):
        # approximate by how many of its students are currently in any triple
        slot_to_day, _ = _build_slot_day_maps(day_slots)
        num_days = len(day_slots)
        cnt = 0
        for stu in course_to_students.get(cc, set()):
            if _triples_from_slots_order_aware(student_slots.get(stu, set()), slot_to_day, num_days):
                cnt += 1
        return (len(course_to_students.get(cc, set())), cnt)

    for tgt_slot in occupied_targets:
        partners = sorted(slot_to_courses.get(tgt_slot, []), key=partner_weight)
        for partner in partners:
            if partner == course:
                continue
            if not _swap_would_be_valid(course, current_slot, partner, tgt_slot,
                                        course_slot_map, conflict_map, course_to_students, student_slots, day_slots):
                continue
            return partner, tgt_slot

    return None, None

def repair_3_in_3(course_slot_map, course_to_students, student_to_courses, conflict_map, preferred_slots,
                  max_passes=10, max_moves=2000, enable_swaps=True):
    print("🛠️ [repair_3_in_3] start (moves + safe swaps, order-aware)", flush=True)

    day_slots = preferred_slots[:]
    student_slots, student_courses_by_slot = _compute_student_slots_map(course_slot_map, student_to_courses)
    initial = _detect_violations_order_aware(student_slots, day_slots)
    print(f"  • Initial violations: {len(initial)}", flush=True)

    moves_done = 0
    passes = 0

    def rank_candidates(stu, triple):
        d0, d1, d2 = triple
        cands = []
        s_mid = day_slots[d1]
        s_left = day_slots[d0]
        s_right = day_slots[d2]
        mid = student_courses_by_slot.get(stu, {}).get(s_mid, [])
        left = student_courses_by_slot.get(stu, {}).get(s_left, [])
        right = student_courses_by_slot.get(stu, {}).get(s_right, [])
        for c in mid:   cands.append((c, s_mid))
        for c in left:  cands.append((c, s_left))
        for c in right: cands.append((c, s_right))
        def key(cs):
            c, _ = cs
            return (len(course_to_students.get(c, set())),
                    -_course_violation_weight(c, student_slots, student_courses_by_slot, day_slots))
        return sorted(cands, key=key)

    while passes < max_passes:
        passes += 1
        student_slots, student_courses_by_slot = _compute_student_slots_map(course_slot_map, student_to_courses)
        violations = _detect_violations_order_aware(student_slots, day_slots)
        print(f"  • Pass {passes}: current violations={len(violations)}", flush=True)
        if not violations:
            break

        changed = False
        processed_pairs = set()
        moved_courses_this_pass = set()

        for stu, triple in violations:
            key = (stu, triple[0])
            if key in processed_pairs:
                continue
            processed_pairs.add(key)

            for course, cur_slot in rank_candidates(stu, triple):
                if course in moved_courses_this_pass:
                    continue

                new_slot = _try_move_course(
                    course=course,
                    course_slot_map=course_slot_map,
                    conflict_map=conflict_map,
                    course_to_students=course_to_students,
                    preferred_slots=day_slots,
                    student_slots=student_slots,
                    current_slot=cur_slot,
                    target_student=stu,
                    target_triplet=triple
                )

                if new_slot is not None:
                    print(f"    ↪️ Move {course}  {cur_slot} → {new_slot}  (student={stu})", flush=True)
                    course_slot_map[course] = new_slot
                    moved_courses_this_pass.add(course)
                    moves_done += 1
                    changed = True

                    for st in course_to_students.get(course, set()):
                        if cur_slot in student_slots[st]:
                            student_slots[st].remove(cur_slot)
                        student_slots[st].add(new_slot)
                    break

                if enable_swaps:
                    partner, tgt_slot = _try_swap_course(
                        course=course,
                        current_slot=cur_slot,
                        course_slot_map=course_slot_map,
                        conflict_map=conflict_map,
                        course_to_students=course_to_students,
                        student_slots=student_slots,
                        preferred_slots=day_slots,
                        target_student=stu,
                        target_triplet=triple
                    )
                    if partner is not None:
                        print(f"    🔁 Swap {course}@{cur_slot} ↔ {partner}@{tgt_slot}  (student={stu})", flush=True)
                        course_slot_map[course], course_slot_map[partner] = tgt_slot, cur_slot
                        moved_courses_this_pass.add(course)
                        moved_courses_this_pass.add(partner)
                        moves_done += 1
                        changed = True

                        for st in course_to_students.get(course, set()):
                            if cur_slot in student_slots[st]:
                                student_slots[st].remove(cur_slot)
                            student_slots[st].add(tgt_slot)
                        for st in course_to_students.get(partner, set()):
                            if tgt_slot in student_slots[st]:
                                student_slots[st].remove(tgt_slot)
                            student_slots[st].add(cur_slot)
                        break

            if moves_done >= max_moves:
                break

        if not changed:
            print("  • No improving move/swap found in this pass; stopping.", flush=True)
            break

        if moves_done >= max_moves:
            print("  • Reached move/swap cap; stopping.", flush=True)
            break

    student_slots, _ = _compute_student_slots_map(course_slot_map, student_to_courses)
    final_violations = _detect_violations_order_aware(student_slots, day_slots)
    print(f"✅ [repair_3_in_3] done  • moves/swaps={moves_done}  • remaining_violations={len(final_violations)}", flush=True)
    return course_slot_map, len(final_violations)


# -------------------------
# CP-SAT Finisher: minimize 3-in-3 triples (order-aware, with bound)
# -------------------------
def optimize_triples_cp_sat(course_list, conflict_map, student_to_courses, fixed_slot_assignment,
                            current_assignment, day_slots, current_best_triples=None,
                            time_limit_seconds=45.0, workers=8):
    """
    course_list: merged course ids
    day_slots: list of even slots in exact day order currently used
    current_best_triples: non-negative int -> adds constraint sum(y) ≤ current_best_triples (never worsen)
    Returns improved dict[course] -> slot; or current_assignment if no improvement.
    """
    print("🧩 [cp-sat] Building model to minimize 3-in-3 (order-aware)…", flush=True)
    try:
        from ortools.sat.python import cp_model
    except Exception as e:
        print(f"⚠️ [cp-sat] OR-Tools not available: {e}. Skipping optimizer.", flush=True)
        return current_assignment

    num_days = len(day_slots)
    days = list(range(num_days))

    # Map slot <-> day
    slot_to_day, _ = _build_slot_day_maps(day_slots)

    model = cp_model.CpModel()

    # Vars
    x = {}  # x[c, d] ∈ {0,1}
    for c in course_list:
        for d in days:
            x[(c, d)] = model.NewBoolVar(f"x[{c},{d}]")

    # Each course exactly one day
    for c in course_list:
        model.Add(sum(x[(c, d)] for d in days) == 1)

    # Conflicts not same day
    seen_pairs = set()
    for c in course_list:
        for n in conflict_map.get(c, set()):
            if (n, c) in seen_pairs or (c, n) in seen_pairs:
                continue
            seen_pairs.add((c, n))
            for d in days:
                model.Add(x[(c, d)] + x[(n, d)] <= 1)

    # Fixed assignments within active days
    for c, slot in (fixed_slot_assignment or {}).items():
        if c in course_list and slot in slot_to_day:
            dfix = slot_to_day[slot]
            model.Add(x[(c, dfix)] == 1)

    # z[s,d]: student s has exam on day d
    z = {}
    students = list(student_to_courses.keys())
    for s in students:
        for d in days:
            z[(s, d)] = model.NewBoolVar(f"z[{s},{d}]")
            cs = [x[(c, d)] for c in student_to_courses.get(s, set()) if c in course_list]
            if cs:
                # z is OR of cs
                # z >= any
                for xc in cs:
                    model.Add(z[(s, d)] >= xc)
                # z <= sum(cs)
                model.Add(z[(s, d)] <= sum(cs))
            else:
                model.Add(z[(s, d)] == 0)

    # y[s,d]: triple on (d,d+1,d+2)
    y = {}
    for s in students:
        for d in range(num_days - 2):
            y[(s, d)] = model.NewBoolVar(f"y[{s},{d}]")
            model.Add(y[(s, d)] <= z[(s, d)])
            model.Add(y[(s, d)] <= z[(s, d + 1)])
            model.Add(y[(s, d)] <= z[(s, d + 2)])
            model.Add(y[(s, d)] >= z[(s, d)] + z[(s, d + 1)] + z[(s, d + 2)] - 2)

    # Objective and upper bound
    total_triples = sum(y.values())
    if current_best_triples is not None:
        model.Add(total_triples <= int(current_best_triples))
    model.Minimize(total_triples)

    # Warm start
    for c, slot in (current_assignment or {}).items():
        if c in course_list and slot in slot_to_day:
            dcur = slot_to_day[slot]
            model.AddHint(x[(c, dcur)], 1)
            for d in days:
                if d != dcur:
                    model.AddHint(x[(c, d)], 0)

    # Solve
    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = float(time_limit_seconds)
    solver.parameters.num_search_workers = int(workers)
    print("🧩 [cp-sat] Solving…", flush=True)
    status = solver.Solve(model)
    print(f"🧩 [cp-sat] Status: {solver.StatusName(status)}  • objective={solver.ObjectiveValue()}", flush=True)

    if status not in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        print("⚠️ [cp-sat] No solution under the bound; keeping current assignment.", flush=True)
        return current_assignment

    # Extract
    new_assignment = {}
    for c in course_list:
        chosen_day = None
        for d in days:
            if solver.Value(x[(c, d)]) == 1:
                chosen_day = d
                break
        if chosen_day is None:
            new_assignment[c] = current_assignment.get(c, day_slots[0])
        else:
            new_assignment[c] = day_slots[chosen_day]

    return new_assignment


# -------------------------
# Persist schedule to DB (one run + rows)
# -------------------------
def _pydate_from_str(yyyy_mm_dd: str) -> date:
    y, m, d = yyyy_mm_dd.split("-")
    return date(int(y), int(m), int(d))

def save_schedule_to_db(course_slot_map, rows, start_date, num_days, xml_file_ids):
    print("🗄️ [save_schedule_to_db] start", flush=True)
    db = SessionLocal()
    try:
        csv_ids = ",".join([str(x) for x in (xml_file_ids or [])])
        run_id = db.execute(
            text("""
                INSERT INTO public.exam_schedule_runs (start_date, num_days, xml_file_ids)
                VALUES (:start_date, :num_days, :xml_file_ids)
                RETURNING id;
            """),
            {"start_date": start_date, "num_days": num_days, "xml_file_ids": csv_ids}
        ).scalar_one()

        course_rows = []
        for (course_code, course_name), slot in course_slot_map.items():
            day_idx = slot // 2
            exam_date_str, time_label = get_day_and_time(slot, start_date)
            course_rows.append({
                "run_id": run_id,
                "group_or_code": course_code,
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

        student_rows = []
        for r in rows:
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


# -------------------------
# MAIN: build schedule + repair + CP-SAT (order-aware) + expand + save
# -------------------------
def schedule_exams_from_db(xml_file_ids, start_date, num_days):
    t_all = _now_ms()
    print("🚀 [schedule_exams_from_db] START (AM-only, even indices + restarts + order-aware repair + CP-SAT)", flush=True)

    total_days = num_days
    print(f"  • num_days={num_days} total_days(AM-only)={total_days}", flush=True)

    course_to_students, student_to_courses, course_map = get_student_course_mappings(xml_file_ids)
    print(f"  • After mapping: courses={len(course_to_students)} students={len(student_to_courses)}", flush=True)

    course_to_students, student_to_courses, group_map, course_to_group = apply_merged_course_mapping(course_to_students, student_to_courses)
    print(f"  • After merge: merged_courses={len(course_to_students)}", flush=True)

    conflict_map = build_conflict_map(student_to_courses)

    # ---------------------------
    # Blended ordering
    # ---------------------------
    degrees = {c: len(conflict_map.get(c, set())) for c in course_to_students.keys()}
    enrollments = {c: len(stus) for c, stus in course_to_students.items()}

    def blended_score(c):
        return 0.6 * enrollments.get(c, 0) + 0.4 * degrees.get(c, 0)

    sorted_courses = sorted(course_to_students.keys(), key=lambda c: blended_score(c), reverse=True)
    course_list = list(sorted_courses)
    print(f"  • Target list size: {len(course_list)}", flush=True)

    # Fixed slots
    fixed_slot_assignment = {}
    fixed_courses_set = set()
    for course_code, slot in FIXED_COURSE_SLOTS.items():
        fixed_slot_assignment[course_code] = slot
        fixed_courses_set.add(course_code)
    if fixed_slot_assignment:
        print(f"  • Fixed slots preset: {fixed_slot_assignment}", flush=True)

    # Base AM slots
    base_slots = [2 * i for i in range(total_days)]
    print(f"  • Base AM (even) slots: {base_slots}", flush=True)

    # Slot order generators
    def slot_order_spread_1(slots):
        n = len(slots)
        mid = n // 2
        left = slots[:mid]
        right = slots[mid:]
        out = []
        for a, b in zip(left, right):
            out.extend([a, b])
        return out

    def slot_order_spread_2(slots):
        left = 0
        right = len(slots) - 1
        out = []
        while left <= right:
            out.append(slots[left])
            if left != right:
                out.append(slots[right])
            left += 1
            right -= 1
        return out

    def slot_order_spread_3(slots):
        order = []
        for start in [0, 5, 2, 7, 4]:
            for i in range(start, len(slots), 5):
                order.append(slots[i])
        seen = set()
        out = []
        for s in order:
            if s not in seen:
                out.append(s); seen.add(s)
        for s in slots:
            if s not in seen:
                out.append(s); seen.add(s)
        return out

    def slot_order_spread_4(slots):
        mid = len(slots) // 2
        order = [slots[mid]]
        offset = 1
        while True:
            left = mid - offset
            right = mid + offset
            if left < 0 and right >= len(slots):
                break
            if left >= 0:
                order.append(slots[left])
            if right < len(slots):
                order.append(slots[right])
            offset += 1
        return order

    slot_orders = [
        base_slots[:],
        slot_order_spread_1(base_slots[:]),
        slot_order_spread_2(base_slots[:]),
        slot_order_spread_3(base_slots[:]),
        slot_order_spread_4(base_slots[:]),
    ]

    print("  • Trying slot orders:", slot_orders, flush=True)

    max_deg = max(degrees.values()) if degrees else 0
    lower_bound = min(total_days, max_deg + 1)
    print(f"  • Degree stats: max_degree={max_deg}  → lower_bound_days={lower_bound}", flush=True)

    def try_with_days_and_order(day_limit: int, pref_slots: list, seed: int):
        slots = pref_slots[:day_limit]
        ds = _dsatur_color(
            course_list,
            conflict_map,
            max_colors=day_limit,
            preferred_slots=slots,
            fixed_slot_assignment=fixed_slot_assignment,
            course_to_students=course_to_students,
            seed=seed
        )
        if ds is not None:
            return ds
        return backtrack_schedule(
            [c for c in course_list if c not in fixed_courses_set],
            conflict_map, slots, fixed_slot_assignment,
            max_ms_per_attempt=10000, max_calls_per_attempt=2_000_000
        )

    def pre_repair_triple_count(assign_map, day_slots):
        student_slots, _ = _compute_student_slots_map(assign_map, student_to_courses)
        return len(_detect_violations_order_aware(student_slots, day_slots))

    best_assignment = None
    best_order = None
    best_seed = None
    best_triples = None

    for order_idx, pref in enumerate(slot_orders):
        for seed in range(5):
            print(f"🔎 Restart: order#{order_idx} seed={seed}", flush=True)
            candidate = try_with_days_and_order(total_days, pref, seed)
            if candidate is None:
                print("   ❌ infeasible with this restart", flush=True)
                continue
            triples = pre_repair_triple_count(candidate, pref[:total_days])
            print(f"   ✅ pre-repair triples={triples}", flush=True)
            if best_triples is None or triples < best_triples:
                best_triples = triples
                best_assignment = candidate
                best_order = order_idx
                best_seed = seed
                if best_triples == 0:
                    break
        if best_triples == 0:
            break

    if best_assignment is None:
        print("❌ Could not fit within requested days.", flush=True)
        raise Exception("❌ Could not find a valid AM-only schedule within available days.")

    course_slot_map = best_assignment
    chosen_order = slot_orders[best_order]
    day_slots = chosen_order[:total_days]
    print(f"🏁 Pre-repair best restart: order#{best_order} seed={best_seed}  • pre-repair triples={best_triples}", flush=True)

    # Shrink days cautiously
    best_days = total_days
    for day_limit in range(total_days - 1, 0, -1):
        print(f"🔎 Trying to shrink to {day_limit} days…", flush=True)
        candidate = try_with_days_and_order(day_limit, chosen_order, best_seed)
        if candidate is not None:
            new_triples = pre_repair_triple_count(candidate, chosen_order[:day_limit])
            if new_triples <= best_triples + 5:
                course_slot_map = candidate
                best_days = day_limit
                best_triples = new_triples
                day_slots = chosen_order[:best_days]
                print(f"   ✅ Works with {day_limit} days; pre-repair triples={new_triples}. Continuing…", flush=True)
                continue
        print(f"   ❌ {day_limit} days not feasible (or too many triples); keeping {best_days}.", flush=True)
        break

    print(f"🏁 Pre-repair days used: {best_days}", flush=True)

    # Order-aware repair
    course_slot_map, remaining = repair_3_in_3(
        course_slot_map=course_slot_map,
        course_to_students=course_to_students,
        student_to_courses=student_to_courses,
        conflict_map=conflict_map,
        preferred_slots=day_slots,
        max_passes=10,
        max_moves=2000,
        enable_swaps=True
    )

    # CP-SAT finisher (never worsen due to bound)
    if remaining > 0:
        print(f"⚠️ After repair, {remaining} 3-in-3 cases remain. Triggering CP-SAT finisher…", flush=True)
        current_assign = dict(course_slot_map)  # merged-key space
        improved = optimize_triples_cp_sat(
            course_list=list(course_to_students.keys()),
            conflict_map=conflict_map,
            student_to_courses=student_to_courses,
            fixed_slot_assignment=FIXED_COURSE_SLOTS,
            current_assignment=current_assign,
            day_slots=day_slots,
            current_best_triples=remaining,        # 🔒 never worse
            time_limit_seconds=60.0,               # a bit more time
            workers=8
        )
        # Evaluate improved with the same order-aware metric
        student_slots_tmp, _ = _compute_student_slots_map(improved, student_to_courses)
        improved_remaining = len(_detect_violations_order_aware(student_slots_tmp, day_slots))
        print(f"🧩 [cp-sat] result triples={improved_remaining}", flush=True)
        if improved_remaining <= remaining:
            course_slot_map = improved
            remaining = improved_remaining
        else:
            print("🧩 [cp-sat] did not improve; keeping heuristic assignment.", flush=True)

    # Expand grouped codes back to individual courses
    course_slot_map = expand_grouped_course_slots(course_slot_map, group_map, course_map)

    # Rebuild student mappings with names
    course_to_students_named = rebuild_course_to_students_with_names(course_to_students, course_map, course_to_group)

    db = SessionLocal()
    student_names = {s.student_id1: s.name for s in db.query(Student).all()}
    db.close()
    print(f"  • Student names fetched: {len(student_names)}", flush=True)

    print("🧾 Building final dataframe…", flush=True)
    rows = []
    missing_slots = 0
    for course, students in course_to_students_named.items():
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
                "Time": time_label,
                "Slot #": slot if slot is not None else "N/A"
            })

    if missing_slots:
        print(f"  ⚠️ Courses without slots after expansion: {missing_slots}", flush=True)

    # Persist
    try:
        run_id = save_schedule_to_db(course_slot_map, rows, start_date, best_days, xml_file_ids)
        print(f"🗂️ Schedule saved with run_id={run_id}", flush=True)
    except Exception as e:
        print(f"⚠️ Schedule persistence failed, continuing to return DataFrame. Error: {e}", flush=True)

    final_schedule_df = pd.DataFrame(rows)
    print(f"✅ [schedule_exams_from_db] DONE (order-aware pipeline) in {_fmt_ms(_now_ms() - t_all)}  • rows={len(final_schedule_df)}", flush=True)
    return final_schedule_df, student_to_courses, course_to_students_named
