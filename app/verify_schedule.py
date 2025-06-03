def verify_same_slot_conflicts(schedule_df):
    print("🔍 Checking for same-slot conflicts...\n")
    conflict_found = False

    student_groups = schedule_df.groupby("Student ID")

    for student, group in student_groups:
        valid_slots = (
            group["Slot #"]
            .astype(str)
            .loc[lambda x: x != "N/A"]
            .astype(int)
            .tolist()
        )

        duplicates = [s for s in set(valid_slots) if valid_slots.count(s) > 1]

        if duplicates:
            print(f"❌ Conflict: {student} has multiple exams in slot(s): {duplicates}")
            conflict_found = True

    if not conflict_found:
        print("✅ No same-slot conflicts found. Schedule is clean.")
