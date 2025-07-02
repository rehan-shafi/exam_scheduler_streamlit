import streamlit as st
from streamlit_ui.calendar_utils import get_slot_label
import math

def show_course_checkboxes(course_labels, num_columns=1):
    selected_courses = []
    course_items = list(course_labels.items())

    rows_per_col = math.ceil(len(course_items) / num_columns)

    with st.container(height=400):  # Scrollable area
        cols = st.columns(num_columns)
        for col_idx in range(num_columns):
            with cols[col_idx]:
                for i in range(col_idx * rows_per_col, min((col_idx + 1) * rows_per_col, len(course_items))):
                    label, course = course_items[i]
                    key = f"chk_{course}"
                    if st.checkbox(label, key=key):
                        selected_courses.append(course)

    # ✅ Instant update and rerun for live highlighting
    #if st.session_state.get("selected_courses") != selected_courses:
    #    st.session_state["selected_courses"] = selected_courses
    #    st.rerun()

    #return selected_courses

    st.session_state["selected_courses"] = selected_courses
    return selected_courses

def show_move_panel():
    if "df_schedule" not in st.session_state:
        return

    df = st.session_state.df_schedule
    student_to_courses = st.session_state.student_to_courses
    exam_dates = st.session_state.exam_dates

    st.subheader("🔄 Move Course to Another Slot")

    df_sorted = df.drop_duplicates("Course Code").copy()
    df_sorted["Slot Int"] = df_sorted["Slot #"].apply(lambda x: 999 if x in ["N/A", None, ""] else int(x))
    df_sorted = df_sorted.sort_values("Slot Int")

    all_courses = df_sorted["Course Code"].tolist()
    course_labels = {}
    for course in all_courses:
        row = df[df["Course Code"] == course].iloc[0]
        slot = row["Slot #"]
        if slot != "N/A":
            label = f"{course} – {get_slot_label(int(slot), exam_dates)}"
        else:
            label = f"{course} – Unscheduled"
        course_labels[label] = course

    selected_courses = show_course_checkboxes(course_labels)

    total_slots = len(exam_dates) * 2
    slot_labels = [get_slot_label(i, exam_dates) for i in range(total_slots)]
    selected_slot = st.selectbox("Select Target Slot:", options=list(range(total_slots)), format_func=lambda i: slot_labels[i])

    if st.button("Apply Move"):
        moved = []
        conflicts = []

        group_map = st.session_state.get("group_map", {})
        course_to_group = {}
        for gid, codes in group_map.items():
            for c in codes:
                course_to_group[c] = gid

        processed = set()

        for course in selected_courses:
            if course in processed:
                continue

            # Get full group
            group_id = course_to_group.get(course)
            group_courses = list(group_map[group_id]) if group_id else [course]

            # Check if any conflict
            group_conflict = False
            group_students = set()

            for gc in group_courses:
                group_students.update(df[df["Course Code"] == gc]["Student ID"].unique())

            for student in group_students:
                student_slots = df[df["Student ID"] == student]["Slot #"].tolist()
                if selected_slot in student_slots:
                    conflicts.append((", ".join(group_courses), student))
                    group_conflict = True
                    break

            if not group_conflict:
                for gc in group_courses:
                    st.session_state.df_schedule.loc[df["Course Code"] == gc, "Slot #"] = selected_slot
                    day_index = selected_slot // 2
                    time_label = "AM" if selected_slot % 2 == 0 else "PM"
                    st.session_state.df_schedule.loc[df["Course Code"] == gc, "Day"] = exam_dates[day_index]
                    st.session_state.df_schedule.loc[df["Course Code"] == gc, "Time"] = time_label
                    moved.append(gc)
                    processed.add(gc)

        if moved:
            st.session_state["df_schedule"] = st.session_state.df_schedule.copy()
            st.session_state.clear_selection = True
            st.success("\n".join([f"✅ Course '{c}' moved to {get_slot_label(selected_slot, exam_dates)}." for c in moved]))
            st.rerun()

        if conflicts:
            for course_group, student in conflicts:
                st.error(f"❌ Conflict: Student {student} already has an exam in {get_slot_label(selected_slot, exam_dates)} for group '{course_group}'.")
