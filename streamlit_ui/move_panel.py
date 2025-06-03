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
    if st.session_state.get("selected_courses") != selected_courses:
        st.session_state["selected_courses"] = selected_courses
        st.rerun()

    return selected_courses

def show_move_panel():
    if "df_schedule" not in st.session_state:
        return

    df = st.session_state.df_schedule
    student_to_courses = st.session_state.student_to_courses
    exam_dates = st.session_state.exam_dates

    st.subheader("🔄 Move Course to Another Slot")

    df_sorted = df.drop_duplicates("Course").copy()
    df_sorted["Slot Int"] = df_sorted["Slot #"].apply(lambda x: 999 if x in ["N/A", None, ""] else int(x))
    df_sorted = df_sorted.sort_values("Slot Int")

    all_courses = df_sorted["Course"].tolist()
    course_labels = {}
    for course in all_courses:
        row = df[df["Course"] == course].iloc[0]
        slot = row["Slot #"]
        if slot != "N/A":
            label = f"{course} – {get_slot_label(int(slot), exam_dates)}"
        else:
            label = f"{course} – Unscheduled"
        course_labels[label] = course

    selected_courses = show_course_checkboxes(course_labels)

    slot_labels = [get_slot_label(i, exam_dates) for i in range(20)]
    selected_slot = st.selectbox("Select Target Slot:", options=list(range(20)), format_func=lambda i: slot_labels[i])

    if st.button("Apply Move"):
        moved = []
        conflicts = []

        for course in selected_courses:
            students = df[df["Course"] == course]["Student ID"].unique()
            slot_conflict = False

            for student in students:
                student_slots = df[df["Student ID"] == student]["Slot #"].tolist()
                if selected_slot in student_slots:
                    conflicts.append((course, student))
                    slot_conflict = True
                    break

            if not slot_conflict:
                st.session_state.df_schedule.loc[df["Course"] == course, "Slot #"] = selected_slot
                day_index = selected_slot // 2
                time_label = "AM" if selected_slot % 2 == 0 else "PM"
                st.session_state.df_schedule.loc[df["Course"] == course, "Day"] = f"{exam_dates[day_index].strftime('%d %B')}"
                st.session_state.df_schedule.loc[df["Course"] == course, "Time"] = time_label
                moved.append(course)

        if moved:
            st.session_state.clear_selection = True
            st.success("\n".join([f"✅ Course '{c}' moved to {get_slot_label(selected_slot, exam_dates)}." for c in moved]))
            st.rerun()

        if conflicts:
            for course, student in conflicts:
                st.error(f"❌ Conflict: Student {student} already has an exam in {get_slot_label(selected_slot, exam_dates)} for course '{course}'.")
