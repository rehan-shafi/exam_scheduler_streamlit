import pandas as pd

def generate_datesheet_table(schedule_df):
    # Step 1: Group by Day and Time
    grouped = schedule_df.groupby(["Day", "Time"])["Course"].unique().reset_index()

    # Step 2: Build a table: rows = Day, cols = AM/PM
    days = [f"Day {i}" for i in range(1, 12)]  # Up to Day 11 (including overflow)
    table = []

    for day in days:
        am_courses = grouped[(grouped["Day"] == day) & (grouped["Time"] == "AM")]["Course"]
        pm_courses = grouped[(grouped["Day"] == day) & (grouped["Time"] == "PM")]["Course"]

        am_text = ", ".join(am_courses.iloc[0]) if not am_courses.empty else ""
        pm_text = ", ".join(pm_courses.iloc[0]) if not pm_courses.empty else ""

        table.append({
            "Day": day,
            "9:00–11:00 AM": am_text,
            "11:30–1:30 PM": pm_text
        })

    return pd.DataFrame(table)

def format_student_to_courses(student_to_courses):
    rows = []

    for student, courses in student_to_courses.items():
        rows.append({
            "Student ID": student,
            "Enrolled Courses": ", ".join(sorted(courses))
        })

    return pd.DataFrame(rows)