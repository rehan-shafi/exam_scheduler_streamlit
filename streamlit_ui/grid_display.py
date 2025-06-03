import streamlit as st
import pandas as pd
from streamlit_ui.calendar_utils import get_slot_label
import re
import pdfkit
import tempfile
import base64

from collections import defaultdict

def display_schedule_grid():
    if "df_schedule" not in st.session_state:
        st.warning("No schedule available to display.")
        return

    df = st.session_state.df_schedule.copy()
    exam_dates = st.session_state.exam_dates

    # Helper: Convert slot number to day and time label
    def get_day_and_shift(slot):
        if slot in ["N/A", None, ""]:
            return ("Unscheduled", "")
        try:
            slot = int(slot)
            date_label = get_slot_label(slot, exam_dates)
            day_match = re.match(r"^(.*?)(\s*[–-]\s*\(.*)?$", date_label)
            day = day_match.group(1).strip() if day_match else "Invalid"
            shift = date_label.split("–")[1].strip().replace(")", "").replace("(", "")
            return (day, shift)
        except:
            return ("Invalid", "")

    # Add day and shift columns
    df[["Day", "Time"]] = df["Slot #"].apply(lambda s: pd.Series(get_day_and_shift(s)))

    # Create structure: {day: {AM: [], PM: []}}
    all_days = []
    for i in range(0, 20, 2):
        label = get_slot_label(i, exam_dates)
        day_match = re.match(r"^(.*?)(\s*[–-]\s*\(.*)?$", label)
        clean_day = day_match.group(1).strip() if day_match else "Invalid"
        all_days.append(clean_day)
        time_slots = ["9:00 AM - 11:00 AM", "11:30 AM - 01:30 PM"]

    schedule_dict = {day: {ts: set() for ts in time_slots} for day in all_days}

    for _, row in df.iterrows():
        day = str(row["Day"]).strip()
        time = str(row["Time"]).strip()
        course = row["Course"]
        if day in schedule_dict and time in schedule_dict[day]:
            schedule_dict[day][time].add(course)

    for day, slots in schedule_dict.items():
        for ts in time_slots:
            schedule_dict[day][ts] = sorted(schedule_dict[day][ts])

    # Build flattened grid
    rows = []
    for day, slots in schedule_dict.items():
        max_len = max(len(slots[time_slots[0]]), len(slots[time_slots[1]]), 1)
        for i in range(max_len):
            row = {
                "Day": day if i == 0 else "",
                time_slots[0]: slots[time_slots[0]][i] if i < len(slots[time_slots[0]]) else "",
                time_slots[1]: slots[time_slots[1]][i] if i < len(slots[time_slots[1]]) else "",
            }
            rows.append(row)

    final_df = pd.DataFrame(rows)

    # PDF Export
    html_table = final_df.to_html(index=False)
    html = f"""
    <html>
    <head>
        <meta charset="UTF-8">
        <style>
            body {{
                font-family: Arial, sans-serif;
                padding: 30px;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                font-size: 14px;
            }}
            th, td {{
                border: 1px solid #444;
                padding: 6px;
                text-align: left;
            }}
            th {{
                background-color: #f2f2f2;
            }}
        </style>
    </head>
    <body>
        <h2 style="margin-top: 30px;">Mustaqbal University - Exam Schedule</h2>
        {html_table}
    </body>
    </html>
    """

    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp_pdf:
        pdfkit.from_string(html, tmp_pdf.name)
        with open(tmp_pdf.name, "rb") as f:
            pdf_bytes = f.read()
        b64_pdf = base64.b64encode(pdf_bytes).decode()

    st.markdown(
        f'<a href="data:application/pdf;base64,{b64_pdf}" download="exam_schedule.pdf">📄 Download Exam Schedule as PDF</a>',
        unsafe_allow_html=True
    )

    # Highlighted grid rendering
    highlighted = set(st.session_state.get("selected_courses", []))

    def highlight_cell(course_name):
        if course_name in highlighted:
            return f'<td style="background-color: #fff7a8;"><b>{course_name}</b></td>'
        elif course_name == "":
            return "<td style='border: 1px solid #444; padding: 6px;'></td>"
        else:
            return f"<td style='border: 1px solid #444; padding: 6px;'>{course_name}</td>"

    html_rows = []
    html_rows.append("<table style='width:100%; border-collapse: collapse; font-family: Arial, sans-serif;'>")
    html_rows.append(
    "<tr>"
    "<th style='border: 1px solid #444; padding: 6px; background-color: #f2f2f2;'>Day</th>"
    "<th style='border: 1px solid #444; padding: 6px; background-color: #f2f2f2;'>9:00 AM - 11:00 AM</th>"
    "<th style='border: 1px solid #444; padding: 6px; background-color: #f2f2f2;'>11:30 AM - 01:30 PM</th>"
    "</tr>"
    )

    for idx, row in final_df.iterrows():
        bg_color = "#f9f9f9" if idx % 2 else "#ffffff"
        html_rows.append(f"<tr style='background-color: {bg_color};'>")
        html_rows.append(f"<td style='border: 1px solid #111; padding: 6px;'><b>{row['Day']}</b></td>")
        html_rows.append(highlight_cell(row['9:00 AM - 11:00 AM']))
        html_rows.append(highlight_cell(row['11:30 AM - 01:30 PM']))
        html_rows.append("</tr>")

    html_rows.append("</table>")
    highlighted_table_html = "\n".join(html_rows)

    st.markdown("### 🗓️ Exam Schedule by Day & Slot (Highlighted)")
    st.markdown(highlighted_table_html, unsafe_allow_html=True)
