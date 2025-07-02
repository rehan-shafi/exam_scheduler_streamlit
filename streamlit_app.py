import streamlit as st
import datetime
from streamlit_ui.grid_display import display_schedule_grid
from streamlit_ui.move_panel import show_move_panel
from streamlit_ui.calendar_utils import generate_exam_dates
from app.scheduler import schedule_exams_from_db
from app.processor import process_uploaded_file
from db.models import Course, Student, CourseStudent
from db.session import SessionLocal

st.set_page_config(layout="wide")
st.markdown("<h3 style='margin-bottom: 0.5rem;'>📘 Mustaqbal University Exam Scheduler</h3>", unsafe_allow_html=True)


with st.expander("⚙️ Setup Options", expanded=True):
    colA, colB = st.columns([1, 2])
    with colA:
        start_date = st.date_input("📅 Select Exam Start Date", value=datetime.date.today())
        num_days = st.number_input("🗓️ Number of Exam Days", min_value=1, max_value=30, value=10)
    with colB:
        regular_file = st.file_uploader("Upload Regular Campus XML", type=["xml"], key="regular")
        visitor_file = st.file_uploader("Upload Visiting Students XML", type=["xml"], key="visitor")

# Step 1: Select exam start date
st.session_state["exam_start_date"] = start_date
st.session_state["num_days"] = num_days

# Step 2: Generate list of working days from selected date
st.session_state["exam_dates"] = generate_exam_dates(start_date,num_days)

# Step 3: Upload file
if regular_file and visitor_file:
    st.success("📤 Uploading and processing files...")

    if "uploaded" not in st.session_state:
        regular_id = process_uploaded_file(regular_file, gender="regular", first_file_id=0)
        st.session_state["regular_xml_id"] = regular_id  # ✅ Save first file ID

        visitor_id = process_uploaded_file(visitor_file, gender="visitor", first_file_id=regular_id)
        st.session_state["visitor_xml_id"] = visitor_id
        st.session_state["uploaded"] = True
        st.session_state["xml_ids"] = [regular_id, visitor_id]

    if st.button("📅 Generate Exam Schedule"):
        final_df, student_to_courses, course_to_students = schedule_exams_from_db(
            st.session_state["xml_ids"], start_date, num_days
        )
        st.session_state["df_schedule"] = final_df
        st.session_state["student_to_courses"] = student_to_courses
        st.session_state["course_to_students"] = course_to_students
        st.session_state["schedule_ready"] = True

# ✅ Final display block (always check this after button)
if st.session_state.get("schedule_ready") and "df_schedule" in st.session_state:
    col1, col2 = st.columns([3, 2])

    with col1:
        display_schedule_grid()

    with col2:
        st.markdown("<div style='height: 2.4rem;'></div>", unsafe_allow_html=True)
        show_move_panel()
else:
    pass


