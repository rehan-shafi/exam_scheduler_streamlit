import streamlit as st
import datetime
from streamlit_ui.grid_display import display_schedule_grid
from streamlit_ui.move_panel import show_move_panel
from streamlit_ui.calendar_utils import generate_exam_dates
from app.processor import extract_student_course_data
from app.scheduler import schedule_exams

st.set_page_config(layout="wide")
st.markdown("<h3 style='margin-bottom: 0.5rem;'>📘 Mustaqbal University Exam Scheduler</h3>", unsafe_allow_html=True)


with st.expander("⚙️ Setup Options", expanded=True):
    colA, colB = st.columns([1, 2])
    with colA:
        start_date = st.date_input("📅 Select Exam Start Date", value=datetime.date.today())
    with colB:
        uploaded_file = st.file_uploader("Upload cleaned student-course Excel file (.xlsx)", type=["xlsx"])

# Step 1: Select exam start date
st.session_state["exam_start_date"] = start_date

# Step 2: Generate list of working days from selected date
if "exam_dates" not in st.session_state:
    st.session_state["exam_dates"] = generate_exam_dates(start_date)

# Step 3: Upload file
if uploaded_file:
    if "df_schedule" not in st.session_state:
        df_schedule, _, student_to_courses, course_to_students = schedule_exams(uploaded_file)
        st.session_state.df_schedule = df_schedule
        st.session_state.course_to_students = course_to_students
        st.session_state.student_to_courses = student_to_courses
        st.session_state.exam_dates = generate_exam_dates(start_date)

    # Reset course selection once
    if st.session_state.get("clear_selection", False):
        st.session_state.selected_courses = []
        st.session_state.clear_selection = False

    col1, col2 = st.columns([3, 2])  # Wider grid (left), narrower move panel (right)

    with col1:
        display_schedule_grid()

    with col2:
        st.markdown("<div style='height: 2.4rem;'></div>", unsafe_allow_html=True)
        show_move_panel()
        

else:
    pass
