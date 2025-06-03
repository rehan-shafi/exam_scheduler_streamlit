# streamlit_ui/state.py

import streamlit as st

def init_state():
    if "df_schedule" not in st.session_state:
        st.session_state["df_schedule"] = None
    if "selected_course" not in st.session_state:
        st.session_state["selected_course"] = None
    if "student_to_courses" not in st.session_state:
        st.session_state["student_to_courses"] = None
