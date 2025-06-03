# 📘 University Exam Scheduler

This is a **Streamlit-based web application** that generates conflict-free university exam schedules from an Excel file. The app allows both **automatic scheduling** and **manual adjustments** via an interactive UI, with support for PDF export.

---

## ✅ Features

- 📅 Select exam start date
- 📥 Upload Excel file (student-course enrollment)
- ⚙️ Automatically assigns courses to conflict-free time slots
- 🖥️ Displays a clean day-wise grid of the exam timetable
- 🔄 Move any course manually to a different time slot
- 📄 Export the final schedule to PDF

---

## 📂 How to Run Locally

### 1. Clone the repository

git clone https://github.com/rehan-shafi/exam_scheduler_streamlit.git
cd exam_scheduler_streamlit

### 2. Install dependencies
pip install -r requirements.txt
⚠️ Note: pdfkit requires wkhtmltopdf to be installed on your system.

Download here: https://wkhtmltopdf.org/downloads.html

Make sure wkhtmltopdf is in your system PATH

### 3. Run the app
streamlit run streamlit_app.py


## 🛠️ Tech Stack
Python
Streamlit
Pandas
OpenPyXL
pdfkit (for export)

## 🧑‍💼 Author
Made by Rehan Shafi – as part of a real-world university use case demo.

## 🌐 Deployment
Soon to be deployed on Streamlit Cloud.
