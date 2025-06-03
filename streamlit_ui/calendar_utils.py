import datetime

def generate_exam_dates(start_date, total_days=10):
    exam_days = []
    current = start_date
    while len(exam_days) < total_days:
        if current.weekday() not in (4, 5):  # Skip Friday (4), Saturday (5)
            exam_days.append(current)
        current += datetime.timedelta(days=1)
    return exam_days

def get_slot_label(slot_index, exam_dates):
    day_index = slot_index // 2
    shift = "(9:00 AM - 11:00 AM)" if slot_index % 2 == 0 else "(11:30 AM - 01:30 PM)"
    return f"{exam_dates[day_index].strftime('%A, %d %B')} – {shift}"
