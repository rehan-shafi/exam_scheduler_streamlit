from db.session import SessionLocal
from app.processor import insert_xml_data

with open("sample_male.xml", "rb") as file:
    db = SessionLocal()
    xml_file_id, root = insert_xml_data(file, gender="male", filename="sample_male.xml", db=db)
    print(f"✅ XML inserted with id: {xml_file_id}")
