from sqlalchemy import Column, Integer, String, Text, ForeignKey, DateTime
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from db.session import Base

class XMLFile(Base):
    __tablename__ = "xml_files"

    id = Column(Integer, primary_key=True)
    filename = Column(Text)
    gender_group = Column(String)
    uploaded_at = Column(DateTime(timezone=True), server_default=func.now())

    courses = relationship("Course", back_populates="xml_file")
    students = relationship("Student", back_populates="xml_file")


class Course(Base):
    __tablename__ = "courses"

    id = Column(Integer, primary_key=True)
    course_code = Column(Text, nullable=False)
    course_name = Column(Text)
    section = Column(String)
    xml_file_id = Column(Integer, ForeignKey("xml_files.id"))

    xml_file = relationship("XMLFile", back_populates="courses")
    students = relationship("CourseStudent", back_populates="course")


class Student(Base):
    __tablename__ = "students"

    id = Column(Integer, primary_key=True)
    student_id1 = Column(Text, nullable=False)
    name = Column(Text)
    major = Column(Text)
    xml_file_id = Column(Integer, ForeignKey("xml_files.id"))

    xml_file = relationship("XMLFile", back_populates="students")
    courses = relationship("CourseStudent", back_populates="student")


class CourseStudent(Base):
    __tablename__ = "course_students"

    id = Column(Integer, primary_key=True)
    course_id = Column(Integer, ForeignKey("courses.id"))
    student_id = Column(Integer, ForeignKey("students.id"))

    course = relationship("Course", back_populates="students")
    student = relationship("Student", back_populates="courses")


class IgnoredCourse(Base):
    __tablename__ = "ignored_courses"

    id = Column(Integer, primary_key=True)
    course_code = Column(Text, unique=True, nullable=False)
    reason = Column(Text)


class MergedCourse(Base):
    __tablename__ = "merged_courses"

    id = Column(Integer, primary_key=True)
    group_id = Column(Text, nullable=False)
    course_code = Column(Text, nullable=False)
