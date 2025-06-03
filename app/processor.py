import pandas as pd
from app.mapping_utils import generate_course_and_student_mappings

def propagate_course_info(df):
    current_course = None

    for i in range(len(df)):
        val = str(df.iat[i, 0]).strip().lower()

        if val.startswith("course-"):
            current_course = df.iat[i, 0]  # Store the full course line
        elif pd.isna(df.iat[i, 0]) or df.iat[i, 0] == "":
            if current_course:
                df.iat[i, 0] = current_course  # Fill down
        else:
            # If a new non-empty, non-course value comes, stop filling
            current_course = None

    return df


def extract_student_course_data(file_path):
    df = pd.read_excel(file_path, sheet_name=0, header=None)

    # Extract full columns D (3), G (6), H (7), J (9)
    selected_df = df[[3, 6, 7, 9]]
    selected_df.columns = ["D", "G", "H", "J"]

    for i in range(len(selected_df)):
        val = str(selected_df.iat[i, 0]).strip().lower()
        if val == "course":
            part1 = str(selected_df.iat[i, 2]).strip()
            part2 = str(selected_df.iat[i, 3]).strip()
            combined = f"{val}-{part1}-{part2}"
            selected_df.iat[i, 0] = combined

    # Keep only column 0 (Course Info) and column 1 (Student ID)
    final_df = selected_df[["D", "G"]].copy()
    final_df.columns = ["Course Info", "Student ID"]

    # Remove rows where both Course Info and Student ID are missing
    final_df.dropna(subset=["Course Info", "Student ID"], how="all", inplace=True)

    # Keep rows where Course Info is either null or starts with "course-"
    final_df = final_df[
        final_df["Course Info"].isna() |
        final_df["Course Info"].str.strip().str.lower().str.startswith("course-")
    ].copy()
    final_df = propagate_course_info(final_df)

    # Normalize and drop rows where Student ID is NaN or contains the string "student id"
    final_df = final_df[
        final_df["Student ID"].notna() &
        (~final_df["Student ID"]
            .astype(str)
            .str.replace(r'\s+', ' ', regex=True)  # collapse multiple spaces
            .str.strip()
            .str.lower()
            .eq("student id")
        )
    ].copy()

    final_df["Course Info"] = final_df["Course Info"].str.replace(r"^course-\s*", "", regex=True)

    # Save to a new CSV file
    output_path = "output/final_course_student_mapping.csv"
    final_df.to_csv(output_path, index=False)

    # Optional print
    #print("\n✅ Final cleaned data (Course Info + Student ID):\n")
    #print(final_df.to_string(index=False))

    
    course_to_students, student_to_courses = generate_course_and_student_mappings(final_df)

    return final_df, output_path,course_to_students, student_to_courses
