import pandas as pd

csv_file_path = "datahost-ld-openapi/dataUpload/Planning-Applications-Decisions-Major-and-Minor-Developments-England-District-by-Development-Type.csv"

df = pd.read_csv(csv_file_path)

original_lines = len(df)

duplicate_rows = df[df.duplicated(subset=df.columns.difference(['Value']))]

num_duplicates = len(duplicate_rows)

filtered_df = df[~df.duplicated(subset=df.columns.difference(['Value']))]

num_final_lines = len(filtered_df)

output_csv_file = "datahost-ld-openapi/dataUpload/Planning-Applications-Decisions-Major-and-Minor-Developments-England-District-by-Development-Type.csv-filtered.csv"
filtered_df.to_csv(output_csv_file, index=False)

print("Original Number of Lines:", original_lines)
print("Number of Duplicate Rows:", num_duplicates)
print("Number of Lines in the Final DataFrame:", num_final_lines)

print("\nDuplicate Rows:")
print(duplicate_rows)

print("\nNon-Duplicate Rows:")
print(filtered_df)
