import pandas as pd
import sys

def filter_csv(file_path, column_to_ignore='Value'):
    """
    Filter duplicate rows from a CSV file and save the filtered DataFrame to a new CSV file.

    Usage:
    python script.py <file_path> [column_to_ignore]

    Arguments:
    - file_path: Path to the CSV file.
    - column_to_ignore (optional): Name of the measure column.
                                   Default is 'Value'.

    Example:
    python script.py "datahost-ld-openapi/dataUpload/Planning-Applications-Decisions-Major-and-Minor-Developments-England-District-by-Development-Type.csv" "Value"

    The output file will be saved in the same directory as the input file with the suffix "-filtered.csv".
    """
    try:
        df = pd.read_csv(file_path)
        original_lines = len(df)
        
        duplicate_rows = df[df.duplicated(subset=df.columns.difference([column_to_ignore]))]
        num_duplicates = len(duplicate_rows)
        
        filtered_df = df[~df.duplicated(subset=df.columns.difference([column_to_ignore]))]
        num_final_lines = len(filtered_df)
        
        output_csv_file = file_path + "-filtered.csv"
        filtered_df.to_csv(output_csv_file, index=False)
        
        print("Original Number of Lines:", original_lines)
        print("Number of Duplicate Rows:", num_duplicates)
        print("Number of Lines in the Final DataFrame:", num_final_lines)
        
        print("\nDuplicate Rows:")
        print(duplicate_rows)
        
        print("\nNon-Duplicate Rows:")
        print(filtered_df)
    
    except FileNotFoundError:
        print("File not found.")

if __name__ == "__main__":
    if len(sys.argv) < 2 or len(sys.argv) > 3:
        print("Usage: python script.py <file_path> [column_to_ignore]")
        sys.exit(1)
        
    file_path = sys.argv[1]
    column_to_ignore = sys.argv[2] if len(sys.argv) == 3 else 'Value'
    filter_csv(file_path, column_to_ignore)
