import csv
import os
import sys

def move_year_rows(input_file, year):
    """
    Move rows containing a specific string (year) from a CSV file to a new CSV file and remove them from the original file.

    Usage:
    python script.py <input_file> <year>

    Arguments:
    - input_file: Path to the input CSV file.
    - year: The year to filter rows by.

    Example:
    python script.py "input.csv" "2022"
    """

    output_file = os.path.splitext(input_file)[0] + f"-{year}.csv"
    
    with open(input_file, 'r', newline='') as csv_in, \
         open(output_file, 'w', newline='') as csv_out:
        
        reader = csv.reader(csv_in)
        writer = csv.writer(csv_out)
        
        column_titles = next(reader)
        writer.writerow(column_titles)
        
        for row in reader:
            if year in row[1]:
                writer.writerow(row)
    
    with open(input_file, 'r', newline='') as csv_in:
        rows = [row for row in csv.reader(csv_in) if year not in row[1]]
    
    with open(input_file, 'w', newline='') as csv_in:
        writer = csv.writer(csv_in)
        writer.writerow(column_titles)  
        writer.writerows(rows)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script.py <input_file> <year>")
        sys.exit(1)

    input_file = sys.argv[1]
    year = sys.argv[2]
    move_year_rows(input_file, year)
