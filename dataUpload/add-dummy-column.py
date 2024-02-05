import csv
import random
import sys
import os

def add_dummy_column(input_file):
    output_file = os.path.splitext(input_file)[0] + "-dummy-column.csv"

    with open(input_file, 'r', newline='') as csv_in, \
         open(output_file, 'w', newline='') as csv_out:
        reader = csv.reader(csv_in)
        writer = csv.writer(csv_out)

        for i, row in enumerate(reader):
            if i == 0:
                row.append('DummyColumn')  # Add header for the new column
            else:
                row.append(str(random.randint(0, 1000)))  # Add random integer to each row
            writer.writerow(row)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <input_file>")
        sys.exit(1)

    input_file = sys.argv[1]
    add_dummy_column(input_file)
