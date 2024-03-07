import csv
import sys

# Define a dictionary to map measurement values to their corresponding units
measurement_units = {
    "IMD rank": "ordinal-position",
    "IMD score": "deprivation-score",
    "Rank": "ordinal-position",
    "Score": "deprivation-score",
    "Decile": "decile-group",
    "Decisions": "application-decisions",
    "Completions": "building-completions"
}

# Function to update the units column based on the measurement value
def update_units(row):
    measurement = row['Measurement']
    if measurement in measurement_units:
        row['Units'] = measurement_units[measurement]
    return row

if len(sys.argv) != 2:
    print("Usage: python script.py input_file")
    sys.exit(1)

# Input file path
input_file = sys.argv[1]

# Create a temporary file to store the updated data
temp_file = input_file + '.tmp'

# Read the CSV file and update the units column
with open(input_file, 'r') as csvfile:
    reader = csv.DictReader(csvfile)
    fieldnames = reader.fieldnames  # Retrieve the original column titles
    rows = [update_units(row) for row in reader]

# Write the updated rows to the temporary file
with open(temp_file, 'w', newline='') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)

# Replace the input file with the temporary file
os.replace(temp_file, input_file)

print("Units updated successfully!")
