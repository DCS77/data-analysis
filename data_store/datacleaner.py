import csv

class DataCleaner:
  '''
  Reads a CSV file, applies fixes and saves to another CSV file
  '''

  def __init__(self):
    return

  # Remove rows which have less or more columns than expected
  def remove_long_or_short_rows(self, input_csv_file, output_csv_file, num_cols):
    with open(output_csv_file, 'w') as write_file:
      writer = csv.writer(write_file, delimiter=',')
 
      with open(input_csv_file, 'r') as read_file:
        reader = csv.reader(read_file, delimiter=',')

        for row in reader:
          if len(row) is num_cols:
            writer.writerow(row)