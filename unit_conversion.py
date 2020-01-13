import csv
import argparse

# This code currently only supports the conversion of millibars to Pascals. For further addition of conversions, the
# funciton convert_units should be amended accordingly, paying attention to: identified column, which conversion needs
# to be done and creating the possibility for further user input to run the script.

def convert_millibar_to_pascal(millibar):
    """Convert a measurement in millibars to Pascals"""

    pascal = millibar * 100

    return pascal


def convert_units(input_file, output_file):
    """Convert units of a variable in the input file and output the new values into the output file"""

    with open(input_file) as input_csv_file:
        csv_reader = csv.reader(input_csv_file, delimiter=',')
        with open(output_file, 'w') as output_csv_file:
            csv_writer = csv.writer(output_csv_file)

            row_count = 0
            for row in csv_reader:
                if row_count == 0:
                    header = row
                    header.append('PA1_Pa')
                    header.append('PA2_Pa')
                    csv_writer.writerow(header)
                    row_count += 1
                else:
                    PA1_mb = float(row[14])
                    PA2_mb = float(row[15])

                    PA1_Pa = convert_millibar_to_pascal(PA1_mb)
                    PA2_Pa = convert_millibar_to_pascal(PA2_mb)

                    row.append(PA1_Pa)
                    row.append(PA2_Pa)

                    csv_writer.writerow(row)
                    row_count += 1

                if row_count % 1000 == 0:
                    print(f'Converted {row_count} rows')

            output_csv_file.close()
        input_csv_file.close()


def main():

    parser = argparse.ArgumentParser(description='Convert units of a column in the '
                                                 'INPUT_FILE and write out the new rows to OUTPUT_FILE')
    parser.add_argument('input_file', help='Input filepath and name')
    parser.add_argument('output_file', help='Output filepath and name')

    args = parser.parse_args()

    convert_units(args.input_file, args.output_file)


if __name__ == "__main__":
    main()