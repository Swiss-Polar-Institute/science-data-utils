from convert_isodatetime_to_timesince import convert_isodatetime_to_timesince_seconds
import csv
import argparse


def convert_isodatetime_in_file(input_file, initialdatetime, output_file):
    """Import rows from a file, get the ISO datetime and convert it to timesince for one row, then append it to an output file."""

    with open(input_file) as input_csv_file:
        csv_reader = csv.reader(input_csv_file, delimiter=',')
        with open(output_file, 'w') as output_csv_file:
            csv_writer = csv.writer(output_csv_file)

            row_count = 0
            for row in csv_reader:
                if row_count == 0:
                    header = row
                    header.append('time')
                    csv_writer.writerow(header)
                    row_count += 1
                else:
                    isodatetime = row[0]
                    timesince_secs = convert_isodatetime_to_timesince_seconds(initialdatetime, isodatetime)
                    row.append(timesince_secs)
                    csv_writer.writerow(row)
                    row_count += 1

                if row_count % 1000 == 0:
                    print(f'Converted {row_count} rows')

            output_csv_file.close()
        input_csv_file.close()


def main():

    parser = argparse.ArgumentParser(description='Convert ISO datetime to time (seconds since 1907-01-01T00:00:00Z) in '
                                                 'INPUT_FILE and write out the new rows to OUTPUT_FILE')
    parser.add_argument('input_file', help='Input filepath and name')
    parser.add_argument('output_file', help='Output filepath and name')

    args = parser.parse_args()

    convert_isodatetime_in_file(args.input_file, '1970-01-01T00:00:00+00:00', args.output_file)


if __name__ == "__main__":
    main()