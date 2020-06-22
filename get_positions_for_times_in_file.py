import argparse
import csv

from datetime_to_position import DatetimeToPosition


def process_file(input_date_time_filepath, output_filepath):
    """
    Get datetimes from input CSV file and find corresponding positions which are the output of the DatetimeToPosition
    function. Output them into a CSV file.
    :param input_date_time_filepath: CSV file containing one column of datetimes in the format YYYY-MM-DDThh:mm:ss,
    no header line
    :param output_filepath: CSV file with single-line header containing input datetime and corresponding latitude and
    longitude
    :return: None
    """
    output_file = open(output_filepath, 'w')

    with open(input_date_time_filepath) as csvfile:
        filereader = csv.reader(csvfile, delimiter=',', )
        next(filereader, None)
        datetime_to_position = DatetimeToPosition()

        for row in filereader:
            print(row[0])
            position = datetime_to_position.datetime_text_to_position(row[0])
            if position is None:
                output_file.write("{}, {}, {}\n".format(row[0], None, None))
            else:
                print(position)
                output_file.write("{}, {}, {}\n".format(row[0], position[0], position[1]))

    output_file.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Ouptut into a file, the latitude and longitude for a given date and time where these are taken "
                    "from an SQLite database.")
    parser.add_argument("input_date_time_filepath",
                        help="Full file path and filename of the file containing the dates and times")
    parser.add_argument("output_filepath", help="Filepath and filename of output file")

    args = parser.parse_args()

    process_file(args.input_date_time_filepath, args.output_filepath)
