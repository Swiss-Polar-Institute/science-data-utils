import os
import csv
import pandas
import datetime

def get_file(filepath, filename):
    """Import a csv file to then check its format meets the requirements of frictionless data"""

    df = pandas.read_csv(filepath+filename, parse_dates=[['date', 'time']])

    return df


def output_csv(df, filepath, filename):
    """Output the dataframe into a csv file, specifying the date and time format."""

    output_file = os.path.join(filepath, filename)
    df.to_csv(output_file, date_format=("%Y-%m-%dT%H:%M:%S"))


def main():

    filepath = "/home/jen/projects/ace_data_management/wip/test_csv_format/"
    input_filename = "ACETPZT_20190416.csv"
    output_filename = "ACETPZT_20190416_formatted.csv"

    df = get_file(filepath, input_filename)

    output_csv(df, filepath, output_filename)


if __name__ == "__main__":
    main()