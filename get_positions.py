import csv
import pandas
import glob
import cruise_track_data_processing_utils
import aggregate_lower_resolution
import argparse
import os


def get_position_data(file):

    date_column_list = [0]

    datatypes = {'latitude': 'float64',
                 'longitude': 'float64',
                 'fix_quality': 'int8',
                 'number_satellites': 'int8',
                 'horiz_dilution_of_position': 'float16',
                 'altitude': 'float16',
                 'altitude_units': 'category',
                 'geoid_height': 'float16',
                 'geoid_height_units': 'category',
                 'device_id': 'int8',
                 'speed': 'float64',
                 'measureland_qualifier_flags_overall': 'int8'}

    # get the data from the csv files
    position_df = pandas.read_csv(file, dtype=datatypes, date_parser=pandas.to_datetime,
                                parse_dates=date_column_list)

    print("Number of rows in position file: ", len(position_df))

    return position_df


def get_all_positions(lat_lon_filepath, lat_lon_resolution):

    lat_lon_filename = 'ace_cruise_track_1'
    path = os.path.join(lat_lon_filepath, lat_lon_filename)

    all_position_files = glob.glob(path + lat_lon_resolution + "*.csv")

    position_dfs = []

    for file in all_position_files:
        position_df = get_position_data(file)
        position_dfs.append(position_df)

    res_position_df = pandas.concat(position_dfs)
    print(res_position_df.dtypes)

    return res_position_df


def get_list_dates(filepath):
    date_column_list = [0]

    datatypes = {}

    dates_df = cruise_track_data_processing_utils.get_data_from_csv_full_path(filepath, datatypes, date_column_list)

    print("Number of rows in dates file: ", len(dates_df))

    return dates_df


def convert_datetime(date_time):

    #pandas.to_datetime(date_time, format='%Y-%m-%d %H:%M:%S')
    date_time_rounded = date_time.replace(microsecond=0)

    return date_time_rounded


def merge_dfs(df1, df2, column_name):

    df3 = pandas.merge(df1, df2, how='left', on=column_name)

    return df3


def process_get_positions(date_time_filepath, lat_lon_filepath, lat_lon_resolution, output_filepath, output_filename):

    input_position_df = get_all_positions(lat_lon_filepath, lat_lon_resolution)

    dates_df = get_list_dates(date_time_filepath)

    input_position_df['date_time'] = input_position_df['date_time'].apply(convert_datetime)

    matched_position_df = merge_dfs(dates_df, input_position_df, "date_time")
    print(matched_position_df.head(10))

    # output the selected dataframe to a csv file
    output_columns = ['date_time', 'latitude', 'longitude', 'measureland_qualifier_flag_overall']

    aggregate_lower_resolution.output_dataframe_to_csv(matched_position_df, output_columns, output_filepath, output_filename)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ouptut into a file, the latitude and longitude for a given date and time where these are listed in an input file.")
    parser.add_argument("date_time_filepath", help="Full file path and filename of the file containing the dates and times")
    parser.add_argument("lat_lon_filepath", help="Filepath to where the directory containing the position data files")
    parser.add_argument("lat_lon_resolution", type=str, choices=("sec", "min", "hour"), help="Select the resolution of the position data would you like to use.")
    parser.add_argument("output_filepath", help="Filepath to output file")
    parser.add_argument("output_filename", help="Output filename")


    args = parser.parse_args()

    process_get_positions(args.date_time_filepath, args.lat_lon_filepath, args.lat_lon_resolution, args.output_filepath, args.output_filename)


