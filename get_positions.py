import csv
import pandas
import glob
import cruise_track_data_processing_utils
import aggregate_lower_resolution


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


def get_all_positions(filepath_position, one_min_res_filename):
    #file_names = filepath_position + one_min_res_filename
    all_min_files = glob.glob(filepath_position + one_min_res_filename)

    #print(all_min_files)
    files = []

    for file in all_min_files:
        position_df = get_position_data(file)
        files.append(position_df)

    one_min_res_position_df = pandas.concat(files)
    #print(one_min_res_position_df.head(10))
    print(one_min_res_position_df.dtypes)

    return one_min_res_position_df


def get_list_dates(filepath, filename):
    date_column_list = [0]

    datatypes = {}

    dates_df = cruise_track_data_processing_utils.get_data_from_csv(filepath, filename, datatypes, date_column_list)

    print("Number of rows in dates file: ", len(dates_df))

    return dates_df


def merge_dfs(df1, df2, column_name):

    df3 = pandas.merge(df1, df2, how='left', on=column_name)

    return df3


def process_get_positions():

    # input variables - position data
    filepath_position = "/home/jen/projects/ace_data_management/wip/cruise_track_data/"

    one_min_res_filename = "ace_cruise_track_1min_*.csv"

    one_min_res_position_df = get_all_positions(filepath_position, one_min_res_filename)

    filepath_dates = "/home/jen/projects/ace_data_management/data_to_archive_post_cruise/project13/genoscope_work"

    # START TIMES
    # input variables - dates/times in csv file
    filename_dates_start = "dates_times_start.csv"

    start_dates_df = get_list_dates(filepath_dates, filename_dates_start)

    start_position_df = merge_dfs(start_dates_df, one_min_res_position_df, "date_time")
    print(start_position_df.head(10))

    # output the dataframe to a csv file
    output_filepath = "/home/jen/projects/ace_data_management/data_to_archive_post_cruise/project13/genoscope_work/"
    output_columns = ['date_time', 'latitude', 'longitude', 'measureland_qualifier_flag_overall']
    start_output_filename = 'datetime_position_matched_start.csv'

    aggregate_lower_resolution.output_dataframe_to_csv(start_position_df, output_columns, output_filepath, start_output_filename)

    # END TIMES
    # input variables - dates/times in csv file
    filename_dates_end = "dates_times_end.csv"

    end_dates_df = get_list_dates(filepath_dates, filename_dates_end)

    end_position_df = merge_dfs(end_dates_df, one_min_res_position_df, "date_time")

    # output the dataframe to a csv file
    output_filepath = "/home/jen/projects/ace_data_management/data_to_archive_post_cruise/project13/genoscope_work/"
    output_columns = ['date_time', 'latitude', 'longitude', 'measureland_qualifier_flag_overall']
    end_output_filename = 'datetime_position_matched_end.csv'

    aggregate_lower_resolution.output_dataframe_to_csv(end_position_df, output_columns, output_filepath,
                                                       end_output_filename)


def main():

    process_get_positions()

if __name__ == "__main__":
        main()


