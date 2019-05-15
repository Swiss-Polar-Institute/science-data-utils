import pandas
import os
import datetime
import cruise_track_data_processing_utils
import matplotlib.pyplot as plt

def investigate_aggregation_by_time(dataframe):
    """investigate approximately how many rows to expect when the dataset is aggregated to minute and hour resolution.
    Output the dataframe with columns which contain just the number of hours, number of minutes and number of seconds, to aid with the aggregation."""

    # create columns with just hours, minutes, seconds.
    dataframe['time_hours'] = pandas.to_datetime(dataframe['date_time']).dt.hour
    dataframe['time_minutes'] = pandas.to_datetime(dataframe['date_time']).dt.minute
    dataframe['time_seconds'] = pandas.to_datetime(dataframe['date_time']).dt.second

    # how many rows have 0 seconds (this will be used to get data on the minute, i.e 12:03:00).
    print("Number of rows with 0 seconds: ", len(dataframe[dataframe['time_seconds']==0]))
    print(dataframe[dataframe['time_seconds']==0].head(5))

    # how many rows have 0 minutes (this will be used to get data on the hour, i.e 13:00:00).
    print("Number of rows with 0 minutes and 0 seconds: ", len(dataframe[(dataframe['time_minutes'] == 0) & (dataframe['time_seconds'] == 0)]))
    print(dataframe[(dataframe['time_minutes'] == 0) & (dataframe['time_seconds'] == 0)].head(5))

    return dataframe


def get_minute_resolution(dataframe):
    """Aggregate the data to get one-minute resolution, where the number of seconds is 0, so each record is on the minute exactly. Output a new dataframe with one-minute resolution. """

    minute_res_df = dataframe[dataframe['time_seconds'] == 0]
    print("Number of rows in minute resolution dataframe: ", len(minute_res_df))

    return minute_res_df


def get_hour_resolution(dataframe):
    """Aggregate the data to get one-hour resolution, where the number of minutes is 0 and the number of seconds is 0, so each record is on the hour exactly. Output a new dataframe with one-hour resolution. """

    hour_res_df = dataframe[(dataframe['time_minutes'] == 0) & (dataframe['time_seconds'] == 0)]
    print("Number of rows in hour resolution dataframe: ", len(hour_res_df))

    return hour_res_df


def format_time(row):
    """Format the date and time into ISO 8601 format YYYY-MM-DDThh:mm:ss+00:00"""

    # ISO 8601 format YYYY-MM-DDThh:mm:ss+00:00
    return row['date_time'].strftime('%Y-%m-%dT%H:%M:%S+00:00')


def output_dataframe_to_csv(dataframe, columns, output_filepath, output_filename):
    """Output a dataframe to a csv file, specifying the columns that are required in the output and the output filepath and filename.
    The index is not included in the output. The datetime is corrected to IS 8601 format."""

    output_file = os.path.join(output_filepath, output_filename)
    print("Output filename: ", output_file)

    # apply the datetime format function
    dataframe['date_time'] = dataframe.apply(format_time, axis=1)

    dataframe.to_csv(output_file, index=False, columns=columns)


def plot_aggregated_data_separate_graphs(dataframe_min, dataframe_hour):
    """Plot the data on separate graphs."""

    # Plot one minute resolution data
    plt.subplot(211)
    plt.scatter(dataframe_min.longitude, dataframe_min.latitude, c="red")
    plt.title("One-minute resolution")
    plt.xlabel("Longitude, decimal degrees E")
    plt.ylabel("Latitude, decimal degrees N")
    plt.grid(True)
    plt.legend()

    # Plot one hour resolution data
    plt.subplot(212)
    plt.scatter(dataframe_hour.longitude, dataframe_hour.latitude, c="red")
    plt.title("One-hour resolution")
    plt.xlabel("Longitude, decimal degrees E")
    plt.ylabel("Latitude, decimal degrees N")
    plt.grid(True)
    plt.legend()

    plt.tight_layout()
    plt.show()


def plot_aggregated_data_same_axes(dataframe_min, dataframe_hour):
    """Plot aggregated data from two different dataframes on the same axes. """

    fig, ax = plt.subplots()

    dataframe_min.plot(x='latitude', y='longitude', ax=ax, legend=True, label='Minute')
    dataframe_hour.plot(x='latitude', y='longitude', ax=ax, legend=True, label='Hour')

    plt.show()


def check_aggregation_output(dataframe):
    """Check the aggregation output has roughly the expected number of lines. """

    print("Group by seconds, count number: ", dataframe.groupby(['time_seconds'])[['time_seconds']].count())

    subset_sec_df = dataframe.loc[(dataframe['time_seconds'] == 0), ['time_hours', 'time_minutes', 'time_seconds']]

    print("Number of rows with 0 seconds, group by minutes: ", subset_sec_df.groupby(['time_minutes'])[['time_minutes']].count())


def main():

    # input variables
    filepath = "/home/jen/projects/ace_data_management/wip/cruise_track_data"
    one_sec_resolution_filename = "track_data_prioritised_2016-12.csv"
    date_column_list = [0]
    
    datatypes = { 'latitude': 'float64',
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

    output_filename_minute = "TEST_ace_cruise_track_1min.csv"
    output_filename_hour = "TEST_ace_cruise_track_1hour.csv"
    columns = ['date_time', 'latitude', 'longitude', 'fix_quality', 'number_satellites', 'horiz_dilution_of_position',
               'altitude', 'altitude_units', 'geoid_height', 'geoid_height_units', 'device_id', 'speed', 'measureland_qualifier_flag_overall']

    # get the data from the csv files (1 second resolution)
    second_res_df = cruise_track_data_processing_utils.get_data_from_csv(filepath, one_sec_resolution_filename, datatypes, date_column_list)

    print("Number of rows in second resolution file: ", len(second_res_df))

    # check what is to be expected once aggregated
    investigate_aggregation_by_time(second_res_df)

    # do one minute resolution
    minute_res_df = get_minute_resolution(second_res_df)
    output_dataframe_to_csv(minute_res_df, columns, filepath, output_filename_minute)

    # do one hour resolution
    hour_res_df = get_hour_resolution(second_res_df)
    output_dataframe_to_csv(hour_res_df, columns, filepath, output_filename_hour)

    # do plots and checking of output aggregations
    plot_aggregated_data_separate_graphs(minute_res_df, hour_res_df)

    plot_aggregated_data_same_axes(minute_res_df, hour_res_df)

    check_aggregation_output(second_res_df)


if __name__ == "__main__":
    main()