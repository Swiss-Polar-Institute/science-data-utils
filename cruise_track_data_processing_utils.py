import csv
import MySQLdb
import datetime
import math
import numpy as np
import os
import pandas
import time

def get_data_from_files(path, filename):
    """Check if the data files exist. If they don't then get the data from the database, otherwise create a list of data files."""

    data_files = []

    if path:
        list_of_files = os.listdir(path)
        print("List of data files:", list_of_files)

        for file in list_of_files:
            if filename in file:
                full_filepath = path + "/" + file
                data_files.append(full_filepath)
            #print(data_files)

    else:
        data_files = []
    print(data_files)
    return data_files


def get_data_from_database(query, db_connection):
    """Get data from the MySQL database."""

    dataframe = pandas.read_sql(query, con=db_connection)

    return dataframe


def create_header_from_file(file_list):
    """Open the first file in the list and create the header from the first line of the first file."""
    with open(file_list[0], 'r') as csvfile:
        contents = csv.reader(csvfile)

        row_number = 0
        for row in contents:
            if row_number ==0:
                header = row
                print("File header: ", header)
            row_number += 1

    return header


def get_data_from_file_list(file_list, header):
    """Create a dataframe after extracting the data from the csv files."""

    rows_of_data = []

    file_list.sort()

    for file in file_list:
        print("Reading file:", file)
        with open(file, 'r') as csvfile:
            contents = csv.reader(csvfile)
            next(contents, None)

            row_number = 0
            for line in contents:
                if len(line) == len(header):
                    rows_of_data.append(line)
                else:
                    print("Line ", row_number, "has an incorrect number of variables. ", file, " ", line)
            row_number += 1

    print("Before pandas.DataFrame")
    t1 = time.time()
    df = pandas.DataFrame(rows_of_data, columns=header)
    print("After pandas.DataFrame, it took:", time.time() - t1, "seconds")
    # df.infer_objects().dtypes

    return df


def output_daily_files(dataframe, path, filename):
    """Create csv files from the data as it is grouped by day."""

    days = dataframe.groupby('date_time_day')

    for day in days.groups:
        output_path = path + filename + "_" + str(day) + '.csv'
        days.get_group(day).to_csv(output_path, index=False)



def get_location(datetime, position_df):
    """Create a tuple of the date_time, latitude and longitude of a location in a dataframe from a given date_time."""

    latitude = position_df[position_df.date_time == datetime].latitude.item()
    longitude = position_df[position_df.date_time == datetime].longitude.item()

    location = (datetime, latitude, longitude)

    return location


def string_to_datetime(dataframe):
    """Convert a date in a string into a python date, where the dataframe and the variable name are known."""

    print("Which variable would you like to convert from a date string to a python date?")
    existing_variable = input()
    print("What would you like to call the new date variable?")
    new_variable = input()

    dataframe[new_variable] = dataframe[existing_variable].dt.strftime('%Y-%m-%d')

    return dataframe


def string_object_to_datetime(dataframe):
    """Convert a date in a string into a python date."""

    print("Which variable would you like to convert from a date string to a python date?")
    existing_variable = input()
    print("What would you like to call the new date variable?")
    new_variable = input()

    dataframe[new_variable] = datetime.strptime(dataframe[existing_variable], '%Y-%m-%d')

    return dataframe


def calculate_distance(origin, destination):
    """Calculate the haversine or great-circle distance in metres between two points with latitudes and longitudes, where they are known as the origin and destination."""

    datetime1, lat1, lon1 = origin
    datetime2, lat2, lon2 = destination
    radius = 6371  # km

    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) * math.sin(dlat / 2) + math.cos(math.radians(lat1)) \
        * math.cos(math.radians(lat2)) * math.sin(dlon / 2) * math.sin(dlon / 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    d = radius * c  # Distance in km
    d_m = d * 1000  # Distance in metres

    return d_m


def knots_two_points(origin, destination):
    """Calculate the speed in knots between two locations which are dictionaries containing latitude, longitude and date_time."""

    distance = calculate_distance(origin, destination)

    datetime1_timestamp, lat1, lon1 = origin
    datetime2_timestamp, lat2, lon2 = destination
    # datetime1 = datetime.datetime.strptime(datetime_str1,"%Y-%m-%d %H:%M:%S.%f")
    # datetime2 = datetime.datetime.strptime(datetime_str2,"%Y-%m-%d %H:%M:%S.%f")

    datetime1 = datetime1_timestamp.timestamp()
    datetime2 = datetime2_timestamp.timestamp()

    seconds = abs((datetime1) - (datetime2))
    # seconds = abs((datetime_str1)-(datetime_str2)).total_seconds()
    conversion = 3600 / 1852  # convert 1 ms-1 to knots (nautical miles per hour; 1 nm = 1852 metres)
    speed_knots = (distance / seconds) * conversion

    if seconds > 0:
        return speed_knots
    else:
        return "N/A"


def set_utc(date_time):
    """Set the timezone to be UTC."""
    utc = datetime.timezone(datetime.timedelta(0))
    date_time = date_time.replace(tzinfo=utc)
    return date_time


def analyse_speed(position_df):
    """Analyse the cruise track to ensure each point lies within a reasonable distance and direction from the previous point."""

    total_data_points = len(position_df)

    earliest_date_time = position_df['date_time'].min()
    latest_date_time = position_df['date_time'].max()

    current_date = earliest_date_time

    previous_position = get_location(earliest_date_time, position_df)
    datetime_previous, latitude_previous, longitude_previous = previous_position

    count_speed_errors = 0

    line_number = -1
    for position in position_df.itertuples():
        line_number += 1
        if line_number == 0:
            continue

        current_position = position[2:5]
        row_index = position[0]

        # print(current_position)
        speed_knots = knots_two_points(previous_position, current_position)

        error_message = ""

        if speed_knots == "N/A":
            error_message = "No speed?"
            position_df.at[row_index, 'measureland_qualifier_flag_speed'] = 10
            #print(position_df['id' == row_index])
        elif speed_knots >= 20:
            error_message += "** Too fast **"
            position_df.at[row_index, 'measureland_qualifier_flag_speed'] = 5
            count_speed_errors += 1
        elif speed_knots < 20:
            position_df.at[row_index, 'measureland_qualifier_flag_speed'] = 2

        if error_message != "":
            print("Error {} Start {} End {}  to position ({:.4f}, {:.4f})   speed: {} knots".format(error_message, previous_position[0], current_position[0],
                                                                            current_position[1], current_position[2],
                                                                            speed_knots))

        previous_position = current_position

    return count_speed_errors


def calculate_bearing(origin, destination):
    """Calculate the direction turned between two points."""

    datetime1, lat1, lon1 = origin
    datetime2, lat2, lon2 = destination

    dlon = math.radians(lon2 - lon1)

    bearing = math.atan2(math.sin(dlon) * math.cos(math.radians(lat2)),
                         math.cos(math.radians(lat1)) * math.sin(math.radians(lat2))
                         - math.sin(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.cos(dlon))

    bearing_degrees = math.degrees(bearing)

    return bearing_degrees


def calculate_bearing_difference(current_bearing, previous_bearing):
    """Calculate the difference between two bearings, based on bearings between 0 and 360."""

    difference = current_bearing - previous_bearing

    while difference < -180:
        difference += 360
    while difference > 180:
        difference -= 360

    return difference


def analyse_course(position_df):
    """Analyse the change in the course between two points regarding the bearing and acceleration - these features need information from previous points."""

    total_data_points = len(position_df)

    earliest_date_time = position_df['date_time'].min()
    current_date = earliest_date_time

    previous_position = get_location(earliest_date_time, position_df)
    datetime_previous, latitude_previous, longitude_previous = previous_position

    previous_bearing = 0
    previous_speed_knots = 0

    count_bearing_errors = 0
    count_acceleration_errors = 0

    line_number = -1
    for position in position_df.itertuples():
        line_number += 1
        if line_number == 0:
            continue

        current_position = position[2:5]
        row_index = position[0]

        # Calculate bearing and change in bearing
        current_bearing = calculate_bearing(previous_position, current_position)
        difference_in_bearing = calculate_bearing_difference(current_bearing, previous_bearing)

        # Calculate acceleration between two points
        current_speed_knots = knots_two_points(previous_position, current_position)

        time_difference = (current_position[0] - previous_position[0]).total_seconds()
        speed_difference_metres_per_sec = (current_speed_knots - previous_speed_knots) * (
                    1852 / 3600)  # convert knots to ms-1

        if time_difference > 0:
            acceleration = speed_difference_metres_per_sec / time_difference
        else:
            acceleration = 0

        # Print errors where data do not meet requirements
        error_message_bearing = ""
        error_message_acceleration = ""

        if difference_in_bearing == "N/A":
            error_message_bearing = "No bearing?"
            position_df.at[row_index, 'measureland_qualifier_flag_course'] = 10
        elif difference_in_bearing >= 5:
            error_message_bearing = "** Turn too tight **"
            position_df.at[row_index, 'measureland_qualifier_flag_course'] = 5
            count_bearing_errors += 1
        elif difference_in_bearing < 5:
            position_df.at[row_index, 'measureland_qualifier_flag_course'] = 2

        if error_message_bearing != "":
            print("Error:  {} Start {} End {} to position ({:.4f}, {:.4f}) bearing change: {} degrees".format(error_message_bearing, previous_position[0],
                                                                                     current_position[0],
                                                                                     current_position[1],
                                                                                     current_position[2],
                                                                                     difference_in_bearing))

        if acceleration == "N/A":
            error_message_acceleration = "No acceleration"
            position_df.at[row_index, 'measureland_qualifier_flag_acceleration'] = 10
        elif acceleration > 1:
            count_acceleration_errors += 1
            error_message_acceleration = "** Acceleration to quick **"
            position_df.at[row_index, 'measureland_qualifier_flag_acceleration'] = 5
        elif acceleration <= 1:
            position_df.at[row_index, 'measureland_qualifier_flag_acceleration'] = 2

        if error_message_acceleration != "":
            print("Error:  {} {} ({:.4f}, {:.4f}) acceleration: {} ms-2".format(error_message_acceleration,
                                                                                current_position[0],
                                                                                current_position[1],
                                                                                current_position[2], acceleration))

        previous_position = current_position
        previous_bearing = current_bearing
        previous_speed_knots = current_speed_knots

    return (count_bearing_errors, count_acceleration_errors)


def get_list_visual_position_errors(error_filename):
    """Get a file containing start and end times of errors and create a dataframe of these."""

    with open(error_filename, 'r') as file:
        contents = csv.reader(file)
        next(contents)

        visual_position_errors = []
        for row in contents:
            visual_position_errors.append(row)
    #print("List of visual error times: ", visual_position_errors)
    return visual_position_errors


def calculate_manual_visual_position_errors(row):
    """Get the list of periods when the track is visually incorrect (manual checking) and flag all of the GPS datapoints that lie in these periods as being bad data."""

    invalid_times = get_list_visual_position_errors('/home/jen/projects/ace_data_management/wip/cruise_track_data/ace_trimble_manual_position_errors.csv')

    for invalid_time in invalid_times:
        #print("Invalid time: ", invalid_time)
        #print("Invalid start time: ", invalid_time[0])
        time_beginning = datetime.datetime.strptime(invalid_time[0], "%Y-%m-%d %H:%M:%S")
        time_ending = datetime.datetime.strptime(invalid_time[1], "%Y-%m-%d %H:%M:%S")

        if row['date_time'] >= time_beginning and row['date_time'] <= time_ending:
            return 5

    return 2


def calculate_measureland_qualifier_flag_overall(row):
    """Calculate the overall data quality flag taking into account the others that have been assigned."""

    if row['measureland_qualifier_flag_speed'] == 5 or row['measureland_qualifier_flag_course'] ==5 or row['measureland_qualifier_flag_acceleration'] ==5 or row['measureland_qualifier_flag_visual'] == 5:
        return 5
    elif row['measureland_qualifier_flag_speed'] == 1 and row['measureland_qualifier_flag_course'] == 1 and row['measureland_qualifier_flag_acceleration'] == 1 and row['measureland_qualifier_flag_visual'] == 1:
        return 1
    else:
        return 2



    # PLOT flags
    # PLOT stats of how many data points have been flagged


def combine_position_dataframes(dataframe1, dataframe2):

    print("Dimensions of dataframe1: ", dataframe1.shape)
    print("Dimensions of dataframe2: ", dataframe2.shape)

    frames = [dataframe1, dataframe2]

    combined_dataframe = pandas.concat(frames)

    print("Dimensions of combined dataframe: ", combined_dataframe.shape)
    #combined_dataframe_sorted = combined_dataframe.sort('date_time')

    print("Sample of combined dataframe: ", combined_dataframe.sample(10))

    return combined_dataframe


def remove_intermediate_columns(dataframe):

    combined_dataframe_dropped_cols = dataframe.drop(columns = ['measureland_qualifier_flag_speed', 'measureland_qualifier_flag_course', 'measureland_qualifier_flag_acceleration'])

    print("Dimensions of combined dataframe after dropping columns:", combined_dataframe_dropped_cols.shape)
    print("Combined dataframe after dropping columns: ", combined_dataframe_dropped_cols)

    return combined_dataframe_dropped_cols


def calculate_number_records_flagged_speed(dataframe):

    instrument_speed = pandas.crosstab(index = dataframe['measureland_qualifier_flag_speed'], columns = dataframe['device_id'], margins=True)

    instrument_speed.columns = ['trimble', 'glonass']
    instrument_speed.index = ['2', '5', '10']

    instrument_speed


def alternate_method(dataframe):

    pandas.pivot_table(dataframe, 'device_id', 'measureland_qualifier_flag_speed')
    course_table = pandas.pivot_table(dataframe, 'device_id', 'measureland_qualifier_flag_course')

    print(course_table)














