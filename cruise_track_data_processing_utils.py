import csv
import MySQLdb
import datetime
import math
import numpy as np
import os
import pandas
import time
import csvkit

def create_concatenated_csvfile(filepath, filename):


    concatenated_filename = filepath + "/" + filename + "_concatenated.csv"

    # Check if the concatenated files exist.
    if not os.path.isfile(concatenated_filename):
        execution = "csvstack " + filepath + "/" + filename + "*.csv " + " > " + concatenated_filename

        print("Will execute:", execution)
        os.system(execution)

        print("Creating concatenated csv file:", concatenated_filename)

    return concatenated_filename


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
    #print(data_files)
    return data_files


def get_data_from_database(query, db_connection):
    """Get data from the MySQL database."""

    dataframe = pandas.read_sql(query, con=db_connection)
    print("Data from database: ", dataframe.head(5))
    print("Size of dataframe from database: ", dataframe.shape)

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


def get_data_from_csv(filepath, filename, datatypes):
    """Get data from a csv file. In the processing this is used for getting data from the concatenated csv file but can be used for any. Write it into a pandas dataframe."""

    concatenated_file = filepath + "/" + filename

    dataframe = pandas.read_csv(concatenated_file, dtype=datatypes, date_parser=pandas.to_datetime, parse_dates=[1, 19])

    return dataframe


def get_concatenated_csv_data(concatenated_filepath, concatenated_filename, device_id, output_create_files_filepath, output_create_files_filename):
    """Create one csv file of all of the data to import."""

    # Create the full file name of the concatenated filename.
    concatenated_file = concatenated_filepath + "/" + concatenated_filename + "_concatenated.csv"
    print("Looking for concatenated file name: ", concatenated_file)

    # Test if the concatenated file exists and if it does, return it.
    if os.path.isfile(concatenated_file):
        print("Concatenated file exists: ", concatenated_file)
        return concatenated_file

    # If it does not exist, test if the individual files exist.
    elif not os.path.isfile(concatenated_file):
        print("Concatenated file does not exist. Create file: ", concatenated_file)
        file_list = get_data_from_files(concatenated_filepath, concatenated_filename)
        # print("File list:", file_list)

        # If the individual files exist, create the concatenated file.
        if len(file_list) > 0:
            print("Individual csv files exist. Creating the concatenated file.")
            concatenated_file = create_concatenated_csvfile(concatenated_filepath, concatenated_filename)
            return concatenated_file

        # If the individual files do not exist, get the data from the database, create the files then concatenate them.
        else:
            database_query = "select * from ship_data_gpggagpsfix where device_id=" + int(
                device_id) + " order by date_time;"
            # print(database_query)
            password = input()

            db_connection = MySQLdb.connect(host='localhost', user='ace', passwd=password, db='ace2016', port=3306);

            track_df = get_data_from_database(database_query, db_connection)
            track_df = string_to_datetime(track_df)

            # Output the data into daily files (as they do not already exist).
            output_daily_files(track_df, output_create_files_filepath, output_create_files_filename)

            concatenated_file = create_concatenated_csvfile(concatenated_filepath, concatenated_filename)
            return concatenated_file


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
    print("This set of files contains ", len(rows_of_data), "rows of data.")

    print("Before pandas.DataFrame")
    t1 = time.time()
    df = pandas.DataFrame(rows_of_data, columns=header)
    print("After pandas.DataFrame, it took:", time.time() - t1, "seconds")
    # df.infer_objects().dtypes

    print("Size of dataframe from list of files: ", df.shape)

    return df


def output_daily_files(dataframe, path, filename):
    """Create csv files from the data as it is grouped by day."""

    days = dataframe.groupby('date_time_day')
    dataframe.groupby('date_time_day').size().reset_index(name='data points per day')

    for day in days.groups:
        print(day.date())
        output_path = path + filename + "_" + str(day.date()) + '.csv'
        print("Creating intermediate flagged data file: ", output_path)
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

    distance_m = calculate_distance(origin, destination)

    datetime1_timestamp, lat1, lon1 = origin
    datetime2_timestamp, lat2, lon2 = destination
    # datetime1 = datetime.datetime.strptime(datetime_str1,"%Y-%m-%d %H:%M:%S.%f")
    # datetime2 = datetime.datetime.strptime(datetime_str2,"%Y-%m-%d %H:%M:%S.%f")

    datetime1 = datetime1_timestamp.timestamp()
    datetime2 = datetime2_timestamp.timestamp()

    seconds = abs((datetime1) - (datetime2))
    # seconds = abs((datetime_str1)-(datetime_str2)).total_seconds()
    conversion = 3600 / 1852  # convert 1 ms-1 to knots (nautical miles per hour; 1 nm = 1852 metres)
    speed_knots = (distance_m / seconds) * conversion

    if seconds > 0:
        return (distance_m, speed_knots)
    else:
        return "N/A"


def set_utc(date_time):
    """Set the timezone to be UTC."""
    utc = datetime.timezone(datetime.timedelta(0))
    date_time = date_time.replace(tzinfo=utc)
    return date_time


def calculate_speed(position_df):
    """Calculate the speed between consectutive points and add this as a variable to the dataframe."""

    print("Calculating speed of track")
    #total_data_points = len(position_df)

    earliest_date_time = position_df['date_time'].min()
    #latest_date_time = position_df['date_time'].max()

    current_date = earliest_date_time

    previous_position = get_location(earliest_date_time, position_df)
    print("Previous position: ", previous_position)
    #datetime_previous, latitude_previous, longitude_previous = previous_position

    # count_speed_errors = 0

    line_number = -1
    for position in position_df.itertuples():
        line_number += 1
        row_index = position[0]

        if line_number == 0:
            position_df.at[row_index, 'measureland_qualifier_flag_speed'] = 1
            continue

        current_position = position[2:5]

        # print(current_position)
        (position_difference_m, speed_knots) = knots_two_points(previous_position, current_position)
        position_df.at[row_index, 'speed'] = speed_knots

        #print("calculated speeds")

        position_df.at[row_index, 'distance'] = position_difference_m

        #print("Checked position difference")

        previous_position = current_position

    print(position_df.isnull())

    return position_df


def analyse_speed(position_df):
    """Analyse the speed that has been calculated and flag the data points accordingly."""

    print("Analysing speed of track")
    upper_bound = get_stats(position_df, 'speed')
    print("Upper bound:", upper_bound)

    # no speed value
    position_df.loc[position_df['speed'].apply(math.isnan), 'measureland_qualifier_flag_speed'] = 10
    print("Rows where the speed is null: ", position_df.loc[position_df['speed'].apply(math.isnan)])
    # speed greater than upper bound
    position_df.loc[position_df['speed'] > upper_bound, 'measureland_qualifier_flag_speed'] = 5

    # speed within allowed limits (0 <= speed <= upper bound)
    position_df.loc[position_df['speed'] <= upper_bound, 'measureland_qualifier_flag_speed'] = 2

    print(position_df['measureland_qualifier_flag_speed'].isnull())

    position_df['measureland_qualifier_flag_speed'] = position_df['measureland_qualifier_flag_speed'].astype(int)

    return position_df


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

    print("Analysing course of track")
    total_data_points = len(position_df)

    earliest_date_time = position_df['date_time'].min()
    current_date = earliest_date_time

    previous_position = get_location(earliest_date_time, position_df)
    datetime_previous, latitude_previous, longitude_previous = previous_position

    previous_bearing = 0
    previous_speed_knots = 0

    count_bearing_errors = 0
    count_acceleration_errors = 0
    count_ship_stationary_bearing_error = 0 # ship at speed <= 0.3 and bearing tight.

    line_number = -1
    for position in position_df.itertuples():
        line_number += 1
        row_index = position[0]

        if line_number == 0:
            position_df.at[row_index, 'measureland_qualifier_flag_course'] = 1
            position_df.at[row_index, 'measureland_qualifier_flag_acceleration'] = 1
            continue

        current_position = position[2:5]

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
        elif difference_in_bearing >= 5 and current_speed_knots > 0.3:
            error_message_bearing = "** Turn too tight **"
            position_df.at[row_index, 'measureland_qualifier_flag_course'] = 5
            count_bearing_errors += 1
        elif difference_in_bearing >= 5 and current_speed_knots <= 0.3:
            error_message_bearing = "** Turn tight but ship stationary **"
            position_df.at[row_index, 'measureland_qualifier_flag_course'] = 3
            count_ship_stationary_bearing_error += 1
        elif difference_in_bearing < 5:
            position_df.at[row_index, 'measureland_qualifier_flag_course'] = 2

        if error_message_bearing != "":
            print("Previous: ", previous_position)
            print("Current: ", current_position)
            print("Current speed: ", current_speed_knots)
            print("Error:  {} Start {}  ({:.4f}, {:.4f})   End {} to position ({:.4f}, {:.4f}) bearing change: {} degrees".format(error_message_bearing,
                                                                                     previous_position[0],
                                                                                     previous_position[1],
                                                                                     previous_position[2],
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

    position_df['measureland_qualifier_flag_course'] = position_df['measureland_qualifier_flag_course'].astype(int)
    position_df['measureland_qualifier_flag_acceleration'] = position_df['measureland_qualifier_flag_acceleration'].astype(int)

    return (count_bearing_errors, count_acceleration_errors, count_ship_stationary_bearing_error)


def get_list_visual_position_errors(invalid_position_filename):
    """Get a file containing start and end times of errors and create a dataframe of these."""

    if not hasattr(get_list_visual_position_errors, "cached"):
        get_list_visual_position_errors.cached = {}

    if invalid_position_filename not in get_list_visual_position_errors.cached:
        with open(invalid_position_filename, 'r') as file:
            contents = csv.reader(file)
            next(contents)

            visual_position_errors = []
            for row in contents:
                time_beginning = datetime.datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S")
                time_ending = datetime.datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S")

                visual_position_errors.append((time_beginning, time_ending, row[2]))
        #print("List of visual error times: ", visual_position_errors)

        get_list_visual_position_errors.cached[invalid_position_filename] = visual_position_errors

    return get_list_visual_position_errors.cached[invalid_position_filename]


# def calculate_manual_visual_position_errors(row):
#     """Get the list of periods when the track is visually incorrect (manual checking) and flag all of the GPS datapoints that lie in these periods as being bad data."""
#
#     invalid_times = get_list_visual_position_errors('/home/jen/projects/ace_data_management/wip/cruise_track_data/ace_trimble_manual_position_errors.csv')
#
#     for invalid_time in invalid_times:
#         #print("Invalid time: ", invalid_time)
#         #print("Invalid start time: ", invalid_time[0])
#         time_beginning = invalid_time[0]
#         time_ending = invalid_time[1]
#
#         if row['date_time'] >= time_beginning and row['date_time'] <= time_ending:
#             return 5
#
#     return 2


def update_visual_position_flag(dataframe, invalid_position_filepath):
    """Flag a data point as being bad data if it lies within the periods defined as being so, visually."""

    print("Comparing visual position errors")

    # Assume the data point is good unless it has been flagged visually.
    if invalid_position_filepath == '':
        dataframe['measureland_qualifier_flag_visual'] = '2'
    else:
        invalid_times = get_list_visual_position_errors(invalid_position_filepath)

        # Assume the data point is good unless it has been flagged visually.
        dataframe['measureland_qualifier_flag_visual'] = '2'

        # Where the data point is recognised as being bad visually, flag it as bad data.
        for invalid_time in invalid_times:
            mask = (dataframe['date_time'] >= invalid_time[0]) & (dataframe['date_time'] <= invalid_time[1])
            dataframe.loc[mask, 'measureland_qualifier_flag_visual'] = 5

    return dataframe


def calculate_measureland_qualifier_flag_overall(row):
    """Calculate the overall data quality flag taking into account the others that have been assigned."""

    #print("Computing overall measureland qualifier flags")

    if row['measureland_qualifier_flag_speed'] == 5 or row['measureland_qualifier_flag_course'] ==5 or row['measureland_qualifier_flag_acceleration'] ==5 or row['measureland_qualifier_flag_visual'] == 5:
        return 5
    elif row['measureland_qualifier_flag_speed'] == 1 and row['measureland_qualifier_flag_course'] == 1 and row['measureland_qualifier_flag_acceleration'] == 1 and row['measureland_qualifier_flag_visual'] == 1:
        return 1
    elif (row['measureland_qualifier_flag_speed'] == 3 or row['measureland_qualifier_flag_course'] == 3 or row['measureland_qualifier_flag_acceleration'] == 3) and (row['measureland_qualifier_flag_speed'] != 5 or row['measureland_qualifier_flag_course'] != 5 or row['measureland_qualifier_flag_acceleration'] != 5):
        return 3
    else:
        return 2


def combine_position_dataframes(dataframe1, dataframe2):
    """Bring together the dataframes from different instrument sources to combine the tracks."""

    # check that the dataframes have the same number of columns
    print("Dimensions of dataframe1: ", dataframe1.shape)
    print("Dimensions of dataframe2: ", dataframe2.shape)

    frames = [dataframe1, dataframe2]

    combined_dataframe = pandas.concat(frames)

    dataframe1.drop(dataframe1.index, inplace=True) # Delete data from dataframe to save memory
    dataframe2.drop(dataframe2.index, inplace=True) # Delete data from dataframe to save memory

    # confirm that the dataframes no longer exist (saving memory)
    print("Dimensions of dataframe1: ", dataframe1.shape)
    print("Dimensions of dataframe2: ", dataframe2.shape)

    # check that all rows of both dataframes have been combined into the new dataframe. Sort by date and time.
    print("Dimensions of combined dataframe: ", combined_dataframe.shape)
    combined_dataframe_sorted = combined_dataframe.sort_values('date_time')

    print("Sample of combined dataframe: ", combined_dataframe_sorted.sample(10))

    return combined_dataframe_sorted


def remove_intermediate_columns(dataframe):
    """Remove the intermediate step qualifier flag columns that are not required in the final output data set."""

    combined_dataframe_dropped_cols = dataframe.drop(columns = ['measureland_qualifier_flag_speed', 'measureland_qualifier_flag_course', 'measureland_qualifier_flag_acceleration', 'measureland_qualifier_flag_visual'])

    print("Dimensions of combined dataframe after dropping columns:", combined_dataframe_dropped_cols.shape)
    print("Combined dataframe after dropping columns: ", combined_dataframe_dropped_cols.sample(10))

    return combined_dataframe_dropped_cols


def choose_rows(rows):
    """Choose rows from the dataframe according to values in one of the columns."""

    # Ensure that the object is not empty.
    assert(len(rows) > 0)

    # The following rows preferentially select data where the device_id=64 (i.e the GLONASS over the Trimble). Also select by data quality. If the data quality is not good, then do not select, even if there is no other point for that time.
    if len(rows) >= 1 and rows[0]['device_id'] == 64 and rows[0]['measureland_qualifier_flag_overall'] == 2:
        return rows[0]

    elif len(rows) >=2 and rows[1]['device_id'] == 64 and rows[1]['measureland_qualifier_flag_overall'] == 2:
        return rows[1]

    elif len(rows) >= 3 and rows[2]['device_id'] == 64 and rows[2]['measureland_qualifier_flag_overall'] == 2:
        return rows[2]

    elif len(rows) >= 1 and rows[0]['device_id'] == 63 and rows[0]['measureland_qualifier_flag_overall'] == 2:
        return rows[0]

    elif len(rows) >=2 and rows[1]['device_id'] == 63 and rows[1]['measureland_qualifier_flag_overall'] == 2:
        return rows[1]

    elif len(rows) >= 3 and rows[2]['device_id'] == 63 and rows[2]['measureland_qualifier_flag_overall'] == 2:
        return rows[2]

    elif len(rows) == 1 and rows[0]['measureland_qualifier_flag_overall'] == 3: # for the first row which has a value of 3, because QC was not able to tell otherwise
        return rows[0]

    return None


def convert_float_nan_to_string_NaN(row):

    i = 0

    result = row[:]

    while i < len(result):

        if type(result[i]) == float and math.isnan(result[i]):

            result[i] = "NaN"

        i = i+1

    return result


def prioritise_data_points(dataframe, output_filepath, output_filename):
    """Create a new dataframe from the prioritised points according to the conditions required. Rows are chosen from small groups which occur at the same time (to seconds)."""

    # Beginning to prioritise data points. Firstly ensure that the data are sorted by date and time.
    dataframe = dataframe.sort_values(['date_time'])

    last_processed_datetime_secs = None

    rows_pending_decision = []

    progress_count = 0

    # open output file
    output_file = output_filepath + output_filename
    print("Output selected rows to ", output_file)
    f = open(output_file, "a+")
    if f:
        print("Output file opened")

    writer = csv.writer(f, delimiter=',')
    writer.writerow(['date_time','latitude','longitude','fix_quality','number_satellites','horiz_dilution_of_position','altitude','altitude_units','geoid_height','geoid_height_units','device_id','speed','measureland_qualifier_flag_overall'])

    selected_count = 0
    non_selected_count = 0

    for row_id, row in dataframe.iterrows():
        row_datetime_secs = row['date_time'].strftime('%Y-%m-%d %H:%M:%S')

        progress_count += 1

        # do batches of 1000 rows at a time to avoid overloading memory
        if progress_count == 1000:
            print("Prioritising data points. Processing:", row_datetime_secs)
            progress_count = 0

        if row_datetime_secs != last_processed_datetime_secs and last_processed_datetime_secs is not None:
            #print("Type:", rows_pending_decision())
            count_rows = 0
            length_selection = len(rows_pending_decision)
            print("-------------------Rows to choose from--------------: (Total = ", length_selection, ")")
            selected_row = choose_rows(rows_pending_decision)
            for item in rows_pending_decision:
                #print("Counting rows: ", count_rows)
                print("Row number: ", count_rows, " Here is the row\n", rows_pending_decision[count_rows][[2,3,4, 12,15, 16]])
                print("Row length: ", len(rows_pending_decision[count_rows]))
                count_rows += 1

            if selected_row is not None:
                # write the selected row out to the file rather than appending it to the dataframe
                print(type(selected_row))

                convert_float_nan_to_string_NaN(selected_row)
                writer.writerow(selected_row[2:16])
                print("-----selected row: \n", selected_row)
                print("-----Selected row: \n", selected_row[[2, 3, 4, 12, 15, 16]])
                selected_count += 1
            else:
                print("Rows of poor quality data, nothing selected")
                non_selected_count += 1

            rows_pending_decision = []

        rows_pending_decision.append(row)

        last_processed_datetime_secs = row_datetime_secs
    f.close()
    if not f:
        print("Output file closed")

    print("Number of rows selected: ", selected_count)
    print("Number of rows where no selection is made: ", non_selected_count)

####STATS#####

def calculate_number_records_flagged_speed(dataframe):

    instrument_speed = pandas.crosstab(index = dataframe['measureland_qualifier_flag_speed'], columns = dataframe['device_id'], margins=True)

    instrument_speed.columns = ['trimble', 'glonass']
    instrument_speed.index = ['2', '5', '10']

    return instrument_speed


def create_pivottable_on_flag(dataframe_name, dataframe, flag_name):

    #pivottable = pandas.pivot_table(dataframe, 'device_id', flag_name)
    pivottable = pandas.pivot_table(dataframe, index= [flag_name], aggfunc = 'count')

    print("Pivot table of qualifier flags from ", dataframe_name, " : ", pivottable)

    return pivottable


def get_stats(dataframe, variable):
    """Get some standard statistics about a variable within a dataframe."""

    print("Maximum value of ", variable, "is: ", dataframe[variable].max(), " in the row ", dataframe[dataframe[variable] == dataframe[variable].max()])
    print("Minimum value of ", variable, "is: ", dataframe[variable].min())
    print("Mean of ", variable, " is: ", dataframe[variable].mean())
    print("Standard deviation of ", variable, " is: ", dataframe[variable].std())
    print("Mode of ", variable, " is: ", dataframe[variable].mode())
    print("Median of ", variable, " is: ", dataframe[variable].median())

    # disregard points that are lower than 2.5 (to avoid stationary periods) as part of the interquartile range and greater than 100, which is only a few points anyway.
    dataframeselection = dataframe.loc[(dataframe[variable] >= 2.5) & (dataframe[variable] < 100)]
    q1 = dataframeselection[variable].quantile(0.25)
    q3 = dataframeselection[variable].quantile(0.75)
    iqr = q3 - q1
    print("Upper quartile of ", variable, " is: ", q3)
    print("Lower quartile of ", variable, " is: ", q1)
    print("Interquartile range of ", variable, " is: ", iqr)
    lower_limit = q1-1.5*iqr
    upper_limit = q3+1.5*iqr
    print("Lower limit for outliers from IQR for ", variable, " is: ", lower_limit)
    print("Upper limit for outliers from IQR for ", variable, " is: ", upper_limit)
    points_above_upper_limit = len(dataframe.loc[(dataframe[variable] > (q3+1.5*iqr)) & (dataframe[variable] < 100)])
    number_of_points = len(dataframe)
    print("Total number of data points", number_of_points)
    print("There are ", points_above_upper_limit, " points that lie above the upper bound, which corresponds to ", (points_above_upper_limit/number_of_points)*100, " %")

    return upper_limit

def get_minmax_stats(dataframe, variable):
    """Get some standard statistics about a variable within a dataframe."""

    print("Maximum value of ", variable, "is: ", dataframe[variable].max())
    print("Minimum value of ", variable, "is: ", dataframe[variable].min())


def get_device_summary(dataframe):
    """Get number of points from each device in prioritised dataframe"""

    print("Total number of points: ", len(dataframe))

    print("The number of rows from each device are as follows: ", dataframe.groupby(['device_id']).size())













