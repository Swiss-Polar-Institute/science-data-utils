import cruise_track_data_processing_utils
import csv
import MySQLdb
import datetime
import math
import numpy as np
import os
import pandas


def process_trimble_gps_data():

    #"""Get data from Trimble GPS."""
    # Check if the data files exist or not.
    path = '/home/jen/projects/ace_data_management/wip/cruise_track_data/'
    filename = 'ace_trimble_gps'

    #file_list = cruise_track_data_processing_utils.get_data_from_files(path, filename)
    #print("File list:", file_list)

    #header = cruise_track_data_processing_utils.create_header_from_file(file_list)
    #print("Header: ", header)

    file_list = ['/home/jen/projects/ace_data_management/wip/cruise_track_data//ace_trimble_gps_2017-03-18.csv', '/home/jen/projects/ace_data_management/wip/cruise_track_data//ace_trimble_gps_2017-03-19.csv']
    header = ['id', 'date_time', 'latitude', 'longitude', 'fix_quality', 'number_satellites', 'horiz_dilution_of_position', 'altitude', 'altitude_units', 'geoid_height', 'geoid_height_units', 'device_id', 'measureland_qualifier_flags_id', 'date_time_day']

    # If they do exist, create a list of files and read these files into a dataframe.
    if len(file_list) > 0:
        trimble_df = cruise_track_data_processing_utils.get_data_from_file_list(file_list, header)
        print(trimble_df.head(5))
        print(trimble_df.dtypes)

    else:
        # If they do not exist, then get the data from the database and instead create the files.
        query_trimble = 'select * from ship_data_gpggagpsfix where device_id=63 order by date_time;'
        password = input()

        db_connection = MySQLdb.connect(host='localhost', user='ace', passwd=password, db='ace2016', port=3306);

        trimble_df = cruise_track_data_processing_utils.get_data_from_database(query_trimble, db_connection)
        trimble_df = cruise_track_data_processing_utils.string_to_datetime(trimble_df)

        path = '/home/jen/projects/ace_data_management/wip/cruise_track_data/'
        filename = 'ace_trimble_gps'

        cruise_track_data_processing_utils.output_daily_files(trimble_df, path, filename)

    print(trimble_df.sample(5))

    # Convert the data types of the columns from the generic object.
    trimble_df['date_time'] = trimble_df['date_time'].astype('datetime64[ns]')
    trimble_df['latitude'] = trimble_df['latitude'].astype('float64')
    trimble_df['longitude'] = trimble_df['longitude'].astype('float64')
    trimble_df['fix_quality'] = trimble_df['fix_quality'].astype('int')
    trimble_df['number_satellites'] = trimble_df['number_satellites'].astype('int')
    trimble_df['horiz_dilution_of_position'] = trimble_df['horiz_dilution_of_position'].astype('float64')
    trimble_df['altitude'] = trimble_df['altitude'].astype('float64')
    trimble_df['altitude_units'] = trimble_df['altitude_units'].astype('str')
    trimble_df['geoid_height'] = trimble_df['geoid_height'].astype('float64')
    trimble_df['geoid_height_units'] = trimble_df['geoid_height_units'].astype('str')
    trimble_df['device_id'] = trimble_df['device_id'].astype('int')
    trimble_df['measureland_qualifier_flags_id'] = trimble_df['measureland_qualifier_flags_id'].astype('int')
    pandas.to_datetime(trimble_df['date_time_day'], format="%Y-%m-%d")

    # Check the speed throughout the cruise track to ensure there are not periods where the ship is travelling impossibly fast. Flag data points.
    print("Analysing speed of track")
    cruise_track_data_processing_utils.analyse_speed(trimble_df)

    # Check the course of the ship throughout the track to ensure it is not making impossible turns or accelerating impossibly fast. Flag data points.
    print("Analysing course of track")
    cruise_track_data_processing_utils.analyse_course(trimble_df)

    print(trimble_df.dtypes)

    # Flag the points where the track has been manually and visually identified as incorrect.
    trimble_df['measurand_qualifier_flag_visual'] = ''

    print("Comparing visual position errors")
    trimble_df['measureland_qualifier_flag_visual'] = trimble_df.apply(cruise_track_data_processing_utils.calculate_manual_visual_position_errors, axis=1)

    # Calculate an overall quality flag, taking into account all of the factors tested above.
    trimble_df['measureland_qualifier_flag_overall'] = trimble_df.apply(cruise_track_data_processing_utils.calculate_measureland_qualifier_flag_overall, axis=1)

    print(trimble_df.head(10))

    # Output the data files where they have been flagged to show the intermediate steps and flagging.
    path = '/home/jen/projects/ace_data_management/wip/cruise_track_data/'
    filename = 'flagging_data_ace_trimble_gps'

    cruise_track_data_processing_utils.output_daily_files(trimble_df, path, filename)

    return trimble_df


def process_glonass_data():

    #"""Get data from GLONASS."""
    # Check if the data files exist or not.
    path = '/home/jen/projects/ace_data_management/wip/cruise_track_data/'
    filename = 'ace_glonass'

    #file_list = get_data_from_files(path, filename)
    #print("File list:", file_list)

    #header = create_header_from_file(file_list)
    #print("Header: ", header)

    file_list = ['/home/jen/projects/ace_data_management/wip/cruise_track_data//ace_glonass_2017-03-18.csv', '/home/jen/projects/ace_data_management/wip/cruise_track_data//ace_glonass_2017-03-19.csv']
    header = ['id', 'date_time', 'latitude', 'longitude', 'fix_quality', 'number_satellites', 'horiz_dilution_of_position', 'altitude', 'altitude_units', 'geoid_height', 'geoid_height_units', 'device_id', 'measureland_qualifier_flags_id', 'date_time_day']

    # If they do exist, create a list of files and read these files into a dataframe.
    if len(file_list) > 0:
        glonass_df = cruise_track_data_processing_utils.get_data_from_file_list(file_list, header)
        print(glonass_df.head(5))
        print(glonass_df.dtypes)

    else:
        # If they do not exist, then get the data from the database and instead create the files.
        query_trimble = 'select * from ship_data_gpggagpsfix where device_id=64 order by date_time;'
        password = input()

        db_connection = MySQLdb.connect(host='localhost', user='ace', passwd=password, db='ace2016', port=3306);

        glonass_df = cruise_track_data_processing_utils.get_data_from_database(query_trimble, db_connection)
        glonass_df = cruise_track_data_processing_utils.string_to_datetime(glonass_df)

        path = '/home/jen/projects/ace_data_management/wip/cruise_track_data/'
        filename = 'ace_glonass'

        cruise_track_data_processing_utils.output_daily_files(glonass_df, path, filename)

    print(glonass_df.sample(5))

    glonass_df.replace('', np.nan, inplace=True)

    # Convert the data types of the columns from the generic object.
    glonass_df['date_time'] = glonass_df['date_time'].astype('datetime64[ns]')
    glonass_df['latitude'] = glonass_df['latitude'].astype('float64')
    glonass_df['longitude'] = glonass_df['longitude'].astype('float64')
    glonass_df['fix_quality'] = glonass_df['fix_quality'].astype('int')
    glonass_df['number_satellites'] = glonass_df['number_satellites'].astype('int')
    glonass_df['horiz_dilution_of_position'] = glonass_df['horiz_dilution_of_position'].astype('float64')
    glonass_df['altitude'] = glonass_df['altitude'].astype('float64')
    glonass_df['altitude_units'] = glonass_df['altitude_units'].astype('str')
    glonass_df['geoid_height'] = glonass_df['geoid_height'].astype('float64')
    glonass_df['geoid_height_units'] = glonass_df['geoid_height_units'].astype('str')
    glonass_df['device_id'] = glonass_df['device_id'].astype('int')
    glonass_df['measureland_qualifier_flags_id'] = glonass_df['measureland_qualifier_flags_id'].astype('int')
    pandas.to_datetime(glonass_df['date_time_day'], format="%Y-%m-%d")

    # Check the speed throughout the cruise track to ensure there are not periods where the ship is travelling impossibly fast. Flag data points.
    print("Analysing speed of track")
    cruise_track_data_processing_utils.analyse_speed(glonass_df)

    # Check the course of the ship throughout the track to ensure it is not making impossible turns or accelerating impossibly fast. Flag data points.
    print("Analysing course of track")
    cruise_track_data_processing_utils.analyse_course(glonass_df)

    print(glonass_df.dtypes)

    # Flag the points where the track has been manually and visually identified as incorrect.
    glonass_df['measurand_qualifier_flag_visual'] = ''

    print("Comparing visual position errors")
    glonass_df['measureland_qualifier_flag_visual'] = glonass_df.apply(cruise_track_data_processing_utils.calculate_manual_visual_position_errors, axis=1)

    # Calculate an overall quality flag, taking into account all of the factors tested above.
    glonass_df['measureland_qualifier_flag_overall'] = glonass_df.apply(cruise_track_data_processing_utils.calculate_measureland_qualifier_flag_overall, axis=1)

    print("Datafram: ", glonass_df.head(10))

    # Output the data files where they have been flagged to show the intermediate steps and flagging.
    path = '/home/jen/projects/ace_data_management/wip/cruise_track_data/'
    filename = 'flagging_data_ace_glonass'

    cruise_track_data_processing_utils.output_daily_files(glonass_df, path, filename)

    return glonass_df


def main():
    # make hte above bits one function adn pass the different bits as arguments

    print("****PROCESSING TRIMBLE GPS DATA ****")
    trimble_df = process_trimble_gps_data()

    #cruise_track_data_processing_utils.alternate_method(trimble_df)

    #print("****PROCESSING GLONASS DATA ****")
    #glonass_df = process_glonass_data()

    #cruise_track_data_processing_utils.alternate_method(glonass_df)

    #combined_track_sorted_df = cruise_track_data_processing_utils.combine_position_dataframes(trimble_df, glonass_df)

    #combined_track_colsremoved_df = cruise_track_data_processing_utils.remove_intermediate_columns(combined_track_sorted_df)

    #flagged_speed = cruise_track_data_processing_utils.calculate_number_records_flagged_speed(combined_track_colsremoved_df)
    #print(flagged_speed)

    #cruise_track_data_processing_utils.alternate_method(combined_track_colsremoved_df)


if __name__ == "__main__":
    main()