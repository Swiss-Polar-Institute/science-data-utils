import matplotlib.pyplot as plt
import matplotlib.cm as cm
import cruise_track_data_processing_utils
import cruise_track_data_plotting
import os
import pandas
import glob
import datetime

def process_track_data(dataframe_name, concatenated_filepath, concatenated_filename, input_filepath, input_filename, device_id, output_create_files_filepath, output_create_files_filename,
                       invalid_position_filepath, output_flagging_filepath, output_flagging_filename):
    """Process track data from some input. Output data as a pandas dataframe and perform some quality assurance and quality checking of the data points."""

    concatenated_file = cruise_track_data_processing_utils.get_concatenated_csv_data(concatenated_filepath, concatenated_filename, device_id, output_create_files_filepath, output_create_files_filename)

    # Read the concatenated csv file into a pandas dataframe.
    datatypes = {'id': 'int32',
              'latitude': 'float64',
              'longitude': 'float64',
              'fix_quality': 'int8',
              'number_satellites': 'int8',
              'horiz_dilution_of_position': 'float16',
              'altitude': 'float16',
              'altitude_units': 'category',
              'geoid_height': 'float16',
              'geoid_height_units': 'category',
              'device_id': 'int8',
              'measureland_qualifier_flags_id': 'int8'}

    track_df = pandas.read_csv(concatenated_file, dtype=datatypes, date_parser=pandas.to_datetime, parse_dates=[1, 13])

    print(track_df.sample(5))
    start_dataframe_length = len(track_df)

    # Check the speed throughout the cruise track to ensure there are not periods where the ship is travelling impossibly fast. Flag data points.
    track_df = cruise_track_data_processing_utils.calculate_speed(track_df)
    print(track_df.info())
    print(track_df['speed'].head(10))
    print(track_df['distance'].head(10))
    track_df = cruise_track_data_processing_utils.analyse_speed(track_df)
    track_df = cruise_track_data_processing_utils.analyse_distance_between_points(track_df)

    #print(track_df['measureland_qualifier_flag_speed'].head(10))
    #print(track_df['measureland_qualifier_flag_distance'].head(10))
    #print(track_df['distance'].head(100))

    # Check the course of the ship throughout the track to ensure it is not making impossible turns or accelerating impossibly fast. Flag data points.
    (count_bearing_errors, count_acceleration_errors, count_ship_stationary_bearing_error) = cruise_track_data_processing_utils.analyse_course(track_df)
    print("Number of bearing errors when the ship is moving: ", count_bearing_errors)
    print("Number of acceleration errors: ", count_acceleration_errors)
    print("Number of bearing errors when the ship is stationary: ", count_ship_stationary_bearing_error)

    # Flag the points where the track has been manually and visually identified as incorrect.
    track_df = cruise_track_data_processing_utils.update_visual_position_flag(track_df, invalid_position_filepath)

    print(track_df.dtypes)

    # Check values of all qualifier flags to make sure they have been assigned correctly:
    print("looking at qualifier flag values for all MQFs:")
    print("Speed:", track_df['measureland_qualifier_flag_speed'].value_counts())
    print("Distance:", track_df['measureland_qualifier_flag_distance'].value_counts())
    print("Course:", track_df['measureland_qualifier_flag_course'].value_counts())
    print("Acceleration:", track_df['measureland_qualifier_flag_acceleration'].value_counts())
    print("Visual:", track_df['measureland_qualifier_flag_visual'].value_counts())

    # Calculate an overall quality flag, taking into account all of the factors tested above.
    print("Calculating overall measureand qualifier flag from individual ones.")
    track_df['measureland_qualifier_flag_overall'] = track_df.apply(cruise_track_data_processing_utils.calculate_measureland_qualifier_flag_overall, axis=1)
    track_df['measureland_qualifier_flag_overall'] = track_df['measureland_qualifier_flag_overall'].astype('int8')
    #print("Dataframe with overall quality flag: ", track_df.head(10))

    print("OVERALL:", track_df['measureland_qualifier_flag_overall'].value_counts())


    # Output the data files where they have been flagged to show the intermediate steps and flagging.
    cruise_track_data_processing_utils.output_daily_files(track_df, output_flagging_filepath, output_flagging_filename)

    end_dataframe_length = len(track_df)

    # Check the lengths of the dataframes
    print("Length of dataframe at start: ", start_dataframe_length)
    print("Length of dataframe at end: ", end_dataframe_length)

    # Calculate statistics of qualifier flags
    pivottable_speed = cruise_track_data_processing_utils.create_pivottable_on_flag(dataframe_name, track_df, 'measureland_qualifier_flag_speed')
    pivottable_distance = cruise_track_data_processing_utils.create_pivottable_on_flag(dataframe_name, track_df, 'measureland_qualifier_flag_distance')
    pivottable_course = cruise_track_data_processing_utils.create_pivottable_on_flag(dataframe_name, track_df, 'measureland_qualifier_flag_course')
    pivottable_acceleration = cruise_track_data_processing_utils.create_pivottable_on_flag(dataframe_name, track_df, 'measureland_qualifier_flag_acceleration')
    pivottable_visual = cruise_track_data_processing_utils.create_pivottable_on_flag(dataframe_name, track_df, 'measureland_qualifier_flag_visual')
    pivottable_visual = cruise_track_data_processing_utils.create_pivottable_on_flag(dataframe_name, track_df, 'measureland_qualifier_flag_overall')


    return track_df


def begin_from_intermediate_files(intermediate_filepath, intermediate_filename):
    """If the intermediate flagged files exist, open these and create a concatenated csv file, to begin the next steps of analysis from here to avoid doing the full set of processing."""

    intermediate_concatenated_file = intermediate_filepath + "/" + intermediate_filename + "_concatenated.csv"

    datatypes = {'id': 'int32',
              'latitude': 'float64',
              'longitude': 'float64',
              'fix_quality': 'int8',
              'number_satellites': 'int8',
              'horiz_dilution_of_position': 'float16',
              'altitude': 'float16',
              'altitude_units': 'category',
              'geoid_height': 'float16',
              'geoid_height_units': 'category',
              'device_id': 'int8',
              'measureland_qualifier_flags_id': 'int8',
              'measureland_qualifier_flag_speed': 'int8',
              'speed': 'float64',
              'measureland_qualifier_flag_course': 'int8',
              'measureland_qualifier_flag_acceleration': 'int8',
              'measureland_qualifier_flag_visual': 'int8',
              'measureland_qualifier_flag_overall': 'int8'
                 }

    if not os.path.isfile(intermediate_concatenated_file):
        concatenated_filename = cruise_track_data_processing_utils.create_concatenated_csvfile(intermediate_filepath, intermediate_filename)
        print("Concatenated filename should now have been created: ", concatenated_filename)

    intermediate_dataframe = pandas.read_csv(intermediate_concatenated_file, dtype=datatypes, date_parser=pandas.to_datetime, parse_dates=[1, 13])

    return intermediate_dataframe


def decide_start_of_processing(dataframe_name, concatenated_filepath, concatenated_filename, input_filepath, input_filename,
                               device_id, output_create_files_filepath, output_create_files_filename, invalid_position_filepath,
                               output_flagging_filepath, output_flagging_filename):


    intermediate_files = output_flagging_filepath + "/" + output_flagging_filename + "*.csv"
    intermediate_file_list = glob.glob(intermediate_files)
    print("Checking if intermediate files, ", intermediate_files, " exist")

    if len(intermediate_file_list) == 0:
        print("Intermediate files do not exist. Doing processing from the beginning.")
        intermediate_df = process_track_data(dataframe_name, concatenated_filepath, concatenated_filename, input_filepath,
                                        input_filename, device_id, output_create_files_filepath, output_create_files_filename,
                                        invalid_position_filepath, output_flagging_filepath, output_flagging_filename)
    else:
        print("Intermediate files already exist.")

    intermediate_df = begin_from_intermediate_files(output_flagging_filepath, output_flagging_filename)
    print("Intermediate data read into dataframe: ", intermediate_df.head())

    return intermediate_df


def process_combined_track_data(dataframe1, dataframe2):
    """Combine the track data sets and prioritise data point selection."""

    print("*****COMBINING TRACK DATA SETS*****")

    # Combine the two dataframes of track data, into one dataframe.
    track_df = cruise_track_data_processing_utils.combine_position_dataframes(dataframe1, dataframe2)
    print("Combined dataframe: ", track_df.head())

    # test writing to parquet
    # track_df.to_parquet("test.parquet")
    #
    # print("Dataframes remaining", track_df)
    #
    # track_df_from_parquet = pandas.read_parquet("test.parquet")
    # print("Reading back from parquet: ", track_df_from_parquet)

    # if track_df.shape == track_df_from_parquet.shape:

        #track_df.drop(track_df.index, inplace=True)  # Delete data from dataframe to save memory

        # Remove the intermediate flagging columns.
    track_df_overall_flags = cruise_track_data_processing_utils.remove_intermediate_columns(track_df)
    print("Combined dataframe with overall flag: ", track_df_overall_flags.head())
    print("Columns of combined dataframe without intermediate flags: ", track_df_overall_flags.dtypes)
        #track_df_from_parquet.drop(track_df_from_parquet.index, inplace=True)  # Delete data from dataframe to save memory
    #else:
        #print("Reading dataframe to and from parquet has not worked correctly: retest this.")
        #exit
    track_df.drop(track_df.index, inplace=True)  # Delete data from dataframe to save memory

    # Write out the combined data set with only the overall qualifier flag to a csv file.
    track_df_overall_flags.to_csv('/home/jen/projects/ace_data_management/wip/cruise_track_data/track_data_combined_overall_flags.csv')

    # For each second, prioritise the data points according to the source and MQF.
    resulting_prioritised_df = cruise_track_data_processing_utils.prioritise_data_points(track_df_overall_flags)

    # delete the dataframe to save memory
    track_df_overall_flags.drop(track_df_overall_flags.index, inplace=True)

    # write out the prioritised data to a csv file
    resulting_prioritised_df.to_csv('/home/jen/projects/ace_data_management/wip/cruise_track_data/track_data_prioritised.csv')


def main():
    """Run the processing for the different tracking instruments."""

    # print("****PROCESSING TRIMBLE GPS DATA ****")

    concatenated_filepath_trimble = '/home/jen/projects/ace_data_management/wip/cruise_track_data/'
    concatenated_filename_trimble = 'ace_trimble_gps'

    input_filepath_trimble_gps = '/home/jen/projects/ace_data_management/wip/cruise_track_data/'
    input_filename_trimble_gps = 'ace_trimble_gps'

    # TESTING USING SUBSET OF FILES.
    # input_filepath_trimble_gps = ''
    # input_filename_trimble_gps = ''
    #
    # file_list = ['/home/jen/projects/ace_data_management/wip/cruise_track_data//ace_trimble_gps_2016-12-24.csv', '/home/jen/projects/ace_data_management/wip/cruise_track_data//ace_trimble_gps_2017-01-18.csv', '/home/jen/projects/ace_data_management/wip/cruise_track_data//ace_trimble_gps_2017-01-19.csv']
    # header = ['id', 'date_time', 'latitude', 'longitude', 'fix_quality', 'number_satellites', 'horiz_dilution_of_position', 'altitude', 'altitude_units', 'geoid_height', 'geoid_height_units', 'device_id', 'measureland_qualifier_flags_id', 'date_time_day']

    device_id_trimble_gps=63

    output_create_files_filepath_trimble_gps = '/home/jen/projects/ace_data_management/wip/cruise_track_data/'
    output_create_files_filename_trimble_gps = 'ace_trimble_gps'

    invalid_position_filepath_trimble_gps = '/home/jen/projects/ace_data_management/wip/cruise_track_data/ace_trimble_manual_position_errors.csv'

    output_flagging_filepath_trimble_gps = '/home/jen/projects/ace_data_management/wip/cruise_track_data/'
    output_flagging_filename_trimble_gps = 'flagging_data_ace_trimble_gps'

    dataframe_name_trimble = 'trimble'

    trimble_intermediate_df = decide_start_of_processing(dataframe_name_trimble, concatenated_filepath_trimble, concatenated_filename_trimble,
                                         input_filepath_trimble_gps, input_filename_trimble_gps, device_id_trimble_gps,
                                         output_create_files_filepath_trimble_gps,
                                         output_create_files_filename_trimble_gps, invalid_position_filepath_trimble_gps,
                                         output_flagging_filepath_trimble_gps, output_flagging_filename_trimble_gps)
    #
    # # trimble_df = process_track_data(dataframe_name_trimble, concatenated_filepath_trimble, concatenated_filename_trimble,
    # #                                  input_filepath_trimble_gps, input_filename_trimble_gps, device_id_trimble_gps,
    # #                                  output_create_files_filepath_trimble_gps,
    # #                                  output_create_files_filename_trimble_gps, invalid_position_filepath_trimble_gps,
    # #                                  output_flagging_filepath_trimble_gps, output_flagging_filename_trimble_gps)
    #
    # # Get some stats about and plot the speed throughout the cruise
    # # cruise_track_data_processing_utils.get_stats(trimble_df, "speed")
    # # trimble_remove_outlier = trimble_df.loc[(trimble_df['speed'] <= 100) & (trimble_df['speed'] > 2.5)]
    # # cruise_track_data_processing_utils.get_stats(trimble_remove_outlier, "speed")
    # #  cruise_track_data_plotting.plot_speed(trimble_remove_outlier, "red", "trimble")
    #
    # cruise_track_data_processing_utils.get_stats(trimble_intermediate_df, "speed")
    # trimble_intermediate_remove_outlier = trimble_intermediate_df.loc[(trimble_intermediate_df['speed'] <= 100) & (trimble_intermediate_df['speed'] > 2.5)]
    # cruise_track_data_processing_utils.get_stats(trimble_intermediate_remove_outlier, "speed")
    # cruise_track_data_plotting.plot_speed(trimble_intermediate_remove_outlier, "red", "trimble")
    #
    # #cruise_track_data_plotting.plot_data_sources_from_dataframe(trimble_intermediate_remove_outlier, 'device_id')
    #
    # print("****PROCESSING GLONASS DATA ****")
    #
    # concatenated_filepath_glonass = '/home/jen/projects/ace_data_management/wip/cruise_track_data/'
    # concatenated_filename_glonass = 'ace_glonass'
    #
    # input_filepath_glonass = '/home/jen/projects/ace_data_management/wip/cruise_track_data/'
    # input_filename_glonass = 'ace_glonass'
    #
    # # TESTING USING SUBSET OF FILES.
    # # input_filepath_glonass = ''
    # # input_filename_glonass = ''
    # #
    # # file_list = ['/home/jen/projects/ace_data_management/wip/cruise_track_data//ace_glonass_2017-03-18.csv', '/home/jen/projects/ace_data_management/wip/cruise_track_data//ace_glonass_2017-03-19.csv']
    # # header = ['id', 'date_time', 'latitude', 'longitude', 'fix_quality', 'number_satellites', 'horiz_dilution_of_position', 'altitude', 'altitude_units', 'geoid_height', 'geoid_height_units', 'device_id', 'measureland_qualifier_flags_id', 'date_time_day']
    #
    # device_id_glonass = 64
    # #
    # output_create_files_filepath_glonass = '/home/jen/projects/ace_data_management/wip/cruise_track_data/'
    # output_create_files_filename_glonass = 'ace_glonass'
    #
    # invalid_position_filepath_glonass = ''
    #
    # output_flagging_filepath_glonass = '/home/jen/projects/ace_data_management/wip/cruise_track_data/'
    # output_flagging_filename_glonass = 'flagging_data_ace_glonass'
    #
    # dataframe_name_glonass = 'glonass'
    #
    # glonass_intermediate_df = decide_start_of_processing(dataframe_name_glonass, concatenated_filepath_glonass, concatenated_filename_glonass,
    #                                                      input_filepath_glonass, input_filename_glonass, device_id_glonass,
    #                                  output_create_files_filepath_glonass, output_create_files_filename_glonass, invalid_position_filepath_glonass,
    #                                  output_flagging_filepath_glonass, output_flagging_filename_glonass)
    #
    # # glonass_df = process_track_data(dataframe_name_glonass, concatenated_filepath_glonass, concatenated_filename_glonass,
    # #                               input_filepath_glonass, input_filename_glonass, device_id_glonass,
    # #                               output_create_files_filepath_glonass, output_create_files_filename_glonass,
    # #                               invalid_position_filepath_glonass, output_flagging_filepath_glonass, output_flagging_filename_glonass)
    #
    # # Get some stats about and plot the speed throughout the cruise (use this version when doing the full processing)
    # # cruise_track_data_processing_utils.get_stats(glonass_df, "speed")
    # # glonass_remove_outlier = glonass_df.loc[(glonass_df['speed'] <= 100) & (glonass_df['speed'] > 2.5)]
    # # cruise_track_data_processing_utils.get_stats(glonass_remove_outlier, "speed")
    # #  cruise_track_data_plotting.plot_speed(glonass_remove_outlier, "red", "glonass")
    #
    # # Get some stats about and plot the speed throughout the cruise (use this version when doing the intermediate processing)
    # cruise_track_data_processing_utils.get_stats(glonass_intermediate_df, "speed")
    # glonass_intermediate_remove_outlier = glonass_intermediate_df.loc[(glonass_intermediate_df['speed'] <= 100) & (glonass_intermediate_df['speed'] > 2.5)]
    # cruise_track_data_processing_utils.get_stats(glonass_intermediate_remove_outlier, "speed")
    # cruise_track_data_plotting.plot_speed(glonass_intermediate_remove_outlier, "red", "glonass")
    #
    # # cruise_track_data_plotting.plot_data_sources_from_dataframe(glonass_intermediate_remove_outlier, 'device_id')
    #
    # # Begin combining the dataframes and datatypes
    # print("Trimble data types to check: ", trimble_intermediate_df.dtypes)
    # print("Glonass data types to check: ", glonass_intermediate_df.dtypes)
    #
    # # # Combine the datasets
    # result_dataframe = process_combined_track_data(trimble_intermediate_df, glonass_intermediate_df)
    #
    # cruise_track_data_plotting.plot_data_sources_from_dataframe(result_dataframe, 'device_id')

    # #### TEST PRIORITISATION RUNS AND COMPLETES ###
    #
    # # read in the combined dataframe from file
    # datatypes = {'id': 'int32',
    #              'latitude': 'float64',
    #              'longitude': 'float64',
    #              'fix_quality': 'int8',
    #              'number_satellites': 'int8',
    #              'horiz_dilution_of_position': 'float16',
    #              'altitude': 'float16',
    #              'altitude_units': 'category',
    #              'geoid_height': 'float16',
    #              'geoid_height_units': 'category',
    #              'device_id': 'int8',
    #              'measureland_qualifier_flags_id': 'int8',
    #              'speed': 'float64',
    #              'measureland_qualifier_flag_overall':'int8'}
    #
    # track_df_overall_flags = pandas.read_csv('/home/jen/projects/ace_data_management/wip/cruise_track_data/track_data_combined_overall_flags_test.csv', dtype=datatypes, date_parser=pandas.to_datetime, parse_dates=[2, 14])
    # print(track_df_overall_flags['geoid_height'].head(5))
    # #print(track_df_overall_flags.dtypes)
    # cruise_track_data_processing_utils.get_device_summary(track_df_overall_flags)
    # points_before_prioritisation = len(track_df_overall_flags)
    #
    # # For each second, prioritise the data points according to the source and MQF and output to a final file of prioritised points.
    # cruise_track_data_processing_utils.prioritise_data_points(track_df_overall_flags, output_filepath='/home/jen/projects/ace_data_management/wip/cruise_track_data/', output_filename='ace_cruise_track_prioritised.csv')
    #
    # # delete the dataframe to save memory
    # track_df_overall_flags.drop(track_df_overall_flags.index, inplace=True)
    #
    # # read in the data from the file we have just produced
    # filepath = '/home/jen/projects/ace_data_management/wip/cruise_track_data/ace_cruise_track_prioritised.csv'
    # columns = ['date_time', 'latitude', 'longitude', 'device_id', 'measureland_qualifier_flag_overall']
    # cruise_track_data = cruise_track_data_plotting.get_data_file(filepath, columns)
    # points_after_prioritisation = len(cruise_track_data)
    #
    # print("Number of data points before prioritisation: ", points_before_prioritisation)
    # print("Number of data points after prioritisation: ", points_after_prioritisation)
    #
    # # get min and max stats to check data set
    # cruise_track_data_processing_utils.get_minmax_stats(cruise_track_data, 'date_time')
    # cruise_track_data_processing_utils.get_minmax_stats(cruise_track_data, 'latitude')
    # cruise_track_data_processing_utils.get_minmax_stats(cruise_track_data, 'longitude')
    #
    # # get device use summary in prioritised dataset
    # cruise_track_data_processing_utils.get_device_summary(cruise_track_data)

    # plot the prioritised latitude longitude data
    # Plot one second resolution data
    # plt.subplot(211)
    # plt.scatter(cruise_track_data.longitude, cruise_track_data.latitude, c="red")
    # plt.title("One-second resolution")
    # plt.xlabel("Longitude, decimal degrees E")
    # plt.ylabel("Latitude, decimal degrees N")
    # plt.grid(True)
    # plt.legend()
    #
    # # Plot sixty-second resolution latitude longitude data
    # sixty_sec_res_gps = cruise_track_data.iloc[::60]
    # plt.subplot(212)
    # plt.scatter(sixty_sec_res_gps.longitude, sixty_sec_res_gps.latitude, c="red")
    # plt.title("Sixty-second resolution")
    # plt.xlabel("Longitude, decimal degrees E")
    # plt.ylabel("Latitude, decimal degrees N")
    # plt.grid(True)
    # plt.legend()
    #
    # plt.tight_layout()
    # plt.show()

    # Plot position data vs time to see coverage of prioritised data set
    #plt.subplot(211)
    # plt.scatter(cruise_track_data.date_time, cruise_track_data.latitude)
    # plt.title("Latitude coverage")
    # plt.xlabel("Date and time, UTC")
    # plt.ylabel("Latitude, decimal degrees N")
    # plt.grid(True)
    # plt.legend()
    #
    # #plt.subplot(211)
    # plt.scatter(cruise_track_data.date_time, cruise_track_data.longitude)
    # plt.title("Longitude coverage")
    # plt.xlabel("Date and time, UTC")
    # plt.ylabel("Longitude, decimal degrees N")
    # plt.grid(True)
    # plt.legend()

    # plt.tight_layout()
    # plt.show()



if __name__ == "__main__":
    main()