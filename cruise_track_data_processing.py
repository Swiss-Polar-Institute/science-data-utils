import cruise_track_data_processing_utils
import os
import pandas
import glob

def process_track_data(dataframe_name, concatenated_filepath, concatenated_filename, input_filepath, input_filename, device_id, output_create_files_filepath, output_create_files_filename, invalid_position_filepath, output_flagging_filepath, output_flagging_filename):
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
    cruise_track_data_processing_utils.analyse_speed(track_df)

    # Check the course of the ship throughout the track to ensure it is not making impossible turns or accelerating impossibly fast. Flag data points.
    (count_bearing_errors, count_acceleration_errors, count_ship_stationary_bearing_error) = cruise_track_data_processing_utils.analyse_course(track_df)
    print("Number of bearing errors when the ship is moving: ", count_bearing_errors)
    print("Number of acceleration errors: ", count_acceleration_errors)
    print("Number of bearing errors when the ship is stationary: ", count_ship_stationary_bearing_error)

    print(track_df.dtypes)

    # Flag the points where the track has been manually and visually identified as incorrect.
    track_df = cruise_track_data_processing_utils.update_visual_position_flag(track_df, invalid_position_filepath)

    # Calculate an overall quality flag, taking into account all of the factors tested above.
    track_df['measureland_qualifier_flag_overall'] = track_df.apply(cruise_track_data_processing_utils.calculate_measureland_qualifier_flag_overall, axis=1)
    track_df['measureland_qualifier_flag_overall'] = track_df['measureland_qualifier_flag_overall'].astype('int8')
    print("Dataframe with overall quality flag: ", track_df.head(10))

    # Output the data files where they have been flagged to show the intermediate steps and flagging.
    cruise_track_data_processing_utils.output_daily_files(track_df, output_flagging_filepath, output_flagging_filename)

    end_dataframe_length = len(track_df)

    # Check the lengths of the dataframes
    print("Length of dataframe at start: ", start_dataframe_length)
    print("Length of dataframe at end: ", end_dataframe_length)

    # Calculate statistics of qualifier flags
    pivottable_speed = cruise_track_data_processing_utils.create_pivottable_on_flag(dataframe_name, track_df, 'measureland_qualifier_flag_speed')
    pivottable_course = cruise_track_data_processing_utils.create_pivottable_on_flag(dataframe_name, track_df, 'measureland_qualifier_flag_course')
    pivottable_acceleration = cruise_track_data_processing_utils.create_pivottable_on_flag(dataframe_name, track_df, 'measureland_qualifier_flag_acceleration')

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

    if len(intermediate_file_list) == 0:
        print("Intermediate files do not exist. Doing processing from the beginning.")
        trimble_df = process_track_data(dataframe_name, concatenated_filepath, concatenated_filename, input_filepath,
                                        input_filename, device_id, output_create_files_filepath, output_create_files_filename,
                                        invalid_position_filepath, output_flagging_filepath, output_flagging_filename)
    else:
        print("Intermediate files already exist.")

    intermediate_df = begin_from_intermediate_files(output_flagging_filepath, output_flagging_filename)
    print("Intermediate data read into dataframe: ", intermediate_df.head())

    return intermediate_df


def process_combined_track_data(dataframe1, dataframe2):
    """Combine the track data sets and prioritise data point selection."""

    # Combine the two dataframes of track data, into one dataframe.
    track_df = cruise_track_data_processing_utils.combine_position_dataframes(dataframe1, dataframe2)

    # Remove the intermediate flagging columns.
    track_df_overall_flags = cruise_track_data_processing_utils.remove_intermediate_columns(track_df)

    print("Columns of combined dataframe without intermediate flags: ", track_df_overall_flags.dtypes)

    resulting_prioritised_df = cruise_track_data_processing_utils.prioritise_data_points(track_df_overall_flags)

    resulting_prioritised_df.to_csv('/tmp/test.csv')


def main():
    """Run the processing for the different tracking instruments."""

    print("****PROCESSING TRIMBLE GPS DATA ****")

    concatenated_filepath_trimble = '/home/jen/projects/ace_data_management/wip/cruise_track_data/'
    concatenated_filename_trimble = 'ace_trimble_gps'

    input_filepath_trimble_gps = '/home/jen/projects/ace_data_management/wip/cruise_track_data/'
    input_filename_trimble_gps = 'ace_trimble_gps'

    # TESTING USING SUBSET OF FILES.
    #input_filepath_trimble_gps = ''
    #input_filename_trimble_gps = ''

    # file_list = ['/home/jen/projects/ace_data_management/wip/cruise_track_data//ace_trimble_gps_2017-03-18.csv', '/home/jen/projects/ace_data_management/wip/cruise_track_data//ace_trimble_gps_2017-03-19.csv']
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

    #trimble_df = process_track_data(dataframe_name, concatenated_filepath_trimble, concatenated_filename_trimble,
                                    # input_filepath_trimble_gps, input_filename_trimble_gps, device_id_trimble_gps,
                                    # output_create_files_filepath_trimble_gps,
                                    # output_create_files_filename_trimble_gps, invalid_position_filepath_trimble_gps,
                                    # output_flagging_filepath_trimble_gps, output_flagging_filename_trimble_gps)

    print("****PROCESSING GLONASS DATA ****")

    concatenated_filepath_glonass = '/home/jen/projects/ace_data_management/wip/cruise_track_data/'
    concatenated_filename_glonass = 'ace_glonass'

    input_filepath_glonass = '/home/jen/projects/ace_data_management/wip/cruise_track_data/'
    input_filename_glonass = 'ace_glonass'

    # TESTING USING SUBSET OF FILES.
    # input_filepath_glonass = ''
    # input_filename_glonass = ''

    # file_list = ['/home/jen/projects/ace_data_management/wip/cruise_track_data//ace_glonass_2017-03-18.csv', '/home/jen/projects/ace_data_management/wip/cruise_track_data//ace_glonass_2017-03-19.csv']
    # header = ['id', 'date_time', 'latitude', 'longitude', 'fix_quality', 'number_satellites', 'horiz_dilution_of_position', 'altitude', 'altitude_units', 'geoid_height', 'geoid_height_units', 'device_id', 'measureland_qualifier_flags_id', 'date_time_day']

    device_id_glonass = 64

    output_create_files_filepath_glonass = '/home/jen/projects/ace_data_management/wip/cruise_track_data/'
    output_create_files_filename_glonass = 'ace_glonass'

    invalid_position_filepath_glonass = ''

    output_flagging_filepath_glonass = '/home/jen/projects/ace_data_management/wip/cruise_track_data/'
    output_flagging_filename_glonass = 'flagging_data_ace_glonass'

    dataframe_name_glonass = 'glonass'

    glonass_intermediate_df = decide_start_of_processing(dataframe_name_glonass, concatenated_filepath_glonass, concatenated_filename_glonass,
                                                         input_filepath_glonass, input_filename_glonass, device_id_glonass,
                                     output_create_files_filepath_glonass, output_create_files_filename_glonass, invalid_position_filepath_glonass,
                                     output_flagging_filepath_glonass, output_flagging_filename_glonass)

    #glonass_df = process_track_data(dataframe_name, concatenated_filepath_glonass, concatenated_filename_glonass, input_filepath_glonass, input_filename_glonass, device_id_glonass,
                                    # output_create_files_filepath_glonass, output_create_files_filename_glonass, invalid_position_filepath_glonass,
                                    # output_flagging_filepath_glonass, output_flagging_filename_glonass)

    # print("*****COMBINING TRACK DATA SETS*****")
    #
    # process_combined_track_data(trimble_df, glonass_df)

    print("Trimble data types to check: ", trimble_intermediate_df.dtypes)
    print("Glonass data types to check: ", glonass_intermediate_df.dtypes)

if __name__ == "__main__":
    main()