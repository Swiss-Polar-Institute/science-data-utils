# Compare the depth of the seabed along the cruise track between two files which each contain lines of the date,
# latitude, longitude and depth value. Depths have come from different sources of bathymetry data. Calculate the
# differences between the depths and output this into csv files.

import csv
import pandas

def file_to_position_depths(file_path):
    """Get the position and depths from one file. Read into memory as a dictionary with (lat, lon) as the key and depth
    as the value."""

    # create the dictionary
    result = {}

    # read the position and depth data from the input csv file
    with open(file_path) as csvfile:
        contents = csv.reader(csvfile)

        next(contents)

        count = 1

        # print progress
        for line in contents:
            if count % 100_000 == 0:
                print(count)

            count += 1

            # get the latitude, longitude and depth from the row in the file
            _, lat, long, depth = line
            lat_long = (float(lat), float(long))

            # if the depth is empty, then continue
            if depth == '':
                continue

            result[lat_long] = float(depth)

    return result


def yield_difference(file_path, position_depths):
    """Using the dictionary of positions and depths, get another file containing the same positions, but a different
    source of data. Calculate the difference in depth at the same positions."""

    # open the file containing the second set of depth data
    with open(file_path) as csvfile:
        contents = csv.reader(csvfile)

        next(contents)

        count = 1

        # for each line (date, position and depth) find the corresponding depth from the first file and calculate the
        # difference
        for line in contents:
            date, lat, long, depth = line

            lat_long = (float(lat), float(long))

            # if the depth is empty, then continue
            if depth == '':
                continue
            depth = float(depth)

            other_file_depth = position_depths.get(lat_long, None)

            # calculate the differences between depths. If a depth is not found, the difference is left blank
            if other_file_depth is not None:
                depth_difference = depth - other_file_depth
            else:
                depth_difference = None

            yield [date, lat_long[0], lat_long[1], depth, other_file_depth, depth_difference]


def get_depth_differences(depth1_file, depth2_file):
    """Get the differences in depth between two files containing this data"""

    position_depths = file_to_position_depths(depth1_file)

    differences = yield_difference(depth2_file, position_depths)

    return differences


def write_csv_large_differences(differences, csv_outfile):
    """Where the depth difference is greater than 100 m, write out the position, depths and difference into a csv file"""

    header = ['Date', 'Lat', 'Long', 'Depth1', 'Depth2', 'Depth difference']
    csv_writer = csv.writer(csv_outfile)
    csv_writer.writerow(header)

    for difference in differences:
        if difference[5] is not None:
            if abs(difference[5]) > 100:
                csv_writer.writerow(difference)


def write_csv_depth_differences(differences, csv_outfile):
    """Write out the depth differences to a csv file"""

    header = ['Date', 'Lat', 'Long', 'Depth1', 'Depth2', 'Depth difference']
    csv_writer = csv.writer(csv_outfile)
    csv_writer.writerow(header)

    for difference in differences:
        csv_writer.writerow(difference)


def calculate_differences_and_write_out(depth1_file, depth2_file, differences_csvfile, large_differences_csvfile):
    """Calculate the depth differences and output two files: one with all differences and one with differences larger than 100 m"""

    differences = get_depth_differences(depth1_file, depth2_file)

    with open(differences_csvfile, 'w') as differencescsvfile:
        write_csv_depth_differences(differences, differencescsvfile)

    with open(large_differences_csvfile, 'w') as largecsvfile:
        write_csv_large_differences(differences, largecsvfile)


def create_dataframe(depth1_file, depth2_file):
    """Create a pandas dataframe of the depth differences. Used for debugging"""
    position_depths = file_to_position_depths(depth1_file)

    differences = yield_difference(depth2_file, position_depths)
    df = pandas.DataFrame(differences, columns=['Date', 'Lat', 'Long', 'Depth1', 'Depth2', 'Depth difference'])

    print(df.head())


if __name__ == '__main__':
    gebco_csv = '/home/jen/projects/ace_data_management/mapping/data/ace_cruise_track_1sec_with_depth_gebco2019.csv'
    rtopo204_csv = '/home/jen/projects/ace_data_management/mapping/data/ace_cruise_track_1sec_with_depth_rtopo2.0.4.csv'

    difference_csv_outfile = '/home/jen/projects/ace_data_management/mapping/data/ace_cruise_track_depth_differences_rtopo204_gebco2019.csv'
    large_csv_outfile = '/home/jen/projects/ace_data_management/mapping/data/ace_cruise_track_depth_differences_rtopo204_gebco2019_greater_than_100m.csv'

    calculate_differences_and_write_out(rtopo204_csv, gebco_csv, difference_csv_outfile, large_csv_outfile)
