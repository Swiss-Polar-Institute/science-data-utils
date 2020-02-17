import csv
import pandas


def file_to_position_depths(file_path):

    result = {}
    with open(file_path) as csvfile:
        contents = csv.reader(csvfile)

        next(contents)

        count = 1

        for line in contents:
            if count % 100_000 == 0:
                print(count)

            count += 1

            _, lat, long, depth = line
            lat_long = (float(lat), float(long))

            if depth == '':
                continue

            try:
                result[lat_long] = float(depth)
            except ValueError:
                print('Line:', count)
                print(line)

    return result


def print_differences(file_path, position_depths):
    with open(file_path) as csvfile:
        contents = csv.reader(csvfile)

        next(contents)

        count = 1

        for line in contents:
            date, lat, long, depth = line

            lat_long = (float(lat), float(long))
            depth = float(depth)

            other_file_depth = position_depths[lat_long]
            print(date, depth, other_file_depth, depth-other_file_depth)

def compare_depths(depth1_file, depth2_file):
    position_depths = file_to_position_depths(depth1_file)

    print_differences(depth2_file, position_depths)

if __name__ == '__main__':
    gebco_csv = '/home/jen/projects/ace_data_management/mapping/data/ace_cruise_track_1sec_with_depth_gebco2019.csv'
    rtopo204_csv = '/home/jen/projects/ace_data_management/mapping/data/ace_cruise_track_1sec_with_depth_rtopo2.0.4.csv'

    compare_depths(gebco_csv, rtopo204_csv)
