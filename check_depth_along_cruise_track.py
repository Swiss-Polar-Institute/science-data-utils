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


def yield_difference(file_path, position_depths):
    with open(file_path) as csvfile:
        contents = csv.reader(csvfile)

        next(contents)

        count = 1

        for line in contents:
            date, lat, long, depth = line

            lat_long = (float(lat), float(long))
            depth = float(depth)

            other_file_depth = position_depths.get(lat_long, None)

            if other_file_depth is not None:
                depth_difference = depth - other_file_depth
            else:
                depth_difference = None

            yield [date, lat_long[0], lat_long[1], depth, other_file_depth, depth_difference]
            # print(date, depth, other_file_depth, depth-other_file_depth)


def write_csv_large_differences(depth1_file, depth2_file, csv_outfile):
    position_depths = file_to_position_depths(depth1_file)

    differences = yield_difference(depth2_file, position_depths)

    header = ['Date', 'Lat', 'Long', 'Depth1', 'Depth2', 'Depth difference']
    csv_writer = csv.writer(csv_outfile)
    csv_writer.writerow(header)

    for difference in differences:
        if difference[5] is not None:
            if abs(difference[5]) > 100:
                csv_writer.writerow(difference)


def create_dataframe(depth1_file, depth2_file):
    position_depths = file_to_position_depths(depth1_file)

    differences = yield_difference(depth2_file, position_depths)
    df = pandas.DataFrame(differences, columns=['Date', 'Lat', 'Long', 'Depth1', 'Depth2', 'Depth difference'])

    print(df.head())
    print('test')

    # for diff_line in yield_difference(depth2_file, position_depths):
    #     print(diff_line)

    # print_differences(depth2_file, position_depths)


if __name__ == '__main__':
    gebco_csv = '/home/jen/projects/ace_data_management/mapping/data/ace_cruise_track_1sec_with_depth_gebco2019.csv'
    rtopo204_csv = '/home/jen/projects/ace_data_management/mapping/data/ace_cruise_track_1sec_with_depth_rtopo2.0.4.csv'

    csv_outfile = '/home/jen/projects/ace_data_management/mapping/data/depth_differences_greater_than_100.csv'

    with open(csv_outfile, 'w') as csvfile:
        write_csv_large_differences(gebco_csv, rtopo204_csv, csvfile)
    # create_dataframe(gebco_csv, rtopo204_csv)
