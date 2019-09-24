import csv
import datetime
import copy


def read_file_to_list(input_file):
    with open(input_file) as csvfile:
        csv_rows = csv.reader(csvfile)

        data = []
        for row in csv_rows:
            data.append(row)

    return data


def combine_multiday_rows(list):
    output_list = []

    list_iter = iter(list)

    first_row = next(list_iter)

    first_time_off_start = datetime.datetime.strptime(first_row[0] + " " + first_row[1], "%Y-%m-%d %H:%M:%S")
    first_time_off_end = datetime.datetime.strptime(first_row[0] + " " + first_row[2], "%Y-%m-%d %H:%M:%S")

    first_step = [first_time_off_start, first_time_off_end, 'off']

    output_list.append(first_step)

    previous_time_off_end = first_time_off_end

    count_midnight_rows_skipped = 0
    for row in list_iter:
        current_time_off_start = datetime.datetime.strptime(row[0] + ' ' + row[1], '%Y-%m-%d %H:%M:%S')
        current_time_off_end = datetime.datetime.strptime(row[0] + ' ' + row[2], '%Y-%m-%d %H:%M:%S')

        while previous_time_off_end + datetime.timedelta(seconds=1) == current_time_off_start:
            previous_time_off_start = current_time_off_start
            previous_time_off_end = current_time_off_end

            row = next(list_iter)

            current_time_off_start = datetime.datetime.strptime(row[0] + ' ' + row[1], '%Y-%m-%d %H:%M:%S')
            current_time_off_end = datetime.datetime.strptime(row[0] + ' ' + row[2], '%Y-%m-%d %H:%M:%S')

            count_midnight_rows_skipped += 1

        output_list.append([previous_time_off_end, current_time_off_start, 'on'])
        output_list.append([current_time_off_start, current_time_off_end, 'off'])

        previous_time_off_start = current_time_off_start
        previous_time_off_end = current_time_off_end

    return output_list

def process_file(input_file):
    """Get the input csv file and convert the times from off and on, to start and end of on/off status.

    :param input_file: file path and name of csv file
    """

    data = read_file_to_list(input_file)
    data.sort()

    list_1 = combine_multiday_rows(data)

    all_data = [] # output list of data
    count_duplicate_lines_skipped = 0 # lines that are skipped because they are duplicates in the original file
    count_midnight_rows_skipped = 0 # lines that are skipped because they go over midnight

    # get the first row of data and create the first "step" out of it
    first_row = data[0]

    first_time_off = datetime.datetime.strptime(first_row[0] + " " + first_row[1], "%Y-%m-%d %H:%M:%S")
    first_time_on = datetime.datetime.strptime(first_row[0] + " " + first_row[2], "%Y-%m-%d %H:%M:%S") - datetime.timedelta(seconds=1) # subtract one second so that on and off time steps do not overlap
    print(first_time_on)
    # this first row has already been designated as off
    first_step = [first_time_off, first_time_on, 'off']

    # set the time the pump was on for the next row, as the time from the first row
    next_time_on = first_time_on + datetime.timedelta(seconds=1) # add the second to get a time that does not overlap with the off row
    previous_time_on = next_time_on

    # add the first row to the list
    all_data.append(first_step)
    previous_line = [first_time_off, first_time_on]

    # create an iterator to avoid having to read the first row of the list (this has already been used)
    data_iter = iter(data)
    next(data_iter) # skips the first line

    for line in data_iter:
        # read the next row in the iterator and get the start and end times for the next step
        current_line = [datetime.datetime.strptime(line[0] + ' ' + line[1], '%Y-%m-%d %H:%M:%S'), datetime.datetime.strptime(line[0] + ' ' + line[2], '%Y-%m-%d %H:%M:%S')]
        next_time_off = current_line[0]
        next_time_on = current_line[1]

        # skip the line if it was the same as the previous line (this will remove the duplicates that existed)
        if current_line == previous_line:
            previous_line = current_line
            count_duplicate_lines_skipped += 1
            continue

        while previous_line[1] + datetime.timedelta(seconds=1) == current_line[0]:
            previous_line = [previous_line[0], datetime.datetime.strptime(line[0] + ' ' + line[2], '%Y-%m-%d %H:%M:%S')]
            line = next(data_iter)

            current_line = [datetime.datetime.strptime(line[0] + ' ' + line[1], '%Y-%m-%d %H:%M:%S'),
                        datetime.datetime.strptime(line[0] + ' ' + line[2], '%Y-%m-%d %H:%M:%S')]

            count_midnight_rows_skipped += 1

        off_starts = previous_line[0]
        off_ends = previous_line[1]

        on_starts = previous_line[1]
        on_ends = current_line[0]

        all_data.append([off_starts, off_ends, 'off'])
        all_data.append([on_starts, on_ends, 'on'])


        # all_data.append([previous_line[0], previous_line[1], 'off'])
        # all_data.append([previous_line[1], current_line[0], 'on'])

        previous_line = current_line

        # Check that the times in the steps make sense
        if previous_time_on > next_time_off:
            raise ValueError('Start time of on row cannot be after the end time:')

        if next_time_off > next_time_on:
            raise ValueError('Start time of off row cannot be after end time:', next_step)

        # check difference between start and end times for each step:
        on_difference = next_time_off - previous_time_on
        off_difference = next_time_on - next_time_off

        if off_difference.total_seconds() == 0 and next_time_off.second == 0:
            # where the off times are the same to the nearest minute
            #print('Off times are the same to the nearest minute', off_difference)

            next_time_on += datetime.timedelta(seconds=59)
            previous_time_off = next_time_off - datetime.timedelta(seconds=1)
            previous_step = [previous_time_on, previous_time_off, 'on']
            next_step = [next_time_off, next_time_on, 'off']
            # create the set-up for reading the next row, by assigning the end time from the line just read as the previous time for the next step
            previous_time_on = next_time_on + datetime.timedelta(seconds=1)
        elif off_difference.total_seconds() == 0 and next_time_off.second != 0:
            # where the off times are the same to the nearest second
            #print('Off times are the same to the nearest second', off_difference)

            next_time_on += datetime.timedelta(seconds=1)
            previous_time_off = next_time_off - datetime.timedelta(seconds=1)
            previous_step = [previous_time_on, previous_time_off, 'on']
            next_step = [next_time_off, next_time_on, 'off']
            # create the set-up for reading the next row, by assigning the end time from the line just read as the previous time for the next step
            previous_time_on = next_time_on + datetime.timedelta(seconds=1)
        else:
            # THIS IS THE ALTERNATIVE WAY OF REMOVING THE ROWS THAT RUN OVER MIDNIGHT
            # if previous_line[1] + datetime.timedelta(seconds=1) == current_line[0]: #and (previous_line[1].time() == datetime.datetime.strptime('23:59:59', '%H:%M:%S').time()):
            #     # if there are rows that have off periods split over more than one day, then combine them into the same row
            #     print('There is a row that goes over midnight')
            #     print('Last row in all_data list: ', all_data[-1])
            #     all_data[-1][1] = current_line[1]
            #     print('Last row in all_data list changed to: ', all_data[-1])
            #     count_midnight_rows_skipped += 1
            #     previous_line = current_line
            #     print()
            #     continue
            # all other cases
            previous_time_off = next_time_off - datetime.timedelta(seconds=1)
            previous_step = [previous_time_on, previous_time_off, 'on']
            next_step_time_on = next_time_on
            next_time_on = next_time_on - datetime.timedelta(seconds=1)
            next_step = [next_time_off, next_time_on, 'off']
            # create the set-up for reading the next row, by assigning the end time from the line just read as the previous time for the next step
            previous_time_on = next_step_time_on

        all_data.append(previous_step)

        all_data.append(next_step)
        previous_line = current_line
        print('---------------------------------------------------------------------------')

    print('Number of duplicate lines skipped: ', count_duplicate_lines_skipped)
    print('Number of midnight lines skipped: ', count_midnight_rows_skipped)

    return all_data

def list_to_csv(list, output_file):
    """Output the list of data to a csv file"""
    with open(output_file, 'w', ) as csvFile:
        writer = csv.writer(csvFile)
        writer.writerow(['start_datetime', 'end_datetime', 'status'])
        writer.writerows(list)

    csvFile.close()


def main():
    input_file = '/home/jen/projects/ace_data_management/data_to_archive_post_cruise/ferrybox/pump.csv'
    output_file = '/home/jen/projects/ace_data_management/data_to_archive_post_cruise/ferrybox/pump_status_nearest_second_others_subtract_second_off_time_subtract_second_midnight_rows_removed.csv'

    status_data = process_file(input_file)
    print(status_data)
    print('total length: ', len(status_data))
    list_to_csv(status_data, output_file)


if __name__ == "__main__":
    main()