import csv
import datetime


def process_file(input_file):
    """Get the input csv file and convert the times from off and on, to start and end of on/off status.

    :param input_file: file path and name of csv file
    """

    with open(input_file) as csvfile:
        csv_rows = csv.reader(csvfile)

        data = []
        for row in csv_rows:
            data.append(row)

    # sort the list of data so that the missing times can be added one line at a time
    data.sort()

    # each step will be defined in a list: start time, end time, status
    previous_step = []
    next_step = []
    previous_line = [] # previous line that was read
    current_line = [] # current line being read
    all_data = [] # output list of data
    count_duplicate_lines_skipped = 0 # lines that are skipped because they are duplicates in the original file

    # get the first row of data and create the first "step" out of it
    first_row = data[0]

    one_second = datetime.timedelta(seconds=1)

    first_time_off = datetime.datetime.strptime(first_row[0] + " " + first_row[1], "%Y-%m-%d %H:%M:%S")
    first_time_on = datetime.datetime.strptime(first_row[0] + " " + first_row[2], "%Y-%m-%d %H:%M:%S") - datetime.timedelta(seconds=1) # subtract one second so that on and off time steps do not overlap

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

    next(data_iter)

    for line in data_iter:
        # read the next row in the iterator and get the start and end times for the next step
        next_time_off = datetime.datetime.strptime(line[0] + " " + line[1], "%Y-%m-%d %H:%M:%S")
        next_time_on = datetime.datetime.strptime(line[0] + " " + line[2], "%Y-%m-%d %H:%M:%S")

        print('LINE READ: ', next_time_off, ' ', next_time_on)
        current_line = [next_time_off, next_time_on]

        # skip the line if it was the same as the previous line (this will remove the duplicates that existed)
        if current_line == previous_line:
            previous_line = current_line
            count_duplicate_lines_skipped += 1
            continue

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
            print('Off times are the same to the nearest minute', off_difference)

            next_time_on += datetime.timedelta(seconds=59)
            previous_time_off = next_time_off - datetime.timedelta(seconds=1)
            previous_step = [previous_time_on, previous_time_off, 'on']
            next_step = [next_time_off, next_time_on, 'off']
            # create the set-up for reading the next row, by assigning the end time from the line just read as the previous time for the next step
            previous_time_on = next_time_on + datetime.timedelta(seconds=1)
        elif off_difference.total_seconds() == 0 and next_time_off.second != 0:
            # where the off times are the same to the nearest second
            print('Off times are the same to the nearest second', off_difference)

            next_time_on += datetime.timedelta(seconds=1)
            previous_time_off = next_time_off - datetime.timedelta(seconds=1)
            previous_step = [previous_time_on, previous_time_off, 'on']
            next_step = [next_time_off, next_time_on, 'off']
            # create the set-up for reading the next row, by assigning the end time from the line just read as the previous time for the next step
            previous_time_on = next_time_on + datetime.timedelta(seconds=1)
        else:
            # all other cases
            previous_time_off = next_time_off - datetime.timedelta(seconds=1)
            previous_step = [previous_time_on, previous_time_off, 'on']
            next_step_time_on = next_time_on
            next_time_on = next_time_on - datetime.timedelta(seconds=1)
            next_step = [next_time_off, next_time_on, 'off']
            # create the set-up for reading the next row, by assigning the end time from the line just read as the previous time for the next step
            previous_time_on = next_step_time_on

        # append these steps to the output list
        print(previous_step)
        print('TIME ON: ', next_time_off - previous_time_on)

        all_data.append(previous_step)

        print(next_step)
        print('TIME OFF: ', next_time_on - next_time_off)

        all_data.append(next_step)
        previous_line = current_line

        print("Number of lines skipped: ", count_duplicate_lines_skipped)

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
    output_file = '/home/jen/projects/ace_data_management/data_to_archive_post_cruise/ferrybox/pump_status_nearest_second_others_subtract_second_off_time_subtract_second.csv'

    status_data = process_file(input_file)
    print(status_data)
    print('total length: ', len(status_data))
    #list_to_csv(status_data, output_file)


if __name__ == "__main__":
    main()