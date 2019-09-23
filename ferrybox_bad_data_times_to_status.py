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

    # get the first row of data and create the first "step" out of it
    first_row = data[0]

    first_time_off = datetime.datetime.strptime(first_row[0] + " " + first_row[1], "%Y-%m-%d %H:%M:%S")
    first_time_on = datetime.datetime.strptime(first_row[0] + " " + first_row[2], "%Y-%m-%d %H:%M:%S")

    # each step will be defined in a list: start time, end time, status
    previous_step = []
    next_step = []

    # this first row has already been designated as off
    first_step = [first_time_off, first_time_on, 'off']

    # set the time the pump was on for the next row, as the time from the first row
    previous_time_on = first_time_on

    # create an iterator to avoid having to read the first row of the list (this has already been used)
    data_iter = iter(data)

    next(data_iter)

    # create an output list for all steps to be added to
    all_data = []

    # add the first row to the list
    all_data.append(first_step)

    for line in data_iter:
        # read the next row in the iterator and get the start and end times for the next step
        next_time_off = datetime.datetime.strptime(line[0] + " " + line[1], "%Y-%m-%d %H:%M:%S")
        next_time_on = datetime.datetime.strptime(line[0] + " " + line[2], "%Y-%m-%d %H:%M:%S")

        # assign these times to the next on and off steps
        previous_step = [previous_time_on, next_time_off, 'on']
        next_step = [next_time_off, next_time_on, 'off']

        # Check that the times in the steps make sense
        if previous_time_on > next_time_off:
            raise ValueError('Start time of on row cannot be after end time:', previous_step)

        if next_time_off > next_time_on:
            raise ValueError('Start time of off row cannot be after end time:', next_step)

        # append these steps to the output list
        print(previous_step)
        all_data.append(previous_step)

        print(next_step)
        all_data.append(next_step)

        # create the set-up for reading the next row, by assigning the end time from the line just read as the previous time for the next step
        previous_time_on = next_time_on

    return all_data



def main():

    input_file = '/home/jen/projects/ace_data_management/data_to_archive_post_cruise/ferrybox/pump.csv'

    status_data = process_file(input_file)
    print(status_data)


if __name__ == "__main__":
    main()