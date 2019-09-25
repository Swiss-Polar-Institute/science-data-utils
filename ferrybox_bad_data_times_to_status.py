import csv
import datetime

def read_file_to_list(input_file):
    """Read a csv file of off times in the format date, time, time to a list in the format date, time, time."""
    with open(input_file) as csvfile:
        csv_rows = csv.reader(csvfile)

        data = []
        for row in csv_rows:
            data.append(row)

    return data


def change_format_from_input_to_datetime(list_d_t_t):
    """Changes format of input list containing off times, from a list of lists each containing a date, time, time, to a list of lists
    containing a date time, date time, both in python datetime type.
    Input: [['2016-12-24', '11:30:00', '12:30:00'],
            ['2016-12-25', '23:45:00', '23:50:00']
            ]

    Output: [[datetime.datetime(2016, 12, 24, 11, 30, 00), datetime.datetime(2016, 12, 24, 12, 30, 00)],
            [datetime.datetime(2016, 12, 25, 23, 45, 00), datetime.datetime(2016, 12, 25, 23, 50, 00)]
            ]
    """
    data_output = []

    for row in list_d_t_t:
        data_output.append([datetime.datetime.strptime(row[0] + " " + row[1], "%Y-%m-%d %H:%M:%S"),
                            datetime.datetime.strptime(row[0] + " " + row[2], "%Y-%m-%d %H:%M:%S")])

    return data_output


def collapse_same_day_off(list_dt_dt):
    """Puts off rows where they pass over midnight, into one row. Input and output lists are both datetime, datetime in
    python datetime type.
    Input: [[datetime.datetime(2016, 12, 24, 11, 30, 00), datetime.datetime(2016, 12, 24, 12, 30, 00)],
                    [datetime.datetime(2016, 12, 25, 20, 45, 00), datetime.datetime(2016, 12, 25, 23, 59, 59)],
                    [datetime.datetime(2016, 12, 26, 00, 00, 00), datetime.datetime(2016, 12, 26, 14, 30, 00)],
                    [datetime.datetime(2016, 12, 27, 13, 0, 00), datetime.datetime(2016, 12, 27, 15, 45, 00)]
                    ]
    
    Output: [[datetime.datetime(2016, 12, 24, 11, 30, 00), datetime.datetime(2016, 12, 24, 12, 30, 00)],
                    [datetime.datetime(2016, 12, 25, 20, 45, 00), datetime.datetime(2016, 12, 26, 14, 30, 00)],
                    [datetime.datetime(2016, 12, 27, 13, 0, 00), datetime.datetime(2016, 12, 27, 15, 45, 00)]
                    ]

    """
    collapsed_list = []

    list_iter = iter(list_dt_dt)

    # read the first row
    previous = next(list_iter)

    collapsed_list.append(previous)

    count_midnight_rows_skipped = 0

    for row in list_iter:
        skipped = False
        while previous[1] + datetime.timedelta(seconds=1) == row[0]: # compare the end of the previous row with the
            # start of the current row and if they are one second apart, then enter the condition to allow the rows
            # to be collapsed (note that this has not been tested where the rows do not go over midnight)
            skipped = True
            previous = row
            row = next(list_iter)
            count_midnight_rows_skipped += 1

        if skipped:
            collapsed_list[-1][1] = previous[1] # change the end time of the previous row to be the end time of the
            # current row (this has already been appended to the list, so it needs to be changed there)
            collapsed_list.append([row[0], row[1]])
        else:
            collapsed_list.append([row[0], row[1]])

        previous = row

    return collapsed_list


def correct_off_seconds_same_minute(pump_log):
    """If an off row has the same start and end times and these are the same minute, we are assuming that the pump was
    off for the entire minute, rather than just the one second.

    This function adds 59 seconds to the end time so that the off row can be a period of time.

    Input: [[datetime.datetime(2016, 12, 24, 11, 30, 00), datetime.datetime(2016, 12, 24, 11, 30, 00)],
            [datetime.datetime(2016, 12, 25, 20, 45, 00), datetime.datetime(2016, 12, 26, 20, 45, 00)]
            ]

    Output: [[datetime.datetime(2016, 12, 24, 11, 30, 00), datetime.datetime(2016, 12, 24, 11, 30, 59)],
            [datetime.datetime(2016, 12, 25, 20, 45, 00), datetime.datetime(2016, 12, 26, 20, 45, 59)]
            ]
    """
    output = []

    for row in pump_log:
        if row[0].second == 0 and row[0] == row[1]:
            row[1] = row[1] + datetime.timedelta(seconds=59)
            output.append([row[0], row[1]])
        else:
            output.append([row[0], row[1]])

    return output


def process_to_on_off(list_dt_dt):
    """Convert the list of off times to a list containing the off and now on periods, which are deduced from the off
    times. This should be the list after it has been collapsed. Output list will contain a status, on or off.

    Input: [[datetime.datetime(2016, 12, 24, 11, 30, 00), datetime.datetime(2016, 12, 24, 12, 30, 00)],
            [datetime.datetime(2016, 12, 25, 20, 45, 00), datetime.datetime(2016, 12, 26, 14, 30, 00)],
            [datetime.datetime(2016, 12, 27, 13, 0, 00), datetime.datetime(2016, 12, 27, 15, 45, 00)]
            ]

    Output: [[datetime.datetime(2016, 12, 24, 11, 30, 00), datetime.datetime(2016, 12, 24, 12, 30, 00), 'off'],
            [datetime.datetime(2016, 12, 24, 12, 30, 00), datetime.datetime(2016, 12, 25, 20, 45, 00), 'on'],
            [datetime.datetime(2016, 12, 25, 20, 45, 00), datetime.datetime(2016, 12, 26, 14, 30, 00), 'off'],
            [datetime.datetime(2016, 12, 26, 14, 30, 00), datetime.datetime(2016, 12, 27, 13, 0, 00), 'on'],
            [datetime.datetime(2016, 12, 27, 13, 0, 00), datetime.datetime(2016, 12, 27, 15, 45, 00) 'off']
            ]

    """
    output_list = []

    list_iter = iter(list_dt_dt)

    # put the first row in the output_list - this is always off
    first_row = next(list_iter)

    previous_time_off_start = first_row[0]
    previous_time_off_end = first_row[1]

    output_list.append([previous_time_off_start, previous_time_off_end, 'off'])

    for row in list_iter:
        current_time_off_start = row[0]
        current_time_off_end = row[1]

        output_list.append([previous_time_off_end, current_time_off_start, 'on']) # the times covered by the periods inbetween the input rows
        output_list.append([current_time_off_start, current_time_off_end, 'off']) # the times covered by the periods of the input rows

        previous_time_off_end = current_time_off_end

    return output_list


def list_to_csv(list, output_file, header):
    """Output the list of data to a csv file with a specified header."""
    with open(output_file, 'w', ) as csvFile:
        writer = csv.writer(csvFile)
        writer.writerow(header)
        writer.writerows(list)

    csvFile.close()


def process_file(input_file):
    """Process the input csv file to produce an output list of time periods with a status of on or off. """
    off_periods = read_file_to_list(input_file)

    off_periods_datetime = change_format_from_input_to_datetime(off_periods)

    off_periods_collapsed = collapse_same_day_off(off_periods_datetime)

    off_periods_corrected_seconds = correct_off_seconds_same_minute(off_periods_collapsed)

    on_off_periods = process_to_on_off(off_periods_corrected_seconds)

    return on_off_periods


def main():
    input_file = '/home/carles/pump.csv'
    output_file = '/home/carles/pump_output.csv'

    on_off_periods = process_file(input_file)

    print(on_off_periods)
    print('total length: ', len(on_off_periods))

    header = ['start_datetime', 'end_datetime', 'status']
    list_to_csv(on_off_periods, output_file, header)


if __name__ == "__main__":
    main()