#!/usr/bin/env python3

import argparse
import datetime_to_position

import get_bottle_firing_times


def insert_text(original_line, new_text, column_number):
    assert column_number > 1
    assert len(new_text) < 11

    position = 22 + (column_number-2) * 11

    new_text = new_text.rjust(11)

    return original_line[:position]+new_text.encode("ascii") + original_line[position:]


def add_latitude_longitude(input_file_path, output_file_path):
    bottles_date_times = get_bottle_firing_times.get_bottles_datetime(input_file_path)

    output_file = open(output_file_path, "wb")

    locator = datetime_to_position.DatetimeToPosition()

    data_line = 1
    with open(input_file_path, "rb") as f:
        for line in f:
            if line.startswith(b"#") or line.startswith(b"*"):
                output_file.write(line)
                continue

            new_line = line
            if data_line == 1:
                new_line = insert_text(line, "LATITUDE", 2)
                new_line = insert_text(new_line, "LONGITUDE", 3)
            elif data_line == 2:
                pass
            else:
                if b"(avg)" in line:
                    information = line.decode("ascii").split(" ")
                    information = get_bottle_firing_times.list_without_empty_values(information)
                    bottle_number = int(information[0])

                    location = locator.datetime_datetime_to_position(bottles_date_times[bottle_number])

                    if location is None:
                        print("Position data do not exist for this date/time: ", input_file_path, "Date time: ", bottles_date_times[bottle_number])

                    latitude_float, longitude_float = locator.datetime_datetime_to_position(bottles_date_times[bottle_number])

                    latitude = "{:.5f}".format(latitude_float)
                    longitude = "{:.5f}".format(longitude_float)

                    new_line = insert_text(line, latitude, 2)
                    new_line = insert_text(new_line, longitude, 3)
                elif b"(sdev)" in line:
                    new_line = insert_text(line, "", 2)
                    new_line = insert_text(new_line, "", 3)
                else:
                    assert False

            output_file.write(new_line)

            data_line += 1

    output_file.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Read CTD file")
    parser.add_argument("input_ctd_file", help="CTD file")
    parser.add_argument("output_ctd_file", help="Will include latitude/longitude")

    args = parser.parse_args()

    add_latitude_longitude(args.input_ctd_file, args.output_ctd_file)
