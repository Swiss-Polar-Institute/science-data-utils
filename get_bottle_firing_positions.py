#!/usr/bin/env python3

import os
import argparse
import datetime
import calendar
import csv

def list_without_empty_values(l):
    return list(filter(None, l))

def get_bottles_datetime(ctd_file):
    print("ctd_file", ctd_file)

    data_line_number = 0

    bottle_date_time = {}

    month_to_number = {v: k for k, v in enumerate(calendar.month_abbr)}

    with open(ctd_file) as f:
        for line in f:
            line = line.rstrip()

            if line.startswith("*") or line.startswith("#"):
                continue

            data_line_number += 1

            if data_line_number == 1:
                column_labels = line.split(" ")
                column_labels = list_without_empty_values(column_labels)
                continue

            elif data_line_number == 2:
                extra_labels = line.split(" ")
                extra_labels = list_without_empty_values(extra_labels)
                continue

            line_fields = line.split(" ")
            line_fields = list_without_empty_values(line_fields)

            len_line_fields = len(line_fields)
            assert len_line_fields == 32 or len_line_fields == 23

            if len_line_fields == 32:
                current_bottle = {}
                current_bottle['number'] = int(line_fields[0])
                current_bottle['month'] = month_to_number[line_fields[1]]
                current_bottle['day'] = int(line_fields[2])
                current_bottle['year'] = int(line_fields[3])

            if len_line_fields == 23:
                current_bottle['time'] = line_fields[0]

                # Converts the bottle information to date_time
                hour, minute, second = current_bottle['time'].split(":")

                d = datetime.datetime(current_bottle['year'], current_bottle['month'], current_bottle['day'],
                                  int(hour), int(minute), int(second))

                bottle_date_time[current_bottle['number']] = d

    return bottle_date_time


def write_to_file(output_file, bottle_date_time):
    with open(output_file, "w") as csvfile:
        csvwriter = csv.writer(csvfile)

        bottle_numbers = sorted(bottle_date_time.keys())

        for bottle_number in bottle_numbers:
            csvwriter.writerow([bottle_number, bottle_date_time[bottle_number]])

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Read CTD file")
    parser.add_argument("input_ctd_file", help="CTD file")
    parser.add_argument("output_bottle_datetime", help="Output file with bottle and datetime")

    args = parser.parse_args()

    bottle_date_time = get_bottles_datetime(args.input_ctd_file)
    write_to_file(args.output_bottle_datetime, bottle_date_time)
