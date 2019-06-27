#!/bin/bash

INPUT_BOTTLE_FILE_PATH="$1"
OUTPUT_BOTTLE_FILE_PATH="$2"
OUTPUT_MATCHED_POSITIONS_FILE_PATH="$3"
LAT_LONG_INPUT_FILE_PATH="$4"

LAT_LONG_RESOLUTION="sec"

for bottle_file in "$INPUT_BOTTLE_FILE_PATH/"*
do
    echo "$bottle_file"

    bottle_file_basename_no_suffix=$(basename $bottle_file .btl)
    bottles_with_date_time="$OUTPUT_BOTTLE_FILE_PATH/$bottle_file_basename_no_suffix.csv"
    ./get_bottle_firing_times.py "$bottle_file" "$bottles_with_date_time"

    output_matched_positions_filename="$bottle_file_basename_no_suffix-positions.csv"

    ./get_positions.py "$bottles_with_date_time" "$LAT_LONG_INPUT_FILE_PATH" "$LAT_LONG_RESOLUTION" "$OUTPUT_MATCHED_POSITIONS_FILE_PATH" "$ouput_matched_positions_filename"
done
