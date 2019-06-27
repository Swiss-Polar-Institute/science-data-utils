#!/bin/bash


INPUT_BOTTLE_FILE_PATH=$1
OUTPUT_BOTTLE_FILE_DIR=$2

for file in "$INPUT_BOTTLE_FILE_PATH"/*.btl
do
        name=$(basename "$file")
        ./ctd_bottle_files_add_latitude_longitude.py "$file" "$OUTPUT_BOTTLE_FILE_DIR/$name"
        echo $file DONE

done
