#!/usr/bin/python3

# This notebook imports the motion data collected on board the R/V Akademik Tryoshnikov during the Antarctic Circumnavigation Expedition (ACE).
 
# Import required packages

import csv
import os
import pandas
import datetime
import time
import io
import numpy as np

# Set up the hard-coded variables to import the data.

input_data_folder = "/home/jen/projects/ace_data_management/wip/motion_data/raw/"
output_data_folder = "/home/jen/projects/ace_data_management/wip/motion_data/check_20200326/"

### List data files

# Get the set of raw motion data files in a list.

def get_input_txt_files(input_data_folder):
    """Create a list of files in a directory and put them into a list."""
    
    list_data_files = []
    
    os.chdir(input_data_folder)
    directory_path = os.getcwd()
    
    for filename in os.listdir(input_data_folder):
        if filename.endswith(".txt"):
            fullpath = directory_path + "/" + filename
            list_data_files.append(fullpath)
    
    return list_data_files


### Check file headers

# The expected header is defined in order to compare that within each file (note that this ignores the first four lines).


def check_file_header(list_data_files): 
    """Check that the header for each of the files in the list is as expected. If it is correct, add one to the number of correct headers that is output and if incorrect, output the filename and a copy of the header line. Also add one to the number of incorrect headers."""
    
    correct_headers = 0
    incorrect_headers = 0
    total_number_files = len(list_data_files)
    
    total_number_headers = 0
    for file in list_data_files:
        total_number_headers += 1
        print("Checking the header of file", total_number_headers, "out of", total_number_files)
        
        with open(file, 'r', encoding="ISO-8859-1") as csvfile: # encoding that of original files - required because of degrees characters
            contents = csv.reader(csvfile, delimiter='\t')
        
            line_number = 0
        
            for line in contents:
                if line_number == 4:
                    if line != expected_header:
                        print("Wrong header: ", file, "  ", line)
                        incorrect_headers += 1
                    else:
                        correct_headers += 1
                
                line_number += 1
    
    total_no_files = correct_headers + incorrect_headers
    
    print("Correct headers: ", correct_headers)
    print("Incorrect headers: ", incorrect_headers)
    print("Total number of files: ", total_no_files)


### Get the date of the data collection and read the data into a list


def read_motion_file_date(filename):
    """Read a file and get the date from the second line in the header. Output the date in the given format."""
    
    with open(filename, 'r', encoding="ISO-8859-1") as data_file: # need encoding because of degree characters
        data_file.readline() # skips first line
        date_line = data_file.readline()
        
        date = date_line.split("\t")[1]
        
        return datetime.datetime.strptime(date, "%d/%m/%Y")
    
    
def optimise_line(line):
    """Convert the values in a line of data that look like numbers, to floats (to optimise the memory usage and make the next stage more efficient). If the value is not a number, then leave it in its original format."""
    for i, value in enumerate(line):
        try:
            line[i] = float(line[i])
        except ValueError:
            pass  
        

def data_to_list(filename, rows_of_data):
    """Read files from a list of files, get the date from each file, then append the date to each line within the file as the line of data is read into a list. Output a list of data from all of the files."""

    date_of_file = read_motion_file_date(filename)

    with open(filename, 'r', encoding="ISO-8859-1") as data_file:
        contents = csv.reader(data_file, delimiter='\t')
        for i in range(5):
            next(contents)

        previous_time = None
        row_count = 0
        for line in contents:

            time_now = time.strptime(line[0], "%H:%M:%S.%f")
            if previous_time is None:
                current_date = date_of_file
            elif time_now < previous_time:
                current_date = current_date + datetime.timedelta(days=1)

            line.insert(0, current_date.strftime("%Y-%m-%d"))
            previous_time = time_now
            optimise_line(line)
            rows_of_data.append(line)

            row_count += 1

        #print(filename, "contains a total number of rows:", row_count)

    return rows_of_data


### Define the column headers


# Get the header for the columns so that this can be assigned to the data frame. Currently this is listed in a csv file so that it can be changed and reimported as necessary. The data contains more columns than are listed in the header in the data files, so we need to ensure that these are assigned correctly in the pandas data frame. 


def define_column_headers(header_file):
    """Import a list of column headers from a csv file and output them as a list to be ready to be set as the column headers for a pandas data frame."""

    header = []

    with open(header_file) as headerfile:
        contents = csv.reader(headerfile)
        header_list = list(contents)
    
        for item in header_list: 
            header.append(item[0])
            
    return header


### Create data frame and import data

def data_to_dataframe(rows_of_data, dataframe, header):
    """Import a list of data into a pandas dataframe and use a list of column names as the header."""
    
    dataframe = dataframe.append(pandas.DataFrame(rows_of_data, columns = header), ignore_index = True)
    
    return dataframe


# Optimise memory usage in the dataframe by converting float64 to float32 (uses less bytes per digit).

# The code below was taken from https://www.kaggle.com/arjanso/reducing-dataframe-memory-size-by-65 and is used to convert the datatype to one that uses less memory.


def reduce_memory_usage(props):
    """Takes a dataframe and converts the data type of each float to float64 (oroignally did it to float32 but wanted more dp for lat and lon, reducing the memory usage."""
    
    #start_mem_usg = props.memory_usage().sum() / 1024**2 
    #print("Memory usage of properties dataframe is :",start_mem_usg," MB")
    NAlist = [] # Keeps track of columns that have missing values filled in. 
    for col in props.columns:
        if props[col].dtype != object:  # Exclude strings
            
            # Print current column type
            #print("******************************")
            #print("Column: ",col)
            #print("dtype before: ",props[col].dtype)
            
            # make variables for Int, max and min
            IsInt = False
            mx = props[col].max()
            mn = props[col].min()
            
            # Integer does not support NA, therefore, NA needs to be filled
            if not np.isfinite(props[col]).all(): 
                NAlist.append(col)
                props[col].fillna(mn-1,inplace=True)  
                   
            # test if column can be converted to an integer
            asint = props[col].fillna(0).astype(np.int64)
            result = (props[col] - asint)
            result = result.sum()
            if result > -0.01 and result < 0.01:
                IsInt = True

            
            # Make Integer/unsigned Integer datatypes
            if IsInt:
                if mn >= 0:
                    if mx < 255:
                        props[col] = props[col].astype(np.uint8)
                    elif mx < 65535:
                        props[col] = props[col].astype(np.uint16)
                    elif mx < 4294967295:
                        props[col] = props[col].astype(np.uint32)
                    else:
                        props[col] = props[col].astype(np.uint64)
                else:
                    if mn > np.iinfo(np.int8).min and mx < np.iinfo(np.int8).max:
                        props[col] = props[col].astype(np.int8)
                    elif mn > np.iinfo(np.int16).min and mx < np.iinfo(np.int16).max:
                        props[col] = props[col].astype(np.int16)
                    elif mn > np.iinfo(np.int32).min and mx < np.iinfo(np.int32).max:
                        props[col] = props[col].astype(np.int32)
                    elif mn > np.iinfo(np.int64).min and mx < np.iinfo(np.int64).max:
                        props[col] = props[col].astype(np.int64)    
            
            # Make float datatypes 32 bit
            else:
                props[col] = props[col].astype(np.float64) # changed this from float32 to avoid losing the accuracy of the latitude and longitude
            
            # Print new column type
            #print("dtype after: ",props[col].dtype)
            #print("******************************")
            
    # Print final result
    #print("___MEMORY USAGE AFTER COMPLETION:___")
    #mem_usg = props.memory_usage().sum() / 1024**2 
    #print("Memory usage is: ",mem_usg," MB")
    #print("This is ",100*mem_usg/start_mem_usg,"% of the initial size")
    return props, NAlist


### Output the data into csv files with one file per date.


def output_daily_files(dataframe, output_data_folder):
    """Output a pandas dataframe to a files where each file contains one day's worth of data. Output the data file to the output data folder."""
    
    output_filename_base = 'ace_hydrins_'

    date_group = dataframe.groupby('pc_date_utc')
    print("Aggregated groups by date with counts:")
    print(dataframe.groupby('pc_date_utc').size())
    print("\nTotal number of records:")
    print(dataframe.groupby('pc_date_utc').size().sum())
    
    for date in date_group.groups:
        date_formatted = datetime.datetime.strptime(date, "%Y-%m-%d")    

        date_string = date_formatted.strftime('%Y%m%d')
       
        output_filename = output_data_folder + output_filename_base + date_string + ".csv"
        
        date_group.get_group(date).to_csv(output_filename, sep=",", header=True, index=False)
        print(date, "file created") # TODO put a better check here that the file exists


### Check that the output files have the same number of data rows as the dataframe had per day.


def get_input_files(input_data_folder):
    
    list_data_files = []
    
    os.chdir(input_data_folder)
    directory_path = os.getcwd()
    
    for filename in os.listdir(input_data_folder):
        if filename.startswith("ace_hydrins_"):
            fullpath = directory_path + "/" + filename
            list_data_files.append(fullpath)
    
    return list_data_files


def check_rows_in_file(list_data_files):

    total_rows = 0
    for filepath in list_data_files:
        filename = os.path.basename(filepath)
        filedate = (filename.split('_')[-1]).split('.')[0] 

        with open(filepath, 'r') as csvfile:
            contents = csv.reader(csvfile)
            next(contents)

            row_count = 0
            for line in contents:
                row_count += 1

            print(filedate, " ", row_count)
        
        total_rows += row_count
    
    print("Total number of rows in files: ", total_rows)


if __name__ == "__main__":

    expected_header = ['Pc - HH:MM:SS.SSS', 'Hydrins - HH:MM:SS.SSS', 'Heading (°)', 'Roll (°)', 'Pitch (°)', 'Heading std. dev. (°)', 'Roll std. dev. (°)', 'Pitch std. dev. (°)', 'North speed (m/s)', 'East speed (m/s)', 'Vert. speed (m/s)', 'Speed norm (knots)', 'North speed std. dev. (m/s)', 'East speed std. dev. (m/s)', 'Vert. speed std. dev. (m/s)', 'Latitude (°)', 'Longitude (°)', 'Altitude (m)', 'Latitude std. dev. (m)', 'Longitude std. dev. (m)', 'Altitude std. dev. (m)', 'Zone I', 'Zone C', 'UTM North (m)', 'UTM East (m)', 'UTM altitude  (m)', 'High level status', 'System status 1', 'System status 2', 'Algo status 1', 'Algo status 2', 'GPS - Latitude (°)', 'GPS - Longitude (°)', 'GPS - Altitude (m)', 'GPS - Mode', 'GPS - Time', 'Manual GPS - Latitude (°)', 'Manual GPS - Longitude (°)', 'Manual GPS - Altitude (m)', 'Manual GPS - Latitude std. dev.', 'Manual GPS - Longitude std. dev.', 'Manual GPS - Altitude std. dev.', '']    

    header_file = "/home/jen/projects/ace_data_management/wip/motion_data/file_header.csv"
    header = define_column_headers(header_file)

    #test_input_data_folder = "/home/jen/projects/ace_data_management/ship_data/motion_data/test/"

    list_motion_data_files = get_input_txt_files(input_data_folder)
    print(len(list_motion_data_files))

    print("Checking headers of files")
    print("\n")

    check_file_header(list_motion_data_files)
 
    print("List contains ",  len(list_motion_data_files), "files")

    rows_of_data = list()
    motiondf = pandas.DataFrame(columns = header)

    no_files_processed = 0
    for filename in list_motion_data_files:
        print(filename)
        total_no_files = len(list_motion_data_files)
    
        data_to_list(filename, rows_of_data)
        motiondf = data_to_dataframe(rows_of_data, motiondf, header)
        rows_of_data = list()
    
        reduce_memory_usage(motiondf)
    
        no_files_processed += 1
        print("Processed", no_files_processed, "out of", total_no_files)
   
#    Pickle the dataframe (output it to a file to remove it from the memory) and check the memory usage again.

    motiondf.to_pickle(output_data_folder + "motiondf.pkl")

    motiondf.reset_index(drop=True, inplace=True)
    motiondf.to_pickle(output_data_folder + "motiondf_noindex.pkl") 

# Output the data files from the dataframe into daily files. 
 
    motiondf = pandas.read_pickle(output_data_folder + 'motiondf_noindex.pkl')

    output_daily_files(motiondf, output_data_folder)

# Check the output files

    data_folder = "/home/jen/projects/ace_data_management/wip/motion_data/check_20200326/"

    list_data_files_to_check = get_input_files(data_folder)

    check_rows_in_file(list_data_files_to_check)
