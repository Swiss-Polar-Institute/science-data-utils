# Source bathymetry RTopo2.0.4 data set: Schaffer, Janin; Timmermann, Ralph; Arndt, Jan Erik; Rosier, Sebastian H R;
# Anker, Paul G D; Callard, S Louise; Davis, Peter E D; Dorschel, Boris; Grob, Henrik; Hattermann, Tore; Hofstede,
# Coen M; Kanzow, Torsten; Kappelsberger, Maria; Lloyd, Jerry M; Ó'Cofaigh, Colm; Roberts, David H (2019): An update
# to Greenland and Antarctic ice sheet topography, cavity geometry, and global bathymetry (RTopo-2.0.4). PANGAEA,
# https://doi.org/10.1594/PANGAEA.905295

# This script was originally created to use the cruise track input data: ACE one-second resolution cruise track: Thomas,
# Jenny, & Pina Estany, Carles. (2019). Quality-checked, one-second cruise track of the Antarctic Circumnavigation
# Expedition (ACE) undertaken during the austral summer of 2016/2017.
# (Version 1.0) [Data set]. Zenodo. http://doi.org/10.5281/zenodo.3260616

import csv
from progress_report import ProgressReport
from netCDF4 import Dataset
import argparse
import os
import glob


def near(array, value):
    """Find the nearest point within the array and return its index"""

    idx = (abs(array - value)).argmin()
    return idx


def list_files_to_process(dir_cruise_track_files, cruise_track_filename_pattern):

    filepath = os.path.join(dir_cruise_track_files, cruise_track_filename_pattern)
    file_list = glob.glob(filepath)

    file_list.sort()

    return file_list


def process_cruise_track(track_data_list, netcdf_file, header, csvfile):
    """Process each cruise track file, get the depth from the NetCDF file and output it into a csvfile"""

    # write to csv file as depth produced
    csv_writer = csv.writer(csvfile)
    csv_writer.writerow(header)

    # provide progress whilst processing: number given is the total number of lines in the input cruise track files
    progress_report = ProgressReport(9_499_414)

    # process each of the track files
    for track_file in track_data_list:
        print('------------- Processing', track_file, ' -------------')

        get_depth_along_track_from_netcdf(netcdf_file, track_file, csv_writer, progress_report)


def get_depth_along_track_from_netcdf(netcdf_file, track_data, csv_writer, progress_report):
    """For each point get the depth from the NetCDF file and output it in a csv file"""

    # get the cruise track points
    csv_file = open(track_data)
    first = True

    # read the netcdf file
    netcdf_data = Dataset(netcdf_file, mode='r')

    # for each track point, get the depth from the netcdf file
    for line in csv_file.readlines():
        if first:
            first = False
            continue

        # each input line is formatted datetime, lat, lon
        sline = line.split(',')
        date_time = sline[0]
        latitude = float(sline[1])
        longitude = float(sline[2])

        # get the indexes of the lat and lon to find the corresponding value of depth in the NetCDF file. This finds
        # the nearest lat and lon as there is unlikely to be the exact values in the NetCDF file. Return the indices.
        ilat = near(netcdf_data.variables['lat'][:], latitude)
        ilon = near(netcdf_data.variables['lon'][:], longitude)

        # get the corresponding depth value and convert it to a float from a numpy masked array
        depth = float(netcdf_data.variables['bedrock_topography'][ilat, ilon])

        result = [date_time, latitude, longitude, depth]

        # write the result to the csv file
        csv_writer.writerow(result)
        progress_report.increment_and_print_if_needed()


def process_files(input_netcdf_file, dir_cruise_track_files, cruise_track_filename_pattern, output_track_depth_filename):

    # input cruise track files
    track_data_list = list_files_to_process(dir_cruise_track_files, cruise_track_filename_pattern)

    # output data
    header = ['date_time', 'latitude', 'longitude', 'depth_m']

    with open(output_track_depth_filename, 'w') as csvfile:
        process_cruise_track(track_data_list, input_netcdf_file, header, csvfile)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Get the input files and output required for calculating the depth along the cruise track.')
    parser.add_argument('input_netcdf_file', help='NetCDF file containing RTopo data', type=str)
    parser.add_argument('dir_cruise_track_files', help='Directory path to input cruise track csv files', type=str)
    parser.add_argument('cruise_track_filename_pattern', help='Pattern to match cruise track filenames', type=str)
    parser.add_argument('output_track_depth_filename', help='Filename to output the cruise track depth data into, in csv format', type=str)

    args = parser.parse_args()

    process_files(args.input_netcdf_file, args.dir_cruise_track_files, args.cruise_track_filename_pattern, args.output_track_depth_filename)