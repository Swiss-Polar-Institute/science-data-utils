# Source bathymetry RTopo2.0.4 data set: Schaffer, Janin; Timmermann, Ralph; Arndt, Jan Erik; Rosier, Sebastian H R;
# Anker, Paul G D; Callard, S Louise; Davis, Peter E D; Dorschel, Boris; Grob, Henrik; Hattermann, Tore; Hofstede,
# Coen M; Kanzow, Torsten; Kappelsberger, Maria; Lloyd, Jerry M; Ó'Cofaigh, Colm; Roberts, David H (2019): An update
# to Greenland and Antarctic ice sheet topography, cavity geometry, and global bathymetry (RTopo-2.0.4). PANGAEA,
# https://doi.org/10.1594/PANGAEA.905295

# ACE one-second resolution cruise track: Thomas, Jenny, & Pina Estany, Carles. (2019). Quality-checked, one-second
# cruise track of the Antarctic Circumnavigation Expedition (ACE) undertaken during the austral summer of 2016/2017.
# (Version 1.0) [Data set]. Zenodo. http://doi.org/10.5281/zenodo.3260616

import csv

from progress_report import ProgressReport
from netCDF4 import Dataset


def near(array, value):
    """Find the nearest point within the array and return its index"""

    idx = (abs(array - value)).argmin()
    return idx


def process_cruise_track(track_data_list, netcdf_file, header, csvfile):
    """Process each cruise track file, get the depth from the NetCDF file and output it into a csvfile"""

    # write to csv file as depth produced
    csv_writer = csv.writer(csvfile)
    csv_writer.writerow(header)

    # provide progress whilst processing: number given is the total number of lines in the shapefiles
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


if __name__ == '__main__':
    # input data
    netcdf_file = '/home/jen/projects/ace_data_management/wip/bathymetry_cruise_track/RTopo2/RTopo-2.0.4_30sec_bedrock_topography.nc'
    track_data_list = ['/home/jen/projects/ace_data_management/data_to_archive_post_cruise/cruise_track/ace_cruise_track_1sec/ace_cruise_track_1sec_2016-12.csv',
                       '/home/jen/projects/ace_data_management/data_to_archive_post_cruise/cruise_track/ace_cruise_track_1sec/ace_cruise_track_1sec_2017-01.csv',
                       '/home/jen/projects/ace_data_management/data_to_archive_post_cruise/cruise_track/ace_cruise_track_1sec/ace_cruise_track_1sec_2017-02.csv',
                       '/home/jen/projects/ace_data_management/data_to_archive_post_cruise/cruise_track/ace_cruise_track_1sec/ace_cruise_track_1sec_2017-03.csv',
                       '/home/jen/projects/ace_data_management/data_to_archive_post_cruise/cruise_track/ace_cruise_track_1sec/ace_cruise_track_1sec_2017-04.csv']

    # output data
    header = ['date_time', 'latitude', 'longitude', 'depth_m']
    output_file = '/home/jen/projects/ace_data_management/mapping/data/ace_cruise_track_1sec_with_depth_rtopo2.0.4.csv'

    with open(output_file, 'w') as csvfile:
        process_cruise_track(track_data_list, netcdf_file, header, csvfile)