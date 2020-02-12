import netCDF4
import csv

from progress_report import ProgressReport
from netCDF4 import Dataset, num2date, date2index


def near(array, value):
    idx = (abs(array - value)).argmin()
    return idx


def process_cruise_track(track_data_list, netcdf_file, header, csvfile):

    # write to csv file as depth produced
    csv_writer = csv.writer(csvfile)
    csv_writer.writerow(header)

    progress_report = ProgressReport(8_576_402)

    for track_file in track_data_list:
        print('------------- Processing', track_file, ' -------------')

        get_depth_along_track_from_netcdf(netcdf_file, track_file, csv_writer, progress_report)



def get_depth_along_track_from_netcdf(netcdf_file, track_data, csv_writer, progress_report):

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

        # get the indexes of the lat and lon to find the corresponding value of depth in the netcdf file. This finds
        # the nearest lat and lon as there is unlikely to be the exact values in the netcdf file.
        ilat = near(netcdf_data.variables['lat'][:], latitude)
        ilon = near(netcdf_data.variables['lon'][:], longitude)

        # get the corresponding depth value and convert it to a float from a numpy masked array
        depth = float(netcdf_data.variables['bedrock_topography'][ilat, ilon])

        result = [date_time, latitude, longitude, depth]

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