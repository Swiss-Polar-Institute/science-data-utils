# Source data: GEBCO 30 second arc gridded bathymetry data: The GEBCO_2014 Grid, version 20150318, http://www.gebco.net

# ACE one-second resolution cruise track: Thomas, Jenny, & Pina Estany, Carles. (2019). Quality-checked, one-second
# cruise track of the Antarctic Circumnavigation Expedition (ACE) undertaken during the austral summer of 2016/2017.
# (Version 1.0) [Data set]. Zenodo. http://doi.org/10.5281/zenodo.3260616

# 30 arc-seconds, equivalent horizontal distances: https://www.ngdc.noaa.gov/mgg/topo/report/s6/s6A.html

import rasterstats
import csv
from progress_report import ProgressReport
import argparse
import os
import glob
import rasterio
import subprocess

def get_list_of_shapefiles(input_cruise_track_data_dir):
    """Get directory of shapefiles and create a list of the files"""
    # /home/jen/projects/ace_data_management/data_to_archive_post_cruise/cruise_track/shapefiles

    filepath = os.path.join(input_cruise_track_data_dir, "*.shp")
    file_list = glob.glob(filepath)

    return file_list


def join_tifs(file_dir, input_filename, merged_file):
    """Join together a number of tif files into one merged file"""

    filepath = os.path.join(file_dir, input_filename)
    files = glob.glob(filepath)

    subprocess.run(['rasterio', 'merge'] + files + [merged_file])



def process_list_of_shapefiles(shapefile_list, raster, header, csvfile):
    """Using a raster input, get values from raster at points in shapefile and output to csvfile"""

    # write to csv file as depth produced
    csv_writer = csv.writer(csvfile)
    csv_writer.writerow(header)

    # provide progress whilst processing: number given is the total number of lines in the shapefiles
    progress_report = ProgressReport(10_326_939)

    # process each of the shapefiles
    for shapefile in shapefile_list:
        print('------------- Processing', shapefile, ' -------------')

        # get point values from raster at points in shapefile
        result = rasterstats.gen_point_query(shapefile, raster, geojson_out=True)

        # for each value obtained from the raster, output a line into a csvfile
        for r in result:
            csv_writer.writerow(
                [r['properties']['date_time'], r['properties']['latitude'], r['properties']['longitude'],
                 r['properties']['value']])
            progress_report.increment_and_print_if_needed()


def process_files(input_cruise_track_data_dir, input_gebco_data_dir, input_bathymetry_data_filename, output_merged_tif_filename, output_track_depth_filename):

    print('Creating list of shapefiles')
    shapefile_list = get_list_of_shapefiles(input_cruise_track_data_dir)

    # testing tif join
    # file_dir = "/home/jen/projects/ace_data_management/external_data/map_bathymetry/gebco/GEBCO_2019_12_Nov_2019_356b1e29d3e1/"
    # input_filename = "gebco_2019_n-30.0_s-90.0*.tif"
    # merged_file = "/home/jen/merged_20200406_2.tif"

    print('Creating merged tiff file')
    join_tifs(input_gebco_data_dir, input_bathymetry_data_filename, output_merged_tif_filename)

    # use one raster file that contains all data
    # raster_joined = '/home/jen/projects/ace_data_management/external_data/map_bathymetry/gebco/GEBCO_2019_12_Nov_2019_356b1e29d3e1/gebco_2019_joined.tif'
    #
    # shapefile_201612 = '/home/jen/projects/ace_data_management/mapping/data/ace_cruise_track_1sec_2016-12.shp'
    # shapefile_201701 = '/home/jen/projects/ace_data_management/mapping/data/ace_cruise_track_1sec_2017-01.shp'
    # shapefile_201702 = '/home/jen/projects/ace_data_management/mapping/data/ace_cruise_track_1sec_2017-02.shp'
    # shapefile_201703 = '/home/jen/projects/ace_data_management/mapping/data/ace_cruise_track_1sec_2017-03.shp'

    # shapefile_list = [shapefile_201612, shapefile_201701, shapefile_201702, shapefile_201703]

    header = ['date_time', 'latitude', 'longitude', 'depth_m']
    # csvfile_out = '/home/jen/projects/ace_data_management/mapping/data/ace_cruise_track_1sec_with_depth_gebco2019.csv'

    with open(output_track_depth_filename, 'w') as csvfile:
        process_list_of_shapefiles(shapefile_list, output_merged_tif_filename, header, csvfile)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Get the input files and output required for calculating the depth along the cruise track.')
    parser.add_argument('input_cruise_track_data_dir', help='Directory containing the input cruise track files, in csv format', type=str)
    parser.add_argument('input_gebco_data_dir', help='Directory to the input GEBCO data, as individual tif files', type=str)
    parser.add_argument('input_bathymetry_data_filename', help='Filepath pattern of tif files eg. basename*.tif', type=str)
    parser.add_argument('output_merged_tif_filename', help='Filename to output the merged tif file into, in tif format', type=str)
    parser.add_argument('output_track_depth_filename', help='Filename to output the cruise track depth data into, in csv format', type=str)

    args = parser.parse_args()

    process_files(args.input_cruise_track_data_dir, args.input_gebco_data_dir, args.input_bathymetry_data_filename,
                  args.output_merged_tif_filename, args.output_track_depth_filename)