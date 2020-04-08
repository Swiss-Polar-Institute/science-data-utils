# Source of bathymetry data: GEBCO 30 second arc gridded bathymetry data: The GEBCO_2014 Grid, version 20150318, http://www.gebco.net

# This script was originally created to use the cruise track input data: ACE one-second resolution cruise track:
# Thomas, Jenny, & Pina Estany, Carles. (2019). Quality-checked, one-second cruise track of the Antarctic
# Circumnavigation Expedition (ACE) undertaken during the austral summer of 2016/2017.
# (Version 1.0) [Data set]. Zenodo. http://doi.org/10.5281/zenodo.3260616

# The script will run with any input csv file of coordinates as long as it contains the columns date_time, latitude and longitude.

# 30 arc-seconds, equivalent horizontal distances: https://www.ngdc.noaa.gov/mgg/topo/report/s6/s6A.html

import rasterstats
import csv
from progress_report import ProgressReport
import argparse
import os
import glob
import geojson
import subprocess


def get_list_of_shapefiles(input_cruise_track_data_dir):
    """Get directory of shapefiles and create a list of the files"""

    filepath = os.path.join(input_cruise_track_data_dir, "*.shp")
    file_list = glob.glob(filepath)

    file_list.sort()

    return file_list


def join_tifs(file_dir, input_filename, merged_file):
    """Join together a number of tif files into one merged file"""

    filepath = os.path.join(file_dir, input_filename)
    files = glob.glob(filepath)

    subprocess.run(['rasterio', 'merge'] + files + [merged_file])


def convert_csv_to_geojson(input_csvfile):
    """Convert a csv to GeoJSON format features, using the latitude and longitude in the file. Date_time in the csv
    file is also used as a property in the features that are created."""

    with open(input_csvfile) as csvfile:
        filereader = csv.DictReader(csvfile, delimiter=',', )

        features = []

        for row in filereader:
            point = geojson.Point((float(row['longitude']), float(row['latitude'])))
            feature = geojson.Feature(geometry=point, properties={'date_time': row['date_time']})
            features.append(feature)

        return features


def process_geojson_features(geojson_features, raster, header, csvfile):
    """Using a raster input, get values of a raster at features in geojson.
    Output the final points and depths to a csv file."""

    # write to csv file as depth found
    csv_writer = csv.writer(csvfile)
    csv_writer.writerow(header)

    # provide progress whilst processing: number given is the total number of features in the cruise track
    number_features_to_process = len(geojson_features)
    progress_report = ProgressReport(number_features_to_process)

    # process each of the shapefiles
    for feature in geojson_features:

        # get point values from raster at points in shapefile
        result = rasterstats.gen_point_query(feature, raster, geojson_out=True)

        # for each value obtained from the raster, output a line into a csvfile
        for r in result:
            csv_writer.writerow(
                [r['properties']['date_time'], r.geometry.coordinates[0], r.geometry.coordinates[1],
                 r['properties']['value']])

            progress_report.increment_and_print_if_needed()


def process_files(input_csvfile, input_gebco_data_dir, input_bathymetry_data_filename, output_merged_tif_filename, output_track_depth_filename):

    print("Converting csv file to geojson")
    geojson_features = convert_csv_to_geojson(input_csvfile)

    print('Creating merged tiff file')
    join_tifs(input_gebco_data_dir, input_bathymetry_data_filename, output_merged_tif_filename)

    header = ['date_time', 'latitude', 'longitude', 'depth']

    with open(output_track_depth_filename, 'w') as csvfile:
        process_geojson_features(geojson_features, output_merged_tif_filename, header, csvfile)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Get the input files and output required for calculating the depth along the cruise track.')
    parser.add_argument('input_csvfile', help='Csv file containing the cruise track', type=str)
    parser.add_argument('input_gebco_data_dir', help='Directory to the input GEBCO data, as individual tif files', type=str)
    parser.add_argument('input_bathymetry_data_filename', help='Filepath pattern of tif files eg. basename*.tif', type=str)
    parser.add_argument('output_merged_tif_filename', help='Filename to output the merged tif file into, in tif format', type=str)
    parser.add_argument('output_track_depth_filename', help='Filename to output the cruise track depth data into, in csv format', type=str)

    args = parser.parse_args()

    process_files(args.input_csvfile, args.input_gebco_data_dir, args.input_bathymetry_data_filename,
                  args.output_merged_tif_filename, args.output_track_depth_filename)