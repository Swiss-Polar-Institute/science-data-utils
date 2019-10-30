# Source data: GEBCO 30 second arc gridded bathymetry data https://www.gebco.net/data_and_products/gridded_bathymetry_data/gebco_30_second_grid/ The GEBCO_2014 Grid, version 20150318, http://www.gebco.net
# ACE one-second resolution cruise track: https://doi.org/10.5281/zenodo.3260616
# 30 arc-seconds, equivalent horizontal distances: https://www.ngdc.noaa.gov/mgg/topo/report/s6/s6A.html

import rasterstats
import csv
from progress_report import ProgressReport


def process_list_of_shapefiles(shapefile_list, raster, header, csvfile):

    # write to csv file as depth produced
    csv_writer = csv.writer(csvfile)
    csv_writer.writerow(header)

    progress_report = ProgressReport(8_576_402)

    for shapefile in shapefile_list:
        print('------------- Processing', shapefile, ' -------------')

        result = rasterstats.gen_point_query(shapefile, raster, geojson_out=True)

        for r in result:
            csv_writer.writerow(
                [r['properties']['date_time'], r['properties']['latitude'], r['properties']['longitude'],
                 r['properties']['value']])
            progress_report.increment_and_print_if_needed()


def main():

    raster_joined = '/home/jen/projects/ace_data_management/external_data/map_bathymetry/gebco/GEBCO2014_geotiff_joined.tif'

    shapefile_201612 = '/home/jen/projects/ace_data_management/mapping/data/ace_cruise_track_1sec_2016-12.shp'
    shapefile_201701 = '/home/jen/projects/ace_data_management/mapping/data/ace_cruise_track_1sec_2017-01.shp'
    shapefile_201702 = '/home/jen/projects/ace_data_management/mapping/data/ace_cruise_track_1sec_2017-02.shp'
    shapefile_201703 = '/home/jen/projects/ace_data_management/mapping/data/ace_cruise_track_1sec_2017-03.shp'

    shapefile_list = [shapefile_201612, shapefile_201701, shapefile_201702, shapefile_201703]

    header = ['longitude', 'latitude', 'depth_m']
    csvfile_out = '/home/jen/projects/ace_data_management/mapping/data/ace_cruise_track_1sec_with_depth.csv'

    with open(csvfile_out, 'w') as csvfile:
        process_list_of_shapefiles(shapefile_list, raster_joined, header, csvfile)


if __name__ == "__main__":
    main()