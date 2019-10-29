# Source data: GEBCO 30 second arc gridded bathymetry data https://www.gebco.net/data_and_products/gridded_bathymetry_data/gebco_30_second_grid/ The GEBCO_2014 Grid, version 20150318, http://www.gebco.net
# ACE one-second resolution cruise track: https://doi.org/10.5281/zenodo.3260616
# 30 arc-seconds, equivalent horizontal distances: https://www.ngdc.noaa.gov/mgg/topo/report/s6/s6A.html

import rasterstats
import ogr
import geojson
import csv


def get_gebco_depth_from_shapefile_points(shapefile, raster, header, csvfile):
    # Code below extract from https://gis.stackexchange.com/questions/46893/getting-pixel-value-of-gdal-raster-under-ogr-point-without-numpy

    # write to csv file as depth produced
    csv_writer = csv.writer(csvfile)
    csv_writer.writerow(header)

    # open shapefile and get points layer at which we want to get the seafloor depth
    shapefile_dataset = ogr.Open(shapefile)
    point_layer = shapefile_dataset.GetLayer()

    # for each point layer get the point and then depth
    for feat in point_layer:
        geom = feat.GetGeometryRef()
        lon, lat = geom.GetX(), geom.GetY()

        depth = rasterstats.point_query(geojson.Point((lon, lat)), raster)[0]

        csv_writer.writerow([lon, lat, depth])


def process_list_of_shapefiles(shapefile_list, raster, header, csvfile):

    for shapefile in shapefile_list:
        print('Processing ', shapefile)
        get_gebco_depth_from_shapefile_points(shapefile, raster, header, csvfile)


def main():

    raster_joined = '/home/jen/projects/ace_data_management/external_data/map_bathymetry/gebco/GEBCO2014_geotiff_joined.tif'

    shapefile_201612 = '/home/jen/projects/ace_data_management/mapping/data/ace_cruise_track_1sec_2016-12.shp'
    shapefile_201701 = '/home/jen/projects/ace_data_management/mapping/data/ace_cruise_track_1sec_2017-01.shp'
    shapefile_201702 = '/home/jen/projects/ace_data_management/mapping/data/ace_cruise_track_1sec_2017-02.shp'
    shapefile_201703 = '/home/jen/projects/ace_data_management/mapping/data/ace_cruise_track_1sce_2017-03.shp'

    shapefile_list = [shapefile_201612, shapefile_201701, shapefile_201702, shapefile_201703]

    header = ['longitude', 'latitude', 'depth_m']
    csvfile_out = '/home/jen/projects/ace_data_management/mapping/data/ace_cruise_track_1sec_with_depth.csv'

    with open(csvfile_out, 'w') as csvfile:
        process_list_of_shapefiles(shapefile_list, raster_joined, header, csvfile)


if __name__ == "__main__":
    main()