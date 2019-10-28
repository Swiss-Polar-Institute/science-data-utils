# Source data: GEBCO 30 second arc gridded bathymetry data https://www.gebco.net/data_and_products/gridded_bathymetry_data/gebco_30_second_grid/ The GEBCO_2014 Grid, version 20150318, http://www.gebco.net
# ACE one-second resolution cruise track: https://doi.org/10.5281/zenodo.3260616
# 30 arc-seconds, equivalent horizontal distances: https://www.ngdc.noaa.gov/mgg/topo/report/s6/s6A.html


from osgeo import gdal
import rasterio
import rasterstats
import geopandas as gpd

raster_file1 = '/home/jen/projects/ace_data_management/external_data/map_bathymetry/gebco/GEBCO2014_0.0_-90.0_90.0_-30.0_30Sec_Geotiff.tif'
raster_file2 = '/home/jen/projects/ace_data_management/external_data/map_bathymetry/gebco/GEBCO2014_-180.0_-90.0_-90.0_-30.0_30Sec_Geotiff.tif'
raster_file3 = '/home/jen/projects/ace_data_management/external_data/map_bathymetry/gebco/GEBCO2014_-90.0_-90.0_0.0_-30.0_30Sec_Geotiff.tif'
raster_file4 = '/home/jen/projects/ace_data_management/external_data/map_bathymetry/gebco/GEBCO2014_90.0_-90.0_180.0_-30.0_30Sec_Geotiff.tif'

raster_joined = '/home/jen/projects/ace_data_management/external_data/map_bathymetry/gebco/GEBCO2014_geotiff_joined.tif'

shapefile_201612='/home/jen/projects/ace_data_management/mapping/data/ace_cruise_track_1hour_2016-12.shp'
shapefile_201701='/home/jen/projects/ace_data_management/mapping/data/ace_cruise_track_1hour_2017-01.shp'
shapefile_201702='/home/jen/projects/ace_data_management/mapping/data/ace_cruise_track_1hour_2017-02.shp'
shapefile_201703='/home/jen/projects/ace_data_management/mapping/data/ace_cruise_track_1hour_2017-03.shp'
shapefile_201704='/home/jen/projects/ace_data_management/mapping/data/ace_cruise_track_1hour_2017-04.shp'

shapefile_list = [shapefile_201612, shapefile_201701, shapefile_201702, shapefile_201703]

# Join all tiff together


points = gpd.read_file(shapefile_201612)

for point in points:
    print(point)
    rasterstats.point_query(point, raster_joined)

print('------------------------------')

for shapefile in shapefile_list:
    pts = rasterstats.point_query(shapefile, raster_joined)
    print(pts)
