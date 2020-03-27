# science-data-utils

This repository contains a set of utilities that are used for management and quality-checking of scientific data files, as well as processing of such files, by the Swiss Polar Institute.

Different utilities provide the ability to work with data from a variety of scientific disciplines, resulting from the [Antarctic Circumnavigation Expedition (ACE)](https://spi-ace-expedition.ch) which took place in the austral summer of 2016/2017. 

Packages required for using these tools are listed in `<requirements.txt>`. Everything is currently set-up for use in a Linux environment.

### Some specific uses are described below:

#### Adding latitude and longitude to SeaBird-processed, CTD (Conductivity, temperature, depth) bottle (.btl) files

Latitude and longitude in NMEA strings were provided directly to the software operating the CTD when the data were collected, but post-cruise, these data were quality-checked and corrected from two distinct sources (DOI: [10.5281/zenodo.3260616](10.5281/zenodo.3260616)). In order to replace the original latitudes and longitudes which correspond to the bottle firing times, we find the time of the bottle fire, get this from the cruise track data and insert it back into the row, before replacing this back into the .btl file. See `<process_ctd_bottle_file_add_latitude_longitude.sh>` which uses `<ctd_bottle_files_add_latitude_longitude.py>`. 

In order to simply extract the latitude and longitude from the cruise track data for the respective bottle files and save these into a separate .btl file for each cast, see the files `<match_bottle_times_positions.sh>`, which uses `<get_bottle_firing_times.py>` and `<get_positions.py>`. 

### Some specific utils are described below:

#### ace_motion_data.py

This util gets the raw ACE motion data files which are in text format and outputs them into more user-friendly csv files. 

Input: raw text files, which were output from a Hydrins Inertial Navigation instrument on board the R/V Akademik Tryoshnikov during the Antarctic Circumnavigation Expedition (ACE). The files contain different "amounts" of data, which are not always daily. Data is at a resolution of one second.

Output: daily csv files containing one-second resolution data. Data are only parsed from the text file input - there is no data processing that is undertaken.

#### check_depth_along_cruise_track.py

This util compares the sea floor depth from two similar files along a cruise track.

Input: two csv files, each containing the date, position and sea floor depth, usually from different sources.

Output: csv files containing the date, position, depths and differences in depth between the two datasets.

#### datetime_to_position.py

This util gets a datetime and finds an associated position from an SQLite database. 

Input: a datetime in the ISO 8601 format YYYY-MM-DDThh:mm:ss, eg. 2010-03-12T06:44:38

Output: a position (latitude and longitude in decimal degrees)

Uses: an SQLite database that contains timestamped positions

#### get_gebco_depth_along_track.py

This util finds the depth of the seafloor along a specified track, using GEBCO Bathymetry data.

Input: csv file with date and positions, and GEBCO bathymetry data for the required area.

Output: csv file with date, position and sea floor depth.

#### get_positions_for_times_in_file.py

This util gets a list of datetimes in a file, finds the associated position from an SQLite database and outputs a file containing the datetime and position.

Input: a csv file with a list of dates and times in the ISO 8601 format YYYY-MM-DDThh:mm:ss, eg. 2010-03-12T06:44:38

Output: a csv file with a list of dates and times, and their respective latitudes and longitudes that are found from an SQLite database. 

Uses: an SQLite database that contains timestamped positions that is used from datetime_to_position.py

#### get_rtopo2_depth_along_track.py

This util finds the depth of the seafloor along a specified track, using RTopo 2.0.4 data.

Input: csv file with date and positions, and RTopo 2.0.4 bathymetry data for the required area.

Output: csv file with date, position and sea floor depth.

## Credits

This code has been authored by Carles Pina Estany and Jenny Thomas.

## License

This code is provided under the MIT license. Please see the LICENSE file for more information. 

