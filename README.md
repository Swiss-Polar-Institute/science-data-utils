# science-data-utils

This repository contains a set of utilities that are used for data management, quality-checking of data, file processing and so on, by the Swiss Polar Institute.

Different utilities provide the ability to work with data from a variety of scientific disciplines, resulting from the [Antarctic Circumnavigation Expedition (ACE)](https://spi-ace-expedition.ch) which took place in the austral summer of 2016/2017. 

Packages required for using these tools are listed in `<requirements.txt>`. Everything is currently set-up for use in a Linux environment.

Some specific tools are described below:

#### Adding latitude and longitude to SeaBird-processed, CTD (Conductivity, temperature, depth) bottle (.btl) files

Latitude and longitude in NMEA strings were provided directly to the software operating the CTD when the data were collected, but post-cruise, these data were quality-checked and corrected from two distinct sources (DOI: [10.5281/zenodo.3260616](10.5281/zenodo.3260616)). In order to replace the original latitudes and longitudes which correspond to the bottle firing times, we find the time of the bottle fire, get this from the cruise track data and insert it back into the row, before replacing this back into the .btl file. See `<process_ctd_bottle_file_add_latitude_longitude.sh>` which uses `<ctd_bottle_files_add_latitude_longitude.py>`. 

In order to simply extract the latitude and longitude from the cruise track data for the respective bottle files and save these into a separate .btl file for each cast, see the files `<match_bottle_times_positions.sh>`, which uses `<get_bottle_firing_times.py>` and `<get_positions.py>`. 



