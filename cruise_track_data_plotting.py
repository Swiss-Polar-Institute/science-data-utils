import matplotlib.pyplot as plt
import matplotlib.cm as cm
import pandas
import datetime
#import seaborn as sns

def get_data_file(filepath, columns):
    """Get a subset of the data from one file and write into a dataframe"""

    #filepath = input("Enter the full path and name of the file")
    dataframe = pandas.read_csv(filepath, usecols=columns, header=0)

    return dataframe


def plot_data_sources_from_file():

    # get some data from GPS
    filepath = '/home/jen/projects/ace_data_management/wip/cruise_track_data/ace_trimble_gps_2017-01-02.csv'
    columns = ['date_time', 'latitude', 'longitude', 'device_id']
    gps_data = get_data_file(filepath, columns)
    sixty_sec_res_gps = gps_data.iloc[::60]

    # get some data from GLONASS
    filepath = '/home/jen/projects/ace_data_management/wip/cruise_track_data/ace_glonass_2017-01-02.csv'
    columns = ['date_time', 'latitude', 'longitude', 'device_id']
    glonass_data = get_data_file(filepath, columns)
    sixty_sec_res_glonass = glonass_data.iloc[::60]

    # Plot one second resolution data
    plt.subplot(211)
    plt.scatter(gps_data.longitude, gps_data.latitude, c="red", label="trimble")
    plt.scatter(glonass_data.longitude, glonass_data.latitude, c="green", label="glonass")
    plt.title("One-second resolution, 2017-01-02")
    plt.xlabel("Longitude, decimal degrees E")
    plt.ylabel("Latitude, decimal degrees N")
    plt.grid(True)
    plt.legend()

    # Plot sixty-second resolution data
    plt.subplot(212)
    plt.scatter(sixty_sec_res_gps.longitude, sixty_sec_res_gps.latitude, c="red", label="trimble")
    plt.scatter(sixty_sec_res_glonass.longitude, sixty_sec_res_glonass.latitude, c="green", label="glonass")
    plt.title("Sixty-second resolution, 2017-01-02")
    plt.xlabel("Longitude, decimal degrees E")
    plt.ylabel("Latitude, decimal degrees N")
    plt.grid(True)
    plt.legend()

    plt.tight_layout()
    plt.show()


def plot_data_sources_from_dataframe(dataframe, category):

    fig, ax = plt.subplots(figsize=(10, 5))

    ax.scatter(dataframe['longitude'], dataframe['latitude'], alpha=0.70, c=dataframe[category], cmap=cm.brg)

    plt.show()


# def get_flagged_glonass_data(filename, columns):
#     # Get GLONASS data
#     filepath = '/home/jen/projects/ace_data_management/wip/cruise_track_data/flagging_data_ace_glonass_2017-01-02.csv'
#     columns = ['date_time', 'latitude', 'longitude', 'speed', 'device_id']
#     glonass_data_flagged = get_data_file(filepath, columns)
#     print(glonass_data_flagged.head(5))
#
#     return glonass_data_flagged


def plot_speed(dataframe1, colour, legend_label):
    """Plot the speed of the vessel throughout the cruise to identify outlying speeds."""

    # Plot speed data
    plt.scatter(dataframe1.iloc[::60].longitude, dataframe1.iloc[::60].speed, c=colour, label=legend_label)
    plt.title("Speed of vessel along track")
    plt.xlabel("Longitude")
    plt.ylabel("Speed of vessel, knots")
    plt.grid(True)
    plt.legend()

    plt.show()

    # Plot of frequency distribution of speed of vessel.
    plt.subplot(211)
    dataframe1['speed'].hist()
    plt.title("Frequency distribution of speed of vessel")
    plt.xlabel("Speed of vessel, knots")
    plt.ylabel("Count")
    plt.grid(True)

    plt.subplot(212)
    dataframe1['speed'].hist(bins=80,range=[0,20])
    plt.title("Frequency distribution of speed of vessel")
    plt.xlabel("Speed of vessel, knots")
    plt.ylabel("Count")
    plt.grid(True)

    plt.tight_layout()
    plt.show()


# filepath = '/home/jen/projects/ace_data_management/wip/cruise_track_data/flagging_data_ace_trimble_gps_2017-01-02.csv'
# columns = ['date_time', 'latitude', 'longitude', 'speed', 'device_id']
# gps_data_flagged = get_data_file(filepath, columns)
# print(gps_data_flagged.head(5))
#
# plot_speed(gps_data_flagged, "red", "trimble")

#def plot_altitude(df):
