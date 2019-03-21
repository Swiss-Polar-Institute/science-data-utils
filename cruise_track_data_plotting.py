import matplotlib.pyplot as plt
import pandas
import datetime

def get_data_file(filepath, columns):
    """Get a subset of the data from one file and write into a dataframe"""

    #filepath = input("Enter the full path and name of the file")
    data = pandas.read_csv(filepath, usecols=columns)

    return data


def plot_data_sources():

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

    # get flagged data
    filepath = '/home/jen/projects/ace_data_management/wip/cruise_track_data/flagging_data_ace_trimble_gps_2017-01-02.csv'
    columns = ['date_time', 'latitude', 'longitude', 'speed', 'device_id']
    gps_data_flagged = get_data_file(filepath, columns)
    print(gps_data_flagged.head(5))

    filepath = '/home/jen/projects/ace_data_management/wip/cruise_track_data/flagging_data_ace_glonass_2017-01-02.csv'
    columns = ['date_time', 'latitude', 'longitude', 'speed', 'device_id']
    glonass_data_flagged = get_data_file(filepath, columns)
    print(glonass_data_flagged.head(5))

    # Plot speed data
    plt.scatter(gps_data_flagged.iloc[::60].longitude, gps_data_flagged.iloc[::60].speed, c='red', label='trimble')
    plt.scatter(glonass_data_flagged.iloc[::60].longitude, glonass_data_flagged.iloc[::60].speed, c='green', label='glonass')
    plt.title("Speed of vessel along track")
    plt.xlabel("Longitude")
    plt.ylabel("Speed of vessel, knots")
    plt.grid(True)
    plt.legend()

    plt.show()

    # Plot of frequency distribution of speed of vessel.
    plt.subplot(211)
    gps_data_flagged['speed'].hist()
    plt.title("Frequency distribution of speed of vessel")
    plt.xlabel("Speed of vessel, knots")
    plt.ylabel("Count")
    plt.grid(True)

    plt.subplot(212)
    gps_data_flagged['speed'].hist(bins=80,range=[0,20])
    plt.title("Frequency distribution of speed of vessel")
    plt.xlabel("Speed of vessel, knots")
    plt.ylabel("Count")
    plt.grid(True)

    plt.tight_layout()
    plt.show()


def plot_overall_combined_track():

    # get combined overall dataset
    overall = get_data_file()

    # subset the data
    overall_subset = overall.loc[(overall['longitude'] >= 18.0) & (overall['longitude'] <= 50)]

    # plot the subsetted combined track
    scatter_plot(overall_subset.longitude, overall_subset.latitude, overall_subset.device_id)
