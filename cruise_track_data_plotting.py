import matplotlib.pyplot as plt
import numpy as np

def scatter_plot(x, y, colour_variable):
    """Create a scatter plot of two variables against eachother on an x and y axis respectively. Colour the points by the colour variable specified."""

    colour = [str(item / 255.) for item in colour_variable]

    plt.scatter(x, y, s=500, c=colour)
    plt.scatter(x, y)
    plt.show()

