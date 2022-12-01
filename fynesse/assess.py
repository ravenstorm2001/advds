from .config import *

from . import access

import mlai
import mlai.plot as plot
import geopandas as gpd
import matplotlib.pyplot as plt

# Setting font of our plots
plt.rcParams.update({'font.size': 22})

"""Place commands in this file to assess the data you have downloaded. How are missing values encoded, how are outliers encoded? What do columns represent, makes rure they are correctly labeled. How is the data indexed. Crete visualisation routines to assess the data (e.g. in bokeh). Ensure that date formats are correct and correctly timezoned."""


def download_data_to_gdf(conn, latitude, longitude, size, from_date, to_date):
    """
    Function that downloads data and adds geometry column.
  
    Argumnets:
      conn : Connection Object - Connection to MariaDB database
      latitude : double - latitude of prediction point
      longitude : double - longitude of prediction point
      size : double - size of the box of interest
      from_date : string - download data from date
      to_date : string - download data to date
    Output:
      data_gdf : GeoPandasDataFrame - dataframe with data and added geometry column
    """
    data = joinPriceAndLocationData(conn, longitude-size/2, longitude+size/2, latitude-size/2, latitude+size/2, from_date, to_date)

    geometry=gpd.points_from_xy(data.longitude, data.lattitude)
    data_gdf = gpd.GeoDataFrame(data, 
                            geometry=geometry)
    data_gdf.crs = "EPSG:4326"
    return data_gdf

def plot_points_interest(pois, edges, data_gdf, latitude = 52.35, longitude = -2.25, size = 0.1):
    """
    Function that downloads data and adds geometry column.
  
    Argumnets:
      pois : DataFrame - points of interest from OSM
      edges - data required to print the map look
      data_gdf : GeoPandasDataFrame - transaction data with added geometry column
      latitude : double - latitude of the prediction point
      longitude : double - longitude of the prediction point
      size : double - size of the box of interest
    Output:
      N/A
    """
    fig, ax = plt.subplots(figsize=plot.big_figsize)
    
    ax.set_title('All Data Points')
    # Plot street edges
    edges.plot(ax=ax, linewidth=1, edgecolor="dimgray")

    data_gdf.plot(ax=ax, c=data_gdf.price/data_gdf.price.max()*100, cmap = 'YlOrRd', markersize=10)

    (north, south, west, east) = calculate_boundaries(latitude = latitude, longitude = longitude, size = size)

    ax.set_xlim([west, east])
    ax.set_ylim([south, north])
    ax.set_xlabel("longitude")
    ax.set_ylabel("latitude")

    # Plot all POIs 
    pois.plot(ax=ax, color="blue", alpha=0.7, markersize=10)
    plt.tight_layout()
    mlai.write_figure(directory="./maps", filename="health-care-pois.svg")

