"""dem elevation data processing script
-----------------------------------------
SRTM 1 Arc-Second Global

Resolution: Approximately 30 meters.
Coverage: Global coverage between latitudes 56°S and 60°N.
Details: This dataset offers finer resolution and is useful for detailed topographic analysis.
"""
import ee
import numpy as np
import os
import pandas as pd
import requests

from datetime import datetime
from PIL import Image
import folium
import json
import multiprocessing
import time

def query_data(lat, lon, url):
    # Service Account, eg "[...]iam.gserviceaccount.com"
    service_account = 'ACCOUNT NAME'
    # Path to the service account key
    json_credentials = 'PATH TO ACCOUNT KEY'

    # Initialize the Earth Engine module.
    SCOPES = [
        'https://www.googleapis.com/auth/earthengine',
        'https://www.googleapis.com/auth/drive'
    ]
    ee.Authenticate(scopes=SCOPES)
    credentials = ee.ServiceAccountCredentials(service_account, json_credentials)
    ee.Initialize(credentials)
    
    # Load the SRTM DEM data
    srtm = ee.Image("USGS/SRTMGL1_003")
    # Clip the SRTM data to the region of interest
    # srtm_clip = srtm.clip(roi)
    # Load slopes
    slope = ee.Terrain.slope(srtm)

    # Define a region of interest with a center point and buffer
    lat, lon = lat, lon #37.7749, -122.4194  # San Francisco, CA
    roi = ee.Geometry.Point(lon, lat).buffer(2500)  # 2.5 km buffer

    # Calculate elevation statistics
    elevation_stats = srtm.reduceRegion(
        reducer=ee.Reducer.minMax().combine(
            reducer2=ee.Reducer.mean(),
            sharedInputs=True
        ).combine(
            reducer2=ee.Reducer.stdDev(),
            sharedInputs=True
        ),
        geometry=roi,
        scale=30,
        maxPixels=1e9
    )

    # Calculate statistics for the slope
    slope_stats = slope.reduceRegion(
        reducer=ee.Reducer.minMax().combine(
            reducer2=ee.Reducer.mean(),
            sharedInputs=True
        ).combine(
            reducer2=ee.Reducer.stdDev(),
            sharedInputs=True
        ),
        geometry=roi,
        scale=30,  # Use an appropriate scale for the resolution of the DEM
        maxPixels=1e9
    )

    # Print the results
    elevation_stats = elevation_stats.getInfo()
    print("Elevation data:")
    elevation_stats["CanonicalUrl"] = url
    print(elevation_stats)

    # Calculate slope and aspect
    slope_stats = slope_stats.getInfo()
    print("Slope data:")
    slope_stats["CanonicalUrl"] = url
    print(slope_stats)

    return elevation_stats, slope_stats
    
def main():
    num_threads = multiprocessing.cpu_count() - 1 or 1
    print(f"num threads: {num_threads}")
    pool = multiprocessing.Pool(processes=num_threads)
    start = time.time()
    processes = []
    elevation_stats = []
    slope_stats = []

    fires = pd.read_csv("../Data/California_Fire_Incidents.csv")

    for _, row in fires.iterrows():
        result = pool.apply_async(func=query_data, args=[row.Latitude, row.Longitude, row.CanonicalUrl])
        processes.append(result)

    for p in processes:
        r = p.get()
        if r[0]:
            elevation_stats.append(r[0])
            slope_stats.append(r[1])

    elevation_df = pd.DataFrame.from_records(data=elevation_stats)
    slope_df = pd.DataFrame.from_records(data=slope_stats)
    
    elevation_df = elevation_df.to_csv("../Data/dem_elevation_data.csv")
    slope_df = slope_df.to_csv("../Data/dem_slope_data.csv")
    
    print(f"\nRuntime: {time.time()-start}")

if __name__ == '__main__':
    main()


# Define visualization parameters
# vis_params = {
#     'min': 0,
#     'max': 3000,
#     'palette': ['0000ff', '00ffff', 'ffff00', 'ff0000', 'ffffff']
# }

# # Create a map centered on the region of interest
# map_center = [lat, lon]
# map2 = folium.Map(location=map_center, zoom_start=12)

# # Define a method for displaying Earth Engine image tiles to folium map.
# def add_ee_layer(self, ee_image_object, vis_params, name):
#   map_id_dict = ee.Image(ee_image_object).getMapId(vis_params)
#   folium.raster_layers.TileLayer(
#     tiles = map_id_dict['tile_fetcher'].url_format,
#     attr = 'Map Data &copy; <a href="https://earthengine.google.com/">Google Earth Engine</a>',
#     name = name,
#     overlay = True,
#     control = True
#   ).add_to(self)

# # Add EE drawing method to folium.
# folium.Map.add_ee_layer = add_ee_layer

# # Add the SRTM data to the map
# map2.add_ee_layer(srtm_clip, vis_params, 'SRTM DEM')

# # Display the map
# map2.render()

# # Display the DEM, slope, and aspect
# print("DEM data:")
# print(srtm_clip.getInfo())

# print("Slope data:")
# print(slope.getInfo())

# print("Aspect data:")
# print(aspect.getInfo())

# # API base url
# url = 'https://earthexplorer.usgs.gov/inventory/json/v/1.4.1/search'
    
# params = {
#     'apiKey': api_key,  # You will need to generate an API key from USGS
#     'datasetName': 'SRTM 1 Arc-Second Global',
#     'spatialFilter': {
#         'filterType': 'mbr',
#         'lowerLeft': {
#             'latitude': 35.0,
#             'longitude': -120.0
#         },
#         'upperRight': {
#             'latitude': 36.0,
#             'longitude': -119.0
#         }
#     },
#     'maxResults': 10,
#     'startingNumber': 1,
#     'sortOrder': 'DESC'
# }
# # Make the request
# response = requests.get(url, params=params)
# data = response.json()

# # Print the results
# print(data)
