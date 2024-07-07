# -*- coding: utf-8 -*-
"""
Created on Wed Jul  3 11:50:28 2024

@author: reese
"""

import os
import fiona
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Change the working directory to the script's directory
os.chdir(script_dir)

# Path to the file geodatabase directory
gdb_path_raw = '../Data/Land Ownership Data/Raw Data/ownership23_1.gdb'

# List all layers (tables) in the file geodatabase
layers = fiona.listlayers(gdb_path_raw)
print("Available layers:", layers)

# Initialize an empty list to store the data
data = []

# Open a specific layer (table) from the file geodatabase
if layers:
    layer_name = layers[0]  # Replace with the desired layer name if needed
    print(f"Opening layer: {layer_name}")
    with fiona.open(gdb_path_raw, layer=layer_name) as layer:
        # Print schema
        print("Schema:", layer.schema)
        # Print CRS (Coordinate Reference System)
        print("CRS:", layer.crs)
        
        # Extract properties and geometries
        for feature in layer:
            properties = feature['properties']
            geometry = feature['geometry']
            
            data.append({
                'Own_Level': properties['Own_Level'],
                'Own_Agency': properties['Own_Agency'],
                'Own_Group': properties['Own_Group'],
                'SHAPE_Length': properties['SHAPE_Length'],
                'SHAPE_Area': properties['SHAPE_Area'],
                'geometry': geometry
            })

# Convert to GeoDataFrame
gdf = gpd.GeoDataFrame(data, geometry='geometry', crs='EPSG:3310')

# Reproject to WGS84 (EPSG:4326)
gdf = gdf.to_crs(epsg=4326)

# Display the first few rows of the GeoDataFrame
print(gdf.head())

# Export to Shapefile
shapefile_path = '../Data/Land Ownership Data/Processed Data/ownership23_1.shp'
gdf.to_file(shapefile_path)

# Export to GeoPackage
geopackage_path = '../Data/Land Ownership Data/Processed Data/ownership23_1.gpkg'
gdf.to_file(geopackage_path, layer='ownership23_1', driver='GPKG')

# Read from GeoPackage
gdf_from_geopackage = gpd.read_file(geopackage_path, layer='ownership23_1')
print("From GeoPackage:\n", gdf_from_geopackage.head())

# Optionally, plot the data
gdf_from_geopackage.plot(column='Own_Group', legend=False)
plt.show()
