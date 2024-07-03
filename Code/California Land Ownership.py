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
gdb_path = '../Data/ownership23_1.gdb'

# List all layers (tables) in the file geodatabase
layers = fiona.listlayers(gdb_path)
print("Available layers:", layers)

# Initialize an empty list to store the data
data = []

# Open a specific layer (table) from the file geodatabase
if layers:
    layer_name = layers[0]  # Replace with the desired layer name if needed
    print(f"Opening layer: {layer_name}")
    with fiona.open(gdb_path, layer=layer_name) as layer:
        # Print schema
        print("Schema:", layer.schema)
        # Print CRS (Coordinate Reference System)
        print("CRS:", layer.crs)
        
        # Extract properties and coordinates
        for feature in layer:
            properties = feature['properties']
            geometry = feature['geometry']
            
            # Create a GeoDataFrame for the geometry
            geom_gdf = gpd.GeoDataFrame({'geometry': [geometry]}, crs=layer.crs)
            
            # Calculate the centroid of the polygon
            centroid = geom_gdf.geometry.centroid
            
            # Reproject the centroid to WGS84 (EPSG:4326)
            centroid_wgs84 = centroid.to_crs(epsg=4326)
            
            centroid_coords = centroid_wgs84.iloc[0].coords[0]
            
            data.append({
                'Own_Level': properties['Own_Level'],
                'Own_Agency': properties['Own_Agency'],
                'Own_Group': properties['Own_Group'],
                'SHAPE_Length': properties['SHAPE_Length'],
                'SHAPE_Area': properties['SHAPE_Area'],
                'Longitude': centroid_coords[0],
                'Latitude': centroid_coords[1]
            })

# Convert to GeoDataFrame
gdf = gpd.GeoDataFrame(data, geometry=gpd.points_from_xy([item['Longitude'] for item in data], [item['Latitude'] for item in data]), crs='EPSG:4326')

# Display the first few rows of the GeoDataFrame
print(gdf.head())

# Export to Shapefile
shapefile_path = '../Data/ownership23_1.shp'
gdf.to_file(shapefile_path)

# Export to GeoJSON
geojson_path = '../Data/ownership23_1.geojson'
gdf.to_file(geojson_path, driver='GeoJSON')

# Export to GeoPackage
geopackage_path = '../Data/ownership23_1.gpkg'
gdf.to_file(geopackage_path, layer='ownership23_1', driver='GPKG')

