# -*- coding: utf-8 -*-
"""
Created on Mon Jun 10 20:35:27 2024

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
gdb_path = '../Data/ds1020/ds1020.gdb'

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
        # Store features in the list
        for feature in layer:
            geometry = feature.get('geometry')
            properties = feature.get('properties')
            if geometry:
                coordinates = geometry['coordinates']
                data_entry = {'coordinates': coordinates}
                # Extract all coverage values
                for cov_type in ['ConCov', 'HdwdCov', 'TreeCov', 'RegenTreeCov', 'ShrubCov', 'HerbCov', 'TotalVegCov']:
                    data_entry[cov_type] = properties.get(cov_type)
                data.append(data_entry)

# Create a DataFrame
df = pd.DataFrame(data)

# Convert the DataFrame to a GeoDataFrame
gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df['coordinates'].apply(lambda x: x[0]),
                                                       df['coordinates'].apply(lambda x: x[1])))

# List of coverage types
coverage_types = ['ConCov', 'HdwdCov', 'TreeCov', 'RegenTreeCov', 'ShrubCov', 'HerbCov', 'TotalVegCov']

# Plotting each coverage type
for cov_type in coverage_types:
    fig, ax = plt.subplots(1, 1, figsize=(10, 6))
    gdf.dropna(subset=[cov_type]).plot(column=cov_type, ax=ax, legend=True,
                                       legend_kwds={'label': f"{cov_type} by Location",
                                                    'orientation': "horizontal"})
    plt.title(f"Map of {cov_type}")
    plt.xlabel("Longitude")
    plt.ylabel("Latitude")
    plt.show()
