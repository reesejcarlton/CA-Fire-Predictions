# -*- coding: utf-8 -*-
"""
Created on Mon Jun 10 16:36:32 2024

@author: reese
"""

import os
import requests
import numpy as np

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

#Get list of each California county
ca_counties_id = [str(x) for x in np.arange(1, 116, 2)]

#Get list of measures

measures = ["tavg", "tmax", "tmin", "pcp"]

for county_id in ca_counties_id:
    
    # Define the relative output directory path
    output_directory = os.path.join(script_dir, "../Data/CA-" + county_id)
    
    for measure in measures:
        
        # Construct the CSV file URL
        csv_url = "https://www.ncei.noaa.gov/access/monitoring/climate-at-a-glance/county/time-series/CA-" + county_id + "/" + measure + "/1/0/2000-2024.csv"

        # Download the CSV file
        try:
            csv_response = requests.get(csv_url, headers={'User-Agent': 'Mozilla/5.0'})
            csv_response.raise_for_status()  # Raise an exception for 4xx or 5xx status codes
        except requests.exceptions.RequestException as e:
            print(f"Failed to download CSV file: {e}")
            # Handle the error here, e.g., return or exit

        # Save the CSV file to the specified output directory
        os.makedirs(output_directory, exist_ok=True)  # Create the output directory if it doesn't exist
        output_file_path = os.path.join(output_directory, measure + '.csv')
        with open(output_file_path, 'wb') as f:
            f.write(csv_response.content)
        
        print("CSV file was downloaded to:", output_file_path)