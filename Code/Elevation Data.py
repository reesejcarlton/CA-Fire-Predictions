import requests
import os
import zipfile
import rasterio
from rasterio.plot import show
import matplotlib.pyplot as plt
import numpy as np
from io import BytesIO
from urllib.parse import urlparse
from bs4 import BeautifulSoup

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Change the working directory to the script's directory
os.chdir(script_dir)

def download_and_extract(url, download_dir):
    # Parse the URL to get the filename
    parsed_url = urlparse(url)
    filename = os.path.basename(parsed_url.path)
    # Create a folder for this download link
    folder_name = os.path.splitext(filename)[0]
    folder_path = os.path.join(download_dir, folder_name)
    os.makedirs(folder_path, exist_ok=True)
    # Download the zip file
    response = requests.get(url)
    zip_data = BytesIO(response.content)
    # Extract the DEM file into the folder
    with zipfile.ZipFile(zip_data, 'r') as zip_ref:
        for file in zip_ref.namelist():
            if file.endswith('.dem'):
                zip_ref.extract(file, folder_path)
                break  # Assuming there is only one DEM file in the zip
        else:
            # If no file with .dem extension is found, extract the first file
            zip_ref.extract(zip_ref.namelist()[0], folder_path)
    # Return the path to the extracted DEM file
    dem_file = [f for f in os.listdir(folder_path) if f.endswith('.dem')][0]
    return os.path.join(folder_path, os.path.splitext(dem_file)[0] + '.dem')

# URL of the page containing the links to the DEM files
page_url = 'https://ww2.arb.ca.gov/resources/documents/harp-digital-elevation-model-files'
response = requests.get(page_url)
# Extract the links to the DEM files
dem_urls = [link['href'] for link in BeautifulSoup(response.content, 'html.parser').find_all('a') if link['href'].endswith('.zip')]

# Directory to store the downloaded files
download_dir = '../Data/dem_files'
os.makedirs(download_dir, exist_ok=True)

# Download and plot each DEM file
for dem_url in dem_urls:
    try:
        # Download and extract the DEM file
        dem_file_path = download_and_extract(dem_url, download_dir)
        
        # Open the DEM file
        with rasterio.open(dem_file_path) as dem:
            # Print basic information about the DEM file
            print("DEM file information:")
            print(f"Driver: {dem.driver}")
            print(f"Width: {dem.width}")
            print(f"Height: {dem.height}")
            print(f"Count: {dem.count}")
            print(f"Dtype: {dem.dtypes}")
            print(f"CRS: {dem.crs}")
            print(f"Transform: {dem.transform}")
            
            # Read the DEM data
            dem_data = dem.read(1)  # Read the first band
            
            # Mask out NoData values
            dem_data = np.ma.masked_equal(dem_data, -32767)
            
            # Plot the DEM data using plt.imshow()
            plt.figure(figsize=(10, 10))
            plt.imshow(dem_data, cmap='terrain', extent=(dem.bounds.left, dem.bounds.right, dem.bounds.bottom, dem.bounds.top))
            plt.title('DEM Data')
            plt.xlabel('Longitude')
            plt.ylabel('Latitude')
            cbar = plt.colorbar()
            cbar.set_label('Elevation (m)')
            plt.show()
    
    except Exception as e:
        print(f"Error processing DEM file: {dem_url}")
        print(e)
        continue  # Skip to the next DEM file if an error occurs
