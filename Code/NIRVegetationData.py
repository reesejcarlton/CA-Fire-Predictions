import planetary_computer as pc
import pandas as pd

import rioxarray
import xarray

import xrspatial.multispectral as ms
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime
from pystac_client import Client

import multiprocessing
from multiprocessing import Queue

# First get lat/long grid
def get_coordinate_grid(lat, lon, dist=5000, coors=1):
    """Create a coordinate grid 

    Args:
        lat (_type_): _description_
        lon (_type_): _description_
        dist (int, optional): Distance in meters in each direction. Defaults to 5000.
        coors (int, optional): Number of coordinates in each direction. Defaults to 1.
    """

    #Creating the offset grid
    mini, maxi = -dist*coors, dist*coors
    n_coord = coors*2+1
    axis = np.linspace(mini, maxi, n_coord)
    X, Y = np.meshgrid(axis, axis)


    #avation formulate for offsetting the latlong by offset matrices
    R = 6378137 #earth's radius
    dLat = X/R
    dLon = Y/(R*np.cos(np.pi*lat/180))
    latO = lat + dLat * 180/np.pi
    lonO = lon + dLon * 180/np.pi

    #stack x and y latlongs and get (lat,long) format
    output = np.stack([lonO, latO]).tolist()
    return output

def select_lowest_cloud_coverage(items):
    """_summary_

    Args:
        items (pystac.ItemCollection): _description_

    Returns:
        item: stac.Item object containing the Item with the lowest cloud_cover value
    """
    item = None
    cover = 100
    for obj in items:
        if obj.properties['eo:cloud_cover'] < cover:
            item = obj
            cover = obj.properties['eo:cloud_cover']
    return item

def process_stac_object(item):
    stac_visual = (
        rioxarray.open_rasterio(pc.sign(item.assets["visual"].href))
        .transpose("y", "x", "band")
    )

    asset_nir = "B08"
    stac_nir_band = (
        rioxarray.open_rasterio(pc.sign(item.assets[asset_nir].href))
        .squeeze()
    )

    asset_red = "B04"
    stac_red_band = (
        rioxarray.open_rasterio(pc.sign(item.assets[asset_red].href))
        .squeeze()
    )

    ndvi = ms.ndvi(stac_nir_band, stac_red_band)

    return stac_visual, stac_nir_band, stac_red_band, ndvi

def query_data(coordinates, time_start, time_end):
    catalog = Client.open(
    'https://planetarycomputer.microsoft.com/api/stac/v1',
    modifier=pc.sign_inplace,
    )
    collections = ["sentinel-2-l2a"]
    

    area_of_interest = {
        "type": "Polygon",
        "coordinates": 
                    [
            [
                [-122.2751, 47.5469],
                [-121.9613, 47.9613],
                [-121.9613, 47.9613],
                [-122.2751, 47.9613],
                [-122.2751, 47.5469],
            ]
        ],
    }

    # Format: "2020-12-01/2020-12-31"
    start = datetime.strptime(time_start, '%Y-%m-%dT%H:%M:%SZ').date()
    end = datetime.strptime(time_end, '%Y-%m-%dT%H:%M:%SZ').date()
    time_range = f"{start}/{end}"

    search = catalog.search(
        collections=collections, intersects=area_of_interest, datetime=time_range
    )
    items = search.item_collection()
    item = select_lowest_cloud_coverage(items)
    return item
    
def process_data(canonical_url, item):
    stac_visual, stac_nir_band, stac_red_band, ndvi = process_stac_object(item)
    # Save to netCDF files
    path = canonical_url[1:-1].replace("/", "-")
    
    # save to .tiff files
    plt.imsave(f"../Data/Wildfire Satellite Visual/tiff/{path}.tiff", stac_visual)
    plt.imsave(f"../Data/Wildfire Satellite NIR/tiff/{path}.tiff", stac_nir_band)
    plt.imsave(f"../Data/Wildfire Satellite RED/tiff/{path}.tiff", stac_red_band)
    plt.imsave(f"../Data/Wildfire NDVI/tiff/{path}.tiff", ndvi)
    
    # save to .png files
    plt.imsave(f"../Data/Wildfire Satellite Visual/png/{path}.png", stac_visual)
    plt.imsave(f"../Data/Wildfire Satellite NIR/png/{path}.png", stac_nir_band)
    plt.imsave(f"../Data/Wildfire Satellite RED/png/{path}.png", stac_red_band)
    plt.imsave(f"../Data/Wildfire NDVI/png/{path}.png", ndvi)
    
    mean_ndvi = np.mean(ndvi)
    return canonical_url, mean_ndvi

def run(first_fire, queue):
    try:
        grid = get_coordinate_grid(lat=first_fire.Latitude, lon=first_fire.Longitude,
                            dist=5000, coors=1)
        item = query_data(coordinates=grid, time_start=first_fire.Started,
                    time_end=first_fire.Extinguished)
        output = process_data(canonical_url=first_fire.CanonicalUrl, item=item)
        queue.put(output)
    except:
        queue.put(first_fire.CanonicalUrl, None)


queue = Queue()
processes = []
urls = []
mean_ndvi_list = []

fires = pd.read_csv("../Data/California_Fire_Incidents.csv")
fires = fires[fires.ArchiveYear > 2014].reset_index()

for i in fires.to_dict(orient='records'):
    proc = multiprocessing.Process(target=run, args=[i, queue])
    proc.start()
    processes.append(proc)

for p in processes:
        r = queue.get()
        if r[1]:
            urls.append(r[0])
            mean_ndvi_list.append(r[1])

for p in processes:
    p.join()

df = pd.DataFrame(data=[urls, mean_ndvi_list], columns=['canonical_url', 'mean_ndvi'])
df.to_csv('../Data/California_Mean_NDVI.csv')
