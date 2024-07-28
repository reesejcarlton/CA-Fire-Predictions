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
import time
from multiprocessing import Queue

# First get lat/long grid
def get_coordinate_grid(lat, lon, dist=256, coors=1):
    """Create a coordinate grid 

    Args:
        lat (_type_): _description_
        lon (_type_): _description_
        dist (int, optional): Distance in meters in each direction. Defaults to 256.
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
    latO = np.append(latO[0], latO[0][0])
    lonO = np.append(lonO[0], lonO[0][0])
    output = [[[lon, lat] for lat, lon in zip(latO.tolist(), lonO.tolist())]]
    #stack x and y latlongs and get (lat,long) format
    # output = np.stack([lonO, latO], axis=-1).tolist()
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
    return item, cover

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
        "coordinates": coordinates
        #             [
        #     [
        #         [-122.2751, 47.5469],
        #         [-121.9613, 47.9613],
        #         [-121.9613, 47.9613],
        #         [-122.2751, 47.9613],
        #         [-122.2751, 47.5469],
        #     ]
        # ],
    }

    # Format: "2020-12-01/2020-12-31"
    start = datetime.strptime(time_start, '%Y-%m-%dT%H:%M:%SZ').date()
    end = datetime.strptime(time_end, '%Y-%m-%dT%H:%M:%SZ').date()
    time_range = f"{start}/{end}"

    search = catalog.search(
        collections=collections, intersects=area_of_interest, datetime=time_range
    )
    items = search.item_collection()
    item, cloud_coverage = select_lowest_cloud_coverage(items)
    return item, cloud_coverage, coordinates
    
def process_data(canonical_url, item):
    stac_visual, stac_nir_band, stac_red_band, ndvi = process_stac_object(item)
    # Save to netCDF files
    path = canonical_url[1:-1].replace("/", "-")
    
    # save to .tiff files removed since it was taking too much space
    # plt.imsave(f"../Data/Wildfire Satellite Visual/tiff/{path}.tiff", stac_visual)
    # plt.imsave(f"../Data/Wildfire Satellite NIR/tiff/{path}.tiff", stac_nir_band)
    # plt.imsave(f"../Data/Wildfire Satellite RED/tiff/{path}.tiff", stac_red_band)
    # plt.imsave(f"../Data/Wildfire NDVI/tiff/{path}.tiff", ndvi)
    
    # save to .png files
    plt.imsave(f"../Data/Wildfire Satellite Visual/{path}.png", stac_visual)
    plt.imsave(f"../Data/Wildfire Satellite NIR/{path}.png", stac_nir_band)
    plt.imsave(f"../Data/Wildfire Satellite RED/{path}.png", stac_red_band)
    plt.imsave(f"../Data/Wildfire NDVI/{path}.png", ndvi)
    
    mean_ndvi = ndvi.mean()
    return canonical_url, mean_ndvi

def run(first_fire):
    try:
        grid = get_coordinate_grid(lat=first_fire.Latitude, lon=first_fire.Longitude,
                            dist=256, coors=1)
        item, cloud_coverage, coordinates = query_data(coordinates=grid, time_start=first_fire.Started,
                    time_end=first_fire.Extinguished)
        canonical_url, mean_ndvi = process_data(canonical_url=first_fire.CanonicalUrl, item=item)
        return canonical_url, mean_ndvi, cloud_coverage, coordinates
    except Exception as e:
        print(e)
        return first_fire.CanonicalUrl, None, None, None

def main():
    num_threads = multiprocessing.cpu_count()-1 or 1
    print(f"num threads: {num_threads}")
    pool = multiprocessing.Pool(processes=4)
    start = time.time()
    processes = []
    urls = []
    mean_ndvi_list = []
    cloud_coverage_list = []
    coordinates = []

    fires = pd.read_csv("../Data/California_Fire_Incidents.csv")
    fires = fires[fires.ArchiveYear > 2014].reset_index()
    # fires = fires[fires.index==0]

    for _, row in fires.iterrows():
        # url, mean_ndvi = run(row)
        # if mean_ndvi:
        #     urls.append(url)
        #     mean_ndvi_list.append(mean_ndvi)
        result = pool.apply_async(func=run, args=[row])
        processes.append(result)

    for p in processes:
        r = p.get()
        if r[1]:
            urls.append(r[0])
            mean_ndvi_list.append(r[1])
            cloud_coverage_list.append(r[2])
            coordinates.append(r[3])

    df = pd.DataFrame(data={'canonical_url': urls, 'mean_ndvi': mean_ndvi_list, 'cloud_coverage': cloud_coverage_list, 'coordinates': coordinates})
    df.to_csv('../Data/California_Mean_NDVI.csv')
    print(f"\nRuntime: {time.time()-start}")

if __name__ == '__main__':
    main()
