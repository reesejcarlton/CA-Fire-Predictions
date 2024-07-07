# -*- coding: utf-8 -*-
"""
Created on Wed Jun 12 11:57:37 2024

@author: reese

Housing Data from https://www.census.gov/programs-surveys/popest/data/tables.1995.List_58029271.html#list-tab-List_58029271
FIPS Data from https://www.census.gov/library/reference/code-lists/ansi.html#cou

"""

import os
import pandas as pd
import requests
from io import StringIO

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Change the working directory to the script's directory
os.chdir(script_dir)

fips_url = "https://www2.census.gov/geo/docs/reference/codes2020/cou/st06_ca_cou2020.txt"

fips_data = requests.get(fips_url)
fips_df = pd.read_csv(StringIO(fips_data.text), lineterminator="\n", sep="|", dtype="string")
fips_df["FULLFP"] = fips_df["STATEFP"].apply(str) + fips_df["COUNTYFP"].apply(str)
fips_df["FULLFP"] = fips_df["FULLFP"].apply(lambda x: int(x[1:]))

#Read data for 2010-2020 housing data
CA_2023_housing = pd.read_excel('../Data/Housing Data/Raw Data/CO-EST2023-HU-06.xlsx', skiprows = 3).drop('Unnamed: 1', axis = 1)
CA_2023_housing = CA_2023_housing.iloc[1:59]
CA_2023_housing.iloc[:, 1:] = CA_2023_housing.iloc[:, 1:].astype(int)
CA_2023_housing.rename(columns={'Unnamed: 0' : 'CTYNAME'}, inplace=True)
CA_2023_housing['CTYNAME'] = CA_2023_housing['CTYNAME'].apply(lambda x: x[1:])
CA_2023_housing['CTYNAME'] = CA_2023_housing['CTYNAME'].str.replace(', California', '', regex=False)
CA_2023_housing = pd.merge(CA_2023_housing, fips_df[["FULLFP", "COUNTYNAME"]], left_on = "CTYNAME", right_on = "COUNTYNAME").drop('COUNTYNAME', axis = 1)
new_order = ['CTYNAME', 'FULLFP', 2020, 2021, 2022, 2023]
CA_2023_housing = CA_2023_housing[new_order]
CA_2023_housing.columns = CA_2023_housing.columns.astype(str)

#Read data for 2010-2020 housing data
CA_2020_housing = pd.read_excel('../Data/Housing Data/Raw Data/CO-EST2019-ANNHU-06.xlsx', skiprows = 3).drop(['Census', 'Estimates Base'], axis = 1)
CA_2020_housing = CA_2020_housing.iloc[1:59]
CA_2020_housing.iloc[:, 1:] = CA_2020_housing.iloc[:, 1:].astype(int)
CA_2020_housing.rename(columns={'Unnamed: 0' : 'CTYNAME'}, inplace=True)
CA_2020_housing['CTYNAME'] = CA_2020_housing['CTYNAME'].apply(lambda x: x[1:])
CA_2020_housing['CTYNAME'] = CA_2020_housing['CTYNAME'].str.replace(', California', '', regex=False)

CA_2020_housing = pd.merge(CA_2020_housing, fips_df[["FULLFP", "COUNTYNAME"]], left_on = "CTYNAME", right_on = "COUNTYNAME").drop('COUNTYNAME', axis = 1)
new_order = ['CTYNAME', 'FULLFP', 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019]
CA_2020_housing = CA_2020_housing[new_order]
CA_2020_housing.columns = CA_2020_housing.columns.astype(str)

#Read data for 2000-2010 housing data
CA_2010_housing = pd.read_excel('../Data/Housing Data/Raw Data/hu-est00int-02-06.xls', skiprows = 3).drop(['Unnamed: 1', 'Unnamed: 12', 'Unnamed: 13'], axis = 1)
CA_2010_housing = CA_2010_housing.iloc[1:59]
CA_2010_housing.iloc[:, 1:] = CA_2010_housing.iloc[:, 1:].astype(int)
CA_2010_housing.rename(columns={'Unnamed: 0' : 'CTYNAME'}, inplace=True)
CA_2010_housing['CTYNAME'] = CA_2010_housing['CTYNAME'].apply(lambda x: x[1:])

CA_2010_housing = pd.merge(CA_2010_housing, fips_df[["FULLFP", "COUNTYNAME"]], left_on = "CTYNAME", right_on = "COUNTYNAME").drop('COUNTYNAME', axis = 1)
new_order = ['CTYNAME', 'FULLFP', 2000, 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009]
CA_2010_housing = CA_2010_housing[new_order]
CA_2010_housing.columns = CA_2010_housing.columns.astype(str)

#Create data for 1960-2000 housing data
counties = ["Alameda County", "Alpine County", "Amador County", "Butte County", "Calaveras County", "Colusa County", 
            "Contra Costa County", "Del Norte County", "El Dorado County", "Fresno County", "Glenn County", 
            "Humboldt County", "Imperial County", "Inyo County", "Kern County", "Kings County", "Lake County", 
            "Lassen County", "Los Angeles County", "Madera County", "Marin County", "Mariposa County", 
            "Mendocino County", "Merced County", "Modoc County", "Mono County", "Monterey County", "Napa County", 
            "Nevada County", "Orange County", "Placer County", "Plumas County", "Riverside County", "Sacramento County", 
            "San Benito County", "San Bernardino County", "San Diego County", "San Francisco County", 
            "San Joaquin County", "San Luis Obispo County", "San Mateo County", "Santa Barbara County", 
            "Santa Clara County", "Santa Cruz County", "Shasta County", "Sierra County", "Siskiyou County", 
            "Solano County", "Sonoma County", "Stanislaus County", "Sutter County", "Tehama County", "Trinity County", 
            "Tulare County", "Tuolumne County", "Ventura County", "Yolo County", "Yuba County"]

housing_units_1990 = [504109, 1319, 12814, 76115, 19153, 6295, 316170, 9091, 61451, 235563, 9329, 
                      51134, 36559, 8712, 198636, 30843, 28822, 10358, 3163310, 30831, 99757, 7700, 33649, 
                      58410, 4672, 10664, 121224, 44199, 37352, 875105, 77879, 11942, 483847, 417574, 12230, 
                      542332, 946240, 328471, 166274, 90200, 251782, 138149, 540240, 91878, 60552, 2166, 20141, 
                      119136, 161062, 132027, 24163, 20403, 7540, 105013, 25175, 228478, 53028, 21245]

housing_units_1980 = [444607, 933, 9446, 61360, 12746, 5334, 251917, 7581, 44987, 193653, 8425, 
                      45389, 32088, 8484, 155702, 25694, 23008, 8899, 2855506, 24607, 92663, 5762, 28998, 
                      50016, 3738, 8397, 103557, 40052, 24759, 721570, 54014, 9451, 295069, 323702, 8750, 
                      370155, 720346, 316608, 136001, 66780, 233200, 114910, 473817, 80863, 47487, 1893, 17504, 
                      84270, 124189, 102472, 20425, 16556, 6350, 88744, 19370, 183384, 43605, 19248]

housing_units_1970 = [378833, 521, 5120, 38110, 7037, 4867, 177732, 5341, 23871, 135696, 6299, 
                      35222, 23401, 6135, 110142, 20131, 12161, 6071, 2541603, 14788, 72000, 3089, 18914, 
                      32708, 2892, 3082, 75794, 26838, 11960, 463199, 30441, 6016, 169136, 212079, 5925, 
                      251295, 450798, 310402, 96476, 37612, 191077, 88806, 336873, 52006, 27449, 1551, 13074, 
                      53762, 78060, 65414, 14102, 10701, 3750, 61904, 11248, 112722, 29736, 14202]

housing_units_1960 = [310312, 168, 3988, 31494, 4677, 4385, 124279, 5831, 15592, 118784, 5956, 
                      34691, 21916, 5104, 97636, 16098, 8565, 5922, 2143227, 13593, 49581, 2436, 17556, 
                      28538, 3317, 1634, 57478, 21176, 9184, 227012, 21362, 5523, 115392, 164576, 5223, 
                      193668, 339442, 310559, 80697, 29399, 141770, 57290, 199922, 40939, 20961, 1495, 13272, 
                      41894, 59784, 51834, 11077, 9143, 4039, 55554, 8157, 60698, 20957, 11113]

# Create a DataFrame
data = {
    "CTYNAME" : counties,
    "Housing_Units_1990": housing_units_1990,
    "Housing_Units_1980": housing_units_1980,
    "Housing_Units_1970": housing_units_1970,
    "Housing_Units_1960": housing_units_1960
}

# Read into DataFrame
CA_2000_housing = pd.DataFrame(data)

CA_2000_housing = pd.merge(CA_2000_housing, fips_df[["FULLFP", "COUNTYNAME"]], left_on = "CTYNAME", right_on = "COUNTYNAME").drop('COUNTYNAME', axis = 1)
new_order = ['CTYNAME', 'FULLFP', 'Housing_Units_1990', 'Housing_Units_1980','Housing_Units_1970', 'Housing_Units_1960']
CA_2000_housing = CA_2000_housing[new_order]

CA_2000_housing_names = {
    "Housing_Units_1990": "1990",
    "Housing_Units_1980": "1980",
    "Housing_Units_1970": "1970",
    "Housing_Units_1960": "1960"
}
CA_2000_housing.rename(columns=CA_2000_housing_names, inplace=True)
CA_2000_housing.columns = CA_2000_housing.columns.astype(str)

#Complete California Housing Time Series
CA_housing_1960_2023 = pd.merge(CA_2000_housing, CA_2010_housing, on=['CTYNAME', 'FULLFP'], suffixes=('_2000', '_2010'))
CA_housing_1960_2023 = pd.merge(CA_housing_1960_2023, CA_2020_housing, on=['CTYNAME', 'FULLFP'], suffixes=('_2010', '_2020'))
CA_housing_1960_2023 = pd.merge(CA_housing_1960_2023, CA_2023_housing, on=['CTYNAME', 'FULLFP'], suffixes=('_2020', '_2023'))

CA_housing_1960_2023.to_csv('../Data/Housing Data/Processed Data/CA Housing 1960-2023.csv')
