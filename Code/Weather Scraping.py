# -*- coding: utf-8 -*-
"""
Created on Mon Jun 10 16:36:32 2024

@author: reese
"""

import os
import requests
import numpy as np
from io import StringIO
import pandas as pd

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Change the working directory to the script's directory
os.chdir(script_dir)

# NOAA data

# from https://www.ncei.noaa.gov/pub/data/cirs/climdiv/
# README file: https://www.ncei.noaa.gov/pub/data/cirs/climdiv/county-readme.txt
# in this file CA state code is 04 (not 06)

noaa_pcp_url = "https://www.ncei.noaa.gov/pub/data/cirs/climdiv/climdiv-pcpncy-v1.0.0-20240705"
noaa_tmax_url = "https://www.ncei.noaa.gov/pub/data/cirs/climdiv/climdiv-tmaxcy-v1.0.0-20240705"
noaa_tmin_url = "https://www.ncei.noaa.gov/pub/data/cirs/climdiv/climdiv-tmincy-v1.0.0-20240705"
noaa_tavg_url = 'https://www.ncei.noaa.gov/pub/data/cirs/climdiv/climdiv-tmpccy-v1.0.0-20240705'
noaa_ccd_url = 'https://www.ncei.noaa.gov/pub/data/cirs/climdiv/climdiv-cddccy-v1.0.0-20240705'
noaa_hhd_url = 'https://www.ncei.noaa.gov/pub/data/cirs/climdiv/climdiv-hddccy-v1.0.0-20240705'

def get_noaa_data_df(target_url):
    noaa_data = requests.get(target_url)    
    noaa_month_colnames= ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]
    noaa_df = pd.read_csv(StringIO(noaa_data.text), lineterminator="\n", sep=r"\s+", header=None, names=["ID", *noaa_month_colnames], index_col=False, dtype="string")
    return noaa_df

pcp_data_df = get_noaa_data_df(noaa_pcp_url)
tmax_data_df = get_noaa_data_df(noaa_tmax_url)
tmin_data_df = get_noaa_data_df(noaa_tmin_url)
tavg_data_df = get_noaa_data_df(noaa_tavg_url)
ccd_data_df = get_noaa_data_df(noaa_ccd_url)
hhd_data_df = get_noaa_data_df(noaa_hhd_url)

# Concatenate NOAA dfs together
noaa_full_df = pd.concat([pcp_data_df, tmax_data_df, tmin_data_df, tavg_data_df, ccd_data_df, hhd_data_df], axis=0)
#%%
# Extract NOAA ID
def extract_noaa_id(df):
    df["STATE_CODE"] = df["ID"].str[:2]
    df["FIPS_CODE"] = df["ID"].str[2:5]
    df["ELEMENT_CODE"] = df["ID"].str[5:7]
    df["YEAR"] = df["ID"].str[7:]
    return df

extract_noaa_id(noaa_full_df)

# Map element codes to descriptive abbreviation

element_code_map = {
# element codes from docs
    "01": "pcp", # Precipitation
    "02": "tavg", # Average Temperature
    "25": "Heating Degree Days",
    "26": "Cooling Degree Days",
    "27": "tmax", # Maximum Temperature
    "28": "tmin" # Minimum Temperature
}

noaa_full_df["NOAA_ELEMENT"] = noaa_full_df["ELEMENT_CODE"].map(element_code_map).fillna(noaa_full_df["ELEMENT_CODE"])
noaa_full_df

noaa_ca_df = noaa_full_df[noaa_full_df["STATE_CODE"] == "04"]

# FIPS Codes

# from https://www.census.gov/library/reference/code-lists/ansi.html#cou
fips_url = "https://www2.census.gov/geo/docs/reference/codes2020/cou/st06_ca_cou2020.txt"

fips_data = requests.get(fips_url)
fips_df = pd.read_csv(StringIO(fips_data.text), lineterminator="\n", sep="|", dtype="string")

# Join to FIPS df
noaa_ca_counties_df = pd.merge(fips_df[["COUNTYFP", "COUNTYNAME"]], noaa_ca_df, left_on="COUNTYFP", right_on="FIPS_CODE")
noaa_ca_counties_df["COUNTYNAME"] = noaa_ca_counties_df["COUNTYNAME"].str.split().apply(lambda row: ' '.join(row[:-1]))

# Remove unnecessary encoding columns
noaa_ca_counties_df = noaa_ca_counties_df[['COUNTYNAME', 'NOAA_ELEMENT', 'YEAR', 'JAN', 'FEB', 'MAR', 'APR', 'MAY',
       'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC', 'ID']]
noaa_ca_counties_df.to_csv("../Data/Weather Data/Processed Data/CA County Weather Data.csv")