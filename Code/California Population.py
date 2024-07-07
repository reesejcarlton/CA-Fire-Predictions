# -*- coding: utf-8 -*-
"""
Created on Sat Jul  6 23:58:32 2024

@author: reese

Population Data from https://www2.census.gov/programs-surveys/popest/datasets/
FIPS Data from https://www.census.gov/library/reference/code-lists/ansi.html#cou

"""

import os
import pandas as pd
from io import StringIO
import requests
import numpy as np

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Change the working directory to the script's directory
os.chdir(script_dir)

fips_url = "https://www2.census.gov/geo/docs/reference/codes2020/cou/st06_ca_cou2020.txt"

fips_data = requests.get(fips_url)
fips_df = pd.read_csv(StringIO(fips_data.text), lineterminator="\n", sep="|", dtype="string")
fips_df["FULLFP"] = fips_df["STATEFP"].apply(str) + fips_df["COUNTYFP"].apply(str)
fips_df["FULLFP"] = fips_df["FULLFP"].apply(lambda x: int(x[1:]))


#Read in 2020-2023 populaton data
pop_2023 = pd.read_csv("../Data/Population Data/Raw Data/co-est2023-alldata.csv", encoding='latin1')
CA_pop_2023 = pop_2023[pop_2023['STNAME'] == "California"]
CA_pop_2023 = pd.merge(CA_pop_2023, fips_df[["FULLFP", "COUNTYNAME"]], left_on = "CTYNAME", right_on = "COUNTYNAME")
CA_pop_2023 = CA_pop_2023[["CTYNAME", "FULLFP", "POPESTIMATE2021", "POPESTIMATE2022", "POPESTIMATE2023"]]

CA_pop_2023_names = {
    "POPESTIMATE2021": "2021",
    "POPESTIMATE2022": "2022",
    "POPESTIMATE2023": "2023"
}

CA_pop_2023.rename(columns=CA_pop_2023_names, inplace=True)


#Read in 2010-2020 population data
pop_2020 = pd.read_csv("../Data/Population Data/Raw Data/co-est2020-alldata.csv", encoding='latin1')
CA_pop_2020 = pop_2020[pop_2020['STNAME'] == "California"]
CA_pop_2020 = pd.merge(CA_pop_2020, fips_df[["FULLFP", "COUNTYNAME"]], left_on = "CTYNAME", right_on = "COUNTYNAME")
CA_pop_2020 = CA_pop_2020[["CTYNAME", "FULLFP", "POPESTIMATE2010", "POPESTIMATE2011", "POPESTIMATE2012", "POPESTIMATE2013", "POPESTIMATE2014", "POPESTIMATE2015", "POPESTIMATE2016", "POPESTIMATE2017", "POPESTIMATE2018", "POPESTIMATE2019", "POPESTIMATE2020"]]

CA_pop_2020_names = {
    "POPESTIMATE2010": "2010",
    "POPESTIMATE2011": "2011",
    "POPESTIMATE2012": "2012",
    "POPESTIMATE2013": "2013",
    "POPESTIMATE2014": "2014",
    "POPESTIMATE2015": "2015",
    "POPESTIMATE2016": "2016",
    "POPESTIMATE2017": "2017",
    "POPESTIMATE2018": "2018",
    "POPESTIMATE2019": "2019",
    "POPESTIMATE2020": "2020"
}

CA_pop_2020.rename(columns=CA_pop_2020_names, inplace=True)

#Read in 2000-2010 population data
pop_2010 = pd.read_csv("../Data/Population Data/Raw Data/co-est00int-tot.csv", encoding='latin1')
CA_pop_2010 = pop_2010[pop_2010['STNAME'] == "California"]
CA_pop_2010 = pd.merge(CA_pop_2010, fips_df[["FULLFP", "COUNTYNAME"]], left_on = "CTYNAME", right_on = "COUNTYNAME")
CA_pop_2010 = CA_pop_2010[["CTYNAME", "FULLFP", "POPESTIMATE2000", "POPESTIMATE2001", "POPESTIMATE2002", "POPESTIMATE2003", "POPESTIMATE2004", "POPESTIMATE2005", "POPESTIMATE2006", "POPESTIMATE2007", "POPESTIMATE2008", "POPESTIMATE2009", "POPESTIMATE2010"]]

CA_pop_2010_names = {
    "POPESTIMATE2000": "2000",
    "POPESTIMATE2001": "2001",
    "POPESTIMATE2002": "2002",
    "POPESTIMATE2003": "2003",
    "POPESTIMATE2004": "2004",
    "POPESTIMATE2005": "2005",
    "POPESTIMATE2006": "2006",
    "POPESTIMATE2007": "2007",
    "POPESTIMATE2008": "2008",
    "POPESTIMATE2009": "2009",
    "POPESTIMATE2010": "2010"
}

CA_pop_2010.rename(columns=CA_pop_2010_names, inplace=True)
CA_pop_2010 = CA_pop_2010.drop('2010', axis = 1)

#Read in 1990-2000 population data
header = [
    "Year", "FIPS Code", "White (Non-Hispanic)", "Black (Non-Hispanic)",
    "American Indian and Alaska Native (Non-Hispanic)", "Asian and Pacific Islander (Non-Hispanic)",
    "White (Hispanic Origin)", "Black (Hispanic Origin)",
    "American Indian and Alaska Native (Hispanic Origin)", "Asian and Pacific Islander (Hispanic Origin)"
]
pop_2000 = pd.read_csv("../Data/Population Data/Raw Data/co-99-10/CO-99-10.txt", sep="\s+", skiprows=33, names=header)

CA_pop_2000 = pd.merge(fips_df[["FULLFP", "COUNTYNAME"]], pop_2000, left_on="FULLFP", right_on="FIPS Code", how = 'inner').drop('FIPS Code', axis = 1)

columns_to_sum = [
    "White (Non-Hispanic)", "Black (Non-Hispanic)", 
    "American Indian and Alaska Native (Non-Hispanic)", 
    "Asian and Pacific Islander (Non-Hispanic)", 
    "White (Hispanic Origin)", "Black (Hispanic Origin)", 
    "American Indian and Alaska Native (Hispanic Origin)", 
    "Asian and Pacific Islander (Hispanic Origin)"
]

CA_pop_2000['Total'] = CA_pop_2000[columns_to_sum].sum(axis=1)
CA_pop_2000 = CA_pop_2000.drop(columns_to_sum, axis = 1)
CA_pop_2000 = pd.pivot_table(CA_pop_2000, values='Total', index=["COUNTYNAME", "FULLFP"],
                             columns=['Year'], aggfunc="sum").reset_index()
CA_pop_2000.rename(columns={"COUNTYNAME" : "CTYNAME"}, inplace=True)


#Read in 1980-1990 population data
pop_1990 = pd.read_csv("../Data/Population Data/Raw Data/pe-02.csv", skiprows= 5, header = 0)
pop_1990 = pop_1990.iloc[1:]
pop_1990['FIPS State and County Codes'] = pop_1990['FIPS State and County Codes'].apply(int)

CA_pop_1990 = pd.merge(fips_df[["FULLFP", "COUNTYNAME"]], pop_1990, left_on="FULLFP", right_on='FIPS State and County Codes', how = 'inner').drop(['FIPS State and County Codes', 'Race/Sex Indicator'], axis = 1)

CA_pop_1990 = CA_pop_1990.groupby(['FULLFP', 'COUNTYNAME', 'Year of Estimate']).sum().reset_index()

columns_to_sum = ['Under 5 years',
                  '5 to 9 years', '10 to 14 years', '15 to 19 years', '20 to 24 years',
                  '25 to 29 years', '30 to 34 years', '35 to 39 years', '40 to 44 years',
                  '45 to 49 years', '50 to 54 years', '55 to 59 years', '60 to 64 years',
                  '65 to 69 years', '70 to 74 years', '75 to 79 years', '80 to 84 years',
                  '85 years and over']

CA_pop_1990['Total'] = CA_pop_1990[columns_to_sum].sum(axis=1)
CA_pop_1990 = CA_pop_1990.drop(columns_to_sum, axis = 1)
CA_pop_1990['Year of Estimate'] = CA_pop_1990['Year of Estimate'].apply(int)
CA_pop_1990 = pd.pivot_table(CA_pop_1990, values='Total', index=["COUNTYNAME", "FULLFP"],
                             columns=['Year of Estimate'], aggfunc="sum").reset_index()
CA_pop_1990.rename(columns={"COUNTYNAME" : "CTYNAME"}, inplace=True)
CA_pop_1990.iloc[:, 2:] = CA_pop_1990.iloc[:, 2:].astype(int)

#Read in 1980-1990 population data
header = ["Code", "Area Name", "1970", "1971", "1972", "1973", "1974"]
pop_1980 = pd.read_fwf("../Data/Population Data/Raw Data/e7079co/E7079CO.DAT", skiprows=31, names=header)
fixed_split = pop_1980["1970"].str.split()
pop_1980["1974"] = pop_1980["1973"]
pop_1980["1973"] = pop_1980["1972"]
pop_1980["1972"] = pop_1980["1973"]
pop_1980[["1970", "1971"]] = fixed_split.apply(pd.Series)

# Function to merge split rows
def merge_split_rows(df):
    for i in range(len(df) - 1):
        if pd.isna(df.loc[i, 'Code']) or df.loc[i, 'Code'] == "":
            df.loc[i - 1, 'Area Name'] = f"{df.loc[i - 1, 'Area Name']} {df.loc[i, 'Area Name']}"
            for col in df.columns[2:]:
                if pd.isna(df.loc[i - 1, col]) or df.loc[i - 1, col] == "":
                    df.loc[i - 1, col] = df.loc[i, col]
            df.drop(index=i, inplace=True)

    df.reset_index(drop=True, inplace=True)
    return df

CA_pop_1980_early = pop_1980.iloc[410:472].reset_index(drop=True)
CA_pop_1980_late = pop_1980.iloc[475:537].reset_index(drop=True)
new_header = ["Code", "Area Name", "1970", "1971", "1972", "1973", "1974"]
CA_pop_1980_late.columns = ["Code", "Area Name", "1975", "1976", "1977", "1978", "1979"]
CA_pop_1980_late = CA_pop_1980_late.reset_index(drop=True)


CA_pop_1980_early = merge_split_rows(CA_pop_1980_early)
CA_pop_1980_early = CA_pop_1980_early.dropna(subset=['Code', 'Area Name']).reset_index(drop=True)

CA_pop_1980_late = merge_split_rows(CA_pop_1980_late)
CA_pop_1980_late = CA_pop_1980_late.dropna(subset=['Code', 'Area Name']).reset_index(drop=True)

CA_pop_1980 = pd.merge(CA_pop_1980_early, CA_pop_1980_late, left_on = ["Code", "Area Name"], right_on = ["Code", "Area Name"])
CA_pop_1980['Area Name'] = CA_pop_1980['Area Name'].str.replace(r' Co.$', ' County').str.replace(r' C$', ' County')
CA_pop_1980['Code'] = CA_pop_1980['Code'].apply(lambda x: int(x[1:]))
CA_pop_1980.iloc[:, 2:] = CA_pop_1980.iloc[:, 2:].astype(int)
new_order = ['Area Name', 'Code', '1970', '1971', '1972', '1973', '1974', '1975', '1976', '1977', '1978', '1979']
CA_pop_1980 = CA_pop_1980[new_order]

CA_pop_1980_names = {
    "Area Name" : "CTYNAME",
    "Code" : "FULLFP"
}

CA_pop_1980.rename(columns=CA_pop_1980_names, inplace=True)

#Complete California Population Time Series
CA_pop_1980_2023 = pd.merge(CA_pop_1980, CA_pop_1990, on=['CTYNAME', 'FULLFP'], suffixes=('_1980', '_1990'))
CA_pop_1980_2023 = pd.merge(CA_pop_1980_2023, CA_pop_2000, on=['CTYNAME', 'FULLFP'], suffixes=('', '_2000'))
CA_pop_1980_2023 = pd.merge(CA_pop_1980_2023, CA_pop_2010, on=['CTYNAME', 'FULLFP'], suffixes=('', '_2010'))
CA_pop_1980_2023 = pd.merge(CA_pop_1980_2023, CA_pop_2020, on=['CTYNAME', 'FULLFP'], suffixes=('', '_2020'))
CA_pop_1980_2023 = pd.merge(CA_pop_1980_2023, CA_pop_2023, on=['CTYNAME', 'FULLFP'], suffixes=('', '_2023'))

#Read in County Area Data
CA_area = pd.read_csv("../Data/Population Data/Raw Data/CA_Counties_Population_Density.csv")
CA_area['NAME10'] = CA_area['NAME10'].apply(lambda x: x + " County")

#Merge area and population df
CA_popden_1980_2023 = pd.merge(CA_pop_1980_2023, CA_area[['NAME10', 'Area_squaremiles']], left_on = 'CTYNAME', right_on = 'NAME10').drop('NAME10', axis = 1)

CA_popden_1980_2023.columns = CA_popden_1980_2023.columns.astype(str)
years = np.arange(1970, 2021)
for i in years:
    year_str = str(i)
    CA_popden_1980_2023[year_str + " Population Density"] = CA_popden_1980_2023[year_str] / CA_popden_1980_2023['Area_squaremiles']


CA_popden_1980_2023.to_csv("../Data/Population Data/Processed Data/CA Population 1980-2023.csv")