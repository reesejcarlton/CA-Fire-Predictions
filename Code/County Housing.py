# -*- coding: utf-8 -*-
"""
Created on Wed Jun 12 11:57:37 2024

@author: reese
"""

import os
import csv

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Change the working directory to the script's directory
os.chdir(script_dir)


# Data to be written to CSV
data = [
    ["County", "Value"],
    ["Alameda", "615,077"],
    ["Alpine", "1,778"],
    ["Amador", "18,450"],
    ["Butte", "100,033"],
    ["Calaveras", "28,065"],
    ["Colusa", "8,068"],
    ["Contra Costa", "415,919"],
    ["Del Norte", "11,423"],
    ["El Dorado", "91,105"],
    ["Fresno", "333,769"],
    ["Glenn", "11,041"],
    ["Humboldt", "63,315"],
    ["Imperial", "57,895"],
    ["Inyo", "9,592"],
    ["Kern", "300,377"],
    ["Kings", "46,645"],
    ["Lake", "34,745"],
    ["Lassen", "12,788"],
    ["Los Angeles", "3,561,069"],
    ["Madera", "50,966"],
    ["Marin", "113,182"],
    ["Mariposa", "10,404"],
    ["Mendocino", "40,926"],
    ["Merced", "85,756"],
    ["Modoc", "5,288"],
    ["Mono", "14,138"],
    ["Monterey", "142,399"],
    ["Napa", "55,460"],
    ["Nevada", "54,258"],
    ["Orange", "1,111,227"],
    ["Placer", "167,134"],
    ["Plumas", "15,864"],
    ["Riverside", "848,597"],
    ["Sacramento", "574,438"],
    ["San Benito", "19,438"],
    ["San Bernardino", "725,896"],
    ["San Diego", "1,224,375"],
    ["San Francisco", "401,452"],
    ["San Joaquin", "245,541"],
    ["San Luis Obispo", "122,979"],
    ["San Mateo", "279,457"],
    ["Santa Barbara", "158,333"],
    ["Santa Clara", "678,496"],
    ["Santa Cruz", "106,728"],
    ["Shasta", "79,194"],
    ["Sierra", "2,361"],
    ["Siskiyou", "24,214"],
    ["Solano", "158,808"],
    ["Sonoma", "205,225"],
    ["Stanislaus", "182,290"],
    ["Sutter", "34,477"],
    ["Tehama", "27,676"],
    ["Trinity", "8,945"],
    ["Tulare", "150,210"],
    ["Tuolumne", "31,628"],
    ["Ventura", "291,019"],
    ["Yolo", "78,531"],
    ["Yuba", "28,693"]
]

# Write data to CSV file
with open('../Data/county_housing_units.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerows(data)

print("CSV file created successfully.")