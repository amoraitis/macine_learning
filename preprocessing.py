from asyncio import constants
import os
import pandas as pd
import common.utils as utils
from common.utils import isBlank
from numpy import NaN
import numpy as np
import constants

from constants import UNKNOWN, UNKNOWN_SHORT

###
### Normalize data
###
def clean_listing_columns(listings_df):
    listings_table_headers = pd.Index(constants.COLUMNS_TO_KEEP)

    listings_csv_headers = pd.Index(listings_df.columns.values.tolist())
    intersected_cols = listings_table_headers.intersection(listings_csv_headers)
    dropped_cols =  [col for col in listings_csv_headers if col not in intersected_cols] 

    for dropped_col in dropped_cols:
        listings_df.pop(dropped_col)

    return listings_df

def normalize_license_data(license):
    if isBlank(license):
        license = 'f'
    else:
        license = 't'

    return license

def fix_price_col(p):
    if isBlank(p) == False:
        p = p[1:]

        if len(p.split(".")) > 0:
            p = p.split(".")[0]
        
        p = int(p.replace(',', ''))
    
    return p

def cleanup_data(row):
    path = row[utils.files[0]]
    listings_df = pd.read_csv(path)
    listings_df = clean_listing_columns(listings_df)

    listings_df["beds"] = listings_df["beds"].fillna(0)
    listings_df["accommodates"] = listings_df["accommodates"].fillna(0)
    listings_df["bedrooms"] = listings_df["bedrooms"].fillna(0)

    listings_df["bathrooms_text"] = listings_df["bathrooms_text"].fillna(UNKNOWN)
    listings_df["host_is_superhost"] = listings_df["host_is_superhost"].fillna(UNKNOWN_SHORT)
    listings_df["host_identity_verified"] = listings_df["host_identity_verified"].fillna(UNKNOWN_SHORT)
    listings_df["name"] = listings_df["name"].fillna(UNKNOWN)
    listings_df["neighbourhood_cleansed"] = listings_df["neighbourhood_cleansed"].fillna(UNKNOWN)
    listings_df["license"] = listings_df["license"].apply(normalize_license_data)
    
    return listings_df

def transform_and_import_data(data_dir):
    description_df = pd.read_csv(os.path.join(data_dir, "description.csv"))
    return cleanup_data(description_df.iloc[0])