import os
import pandas as pd
import pyodbc
import common.utils as utils
from common.utils import isBlank
from numpy import NaN
import common.geo_utils as geo_utils
import numpy as np
from timeit import default_timer as timer

from constants import UNKNOWN, UNKNOWN_SHORT

###
### Normalize data
###
def clean_listing_columns(listings_df):
    listings_table_headers = pd.Index(utils.getTableColumns(conn, 'listings'))

    listings_csv_headers = pd.Index(listings_df.columns.values.tolist())
    intersected_cols = listings_table_headers.intersection(listings_csv_headers)
    dropped_cols =  [col for col in listings_csv_headers if col not in intersected_cols] 

    for dropped_col in dropped_cols:
        listings_df.pop(dropped_col)

    return listings_df

def fix_price_col(p):
    if isBlank(p) == False:
        p = p[1:]

        if len(p.split(".")) > 0:
            p = p.split(".")[0]
        
        p = int(p.replace(',', ''))
    
    return p

def cleanup_data(row):
    listings_df = pd.read_csv(row[utils.files[0]])
    listings_df = clean_listing_columns(listings_df)

    listings_df["beds"] = listings_df["beds"].fillna(0)
    listings_df["accommodates"] = listings_df["accommodates"].fillna(0)
    listings_df["bedrooms"] = listings_df["bedrooms"].fillna(0)

    listings_df["bathrooms_text"] = listings_df["bathrooms_text"].fillna(UNKNOWN)
    listings_df["host_is_superhost"] = listings_df["host_is_superhost"].fillna(UNKNOWN_SHORT)
    listings_df["host_identity_verified"] = listings_df["host_identity_verified"].fillna(UNKNOWN_SHORT)
    listings_df["host_name"] = listings_df["host_name"].fillna(UNKNOWN)
    listings_df["name"] = listings_df["name"].fillna(UNKNOWN)
    listings_df["neighbourhood_cleansed"] = listings_df["neighbourhood_cleansed"].fillna(UNKNOWN)

    listings_df["city_name"] = listings_df.apply(lambda r: row[1].split(',')[0], axis=1)

    pois_resulted = geo_utils.load_poi_data(listings_df[["id", "longitude", "latitude"]], row[0])
    listings_df = pd.merge(listings_df, pois_resulted, on=['id'], how='outer')
    transit_resulted = geo_utils.load_transit_data(listings_df[["id", "longitude", "latitude"]], row[0])
    listings_df = pd.merge(listings_df, transit_resulted, on=['id'], how='outer')

    chunk_size=250000
    rows = 0

    for chunk in pd.read_csv(row[utils.files[1]], chunksize=chunk_size, iterator=True):
        chunk = chunk[np.in1d(chunk["listing_id"], listings_df["id"])]
        
        chunk["available"] = chunk["available"].replace("", NaN)
        chunk['available'] = chunk['available'].fillna(UNKNOWN_SHORT)
        chunk["price"] = chunk["price"].apply(fix_price_col)
        chunk["adjusted_price"] = chunk["adjusted_price"].apply(fix_price_col)
        rows+=len(chunk)

def transform_and_import_data(data_dir):
    description_df = pd.read_csv(os.path.join(data_dir, "description.csv"))
    description_df.apply(insert_data_into_db, axis=1)