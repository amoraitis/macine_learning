import geopandas
import numpy as np
from sklearn.neighbors import BallTree
import pandas as pd

def load_poi_data(df_coord, city_name):
    geo = geopandas.read_file(f"./data/italy-{city_name}.geojson", crs='EPSG:4326').dropna()
    geo["longitude"] = pd.to_numeric(geo.geometry.x.replace(".", ","))
    geo["latitude"] = pd.to_numeric(geo.geometry.y.replace(".", ","))
    geo = geo.drop(columns=["geometry", "cmt", "desc"])
    geo = pd.DataFrame(geo)
    
    listings_gps = df_coord[["latitude", "longitude"]].values
    listings_radians = np.radians(listings_gps)
    geo_gps = geo[["latitude", "longitude"]].values
    geo_radians = np.radians(geo_gps)

    r_km = 6371
    tree = BallTree(geo_radians, leaf_size=10, metric='haversine')
    distances, indices = tree.query(listings_radians, k = len(geo_radians))
    zipped = pd.DataFrame(zip(df_coord["id"], distances * r_km, indices), columns=list(["id", "n_pois_3klm_radius", "ids"]))

    return zipped.apply(filter_zipped, axis=1).drop(columns="ids")

def filter_zipped(row):
    row[1] = len(list(filter(lambda i: i<= 3, row[1])))
    return row

def load_transit_data(df_coord, city_name):
    geo = pd.read_csv(f"./data/{city_name}-stops.csv")
    if("parent_station" in geo.columns.values):
        geo = geo[geo["parent_station"].isna()] 
    geo["longitude"] = pd.to_numeric(geo.stop_lon.replace(".", ","))
    geo["latitude"] = pd.to_numeric(geo.stop_lat.replace(".", ","))
    geo = geo[["stop_id", "latitude", "longitude"]]
    
    
    listings_gps = df_coord[["latitude", "longitude"]].values
    listings_radians = np.radians(listings_gps)
    geo_gps = geo[["latitude", "longitude"]].values
    geo_radians = np.radians(geo_gps)

    r_km = 6371
    tree = BallTree(geo_radians, leaf_size=10, metric='haversine')
    distances, indices = tree.query(listings_radians, k = 1)
    zipped = pd.DataFrame(zip(df_coord["id"], distances * r_km * 1000, indices), columns=list(["id", "nearest_stop_m", "ids"]))
    return zipped.apply(extract_distance, axis=1).drop(columns="ids")
    
def extract_distance(row):
    row[1] = row[1][0]
    return row

