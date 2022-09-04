import os
from tokenize import String
import numpy as np
import requests
import shutil
import gzip
import haversine as hs
import pandas as pd
import chromedriver_autoinstaller
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import re
import string


files = ["listings.csv.gz"]

def isBlank(myString):
    if myString and myString != None and myString != np.nan and str(myString).strip():
        #myString is not None AND myString is not empty or blank
        return False
    #myString is None OR myString is empty or blank
    return True

def delete_file(filename):
    if os.path.exists(filename):
        os.remove(filename)
    else:
        print(f"{filename} does not exist")

def try_create_dir(path: str) -> bool:
    if os.path.exists(path) == False:
        os.makedirs(path)
        return True
    else:
        return False

def copy_stream_to_file(stream, file):
    with open(file, 'wb') as handler:
        handler.write(stream)


# now create the folder structure
def download_data(x):
    full_path = os.path.join(Constants().DATA_PATH, x.name)
    try_create_dir(full_path)        

    for file in files:
        file_path = os.path.join(full_path, file)

        if os.path.exists(file_path) or (file.endswith("gz") and os.path.exists(file_path[:-3])):
            continue

        r = requests.get(x[file], allow_redirects=True)
        copy_stream_to_file(r.content, file_path)

        if file.endswith("gz"):
            with gzip.open(file_path, 'rb') as f_in:
                with open(file_path[:-3], 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            delete_file(file_path)
            file_path = file_path[:-3]
        
        x[file] = file_path

    return x
    
def extract_values(x, soup):
    # Extract the urls to the files we are interested in.
    # Remarks: omit archived data.
    table = soup.find('table', class_=x.name).tbody
    [tr.extract() for tr in table.find_all('tr',class_=["archived"])]

    for tr in table.find_all('tr'):
        if tr.contents[2].text in files:
            f = tr.contents[2].text
            td = tr.find("td", text=f)
            x[f] = td.contents[0].attrs['href']


def normalize_license_data(license):
    if isBlank(license):
        license = 'f'
    else:
        license = 't'

    return license

def clean_listing_columns(listings_df):
    listings_table_headers = pd.Index(Constants().COLUMNS_TO_KEEP)

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
    path = row[files[0]]
    listings_df = pd.read_csv(path)
    
    listings_df = clean_listing_columns(listings_df)
    listings_df["license"] = listings_df["license"].apply(normalize_license_data)
    listings_df["price"] = listings_df["price"].apply(fix_price_col)
    
    num_cols = ["accommodates", "availability_30", "availability_60", "availability_90", "availability_365", "maximum_nights", "minimum_nights", "number_of_reviews", "price"]
    listings_df[num_cols] = listings_df[num_cols].apply(pd.to_numeric, errors='coerce')
    
    syntagma_coords = (37.9758646,23.737232)

    listings_df["distance_from_city_centre"] = listings_df.apply(lambda x: hs.haversine((x['latitude'], x['longitude']), syntagma_coords), axis = 1)
    return listings_df

def transform_and_import_data(data_dir):
    description_df = pd.read_csv(os.path.join(data_dir, "description.csv"))
    return cleanup_data(description_df.iloc[0])

# Remove emojis from the text. 
def remove_emoji(text):
    emoji_pattern = re.compile("["
                           u"\U0001F600-\U0001F64F"  # emoticons
                           u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                           u"\U0001F680-\U0001F6FF"  # transport & map symbols
                           u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                           u"\U00002702-\U000027B0"
                           u"\U000024C2-\U0001F251"
                           "]+", flags=re.UNICODE)
    return emoji_pattern.sub(r'', text)


# Removing any urls that have left in the text.
def remove_url(text): 
    url_pattern  = re.compile('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+')
    return url_pattern.sub(r'', text)


# Clean the rest of the text.
def clean_text(text ): 
    delete_dict = {sp_character: '' for sp_character in string.punctuation} 
    delete_dict[' '] = ' ' 
    table = str.maketrans(delete_dict)
    text1 = text.translate(table)
    #print('cleaned:'+text1)
    textArr= text1.split()
    text2 = ' '.join([w for w in textArr if ( not w.isdigit() and  ( not w.isdigit() and len(w)>2))]) 
    
    return text2.lower()

def clean_text2(text):
    """
    Pre process and convert texts to a list of words
    :param text:
    :return:
    """

    #text = str(text)
    #text = text.lower()

    # Clean the text
    text = re.sub(r"[^A-Za-z0-9^,!.\/'+-=]", " ", text)
    text = re.sub(r"what's", "what is ", text)
    text = re.sub(r"\'s", " ", text)
    text = re.sub(r"\'ve", " have ", text)
    text = re.sub(r"can't", "cannot ", text)
    text = re.sub(r"n't", " not ", text)
    text = re.sub(r"i'm", "i am ", text)
    text = re.sub(r"\'re", " are ", text)
    text = re.sub(r"\'d", " would ", text)
    text = re.sub(r"\'ll", " will ", text)
    text = re.sub(r",", " ", text)
    text = re.sub(r"\.", " ", text)
    text = re.sub(r"!", " ! ", text)
    text = re.sub(r"\/", " ", text)
    text = re.sub(r"\^", " ^ ", text)
    text = re.sub(r"\+", " + ", text)
    text = re.sub(r"\-", " - ", text)
    text = re.sub(r"\=", " = ", text)
    text = re.sub(r"'", " ", text)
    text = re.sub(r"(\d+)(k)", r"\g<1>000", text)
    text = re.sub(r":", " : ", text)
    text = re.sub(r"\0s", "0", text)
    text = re.sub(r"e - mail", "email", text)
    text = re.sub(r"j k", "jk", text)
    text = re.sub(r"\s{2,}", " ", text)

    return text

class Constants:
  DATA_PATH = "/maching_learning_data"
  INSIDE_AIRBNB_DATA_URL = "http://insideairbnb.com/get-the-data.html"

  # define here the cities to download. It is the city name in the top of the get data page sections.
  CITIES = ["Athens, Attica, Greece"]

  COLUMNS_TO_KEEP = ['id', 'picture_url', 'name', 'description',
        'neighborhood_overview', 'host_response_time',
        'host_is_superhost', 'host_listings_count', "host_since",
        'host_total_listings_count', 'host_identity_verified',
        'neighbourhood_cleansed', 'latitude', 'amenities',
        'longitude', 'property_type', 'room_type', 'accommodates',
        'bathrooms_text', 'bedrooms', 'beds', 'price',
        'minimum_nights', 'maximum_nights',
        'availability_30', 'availability_60', 'availability_90',
        'availability_365', 'number_of_reviews', 'review_scores_rating', 'review_scores_accuracy',
        'review_scores_cleanliness', 'review_scores_checkin',
        'review_scores_communication', 'review_scores_location',
        'review_scores_value', 'license', 'instant_bookable']

  UNKNOWN = "Unknown"
  UNKNOWN_SHORT = "u"