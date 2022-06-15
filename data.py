# py -m pip install pandas
from tokenize import String
import requests
from bs4 import BeautifulSoup
import pandas as pd
import gzip
import shutil
import os
from utils import files, delete_file
import chromedriver_autoinstaller
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import time
import utils
import constants

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

# now create the folder structure
def download_data(x, should_download_images = False):
    full_path = os.path.join(BASE_DIR, x.name)
    utils.try_create_dir(full_path):
        

    for file in files:
        file_path = os.path.join(full_path, file)

        if os.path.exists(file_path) or (file.endswith("gz") and os.path.exists(file_path[:-3])):
            continue

        r = requests.get(x[file], allow_redirects=True)
        utils.copy_stream_to_file(r.content, file_path)

        if file.endswith("gz"):
            with gzip.open(file_path, 'rb') as f_in:
                with open(file_path[:-3], 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            delete_file(file_path)
            file_path = file_path[:-3]
        
        x[file] = file_path

    if should_download_images:
        download_images_for(x)

    return x

def download_images_for(row):
    listings = pd.read_csv(row[utils.files[0]], usecols=['id'])
    save_images_from_url_for(listings['id'].tolist(), f'{BASE_DIR}//{row.name}')

def save_images_from_url_for(ids: list, base_path: String):
    opt = webdriver.ChromeOptions()
    opt.add_argument("--start-maximized")

    chromedriver_autoinstaller.install()
    driver = webdriver.Chrome(options=opt)
    urls = {}
    photo_ids = {}

    for id in ids:
        current_listing_photos_dir = f'{base_path}//photos//{id}'

        if utils.try_create_dir(current_listing_photos_dir) == False:
            continue

        driver.get(constants.PHOTOS_URL_FORMAT.format(id))

        html = driver.find_element(By.TAG_NAME, 'html')
        html.send_keys(Keys.END)
        time.sleep(1)

        elems = driver.find_elements(By.CSS_SELECTOR, constants.PHOTOS_TARGET_CLASSNAME)
        elems = list(filter(lambda x: str.isnumeric(x.get_attribute('id')), elems))
        urls[id] = list(map(lambda x: x.get_attribute('src'), elems))
        photo_ids[id] = list(map(lambda x: x.get_attribute('id'), elems))

        for i in range(len(urls[id])):
            img_data = requests.get(urls[id][i]).content
            path_to_photo = f'{current_listing_photos_dir}//{photo_ids[id][i]}.jpg'
            utils.copy_stream_to_file(img_data, path_to_photo)

    driver.quit()

# instance vars
BASE_DIR = constants.DATA_PATH

def main():
    print("Data will be written into: " + BASE_DIR)

    dict_of_cities = pd.DataFrame(columns=["list_of_cities"]+files)
    dict_of_cities["list_of_cities"] = constants.CITIES
    dict_of_cities.index = dict_of_cities["list_of_cities"].apply(lambda x: x.split(',')[0].lower())

    page = requests.api.get(constants.INSIDE_AIRBNB_DATA_URL)
    soup = BeautifulSoup(page.content, "html.parser")
        
    dict_of_cities.apply(lambda x: extract_values(x, soup), axis=1)

    if os.path.exists(BASE_DIR) == False:
        os.makedirs(BASE_DIR)
    elif __debug__:
        # only for debug
        shutil.rmtree(BASE_DIR, ignore_errors=True)
        os.makedirs(BASE_DIR)

    dict_of_cities.apply(download_data, axis=1, should_download_images= constants.SHOULD_DOWNLOAD_IMAGES)
    dict_of_cities.index.names = ['city_id']
    dict_of_cities.to_csv(os.path.join(BASE_DIR, "description.csv"))

if __name__ == "__main__":
    main()
# schema.create_schema()
# schema.transform_and_import_data()

