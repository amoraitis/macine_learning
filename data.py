# py -m pip install pandas
from importlib.resources import path
import requests
from bs4 import BeautifulSoup
import pandas as pd
import gzip
import shutil
import os
from utils import files, data_path, delete_file
import schema
import chromedriver_autoinstaller
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import time
import matplotlib.image as img
import utils


def extract_values(x, soup):
    table = soup.find('table', class_=x.name).tbody
    [tr.extract() for tr in table.find_all('tr',class_=["archived"])]

    for tr in table.find_all('tr'):
        if tr.contents[2].text in files:
            f = tr.contents[2].text
            td = tr.find("td", text=f)
            x[f] = td.contents[0].attrs['href']

# now create the folder structure
def download_data(x):
    full_path = os.path.join(base_dir, x.name)

    if(os.path.exists(full_path) == False):
        os.mkdir(full_path)

    for file in files:
        file_path = os.path.join(full_path, file)
        if os.path.exists(file_path) or (file.endswith("gz") and os.path.exists(file_path[:-3])):
            continue

        r = requests.get(x[file], allow_redirects=True)

        with open(file_path, 'wb') as f:
            f.write(r.content)

        if file.endswith("gz"):
            with gzip.open(file_path, 'rb') as f_in:
                with open(file_path[:-3], 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            delete_file(file_path)
            file_path = file_path[:-3]
        
#         x[file] = file_pathimg = PIL.Image.open(urllib.request.urlopen('https://a0.muscache.com/im/pictures/monet/Luxury-553449454187790697/original/f2dba9f3-a8f2-4c42-bab3-d2811d0837a7?im_w=720'))
# width, height = img.size

# with open('myfile.txt', 'a') as myFile:
#     for y in range(height):
#         for x in range(width):
#             pixel = img.getpixel((x, y))
#             myFile.write(f'{pixel[0]} {pixel[1]} {pixel[2]}')
#             myFile.write(' ')
#         myFile.write('\n')

def get_url_images(ids, base_path):
    opt = webdriver.ChromeOptions()
    opt.add_argument("--start-maximized")

    chromedriver_autoinstaller.install()
    driver = webdriver.Chrome(options=opt)
    urls = {}
    photo_ids = {}
    for id in ids:
        driver.get(f'https://www.airbnb.gr/rooms/{id}/photos')

        html = driver.find_element(By.TAG_NAME, 'html')
        html.send_keys(Keys.END)
        time.sleep(1)

        elems = driver.find_elements(By.CSS_SELECTOR, "img._6tbg2q")
        elems = list(filter(lambda x: str.isnumeric(x.get_attribute('id')), elems))
        urls[id] = list(map(lambda x: x.get_attribute('src'), elems))
        photo_ids[id] = list(map(lambda x: x.get_attribute('id'), elems))
        
        for i in range(len(urls[id])):
            img_data = requests.get().content
            path_to_photo = f'./url_{photo_ids[id][i]}.jpg'
            with open(path_to_photo, 'wb') as handler:
                handler.write(img_data)

            batman_image = img.imread(urllib.request.urlopen(urls[id][i]))
            utils.delete_file()
            with open(path_to_photo, 'a') as handler:
                for row in batman_image:
                    for temp_r, temp_g, temp_b in row:
                        handler.write(f'({temp_r},{temp_g},{temp_b})')
    driver.quit()

# instance vars
base_dir = data_path

# define here the cities to download. It is the city name in the top of the get data page sections.
list_of_cities = ["London, England, United Kingdom"]

def main():
    print("Data will be written into: " + base_dir)

    dict_of_cities = pd.DataFrame(columns=["list_of_cities"]+files)
    dict_of_cities["list_of_cities"] = list_of_cities
    dict_of_cities.index = dict_of_cities["list_of_cities"].apply(lambda x: x.split(',')[0].lower())

    URL = "http://insideairbnb.com/get-the-data.html"
    page = requests.api.get(URL)
    soup = BeautifulSoup(page.content, "html.parser")
        
    dict_of_cities.apply(lambda x: extract_values(x, soup), axis=1)

    if os.path.exists(base_dir) == False:
        os.mkdir(base_dir)

    dict_of_cities.apply(download_data, axis=1)
    dict_of_cities.index.names = ['city_id']
    dict_of_cities.to_csv(os.path.join(base_dir, "description.csv"))
# schema.create_schema()
# schema.transform_and_import_data()

