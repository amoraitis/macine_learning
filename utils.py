import os
import numpy as np

files = ["listings.csv.gz", "calendar.csv.gz", "reviews.csv.gz"]

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

def create_dir(path):
    if os.path.exists(path) == False:
        os.makedirs(path)
    else:
        print(f'{path} already exists')

def copy_stream_to_file(stream, file):
    with open(file, 'wb') as handler:
        handler.write(stream)