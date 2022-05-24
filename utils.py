import os

data_path = "C://maching_learning_data"
files = ["listings.csv.gz", "calendar.csv.gz", "reviews.csv.gz"]


def isBlank (myString):
    if myString and myString != None and myString != np.nan and str(myString).strip():
        #myString is not None AND myString is not empty or blank
        return False
    #myString is None OR myString is empty or blank
    return True

def delete_file(filename):
    if os.path.exists(filename):
        os.remove(filename)
    else:
        print("The file does not exist")
