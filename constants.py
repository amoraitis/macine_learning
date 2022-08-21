# TODO: Create class that reads from configuration file.

PHOTOS_URL_FORMAT = "https://www.airbnb.gr/rooms/{0}/photos"
PHOTOS_TARGET_CLASSNAME = "img._6tbg2q"

DATA_PATH = "C://maching_learning_data"
INSIDE_AIRBNB_DATA_URL = "http://insideairbnb.com/get-the-data.html"

# define here the cities to download. It is the city name in the top of the get data page sections.
CITIES = ["Athens, Attica, Greece"]

COLUMNS_TO_KEEP = ['id', 'name', 'description',
       'neighborhood_overview', 'host_response_time',
       'host_is_superhost', 'host_listings_count',
       'host_total_listings_count', 'host_identity_verified',
       'neighbourhood_cleansed', 'latitude',
       'longitude', 'property_type', 'room_type', 'accommodates',
       'bathrooms_text', 'bedrooms', 'beds', 'amenities', 'price',
       'minimum_nights', 'maximum_nights', 'minimum_minimum_nights',
       'maximum_minimum_nights', 'minimum_maximum_nights',
       'maximum_maximum_nights', 'minimum_nights_avg_ntm', 'maximum_nights_avg_ntm',
       'availability_30', 'availability_60', 'availability_90',
       'availability_365', 'number_of_reviews', 'review_scores_rating', 'review_scores_accuracy',
       'review_scores_cleanliness', 'review_scores_checkin',
       'review_scores_communication', 'review_scores_location',
       'review_scores_value', 'license', 'instant_bookable']

# Decide here if you want to download images, make sure to omit them if you are interested only in other features.
SHOULD_DOWNLOAD_IMAGES = False

UNKNOWN = "Unknown"
UNKNOWN_SHORT = "u"