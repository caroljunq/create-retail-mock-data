import mimesis
import json
import common_functions
import random
import numpy as np

# reading config
with open('../config.json') as data:
    config = json.load(data)

# setting up variables
out_path = config["output_path_files"]
outfile = config["products"]["outfile"]
outsize = config["products"]["total"]
language = config["language"]
suppliers_id_range = range(1,config["suppliers"]["total"] + 1)

# loading mocking data type
food = mimesis.Food(language)
person = mimesis.Person(language)
dates = mimesis.Datetime(language)
code = mimesis.Code(language)

# 5 categories
categories_prob = common_functions.random_probabilities(1,5)
suppliers_prob  = common_functions.random_probabilities(1,len(suppliers_id_range))


# generating products
with open(out_path + outfile, 'w') as csvfile:
    for i in range(outsize):
        product_id = i + 1
        print(i)
        category_id = np.random.choice([1,2,3,4,5], p=categories_prob)
        # dish category
        if category_id == 1:
            name = food.dish()
        # drink
        if category_id == 2:
            name = food.drink()
        # fruit
        if category_id == 3:
            name = food.fruit()
        # spices
        if category_id == 4:
            name = food.spices()
        # vegetable
        if category_id == 5:
            name = food.vegetable()
        name = name.replace(",","") # removing possible commas ,
        weight = random.random() * 1000
        price = random.random() * 10
        description = "food food food food food food food food food food food food food food food food food"
        supplier_id = np.random.choice(suppliers_id_range, p=suppliers_prob)
        barcode = code.ean()
        csvfile.write(f"{product_id},{name},{category_id},{weight},{price},{description},{supplier_id},{barcode})\n")

     