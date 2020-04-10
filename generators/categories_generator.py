# This is dataset is composed by 5 categories, but you can change or add/remove.
# The original categories are: dish, drink, fruit, spices, vegetable
import mimesis
import json
import common_functions
import numpy as np

# reading config
with open('config.json') as data:
    config = json.load(data)

# setting up variables
out_path = config["output_path_files"]
outfile = config["categories"]["outfile"]
language = config["language"]
categories = config[language]["categories"]
outsize = len(categories)

with open(out_path + outfile, 'w') as csvfile:
    for i in range(outsize):
        print(i)
        category_id = i + 1
        csvfile.write(f"{category_id},{categories[i]},dummy dummy dummy dummy\n")


