# The categories are fixed, do not change them.
import mimesis
import json
import common_functions
import numpy as np

# reading config
with open('../config.json') as data:
    config = json.load(data)

# setting up variables
out_path = config["output_path_files"]
outfile = config["categories"]["outfile"]

language = config["language"]

categories = {
    "en": ["dish","drink","fruit","spices","vegetable"],
    "pt-br":["prato","bebida","fruta","tempero","vegetal"]
}

outsize = len(categories[language])

with open(out_path + outfile, 'w') as csvfile:
    for i in range(outsize):
        print(i)
        category_id = i + 1
        csvfile.write(f"{category_id},{categories[language][i]},dummy dummy dummy dummy\n")


