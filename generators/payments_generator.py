# This is dataset is composed by default payments methods, but you can change or add/remove in the config.
import mimesis
import json
import common_functions
import numpy as np

# reading config
with open('config.json') as data:
    config = json.load(data)

# setting up variables
out_path = config["output_path_files"]
outfile = config["payments"]["outfile"]
language = config["language"]
payments = config[language]["payments"]
outsize = len(payments)

with open(out_path + outfile, 'w') as csvfile:
    for i in range(outsize):
        print(i)
        payment_id = i + 1
        csvfile.write(f"{payment_id},{payments[i]},dummy dummy dummy dummy\n")


