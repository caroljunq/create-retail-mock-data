# This is dataset is composed by default payments methods, but you can change or add/remove in the config.
import mimesis
import json
import common_functions
import numpy as np
import pandas as pd

# reading config
with open('../config.json') as data:
    config = json.load(data)

# setting up variables
out_path = config["output_path_files"]
index_payment_start = config["payments"]["index_start"]
outfile = config["payments"]["outfile"]
language = config["language"]
payments = config[language]["payments"]
header_in_csv = True if config["header_in_csv"] == "True" else False
outsize = len(payments)

payms = []

for i in range(outsize):
    print(i + 1,"processed")
    payment_id = i + index_payment_start
    payms.append((payment_id,payments[i],"dummy dummy dummy dummy"))

# creating a data frame with the final results
df = pd.DataFrame(payms)

# columns names aka header
columns_names = ["payment_id","title","description"]

print("Saving file...")
# Defining if header will be included
header = False if header_in_csv == False else columns_names

# writing file
f = df.to_csv(out_path + outfile,header=header,sep=",",index=False)
print("File was saved at path {}".format(out_path + outfile))



