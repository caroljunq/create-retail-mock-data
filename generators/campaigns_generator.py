# This is dataset is composed by default campaigns, but you can change or add/remove them in the config.
import json
import pandas as pd
import random

# reading config
with open('../config.json') as data:
    config = json.load(data)

# setting up variables
out_path = config["output_path_files"]
index_payment_start = config["campaigns"]["index_start"]
outfile = config["campaigns"]["outfile"]
language = config["language"]
campaigns = config[language]["campaigns"]
header_in_csv = True if config["header_in_csv"] == "True" else False
outsize = len(campaigns)

camps = []

for i in range(outsize):
    print(i + 1,"processed")
    campaign_id = i + index_payment_start
    discount = round(random.uniform(0.1, 0.5),2)
    camps.append((campaign_id,campaigns[i],discount))

# creating a data frame with the final results
df = pd.DataFrame(camps)

# columns names aka header
columns_names = ["campaign_id","title","discount"]

print("Saving file...")
# Defining if header will be included
header = False if header_in_csv == False else columns_names

# writing file
f = df.to_csv(out_path + outfile,header=header,sep=",",index=False)
print("File was saved at path {}".format(out_path + outfile))