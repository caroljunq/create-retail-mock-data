# This is dataset is composed by default campaigns, but you can change or add/remove them in the config.
import json
import pandas as pd
import random

# reading config
with open('../config.json') as data:
    config = json.load(data)

# setting up variables
out_path = config["output_path_files"]
outfile = config["campaigns"]["outfile"]
language = config["language"]
campaigns = config[language]["campaigns"]
outsize = len(campaigns)

camps = []

for i in range(outsize):
    print(i + 1,"processed")
    campaign_id = i + 1
    discount = round(random.uniform(0.1, 0.5),2)
    camps.append((campaign_id,campaigns[i],discount))

# adding "no campaign"
camps.append((0,"None",0))

# creating a data frame with the final results
df = pd.DataFrame(camps)

# columns names aka header
columns_names = ["campaign_id","title","discount"]

print("Saving file...")

# writing file
f = df.to_csv(out_path + outfile,header=columns_names,sep=",",index=False)
print("File was saved at path {}".format(out_path + outfile))