# The categories are fixed, do not change them.
import json
import pandas as pd

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

catgs = []

for i in range(outsize):
    print(i + 1,"processed")
    category_id = i + 1
    catgs.append((category_id,categories[language][i],"dummy dummy dummy dummy"))

# creating a data frame with the final results
df = pd.DataFrame(catgs)

# columns names aka header
columns_names = ["category_id","title","description"]

print("Saving file...")

# writing file
f = df.to_csv(out_path + outfile,header=columns_names,sep=",",index=False)
print("File was saved at path {}".format(out_path + outfile))

