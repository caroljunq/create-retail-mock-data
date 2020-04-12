# This is dataset is composed by default payments methods, but you can change or add/remove in the config.
import json
import pandas as pd

# reading config
with open('../config.json') as data:
    config = json.load(data)

# setting up variables
out_path = config["output_path_files"]
outfile = config["payments"]["outfile"]
language = config["language"]
payments = config[language]["payments"]
outsize = len(payments)

payms = []

for i in range(outsize):
    print(i + 1,"processed")
    payment_id = i + 1
    payms.append((payment_id,payments[i],"dummy dummy dummy dummy"))

# creating a data frame with the final results
df = pd.DataFrame(payms)

# columns names aka header
columns_names = ["payment_id","title","description"]

print("Saving file...")

# writing file
f = df.to_csv(out_path + outfile,header=columns_names,sep=",",index=False)
print("File was saved at path {}".format(out_path + outfile))



