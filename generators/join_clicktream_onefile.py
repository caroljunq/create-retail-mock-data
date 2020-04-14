import pandas as pd
import json


# reading config
with open('../config.json') as data:
    config = json.load(data)


print("Initializing...")
out_path = config["output_path_files"]
click_success_file = config["clickstream_succeed"]["outfile"]
click_no_success_file = config["clickstream_no_succeed"]["outfile"]

outfile = config["joined_clickstream_file"]

print("Processing files...")

click_success = pd.read_csv(out_path + click_success_file)
click_no_success = pd.read_csv(out_path + click_no_success_file)

# appending data
total_df = click_success.append(click_no_success,ignore_index=True)

print(total_df.shape)

columns_names = ["customer_id","order_date","campaign_id","media_source","product_id"]

print("Saving file...")

# the index of tbe rows starts in 1
total_df.index +=  1

# naming first columns
total_df.index.name = "row_id"

# saving data as csv
total_df.to_csv(out_path + outfile,sep=",",header=columns_names)
