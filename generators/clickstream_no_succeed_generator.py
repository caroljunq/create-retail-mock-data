import json
import common_functions
import random
import numpy as np
import multiprocessing as mp
import pandas as pd

# reading config
with open('../config.json') as data:
    config = json.load(data)

# general config
machine_cores = int(config["n_cores"])
out_path = config["output_path_files"]
language = config["language"]

# clickstream config
outfile = config["clickstream_no_succeed"]["outfile"]
outsize = config["clickstream_no_succeed"]["total"]
click_start_date = config["clickstream_no_succeed"]["click_start_date"]
click_end_date = config["clickstream_no_succeed"]["click_end_date"]

# config of media sources
media_sources = config["media"]["sources"]
media_prob = config["media"]["percentages"]

# data processing config
amounts_cpu = config["data_processing"]["amount_in_cpu"]
auto_batch = True if config["data_processing"]["auto_batch_based_in_cpu"] == "True" else False

# config of campaigns
n_campaings = len(config[language]["campaigns"])
campaigns_id_range = list(range(1,n_campaings + 1))

# config of customers
n_customers = config["customers"]["total"]

# config of products
n_products = config["products"]["total"]

# calculating random probabilities
campaigns_prob = common_functions.random_probabilities(1,n_campaings)

# Global clickstream array
clickstream = []

def generate_clickstream(amount):
    # Generates no succeed clicks
    global clickstream

    results = []

    for _ in range(amount):
        customer_id = random.randint(1, n_customers) 
        order_date = common_functions.random_date(click_start_date, click_end_date, random.random())
        campaign_id = np.random.choice(campaigns_id_range, p=campaigns_prob)
        media = np.random.choice(media_sources, p=media_prob)  
        product_id = random.randint(1, n_products) 
        results.append((customer_id,order_date,campaign_id,media,product_id))
    
    return results

def collect_clickstream(results):
    # collect intermediate results
    global clickstream
    # collect clickstream info and add to the clickstream array
    clickstream = clickstream + list(results)

    # print the process
    print("{} processed".format(len(clickstream)))

# setting the number of cores used by the process, aka how many processes will run in parallel 
print("Initializing using {} cores".format(machine_cores))
pool = mp.Pool(machine_cores)

# numbers of generated items in each loop
amounts = int(outsize/machine_cores) if auto_batch else amounts_cpu
number_of_loops = int(outsize/amounts)
residue = outsize - amounts * number_of_loops

# first generating residue
pool.apply_async(generate_clickstream, args=(residue,), callback=collect_clickstream)

# generating clickstream in parallel 
for i in range(number_of_loops):
    pool.apply_async(generate_clickstream, args=(amounts,), callback=collect_clickstream)

# closing pool
pool.close()
pool.join()

# creating a data frame with the final results
df = pd.DataFrame(clickstream)

# columns names aka header
columns_names = ["customer_id","order_date","campaign_id","media_source","product_id"]

print("Saving file...")
# writing file
df.to_csv(out_path + outfile,header=columns_names,sep=",",index=False)
print("File was saved at path {}".format(out_path + outfile))



