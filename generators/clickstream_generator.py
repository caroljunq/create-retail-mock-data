import mimesis
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
outfile = config["clickstream"]["outfile"]
outsize = config["clickstream"]["total"]
click_start_date = config["clickstream"]["click_start_date"]
click_end_date = config["clickstream"]["click_end_date"]

index_clickstream_start = 1

# config of media sources
media_sources = config["media"]["sources"]
media_prob = config["media"]["percentages"]

# data processing config
amounts_cpu = config["data_processing"]["amount_in_cpu"]
auto_batch = True if config["data_processing"]["auto_batch_based_in_cpu"] == "True" else False

# config of campaigns
n_campaings = len(config[language]["campaigns"])
index_campaign_start = config["campaigns"]["index_start"]
campaigns_id_range = list(range(index_campaign_start,index_campaign_start + n_campaings))

# config of customers
n_customers = config["customers"]["total"]
index_start_customer = config["customers"]["index_start"]

# config of products
n_products = config["products"]["total"]
index_start_product = config["products"]["index_start"]

# calculating random probabilities
campaigns_prob = common_functions.random_probabilities(index_campaign_start, index_campaign_start + n_campaings - 1)

# Global clickstream array
clickstream = []

def generate_clickstream(amount,index_start):
    # Generates clickstream info
    global clickstream

    results = []

    for i in range(amount):
        click_index = i + index_start 
        customer_id = random.randint(index_start_customer, index_start_customer + n_customers - 1) 
        order_date = common_functions.random_date(click_start_date, click_end_date, random.random())
        campaign_id = np.random.choice(campaigns_id_range, p=campaigns_prob)
        media = np.random.choice(media_sources, p=media_prob)  
        product_id = random.randint(index_start_product, index_start_product + n_products - 1) 
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
pool.apply_async(generate_clickstream, args=(residue, index_clickstream_start), callback=collect_clickstream)

# generating clickstream in parallel 
for i in range(number_of_loops):
    pool.apply_async(generate_clickstream, args=(amounts, (i * amounts) + residue + index_clickstream_start), callback=collect_clickstream)

# closing pool
pool.close()
pool.join()

# creating a data frame with the final results
df = pd.DataFrame(clickstream)

# columns names aka header
columns_names = ["customer_id","order_date","campaign_id","media_source","product_id"]

print("Saving file...")

# writing file
f = df.to_csv(out_path + outfile,header=columns_names,sep=",",index=False)
print("File was saved at path {}".format(out_path + outfile))



