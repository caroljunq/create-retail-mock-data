# Generates orders items and clickstream orders succeed
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

# data processing config
amounts_cpu = config["data_processing"]["amount_in_cpu"]
auto_batch = True if config["data_processing"]["auto_batch_based_in_cpu"] == "True" else False

# orders items config
outfile = config["orders_items"]["outfile"]
number_max_prod_per_order = config["orders_items"]["number_max_prod_per_order"]

# orders config
orders_file = config["orders"]["outfile"]
n_orders = config["orders"]["total"]

# products config
n_products =  config["products"]["total"]

# orders info
orders = pd.read_csv(out_path + orders_file)

# clickstream succeed
clickstream_outfile = config["clickstream_succeed"]["outfile"]

# random probabilities
num_products_prob = common_functions.random_probabilities(1, number_max_prod_per_order)
quantities = list(range(1,number_max_prod_per_order + 1))

# Global orders items and clickstrem succeed array
orders_items = []
clickstream_succeed = []
ids = set()

def generate_orders_items(amount, index_start):
    # generates orders items and succeed clickstream
    
    orders_items_partial = []
    clickstream_succeed_partial = []

    for i in range(amount):
        index = index_start + i - 1
        order_id = orders["order_id"][index]
        customer_id = orders["customer_id"][index]
        order_date = orders["order_date"][index]
        campaign_id = orders["campaign_id"][index]
        media_source = orders["media_source"][index]
        num_products = np.random.choice(quantities, p=num_products_prob)  
        for _ in range(num_products):
            quantity = random.randint(1,100) # random quantity, between 1 and 100 products
            
            product_id = random.randint(1,n_products)
    
            orders_items_partial.append((order_id, product_id, quantity))
            
            if campaign_id != 0:
                clickstream_succeed_partial.append((customer_id,order_date,campaign_id,media_source,product_id))

    return (orders_items_partial,clickstream_succeed_partial)

def collect_orders_items(results):
    # shows how many orders items were processed
    global orders_items
    global clickstream_succeed

    orders_items = orders_items + results[0]
    clickstream_succeed = clickstream_succeed + results[1]


    # print the process
    print("{} processed".format(len(orders_items)))

# The number of orders items row will be at least equal to the order size.
# 
outsize = n_orders

# setting the number of cores used by the process, aka how many processes will run in parallel 
print("Initializing using {} cores".format(machine_cores))
pool = mp.Pool(machine_cores)

# numbers of generated items in each loop
amounts = int(outsize/machine_cores) if auto_batch else amounts_cpu
number_of_loops = int(outsize/amounts)
residue = outsize - amounts * number_of_loops

# first generating residue
pool.apply_async(generate_orders_items, args=(residue, 1), callback=collect_orders_items)

# generating orders in  parallel 
for i in range(number_of_loops):
    pool.apply_async(generate_orders_items, args=(amounts, (i * amounts) + residue + 1), callback=collect_orders_items)

# closing pool
pool.close()
pool.join()

# creating a data frame with the final results
df_orders_items = pd.DataFrame(orders_items)
df_clickstream_succeed = pd.DataFrame(clickstream_succeed)

# columns names aka header
columns_orders_items = ["order_id","product_id","quantity"]
columns_clickstream = ["customer_id","order_date","campaign_id","media_source","product_id"]

print("Saving files...")

# writing file
df_orders_items.to_csv(out_path + outfile, header=columns_orders_items,sep=",",index=False)
df_clickstream_succeed.to_csv(out_path + clickstream_outfile,header=columns_clickstream,sep=",",index=False)

print("File 1 was saved at path {}".format(out_path + outfile))
print("File 2 was saved at path {}".format(out_path + clickstream_outfile))