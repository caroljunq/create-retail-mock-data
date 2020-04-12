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

# config of media sources
media_sources = config["media"]["sources"]
media_prob = config["media"]["percentage"]

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



