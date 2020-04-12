# Generates orders
import json
import common_functions
import random
import numpy as np
import multiprocessing as mp
import pandas as pd
from datetime import datetime, timedelta

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

# orders config
outfile = config["orders"]["outfile"]
outsize = config["orders"]["total"]
order_start_date = config["orders"]["order_start_date"]
order_end_date = config["orders"]["order_end_date"]
percentage_no_campaign = config["orders"]["percentage_no_campaign"]

# customer config
n_customers = config["customers"]["total"]
customer_file = out_path + config["customers"]["outfile"]
customers = pd.read_csv(customer_file)

# campaign config
campaigns = config[language]["campaigns"]
n_campaigns = len(campaigns) 
campaigns_id_range = list(range(n_campaigns + 1)) # Campaings include Id = 0 --> No Campaign

# media config
media_sources = config["media"]["sources"]
media_prob = config["media"]["percentages"]

# payments config
payments = config[language]["payments"]
n_payments = len(payments)
payments_id_range = list(range(1,n_campaigns + 1))

# calculating random probabilities
payments_prob = common_functions.random_probabilities(1,n_payments)
campaigns_prob = common_functions.random_prob_sum(0, n_campaigns, [0], percentage_no_campaign) # probabilities of campaigns
delivery_days_prob = common_functions.random_probabilities(1,5) # delivery days of a orders (between 1 and 5 days)

date_format = '%Y-%m-%d %H:%M:%S'

# Global orders array
orders = []

def generate_orders(amount, index_start):
    # generate orders

    # saves intermediary results
    results = set()
    index = index_start

    while len(results) < amount:
        customer_id = random.randint(1, n_customers)
        order_date = common_functions.random_date(order_start_date, order_end_date, random.random())
        customer_register_date = customers['register_date'][customer_id - 1]
        if order_date >= customer_register_date: # ensuring order occurs after user register_date
            delta_delivery = int(np.random.choice([1,2,3,4,5], p=delivery_days_prob)) 
            delivery_date = datetime.strptime(order_date, date_format) + timedelta(days=delta_delivery)
            campaign_id = int(np.random.choice(campaigns_id_range, p=campaigns_prob))
            media_source = np.random.choice(media_sources, p=media_prob) if campaign_id != 0 else 'None'
            payment_method = np.random.choice(payments_id_range, p=payments_prob)
            results.add((index,customer_id,order_date,delivery_date.strftime(date_format),campaign_id,media_source,payment_method))
            index = index + 1

    return results

def collect_orders(results):
    # collect orders info and add to the global orders array
    global orders

    orders = orders + list(results)

    # print the process
    print("{} processed".format(len(orders)))


# setting the number of cores used by the process, aka how many processes will run in parallel 
print("Initializing using {} cores".format(machine_cores))
pool = mp.Pool(machine_cores)

# numbers of generated items in each loop
amounts = int(outsize/machine_cores) if auto_batch else amounts_cpu
number_of_loops = int(outsize/amounts)
residue = outsize - amounts * number_of_loops

# first generating residue
pool.apply_async(generate_orders, args=(residue, 1), callback=collect_orders)

# generating orders in  parallel 
for i in range(number_of_loops):
    pool.apply_async(generate_orders, args=(amounts, (i * amounts) + residue + 1), callback=collect_orders)

# closing pool
pool.close()
pool.join()

# creating a data frame with the final results
df = pd.DataFrame(orders)

# columns names aka header
columns_names = ["order_id","customer_id","order_date","delivery_date","campaign_id","media_source","payment_method"]

print("Saving file...")

# writing file
df.to_csv(out_path + outfile,header=columns_names,sep=",",index=False)
print("File was saved at path {}".format(out_path + outfile))