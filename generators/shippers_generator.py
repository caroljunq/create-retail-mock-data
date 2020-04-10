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

# setting up variables
machine_cores = int(config["n_cores"])
header_in_csv = True if config["header_in_csv"] == "True" else False
out_path = config["output_path_files"]
index_shipper_start = config["shippers"]["index_start"]
outfile = config["shippers"]["outfile"]
outsize = config["shippers"]["total"]
language = config["language"]

# data processing config
amounts_cpu = config["data_processing"]["amount_in_cpu"]
auto_batch = True if config["data_processing"]["auto_batch_based_in_cpu"] == "True" else False
 
# loading mocking data type
business = mimesis.Business(language)
address = mimesis.Address(language)
dates = mimesis.Datetime(language)
person = mimesis.Person(language)

# 245 countries
country_prob = common_functions.random_probabilities(1,245)

shippers = []

def generate_shippers(amount,index_start):
    # generates shippers' info
    # amount: number of shippers to generate
    # index_start: from what index starts

    global shippers

    results = set()

    for i in range(amount):
        shipper_id = index_start + i 
        phone_number = person.telephone()
        responsible_name = person.full_name().replace(",","") # removing possible commas
        title = business.company().replace(",","") # removing possible commas
        email = person.email()
        postalcode = str(address.postal_code())
        country_id = np.random.choice(list(range(1,245 + 1)), p=country_prob)
        city = address.city().replace(',','') # removing possible commas
        state = address.state().replace(',','') # removing possible commas
        register_date = dates.formatted_datetime('%Y-%m-%d %H:%M:00')
        results.add((shipper_id,phone_number,responsible_name,title,email,country_id,city,state,register_date))

    return results

def collect_shippers(results):
    # collect shipper info and add to the array of shippers

    global shippers

    shippers = shippers + list(results)

    # print the process
    print("{} processed".format(len(shippers)))


# setting the number of cores used by the process, aka how many processes will run in parallel 
print("Initializing using {} cores".format(machine_cores))
pool = mp.Pool(machine_cores)

# numbers of generated items in each loop
amounts = int(outsize/machine_cores) if auto_batch else amounts_cpu
number_of_loops = int(outsize/amounts)
residue = outsize - amounts * number_of_loops

# first generating residue
pool.apply_async(generate_shippers, args=(residue, index_shipper_start), callback=collect_shippers)

# generating shippers in  parallel 
for i in range(number_of_loops):
    pool.apply_async(generate_shippers, args=(amounts, (i * amounts) + residue + index_shipper_start), callback=collect_shippers)

# closing pool
pool.close()
pool.join()

# creating a data frame with the final results
df = pd.DataFrame(shippers)

# columns names aka header
columns_names = ["shipper_id","phone_number","responsible_name","title","email","country_id","city","state","register_date"]

print("Saving file...")
# Defining if header will be included
header = False if header_in_csv == False else columns_names

# writing file
f = df.to_csv(out_path + outfile,header=header,sep=",",index=False)
print("File was saved at path {}".format(out_path + outfile))







