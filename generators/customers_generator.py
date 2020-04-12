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
# general config
machine_cores = int(config["n_cores"])
out_path = config["output_path_files"]
language = config["language"]

# customer config
outfile = config["customers"]["outfile"]
outsize = config["customers"]["total"]
max_age = config["customers"]["max_age"]
min_age = config["customers"]["min_age"]
joined_start_date = config["customers"]["joined_start_date"]
joined_end_date = config["customers"]["joined_end_date"]
genders = config[language]["genders"]

# data processing config
amounts_cpu = config["data_processing"]["amount_in_cpu"]
auto_batch = True if config["data_processing"]["auto_batch_based_in_cpu"] == "True" else False

# loading mocking data type
business = mimesis.Business(language)
person = mimesis.Person(language)
address = mimesis.Address(language)
dates = mimesis.Datetime(language)

# Calculating random probabilities
# 245 countries
country_prob = common_functions.random_probabilities(1,245)
ages_probab  = common_functions.random_probabilities(min_age, max_age)
gender_prob = common_functions.random_probabilities(1,len(genders))

# Global customers array
customers = []

def generate_customers(amount,index_start):
    # generates customers' info
    # amount: number of customer to generate
    # index_start: from what index starts
    global customers

    results = set()
    for i in range(amount):
        customer_id = index_start + i 
        customer_name = person.full_name().replace(',','') # removing possible commas
        email = person.email()
        country_id = np.random.choice(list(range(1,245 + 1)), p=country_prob)
        phoneNumber = person.telephone()
        age = np.random.choice(list(range(min_age,max_age + 1)), p=ages_probab)
        gender = np.random.choice(genders, p=gender_prob)
        city = address.city().replace(',','')
        state = address.state().replace(',','')
        postalcode = str(address.postal_code())
        birthdate = dates.formatted_datetime('%Y-%m-%d %H:%M:00')
        date_joined = common_functions.random_date(joined_start_date, joined_end_date, random.random())
        results.add((customer_id,customer_name,email,age,country_id,phoneNumber,gender,birthdate,date_joined,city,state,postalcode))

    return results

def collect_customers(results):
    global customers
    # collect customer info and add to the array of customers
    customers = customers + list(results)

    # print the process
    print("{} processed".format(len(customers)))

# setting the number of cores used by the process, aka how many processes will run in parallel 
print("Initializing using {} cores".format(machine_cores))
pool = mp.Pool(machine_cores)

# numbers of generated items in each loop
amounts = int(outsize/machine_cores) if auto_batch else amounts_cpu
number_of_loops = int(outsize/amounts)
residue = outsize - amounts * number_of_loops

# first generating residue
pool.apply_async(generate_customers, args=(residue, 1), callback=collect_customers)

# generating customer in  parallel 
for i in range(number_of_loops):
    pool.apply_async(generate_customers, args=(amounts, (i * amounts) + residue + 1), callback=collect_customers)

# closing pool
pool.close()
pool.join()

# creating a data frame with the final results
df = pd.DataFrame(customers)

# columns names aka header
columns_names = ["customer_id","customer_name","email","age","country_id","phoneNumber","gender","birthdate","register_date","city","state","postalcode"]

print("Saving file...")

# writing file
f = df.to_csv(out_path + outfile,header=columns_names,sep=",",index=False)
print("File was saved at path {}".format(out_path + outfile))

