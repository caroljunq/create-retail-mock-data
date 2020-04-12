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

# products config
outfile = config["products"]["outfile"]
outsize = config["products"]["total"]

# config suppliers
n_suppliers = config["suppliers"]["total"]
supplier_id_range = list(range(1, n_suppliers + 1))

# data processing config
amounts_cpu = config["data_processing"]["amount_in_cpu"]
auto_batch = True if config["data_processing"]["auto_batch_based_in_cpu"] == "True" else False

# loading mocking data type
food = mimesis.Food(language)
person = mimesis.Person(language)
dates = mimesis.Datetime(language)
code = mimesis.Code(language)

# 5 categories
categories_prob = common_functions.random_probabilities(1,5)
suppliers_prob  = common_functions.random_probabilities(1, n_suppliers)

products = []

def generate_products(amount,index_start):
    # generates products' info
    # amount: number of products to generate
    # index_start: from what index starts

    results = set()

    for i in range(amount):
        product_id = index_start + i 
        category_id = np.random.choice([1,2,3,4,5], p=categories_prob)
        # dish category
        if category_id == 1:
            name = food.dish()
        # drink
        if category_id == 2:
            name = food.drink()
        # fruit
        if category_id == 3:
            name = food.fruit()
        # spices
        if category_id == 4:
            name = food.spices()
        # vegetable
        if category_id == 5:
            name = food.vegetable()
        name = name.replace(",","") # removing possible commas ,
        weight = random.random() * 1000
        price = random.random() * 10
        description = "food food food food food food food food food food food food food food food food food"
        supplier_id = 1
        barcode = code.ean()

        results.add((product_id,name,category_id,weight,price,description,supplier_id,barcode))

    return results

def collect_products(results):
    # collect products info and add to the array of products

    global products

    products = products + list(results)

    # print the process
    print("{} processed".format(len(products)))


# setting the number of cores used by the process, aka how many processes will run in parallel 
print("Initializing using {} cores".format(machine_cores))
pool = mp.Pool(machine_cores)

# numbers of generated items in each loop
amounts = int(outsize/machine_cores) if auto_batch else amounts_cpu
number_of_loops = int(outsize/amounts)
residue = outsize - amounts * number_of_loops

# first generating residue
pool.apply_async(generate_products, args=(residue, 1), callback=collect_products)

# generating products in  parallel 
for i in range(number_of_loops):
    pool.apply_async(generate_products, args=(amounts, (i * amounts) + residue + 1), callback=collect_products)

# closing pool
pool.close()
pool.join()

# creating a data frame with the final results
df = pd.DataFrame(products)

# columns names aka header
columns_names = ["product_id","name","category_id","weight","price","description","supplier_id","barcode"]

print("Saving file...")

# writing file
f = df.to_csv(out_path + outfile,header=columns_names,sep=",",index=False)
print("File was saved at path {}".format(out_path + outfile))


        

     