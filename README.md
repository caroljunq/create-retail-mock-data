In construction...
# create-retail-mock-data
Python scripts to generate mock csv retail data ()

# Describing Dataset
The dataset is generated with module mimesis for python. This retail dataset simulates an food ecommerce including products vegetable, drinks, dishes, spices, and fruits.


# config.json
In this file you can config the numbers of items to generate for each table (customers, orders, etc.)
Don't forget to describe each field and its possible values.

# Sequence to generate files
1- Customers,Suppliers,Categories, Payments or Campaigns
2- Products
3- Clickstream
4- Orders
5- orders_items
- Don't forget to describe the importance to use the same config.json for all generators (change in the paremeters)
- remember there are some files depending on the previous execution of other scripts/generated files

- joined date customers and orders

# Required Python Modules
- mimesis --> https://mimesis.readthedocs.io/
- numpy
- datetime
- time
- random
- json
- multiprocessing
- pandas

# Data Schema
- types and fields of each table


