import mimesis
import json
import common_functions
import numpy as np

# reading config
with open('config.json') as data:
    config = json.load(data)

# setting up variables
out_path = config["output_path_files"]
outfile = config["suppliers"]["outfile"]
outsize = config["suppliers"]["total"]
language = config["language"]
 
# loading mocking data type
business = mimesis.Business(language)
address = mimesis.Address(language)
dates = mimesis.Datetime(language)
person = mimesis.Person(language)

# 245 countries
country_prob = common_functions.random_probabilities(1,245)

# generating suppliers
with open(out_path + outfile, 'w') as csvfile:
    for i in range(outsize):
        id = i + 1
        print(id)
        supplier_id = id
        phone_number = person.telephone()
        responsible_name = person.full_name().replace(",","")
        title = business.company().replace(",","")
        email = person.email()
        postalcode = str(address.postal_code())
        country_id = np.random.choice(list(range(1,245 + 1)), p=country_prob)
        city = address.city().replace(',','')
        state = address.state().replace(',','')
        register_date = dates.formatted_datetime('%Y-%m-%d %H:%M:00')
        csvfile.write(f"{supplier_id},{phone_number},{responsible_name},{title},{email},{country_id},{city},{state},{register_date}\n")
