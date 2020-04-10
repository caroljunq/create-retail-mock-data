import mimesis
import json
import common_functions
import random
import numpy as np

# reading config
with open('config.json') as data:
    config = json.load(data)

# setting up variables
out_path = config["output_path_files"]
outfile = config["customers"]["outfile"]
outsize = config["customers"]["total"]
max_age = config["customers"]["max_age"]
min_age = config["customers"]["min_age"]
joined_start_date = config["customers"]["joined_start_date"]
joined_end_date = config["customers"]["joined_end_date"]
language = config["language"]
genders = config[language]["genders"]


# loading mocking data type
business = mimesis.Business(language)
person = mimesis.Person(language)
address = mimesis.Address(language)
dates = mimesis.Datetime(language)

# 245 countries
country_prob = common_functions.random_probabilities(1,245)
ages_probab  = common_functions.random_probabilities(min_age, max_age)
gender_prob = common_functions.random_probabilities(1,len(genders))

# generating customers
with open(out_path + outfile, 'w') as csvfile:
    for i in range(outsize):
        id = i + 1
        print(id)
        customer_id = id
        customer_name = person.full_name().replace(',','')
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
        csvfile.write(f"{customer_id},{customer_name},{email},{age},{country_id},{phoneNumber},{gender},{birthdate},{date_joined},{city},{state},{postalcode}\n")





