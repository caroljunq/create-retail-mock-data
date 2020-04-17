import json

# reading config
with open('../config.json') as data:
    config = json.load(data)

# apache logs 
outfile = config["apache_logs"]["outfile"]
out_path = config["output_path_files"]

outfile_processed = config["apache_logs"]["outfile_processed"]

file_path = out_path + outfile

# reading lines from apache-logs-file
with open(file_path) as f:
    lines = f.readlines()

logs = ""

for line in lines:
    elems = line.split(' ')     
    order_date_date = elems[3][1:]
    order_date_hour = elems[4]
    params = elems[11].replace('Mozilla/5.0','').replace('"','').split('&')
    split_params = [elem.split('=') for elem in params]
    params_values = [elem[1] for elem in split_params]
    customer_id = params_values[0]
    campaign_id = params_values[1]
    media_source = params_values[2]
    prod_id = params_values[3]
    logs += f'{customer_id},{order_date_date} {order_date_hour},{campaign_id},{media_source},{prod_id}\n'
        
# saving file
print("Saving file...")
file_path_processed = out_path + outfile_processed
file_logs = open(file_path_processed,"w")
file_logs.write(logs) 
file_logs.close()

print("Finishing...")
