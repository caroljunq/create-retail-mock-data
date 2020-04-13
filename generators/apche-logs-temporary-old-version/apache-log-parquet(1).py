import json
import boto3


def lambda_handler(event, context):
    s3 = boto3.client('s3')
    logs = ''
    
    file_obj = event['Records'][0]
    filename = str(file_obj['s3']['object']['key'])
    filename_last = filename.split("/")[-1]
    fileObj = s3.get_object(Bucket='raw-data-octank876',Key=filename)
    filecontent = fileObj["Body"].read().decode("utf-8")
    for line in filecontent.splitlines():
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
        print(order_date_date)
        print(order_date_hour)
        print(customer_id)
        print(campaign_id)
        print(media_source)
        print(prod_id)
        logs += f'{order_date_date} {order_date_hour},{customer_id},{campaign_id},{media_source},{prod_id}\n'
        
    s3.put_object(Body=logs, Bucket='data-processed-octank876', Key='apache_logs/'+ filename_last)
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
