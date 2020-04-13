import random
import pandas as pd
import numpy as np
import mimesis
import time
import boto3

internet = mimesis.Internet()
firehose = boto3.client('firehose',region_name='us-east-1')

def str_time_prop(start, end, format, prop):
    """Get a time at a proportion of a range of two formatted times.

    start and end should be strings specifying times formated in the
    given format (strftime-style), giving an interval [start, end].
    prop specifies how a proportion of the interval to be taken after
    start.  The returned time will be in the specified format.
    """

    stime = time.mktime(time.strptime(start, format))
    etime = time.mktime(time.strptime(end, format))

    ptime = stime + prop * (etime - stime)

    return time.strftime(format, time.localtime(ptime))


def random_date(start, end, prop):
    return str_time_prop(start, end, '%Y-%m-%d %H:%M:%S', prop)


# 203.93.245.97 - oracleuser [28/Sep/2000:23:59:07 -0700] "GET 
# /files/search/search.jsp?s=driver&a=10 HTTP/1.0" 200 2374 
# "http://datawarehouse.us.oracle.com/datamining/contents.htm" "Mozilla/4.7 [en] 
# (WinNT; I)"

log_add2 = [
'"Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:59.0) Gecko/20100101 Firefox/59.0"'
,'"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0"'
,'"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Safari/59.0"'
,'"Mozilla/5.0 (Windows; Intel x86) Gecko/20100101 Firefox/59.0"'
,'"Mozilla/5.0 (Windows; Intel x86) Gecko/20100101 Chrome/64.0"'
,'"Mozilla/5.0 (Windows; Intel x86) Gecko/20100101 IE/32.0"'
,'"Mozilla/5.0 (Linux; Intel x86) Gecko/20100101 Firefox/59.0"'
,'"Mozilla/5.0 (Linux; Intel x86) Gecko/20100101 Chrome/64.0"'
,'"Mozilla/5.0 (Apple; IOS 11.0.2; IPhone6) AppleWebKit/537.36 (KHTML, like Gecko) Safari/59.00"'
,'"Mozilla/5.0 (Apple; IOS 11.0.2; IPhone6s) AppleWebKit/537.36 (KHTML, like Gecko) Safari/59.00"'
,'"Mozilla/5.0 (Apple; IOS 11.0.2; IPhone7) AppleWebKit/537.36 (KHTML, like Gecko) Safari/59.00"'
,'"Mozilla/5.0 (Apple; IOS 11.0.2; IPhone8) AppleWebKit/537.36 (KHTML, like Gecko) Safari/59.00"'
,'"Mozilla/5.0 (Apple; IOS 11.0.2; IPhone8) AppleWebKit/537.36 (KHTML, like Gecko) Safari/59.00"'
,'"Mozilla/5.0 (Apple; IOS 11.0.2; IPhone8) AppleWebKit/537.36 (KHTML, like Gecko) Safari/59.00"'
,'"Mozilla/5.0 (Apple; IOS 11.0.2; IPhone8s) AppleWebKit/537.36 (KHTML, like Gecko) Safari/59.00"'
,'"Mozilla/5.0 (Apple; IOS 11.0.2; IPhoneX) AppleWebKit/537.36 (KHTML, like Gecko) Safari/59.00"'
,'"Mozilla/5.0 (Apple; IOS 11.0.2; IPhoneX) AppleWebKit/537.36 (KHTML, like Gecko) Safari/59.00"'
,'"Mozilla/5.0 (Apple; IOS 11.0.2; IPhoneX) AppleWebKit/537.36 (KHTML, like Gecko) Safari/59.00"'
,'"Mozilla/5.0 (Apple; IOS 11.0.2; IPhone6s) AppleWebKit/537.36 (KHTML, like Gecko) Safari/59.00"'
,'"Mozilla/5.0 (Apple; IOS 11.0.2; IPhone7) AppleWebKit/537.36 (KHTML, like Gecko) Safari/59.00"'
,'"Mozilla/5.0 (Apple; IOS 11.0.2; IPhone8) AppleWebKit/537.36 (KHTML, like Gecko) Safari/59.00"'
,'"Mozilla/5.0 (Apple; IOS 11.0.2; IPhone8) AppleWebKit/537.36 (KHTML, like Gecko) Safari/59.00"'
,'"Mozilla/5.0 (Apple; IOS 11.0.2; IPhone8) AppleWebKit/537.36 (KHTML, like Gecko) Safari/59.00"'
,'"Mozilla/5.0 (Apple; IOS 11.0.2; IPhone8s) AppleWebKit/537.36 (KHTML, like Gecko) Safari/59.00"'
,'"Mozilla/5.0 (Apple; IOS 11.0.2; IPhoneX) AppleWebKit/537.36 (KHTML, like Gecko) Safari/59.00"'
,'"Mozilla/5.0 (Apple; IOS 11.0.2; IPhoneX) AppleWebKit/537.36 (KHTML, like Gecko) Safari/59.00"'
,'"Mozilla/5.0 (Apple; IOS 11.0.2; IPhoneX) AppleWebKit/537.36 (KHTML, like Gecko) Safari/59.00"'
,'"Mozilla/5.0 (Apple; IOS 11.0.2; IPhone6s) AppleWebKit/537.36 (KHTML, like Gecko) Safari/59.00"'
,'"Mozilla/5.0 (Apple; IOS 11.0.2; IPhone7) AppleWebKit/537.36 (KHTML, like Gecko) Safari/59.00"'
,'"Mozilla/5.0 (Apple; IOS 11.0.2; IPhone8) AppleWebKit/537.36 (KHTML, like Gecko) Safari/59.00"'
,'"Mozilla/5.0 (Apple; IOS 11.0.2; IPhone8) AppleWebKit/537.36 (KHTML, like Gecko) Safari/59.00"'
,'"Mozilla/5.0 (Apple; IOS 11.0.2; IPhone8) AppleWebKit/537.36 (KHTML, like Gecko) Safari/59.00"'
,'"Mozilla/5.0 (Apple; IOS 11.0.2; IPhone8s) AppleWebKit/537.36 (KHTML, like Gecko) Safari/59.00"'
,'"Mozilla/5.0 (Apple; IOS 11.0.2; IPhoneX) AppleWebKit/537.36 (KHTML, like Gecko) Safari/59.00"'
,'"Mozilla/5.0 (Apple; IOS 11.0.2; IPhoneX) AppleWebKit/537.36 (KHTML, like Gecko) Safari/59.00"'
,'"Mozilla/5.0 (Apple; IOS 11.0.2; IPhoneX) AppleWebKit/537.36 (KHTML, like Gecko) Safari/59.00"'
,'"Mozilla/5.0 (Linux; Android 8.1.0; ONEPLUS) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0"'
,'"Mozilla/5.0 (Linux; Android 7.1.0; MOTOROLA) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 8.1.0; MOTOROLA) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 6.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 7.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 8.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 6.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 7.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 8.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 6.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 7.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 8.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 6.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 7.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 8.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 6.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 7.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 8.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 6.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 7.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 8.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 6.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 7.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 8.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 6.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 7.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 8.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 6.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 7.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 8.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 6.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 7.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 8.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 6.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 7.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 8.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 6.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 7.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 8.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 6.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 7.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 8.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 7.1.0; MOTOROLA) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 8.1.0; MOTOROLA) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 7.1.0; MOTOROLA) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 8.1.0; MOTOROLA) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 7.1.0; MOTOROLA) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 8.1.0; MOTOROLA) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 7.1.0; MOTOROLA) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 8.1.0; MOTOROLA) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 7.1.0; MOTOROLA) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 8.1.0; MOTOROLA) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 7.1.0; MOTOROLA) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 8.1.0; MOTOROLA) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 7.1.0; MOTOROLA) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 8.1.0; MOTOROLA) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 7.1.0; MOTOROLA) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 8.1.0; MOTOROLA) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 7.1.0; MOTOROLA) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 8.1.0; MOTOROLA) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 7.1.0; MOTOROLA) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 8.1.0; MOTOROLA) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 6.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0"'
,'"Mozilla/5.0 (Linux; Android 7.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0"'
,'"Mozilla/5.0 (Linux; Android 8.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0"'
,'"Mozilla/5.0 (Linux; Android 8.1.0; LG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 7.1.0; LG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 6.1.0; LG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 6.1.0; LG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0"'
,'"Mozilla/5.0 (Linux; Android 6.1.0; LG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0"'
,'"Mozilla/5.0 (Linux; Android 6.1.0; LG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0"'
]

"104.123.196.99 - - [2018-04-09 00:28:24 +0000] ""GET / HTTP/1.1"" 200 38 ""id_cliente=67027&id_campaign=8&source=Website-ads&prod_id=1411 ""Mozilla/5.0 (Linux; Android 6.1.0; LG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0"""

log_size2 = len(log_add2)
logs = []


num_logs = 50
logs = []
for i in range(num_logs):	
    print(i)
    customer_id = random.randint(1,200000) # 2000000 custumers
    order_date = random_date('2017-1-1 00:00:00','2018-12-31 23:59:59',random.random())
    campaign_id = random.randint(1,10)
    media_source = np.random.choice(['Twitter', 'E-mail', 'Facebook', 'Website-ads'], p=[0.15, 0.10, 0.35, 0.40])  
    product_id = random.randint(1,8000)
    ip = internet.ip_v4()
    seed2 = random.randint(0,log_size2-1)
    log = str(ip)+" - - ["+order_date+" +0000] \"GET / HTTP/1.1\" 200 38 \"id_cliente="+str(customer_id)+"&id_campaign="+str(campaign_id)+"&source="+str(media_source)+"&prod_id="+str(product_id)+"\""+log_add2[seed2] + "\n"
    logs.append(log)
    response = firehose.put_record(
        DeliveryStreamName='clickstream',
        Record={
            'Data': log
         }
    )

print("Acabei de gerar.")

# df = pd.DataFrame(logs)
# print("Salvando arquivo")

# df.to_csv('apache_logs.txt',header=False,sep=",",index=False)