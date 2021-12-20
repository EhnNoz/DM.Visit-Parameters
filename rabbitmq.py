import pika
import json
import ast
import pandas as pd

credentials = pika.PlainCredentials(username='admin', password='admin')
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='192.168.143.17', port=5672, credentials=credentials))

channel = connection.channel()

channel.queue_declare(queue='uniqueuser', durable=True)

epg = pd.read_excel(r'F:\clean_epg\u_user\uu_user_18_12_2021.xlsx', index_col=False)


# epg = pd.read_excel(r'F:\clean_epg\epg_03_09_2021.xlsx', index_col=False)

epg=epg.astype(str)

epg["Time_Play_x"] = pd.to_datetime(epg["Time_Play_x"])
epg["time_stamp_y_new"] = pd.to_datetime(epg["time_stamp_y_new"])
epg['Time_Play_x'] = epg['Time_Play_x'].apply(lambda x: pd.Timestamp(x).strftime('%Y-%m-%dT%H:%M:%S'))
epg['time_stamp_y_new'] = epg['time_stamp_y_new'].apply(lambda x: pd.Timestamp(x).strftime('%Y-%m-%dT%H:%M:%S'))


# epg["time_stamp_x"] = pd.to_datetime(epg["time_stamp_x"])
# epg["time_stamp_y"] = pd.to_datetime(epg["time_stamp_y"])
# epg['time_stamp_x_new'] = epg['time_stamp_x'].apply(lambda x: pd.Timestamp(x).strftime('%Y-%m-%dT%H:%M:%S'))
# epg['time_stamp_y_new'] = epg['time_stamp_y'].apply(lambda x: pd.Timestamp(x).strftime('%Y-%m-%dT%H:%M:%S'))
# epg["Time_Play_x"]=epg["time_stamp_x_new"]
# epg["Name_Item"]=epg["content_name"]
# epg["channel"]=epg["channel_name"]
# del epg["time_stamp_x"]
# del epg["time_stamp_y"]
# del epg["content_name"]
# del epg["channel_name"]
# del epg["time_stamp_x_new"]

d_epg = epg.to_dict('records')

# *****************
# from kafka import KafkaProducer
#
# producer = KafkaProducer(bootstrap_servers="192.168.143.40:9092")
#
# for q in d_epg:
#     msg2 = q
#
#     producer.send('test-view', bytes(str(msg2), 'utf-8'))
#*****************


# ****************
for q in d_epg:
    msg2 = q
    print(msg2)
    channel.basic_publish(exchange='', routing_key='uniqueuser',
                          properties=pika.BasicProperties(content_type='application/json'),
                          body=json.dumps(msg2, ensure_ascii=False))

connection.close()
# *****************
# epg = pd.read_excel(r'F:\clean_epg\epg_01_08_2021.xlsx', index_col=False)
# epg.to_csv(r'F:\clean_epg\epg_31_08_2021.csv', index_label=False)
