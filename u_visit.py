import pandas as pd
from datetime import datetime
from datetime import timedelta
import time
import json
from elasticsearch import Elasticsearch

es = Elasticsearch(hosts="http://norozzadeh:Kibana@110$%^@192.168.143.34:9200")

#______Read event  & epg file_______

s_point='2021-12-18'
event_i=1
session_i=1

# time.sleep(59000)

for day in range(0,365):

    t1 = time.perf_counter()
    event_i = 1
    session_i = 1
    print(day)
    # print(event_i)
    # print(session_i)

    start = datetime.strptime(s_point, '%Y-%m-%d')
    start = start + timedelta(days=day)
    _start=start - timedelta(days=1)
    lable = datetime.strftime(start, '%d_%m_%Y')



    try:

        _end = datetime.strftime(start, '%Y-%m-%d')
        _start_ = datetime.strftime(_start, '%Y-%m-%d')

        body = {'query': {'bool': {'must': [{'match_all': {}},
                                            {'range': {'@timestamp': {'gte': '{}T20:35:00.000Z'.format(_start_),
                                                                      'lt': '{}T20:25:59.000Z'.format(_end)}}}]}}}




        def process_hits(hits):
            _df1 = pd.DataFrame()
            for item in hits:
                _res = json.dumps(item, indent=2)
                _df0 = pd.DataFrame(item['_source'], columns=item['_source'].keys(), index=[0])
                _df1 = _df1.append(_df0, ignore_index=True)
            return _df1


        data = es.search(
            index='live-action',
            scroll='2m',
            size=10000,
            body=body
        )

        sid = data['_scroll_id']
        scroll_size = len(data['hits']['hits'])
        sum = 0
        event = pd.DataFrame()
        while scroll_size > 0:

            _a = process_hits(data['hits']['hits'])
            event = event.append(_a, ignore_index=True)

            data = es.scroll(scroll_id=sid, scroll='2m')


            sid = data['_scroll_id']

            scroll_size = len(data['hits']['hits'])
            #
            # read_file = read_file[['time_stamp', '@version', 'sys_id', 'time_code', '@timestamp', 'service_id', 'session_id',
            #            'content_name', 'channel_name', 'content_id', 'content_type_id', 'action_id']]


            event = event.astype(str)

            sum = scroll_size + sum
            print(sum)

        event.to_csv(r'F:\clean_epg\epg\____epg_____{}.csv', index=False)

        exist=1

        # event = pd.read_csv(r'F:\clean_epg\epg\____epg_____{}.csv',index_col=False)
        print('a')
    except:
        event_i=0
    try:
        #
        _end = datetime.strftime(start, '%Y-%m-%d')
        _start_ = datetime.strftime(_start, '%Y-%m-%d')

        body = {'query': {'bool': {'must': [{'match_all': {}},
                                            {'range': {'@timestamp': {'gte': '{}T20:35:00.000Z'.format(_start_),
                                                                      'lt': '{}T20:25:59.000Z'.format(_end)}}}]}}}





        def process_hits(hits):
            _df1 = pd.DataFrame()
            for item in hits:
                _res = json.dumps(item, indent=2)
                _df0 = pd.DataFrame(item['_source'], columns=item['_source'].keys(), index=[0])
                _df1 = _df1.append(_df0, ignore_index=True)
            return _df1


        data = es.search(
            index='live-login',
            scroll='2m',
            size=10000,
            body=body
        )

        sid = data['_scroll_id']
        scroll_size = len(data['hits']['hits'])
        sum = 0
        session = pd.DataFrame()
        while scroll_size > 0:

            _a = process_hits(data['hits']['hits'])
            session = session.append(_a, ignore_index=True)

            data = es.scroll(scroll_id=sid, scroll='2m')


            sid = data['_scroll_id']

            scroll_size = len(data['hits']['hits'])



            session = session.astype(str)

            sum = scroll_size + sum
            print(sum)

        session.to_csv(r'F:\clean_epg\epg\____sepg____{}.csv', index=False)


        # session = pd.read_csv(r'F:\clean_epg\epg\____sepg____{}.csv',index_col=False)
        print('b')

    except:
        session_i=0

    if event_i!=0 and session_i!=0:

        try:
            print('c')

            event_s = pd.merge(event,session,on='session_id')
            event_s=event_s.drop_duplicates(['user_id','content_name','channel_name'])
            event_s['date'] = str(start)

            event_s = event_s[['content_name', 'channel_name',
                               'user_id', 'time_stamp_x', 'sys_id_y',
                               'user_agent', 'referer', 'xReferer', 'date']]

            event_s.to_csv(r'F:\clean_epg\epg\sepg____{}.csv', index=False)


            # event_s["time_stamp_x"] = pd.to_datetime(event_s["time_stamp_x"])
            # event_s['time_stamp_x'] = event_s['time_stamp_x'] - pd.Timedelta(hours=4, minutes=50)
            # event_s['time_stamp_x'] = event_s['time_stamp_x'].apply(lambda x: pd.Timestamp(x).strftime('%Y-%m-%dT%H:%M:%S'))
            #__________Epg Kohzadi____________________
            from sqlalchemy import create_engine
            engine = create_engine('postgresql://postgres:nrz1371@localhost/samak')

            v_q = _end
            my_str = "\'" + v_q + "\'"
            epg = pd.read_sql_query('SELECT * FROM epg_get where "e_dete" = date {}'.format(my_str),con=engine.connect())
            # print(epg['s_time'])
            epg["s_time"]=epg["s_time"].astype(str)
            epg["e_time"] = epg["e_time"].astype(str)

            epg["Time_Play_x"] = pd.to_datetime(epg["Time_Play"])
            epg['Time_Play_x'] = epg['Time_Play_x'] - pd.Timedelta(hours=3, minutes=30)
            epg['Time_Play_x'] = epg['Time_Play_x'].apply(lambda x: pd.Timestamp(x).strftime('%Y-%m-%dT%H:%M:%S'))

            epg["EP_x"] = pd.to_datetime(epg["EP"])
            epg['EP_x'] = epg['EP_x'] - pd.Timedelta(hours=3, minutes=30)
            epg['EP_x'] = epg['EP_x'].apply(lambda x: pd.Timestamp(x).strftime('%Y-%m-%dT%H:%M:%S'))
            epg.rename(columns={'channel': 'channel_name'}, inplace=True)

            epg["Time_Play_x"] = pd.to_datetime(epg["Time_Play_x"])
            epg["EP_x"] = pd.to_datetime(epg["EP_x"])
            epg.to_csv(r'F:\clean_epg\epg\sep_g____{}.csv', index=False)
            event_s["time_stamp_x"] = pd.to_datetime(event_s["time_stamp_x"])
            event_s['time_stamp_x'] = event_s['time_stamp_x'].apply(lambda x: pd.Timestamp(x).strftime('%Y-%m-%dT%H:%M:%S'))
            event_s['time_stamp_x'] = pd.to_datetime(event_s["time_stamp_x"])
            event_s = (event_s.merge(epg, on=['channel_name'], how='left').query('Time_Play_x <= time_stamp_x <= EP_x'))

            event_s['content_name']=event_s['Name_Item']
            # event_s['content_name2'] = epg['Name_Item'].query('Time_Play_x <= time_stamp_x <= EP_x')
            event_s['time_stamp_y']=event_s['Time_Play_x']
            event_s['Time_Play_x'] = event_s['time_stamp_x']
            event_s['time_stamp_y_new'] = event_s['time_stamp_y']
            event_s["Name_Item"]=event_s["content_name"]
            event_s["channel"]=event_s["channel_name"]
            del event_s["time_stamp_x"]
            del event_s["time_stamp_y"]
            del event_s["content_name"]
            del event_s["channel_name"]
            # del event_s["time_stamp_x"]
            event_s = event_s[['Name_Item', 'channel',
                                      'user_id', 'Time_Play_x', 'sys_id_y',
                                      'user_agent', 'referer', 'xReferer','time_stamp_y_new']]

            event_s.to_excel('F:/clean_epg/u_user/uu_user_{}.xlsx'.format(lable),
                             columns=['Name_Item', 'channel',
                                      'user_id', 'Time_Play_x', 'sys_id_y',
                                      'user_agent', 'referer', 'xReferer','time_stamp_y_new'], index=False)
            #--------------------------
            # event_s.to_excel('F:/clean_epg/u_user/u_user_{}.xlsx'.format(lable),columns=['content_name','channel_name',
            #                                                   'user_id','time_stamp_x','sys_id_y',
            #                                                   'user_agent','referer','xReferer'], index=False)
            event_s=event_s.astype(str)
            #///////////////////////////////
            import pika
            import json
            import ast

            credentials = pika.PlainCredentials(username='admin', password='admin')
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host='192.168.143.17', port=5672, credentials=credentials))

            channel = connection.channel()

            channel.queue_declare(queue='uniqueuser', durable=True)

            d_event_s = event_s.to_dict('records')

            for q in d_event_s:
                msg2 = q
                # channel.basic_publish(exchange='', routing_key='uniqueuser', body=json.dumps(msg2))
                channel.basic_publish(exchange='', routing_key='uniqueuser',
                                      properties=pika.BasicProperties(content_type='application/json'),
                                      body=json.dumps(msg2, ensure_ascii=False))

            connection.close()
            #/////////////////////////////////////
            
            #*****************
            # from kafka import KafkaProducer
            # producer = KafkaProducer(bootstrap_servers='192.168.143.40:9092')
            # d_event_s = event_s.to_dict('records')
            #
            # for q in d_event_s:
            #     msg2 = q
            #     producer.send('uniqueuser', bytes(str(msg2), 'utf-8'))


            #****************


            print(len(event_s),len(event),len(session))

        except:
            pass

    t2 = time.perf_counter()
    dt=t2-t1
    loop = 86400
    # res = 86400 - dt

    ct = start
    ct=ct+timedelta(days=2)
    tz = datetime.now()
    if tz>ct:
        res=10
        print(res)
    else:
        s_tz = tz.second + tz.minute * 60 + tz.hour * 3600
        res=90000-s_tz
        print(res)


    print(t2 - t1)
    print(res)
    time.sleep(res)


