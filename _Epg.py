import requests
import json
from urllib.parse import urlencode
from urllib.request import Request, urlopen
import ast
import pandas as pd
from dateutil.tz import tzlocal
# import datetime as dt
from datetime import timedelta
from datetime import datetime
import time

from sqlalchemy import create_engine
engine = create_engine('postgresql://postgres:nrz1371@localhost/samak')




url = 'https://epgservices.irib.ir:84/Service_EPG.svc/GetEpgNetwork'
epg1=pd.DataFrame(columns=['ID_Day_Item', 'Name_Item', 'Time_Play', 'EP', 'DTDay', 'Length',
       'Dec_Full', 'Dec_Summary', 'ID_Kind', 'channel'])

chan_table=pd.read_sql_query('SELECT * FROM public."Epg_Chann02"',con=engine.connect())
# print(len(chan_table))
# chan_table=pd.read_excel(r'E:\sourcecode\epg_nginx01\chan_table3.xlsx', index_col=False)
chan_table['code']=chan_table['code'].astype(str)

v_point = '12/18/2021'
v_point = datetime.strptime(v_point, "%m/%d/%Y")
print('-----Epg GET-----')

# time.sleep(12000)

for day in range(0,365):

    epg1=pd.DataFrame()
    from datetime import datetime

    t1 = time.perf_counter()
    v_start = v_point + timedelta(days=day, hours=0, minutes=0, seconds=0)
    fv_start=datetime.strftime(v_start,"%m/%d/%Y")
    v_lable=datetime.strftime(v_start,"%d_%m_%Y")

    print(v_start)

    az = ",\"DTStart\":\"{}\",\"DTEnd\":\"{}\"".format(fv_start,fv_start)




    for i in range(21, 230):
        ax = "\"SID_Network\":{}".format(i)
        # az = ",\"DTStart\":\"07/12/2021\",\"DTEnd\":\"07/12/2021\""
        ay2 = '{' + str(ax) + str(az) + '}'
        ay = "{\"SID_Network\":31}"
        myobj = {"JsonData": ay2, "Key": "EPG99f06e12YHNbgtrfvCDEolmnbvc"}
        print(ay2)
        # s = requests.session()
        headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}

        x = requests.post(url, json=myobj)
        a = x.content
        a = a.decode('utf-8')
        print(a)
        # ast.literal_eval(a)
        try:
            json_acceptable_string = a.replace("'", "\"")
            d = json.loads(json_acceptable_string)
            b = d.get('JsonData')
            x = ast.literal_eval(b)
            # print(type(x[1]))
            epg = pd.DataFrame(x)
            epg['channel2'] = i
            for w in range(0, len(chan_table)):
                epg_code = chan_table.loc[w, 'epg_code']
                code = chan_table.loc[w, 'code']
                if i == epg_code:
                    epg['channel'] = str(code)
                    print(code)
                    break
        except:
            pass

        epg1 = epg1.append(epg)
    # epg1.to_excel('E:\sourcecode\epg_nginx01\epg_table_n.xlsx', index=False)

    import datetime

    # epg= pd.read_excel(r'E:\sourcecode\epg_nginx01\epg_table.xlsx', index_col=False)
    #
    date2 = pd.to_datetime(epg1.Time_Play, errors='coerce')
    epg1 = epg1.assign(s_date=date2.dt.date, s_time=date2.dt.time)
    #
    date3 = pd.to_datetime(epg1.EP, errors='coerce')
    epg1 = epg1.assign(e_dete=date3.dt.date, e_time=date3.dt.time)
    #
    epg2 = epg1[(epg1['s_date'] == datetime.date(2021, 4, 27))]
    #
    # epg1.to_excel(r'E:\sourcecode\epg_nginx01\epg_table_t.xlsx', index=False)
    epg1 = epg1[["ID_Day_Item", "Name_Item", "Time_Play", "EP", "DTDay", "Length", "Dec_Full", "Dec_Summary", "ID_Kind",
                 "channel2", "channel", "ID_Program", "s_date", "s_time", "e_dete", "e_time"]]

    epg1.to_excel(r'F:\clean_epg\epg\epg_{}.xlsx'.format(v_lable), index=False)
    epg1.to_sql('epg_get', engine.connect(), if_exists='append',index=False)

    t2=time.perf_counter()
    dt21 = t2 - t1
    loop = 86400
    # res = 86400 - dt21
    from datetime import datetime
    from datetime import timedelta
    ct = v_start
    ct=ct+timedelta(days=2)
    tz = datetime.now()

    if tz>ct:
        res=100

    else:
        s_tz = tz.second + tz.minute * 60 + tz.hour * 3600
        res=87000-s_tz

    print(t2-t1)
    print(res)
    time.sleep(res)
    # time.sleep(86400)









