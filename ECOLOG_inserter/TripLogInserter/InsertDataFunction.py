import pyodbc
import numpy as np
import pandas as pd
import sys
from .DatabaseAccess_config import driver, server, database, uid, pwd, trusted_connection, LOG_PATH
from . import TripLogInserter as tli

#　注意事項！！！　この関数を使う前に、挿入先のテーブル名とカラム名のobjectを定義しないといけない。

#　その挿入先のテーブル名とカラム名のobjectは関数のようにする。その関数の形式は引数なしで、String型のテーブル名やカラム名を返すもの
'''
例としてはこれ：

def table_name_SEGMENTS_ZDC_50m():
    return "SEGMENTS_ZDC_50M"

def column_list_SEGMENTS_ZDC():
    return "(SEGMENT_ID,SEGMENT_LENGTH,START_LATITUDE,START_LONGITUDE,END_LATITUDE,END_LONGITUDE,START_ADAS_ELEVATION_MILLI_METER,END_ADAS_ELEVATION_MILLI_METER,SLOPE_ANGLE_THETA,COS_THETA,SIN_THETA,IS_ADAS_NULL,IS_GET_ZDC_DATA)"

'''

#　下のinsert_data関数を使いたいとき、上の例と加えると以下のように：
''' 
insert_data(table_name_SEGMENTS_ZDC_50m() , column_list_SEGMENTS_ZDC() , df_insert): 
'''

#　ここから実装
def insert_data(table_name_funtion,column_list_funtion,data):
    #print('デバッグのため、インサートはしない')
    
    connect= pyodbc.connect('DRIVER='+driver+';SERVER='+server+';DATABASE='+database+';PORT=1433;Trusted_Connection='+trusted_connection+';')
    cursor = connect.cursor()
    for i in range(len(data)):
        '''if 'TRIPS' in table_name_funtion:
            print(table_name_funtion)
            print(i)
            print(data)
            print(data.iloc[i])'''
        list1 = data.iloc[i].to_list()

        str_list1 = str(tuple(list1)).replace('\'NULL\'', 'NULL')
        stmt = """
        INSERT INTO {} {} 
        VALUES {}
        """.format(table_name_funtion, column_list_funtion ,str_list1)
        try:
            cursor.execute(stmt)
            cursor.commit()
        except pyodbc.IntegrityError as err:
            # 主キー違反の場合には読み飛ばす
            continue
        except Exception as e:
            print(e)
            print(stmt)
            sys.exit()
    cursor.close()
    connect.close()
    tli.writeLog(LOG_PATH, '\n  inserted to ' + table_name_funtion + '  ' + str(len(data)) + 'records')


def update_trip_data(table_name_funtion, trip_start_latitue,trip_start_longitude,trip_end_latitue,trip_end_longitude,sum_consumed_energy,direction,trip_id):
    query = '''\
UPDATE {} 
SET START_LATITUDE = {}
    ,START_LONGITUDE = {}
    ,END_LATITUDE = {}
    ,END_LONGITUDE = {}
    ,CONSUMED_ENERGY = {}
    ,TRIP_DIRECTION = {}
WHERE TRIP_ID = {}
    '''.format(table_name_funtion, trip_start_latitue,trip_start_longitude,trip_end_latitue,trip_end_longitude,sum_consumed_energy,direction, trip_id)
    connect= pyodbc.connect('DRIVER='+driver+';SERVER='+server+';DATABASE='+database+';PORT=1433;Trusted_Connection='+trusted_connection+';')
    cursor = connect.cursor()
    try:
        cursor.execute(query)
        cursor.commit()
    except Exception as e:
        print(e)
        print(query)
        sys.exit()
    cursor.close()
    connect.close()