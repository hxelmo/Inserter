'''
GPSファイルのパスを入力とし、各種IDをデータベースから取得するメソッド
'''

import sys
import pyodbc
import shutil
import re
import pandas as pd
import numpy as np
from pathlib import Path
from os import path
import datetime
import warnings
warnings.simplefilter('ignore')

from .DatabaseAccess_config import  driver, server, database, uid, pwd, trusted_connection, DRIVER_TABLE, CAR_TABLE, SENSOR_TABLE, TRIPS_TABLE


def get_ids(GPS_File_Path):
    print('GPS_File_Path:' + GPS_File_Path)

    array_GPS_File_Path = GPS_File_Path.split('\\')

    Driver_name = array_GPS_File_Path[-4]
    Car_name = array_GPS_File_Path[-3]
    Sensor_name = array_GPS_File_Path[-2]

    #print(Driver_name + ' ' + Car_name + ' ' + Sensor_name)
    
    connect= pyodbc.connect('DRIVER='+driver+';SERVER='+server+';DATABASE='+database+';Trusted_Connection='+trusted_connection+';')
    cursor = connect.cursor()

    if database == 'ECOLOGDBver3':
        Driver_id=1
        Sensor_id=34
        Car_id=22
    else:

        sql = """SELECT DRIVER_ID FROM {} WHERE PATH_NAME = '{}'""".format(DRIVER_TABLE, Driver_name)
        #print(sql)
        cursor.execute(sql)
        rows = cursor.fetchall()
        if len(rows) > 0:
            Driver_id = int(str(rows[0]).replace(', )', '').replace('(', '').replace('\'', ''))
            print('Driver_ID:', Driver_id)
        else:
            print('---ドライバーID取得エラー---')

        sql = """SELECT CAR_ID FROM {} WHERE PATH_NAME = '{}'""".format(CAR_TABLE, Car_name)
        #print(sql)
        cursor.execute(sql)
        rows = cursor.fetchall()
        if len(rows) > 0:
            Car_id = int(str(rows[0]).replace(', )', '').replace('(', '').replace('\'', ''))
            print('CAR_ID:', Car_id)
        else:
            print('---車ID取得エラー---')
        
        sql = """SELECT SENSOR_ID FROM {} WHERE PATH_NAME = '{}'""".format(SENSOR_TABLE, Sensor_name)
        #print(sql)
        cursor.execute(sql)
        rows = cursor.fetchall()
        if len(rows) > 0:
            Sensor_id = int(str(rows[0]).replace(', )', '').replace('(', '').replace('\'', ''))
            print('SENSOR_ID:', Sensor_id)
        else:
            print('---センサーID取得エラー---')

    cursor.close()
    connect.close()

    return Driver_id, Car_id, Sensor_id

def update_trip_time(Trip_id, trip_start_time, trip_end_time, trip_start_epochtime, trip_end_epochtime):
    is_update_triptime = False
    sql = """SELECT START_TIME, END_TIME FROM {} WHERE TRIP_ID = {}""".format(TRIPS_TABLE, Trip_id)
    connect= pyodbc.connect('DRIVER='+driver+';SERVER='+server+';DATABASE='+database+';Trusted_Connection='+trusted_connection+';')
    df_start_and_end = pd.read_sql(sql, connect)

    print(str(df_start_and_end.at[0, 'START_TIME']))
    print(re.sub('\.[0-9]+', '', str(df_start_and_end.at[0, 'START_TIME'])))
    print(datetime.datetime.strptime(re.sub('\.[0-9]+', '', str(df_start_and_end.at[0, 'START_TIME'])), '%Y-%m-%d %H:%M:%S'))
    print(datetime.datetime.strptime(re.sub('\.[0-9]+', '', str(df_start_and_end.at[0, 'START_TIME'])), '%Y-%m-%d %H:%M:%S').timestamp())

    if len(df_start_and_end) > 0:
        database_trip_start_time = datetime.datetime.strptime(re.sub('\.[0-9]+', '', str(df_start_and_end.at[0, 'START_TIME'])), '%Y-%m-%d %H:%M:%S').timestamp()
        database_trip_end_time = datetime.datetime.strptime(re.sub('\.[0-9]+', '', str(df_start_and_end.at[0, 'END_TIME'])), '%Y-%m-%d %H:%M:%S').timestamp()
    else:
        print('---START_TIME・END_TIME取得エラー---')
    
    if trip_start_epochtime < database_trip_start_time:
        update_starttime_sql = "UPDATE {} SET START_TIME = '{}' WHERE TRIP_ID = {}".format(TRIPS_TABLE, trip_start_time, Trip_id)
        print('update start_time  ' + str(database_trip_start_time) + ' -> ' + str(trip_start_epochtime) + '  ' + str(trip_start_time))
        print(update_starttime_sql)
        cursor = connect.cursor()
        cursor.execute(update_starttime_sql)
        connect.commit()
        cursor.close()
        is_update_triptime = True
    if database_trip_end_time < trip_end_epochtime:
        update_endtime_sql = "UPDATE {} SET END_TIME = '{}' WHERE TRIP_ID = {}".format(TRIPS_TABLE, trip_end_time, Trip_id)
        print('update end_time  ' + str(database_trip_end_time) + ' -> ' + str(trip_end_epochtime) + '  ' + str(trip_end_time))
        print(update_endtime_sql)
        cursor = connect.cursor()
        cursor.execute(update_endtime_sql)
        connect.commit()
        cursor.close()
        is_update_triptime = True

    connect.close()

    return is_update_triptime

if __name__ == "__main__":
    #GPS_File_Path = '\\\\itsserver\\ecolog_logdata_itsserver\\tommy\\LEAF2020\\pixel5\\DrivingLoggerUnsentLog'

    get_ids(GPS_File_Path)
