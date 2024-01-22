import sys
import csv
import pyodbc
import shutil
import re
import pandas as pd
import numpy as np
from pathlib import Path
from os import path
from datetime import datetime

from .DatabaseAccess_config import  driver, server, database, uid, pwd, trusted_connection, ECOLOG_TABLE, TRIPS_TABLE

#現在インサートされているトリップの中で、最大のトリップID
def get_max_tripID():

    connect= pyodbc.connect('DRIVER='+driver+';SERVER='+server+';DATABASE='+database+';PORT=1433;Trusted_Connection='+trusted_connection+';')
    cursor = connect.cursor()
    
    sql = """SELECT MAX(TRIP_ID) AS max_id FROM""" + ECOLOG_TABLE   #.format(str())

    cursor.execute(sql)
    rows = cursor.fetchall()
    #print(rows[0][0])
    
    cursor.close()
    connect.close()

    if rows[0][0] is None:
        max_tripid = 10001  #トリップIDの最小値
    else:
        max_tripid = rows[0][0] + 1
    
    return int(max_tripid)

def get_trip_id(gps_data):
    is_new_trip_id = False
    is_already_inserted = False
    trip_start_time = gps_data.loc[0]['JST']
    trip_end_time = gps_data.loc[len(gps_data)-1]['JST']
    trip_driver_id = gps_data.loc[0]['DRIVER_ID']
    trip_car_id = gps_data.loc[0]['CAR_ID']
    trip_sensor_id = gps_data.loc[0]['SENSOR_ID']

    connect= pyodbc.connect('DRIVER='+driver+';SERVER='+server+';DATABASE='+database+';PORT=1433;Trusted_Connection='+trusted_connection+';')
    cursor = connect.cursor()
    
    sql = """SELECT DISTINCT TRIP_ID \nFROM {} \nWHERE JST BETWEEN '{}' AND '{}' AND DRIVER_ID = {} AND CAR_ID = {}""".format(ECOLOG_TABLE,trip_start_time,trip_end_time,trip_driver_id,trip_car_id)
    #print(sql)

    cursor.execute(sql)
    rows = cursor.fetchall()

    #print('rows:')
    #print(rows)

    if len(rows) > 0:
        trip_id = round(float(str(rows[0]).replace(', )', '').replace('(', '')), 3)
        same_log_check_sql = """SELECT DISTINCT TRIP_ID \nFROM {} \nWHERE TRIP_ID = {} AND SENSOR_ID = {}""".format(ECOLOG_TABLE, trip_id, trip_sensor_id)
        cursor.execute(same_log_check_sql)
        rows2 = cursor.fetchall()
        if len(rows2) > 0:
            if int(str(rows2[0]).replace(', )', '').replace('(', '')) == trip_id:
                print('インサート済みGPSデータです')
                is_already_inserted = True
            else:
                print('このメッセージは絶対に出ません絶対に')
                quit()
        else:
            print('TRIP_ID割り当て済みのトリップの新規GPSデータです')
    elif len(rows) == 0:
        trip_id = get_max_tripID()
        is_new_trip_id = True
    else:
        print('トリップID割り当てで想定外の分岐')
        trip_id = -1

    cursor.close()
    connect.close() 

    return int(trip_id), is_new_trip_id, is_already_inserted


def all_func(gps_data):

    trip_id, is_new_trip_id, is_already_inserted = get_trip_id(gps_data)

    trip_record = gps_data
    trip_record['TRIP_ID'] = trip_id

    #print(trip_record)
    
    return trip_record, trip_id, is_new_trip_id, is_already_inserted


if __name__ == "__main__":
    print('このプログラムは単体での動作を想定してません')
    print('GPSデータをなんとかして入力してください')

    #all_func()