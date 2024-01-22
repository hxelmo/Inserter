'''
インサート対象ファイルを列挙し、トリップごとに仮IDを振るプログラム
トリップIDの割り振りは後で行う

'''

import sys
import csv
import pyodbc
import shutil
import re
import pandas as pd
import numpy as np
from pathlib import Path
from os import path
import os
import datetime
import warnings
warnings.simplefilter('ignore')

from Manager_config import  StartDate_result, EndDate_result, ITSSERVER_File_Path, judge_same_trip_seconds
from TripLogInserter.DatabaseAccess_config import  driver, server, database, trusted_connection, SENSOR_TABLE, CAR_TABLE, DRIVER_TABLE


def search_filelist(driver_id, car_id, sensor_id):

    GPS_File_Path = get_GPS_filepath(driver_id, car_id, sensor_id)
    print('GPS_File_Path:', GPS_File_Path)
    
    p = Path(path.join(path.dirname(__file__), GPS_File_Path))

    filenames = []

    for file in p.iterdir():
        if file.is_dir():
            continue

        if re.match('[0-9]{14}(UnsentGPS)(.csv)', file.name):
            file_date = int(file.name[0:8])
            
            if (StartDate_result <= file_date) & (EndDate_result > file_date):
                #filenames.append(GPS_File_Path+'\\{}'.format(file.name))
                if os.stat(GPS_File_Path + '\\' + file.name).st_size > 150:   #空or1レコードしかないファイルは読み飛ばす
                    filenames.append(file.name)
                    #print('対象ファイル： '+file.name+'  OK')
                else:
                    print('対象外ファイル： ' + file.name + '  filesize:' + str(os.stat(GPS_File_Path + '\\' + file.name).st_size))

    filenames.sort(reverse=False)

    return filenames, GPS_File_Path


def get_GPS_filepath(driver_id, car_id, sensor_id):

    connect= pyodbc.connect('DRIVER='+driver+';SERVER='+server+';DATABASE='+database+';Trusted_Connection='+trusted_connection+';')
    cursor = connect.cursor()
    #if database == 'ECOLOGDBver3':  #超応急処置（tommy,LEAF2020,pixel5）のみ
    #    sensor_name='pixel5'
    #    car_name='LEAF2020'
    #    driver_name='tommy'
    #else:
    sensor_sql = """SELECT PATH_NAME FROM {} WHERE SENSOR_ID = {}""".format(SENSOR_TABLE, str(sensor_id))
    #print("try connecting sensor")
    cursor.execute(sensor_sql)
    rows = cursor.fetchall()
    if len(rows) > 0:
        sensor_name = str(rows[0]).replace(', )', '').replace('(', '').replace('\'', '').replace(' ', '')
        print('SENSOR_PATH_NAME:', sensor_name)
    else:
        print('Sensorファイルディレクトリ参照エラー')

    
    
    car_sql = """SELECT PATH_NAME FROM {} WHERE CAR_ID = {}""".format(CAR_TABLE, str(car_id))
    cursor.execute(car_sql)
    rows = cursor.fetchall()
    if len(rows) > 0:
        car_name = str(rows[0]).replace(', )', '').replace('(', '').replace('\'', '').replace(' ', '')
        print('CAR_PATH_NAME:', car_name)
    else:
        print('Carファイルディレクトリ参照エラー')
    
    driver_sql = """SELECT PATH_NAME FROM {} WHERE DRIVER_ID = {}""".format(DRIVER_TABLE, str(driver_id))
    cursor.execute(driver_sql)
    rows = cursor.fetchall()
    if len(rows) > 0:
        driver_name = str(rows[0]).replace(', )', '').replace('(', '').replace('\'', '').replace(' ', '')
        print('DRIVER_PATH_NAME:', driver_name)
    else:
        print('Driverファイルディレクトリ参照エラー')

    cursor.close()
    connect.close()

    GPS_File_Path = ITSSERVER_File_Path + '\\' + driver_name + '\\' + car_name + '\\' + sensor_name + '\\DrivingLoggerUnsentLog'

    return GPS_File_Path


def judge_same_trip(filename, before_jst):

    file_Datetime = pd.to_datetime(filename[0:14])
    
    second_diff = file_Datetime - before_jst
    #print(second_diff.total_seconds())

    if second_diff.total_seconds() < judge_same_trip_seconds:
        #print('{}:前のファイルからの時間差が{}秒未満のため、同一トリップとして扱います'.format(filename, judge_same_trip_seconds))
        return 1
    else:
        #print('前のファイルからの時間差が{}秒以上のため、トリップを分割します'.format(judge_same_trip_seconds))
        return 0


def all_func(driver_id, car_id, sensor_id):

    filenames, GPS_File_Path = search_filelist(driver_id, car_id, sensor_id)
    #print(filenames)

    temp_tripId = 0
    before_jst = datetime.datetime(2000,1,1)

    df_result = pd.DataFrame({'TEMP_TRIP_ID':list(range(len(filenames))),
                              'SENSOR_ID':list(range(len(filenames))),
                              'FILE_NAME':filenames})

    for filename in filenames:
    #print('filename:', filename)
        
        df_gpsfile = pd.read_csv(GPS_File_Path + '\\' + filename)
        if judge_same_trip(filename, before_jst):
            df_result.TEMP_TRIP_ID[df_result.FILE_NAME.isin([filename])] = temp_tripId
        else:
            temp_tripId = temp_tripId + 1
            df_result.TEMP_TRIP_ID[df_result.FILE_NAME.isin([filename])] = temp_tripId
        df_result.SENSOR_ID[df_result.FILE_NAME.isin([filename])] = sensor_id
        
        print(str(filename) + ':' + str(temp_tripId))
        print('GPS_File_Path:', GPS_File_Path)
        before_jst = pd.to_datetime(df_gpsfile.iloc[-1,0])
        #print(before_jst)

    #print('finish: FileSearcher.py')
    #print(df_result)

    return df_result, GPS_File_Path


if __name__ == "__main__":

    all_func(Driver_id, Car_id, Sensor_id)
