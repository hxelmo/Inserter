import os
import sys
import pyodbc
import datetime

#データベース接続
driver='{SQL Server}'
server = 'ecologdb2020'
#database = 'ECOLOGDBver3'
database = 'ECOLOGDBver4'
uid = 'demo'
pwd = 'q1q1Q!Q!'
trusted_connection ='yes'

#GPS_RAWテーブル
GPS_RAW_TABLE = '[' + database + '].[dbo].[ANDROID_GPS_RAW]'
GPS_DOPPLER_RAW_TABLE = '[' + database + '].[dbo].[ANDROID_GPS_RAW_Doppler]'
ILLUMINANCE_TABLE = '[' + database + '].[dbo].[ILLUMINANCE_RAW_ver2]'

#ECOLOGテーブル
ECOLOG_TABLE = '[' + database + '].[dbo].[ECOLOG_LINKS_LOOKUP2]'
ECOLOG_DOPPLER_TABLE = '[' + database + '].[dbo].[ECOLOG_Doppler_NotMM]'
ECOLOG_DOPPLER_MM_TABLE = '[' + database + '].[dbo].[ECOLOG_Doppler]'
ECOLOG_MM_TABLE = '[' + database + '].[dbo].[ECOLOG_MM_LINKS_LOOKUP2]'

#寄り道テーブル
ECOLOG_DOPPLER_YORIMICHI_TABLE = '[' + database + '].[dbo].[ECOLOG_Doppler_NotMM_Yorimichi]'
'''
#TRIPS_RAWテーブル
TRIPS_RAW_TABLE = '[' + database + '].[dbo].[TRIPS_RAW_LINKS_LOOKUP2]'
TRIPS_RAW_DOPPLER_TABLE = '[' + database + '].[dbo].[TRIPS_RAW_Doppler_NotMM]'
TRIPS_RAW_DOPPLER_MM_TABLE = '[' + database + '].[dbo].[TRIPS_RAW_Doppler]'
TRIPS_RAW_MM_TABLE = '[' + database + '].[dbo].[TRIPS_MM_LINKS_RAW_LOOKUP2]'
'''
#TRIPSテーブル
TRIPS_TABLE = '[' + database + '].[dbo].[TRIPS_LINKS_LOOKUP2]'
TRIPS_DOPPLER_TABLE = '[' + database + '].[dbo].[TRIPS_Doppler_NotMM]'
TRIPS_DOPPLER_MM_TABLE = '[' + database + '].[dbo].[TRIPS_Doppler]'
TRIPS_MM_TABLE = '[' + database + '].[dbo].[TRIPS_MM_LINKS_LOOKUP2]'

#インスタンステーブル
DRIVER_TABLE = '[' + database + '].[dbo].[DRIVERS]'
CAR_TABLE = '[' + database + '].[dbo].[CARS]'
SENSOR_TABLE = '[' + database + '].[dbo].[SENSORS]'
ELECTRIC_VEHICLE_TABLE = '[' + database + '].[dbo].[ELECTRIC_VEHICLES]'
DRIVER1_VEHICLE_IDENTIFICATION_NUMBER = 'ZE1-096500'
PLACE_TABLE = '[' + database + '].[dbo].[PLACE]'

#LEAFSPYテーブル
LEAFSPY_ALL_TABLE = '[' + database + '].[dbo].[LEAFSPY_RAW_PROJECTED_ALL]'
LEAFSPY_TRIP_RECORD = '[' + database + '].[dbo].[LEAFSPY_TRIP_RECORD]'
#LEAFSPY_TRIP = '[' + database + '].[dbo].[LEAFSPY_TRIP]'   #TRIPSテーブルがあれば十分なので、使わない

#ファイルパス
ITSServerPath = '\\\\itsserver'
IlluminanceInsertedPath = '\\\\tommylab.ynu.ac.jp\\dfs\\work\\ECOLOG\\プログラム&ソースコード\\2022\\IlluminanceDataInserter_20220426\\inserted\\'
LEAFSPY_ToinsertPath = '\\\\itsserver\\ecolog_logdata_itsserver\\LEAF_ChargeLog\\GIDsLog'
LEAFSPY_InsertedPath = '\\\\tommylab.ynu.ac.jp\\dfs\\work\\ECOLOG\\LEAFSPY_inserted\\'
LOG_PATH = '\\\\tommylab.ynu.ac.jp\\dfs\\work\\ECOLOG\\プログラム&ソースコード\\2022\ECOLOGinserter_20221209\\Log\\' + str(datetime.datetime.now().date()) + '.md'
#ECOLOG_CSV_OUTPUT_PATH = '\\\\tommylab.ynu.ac.jp\\dfs\\home\\shichiry\\ECOLOG\\inserter\\ECOLOG_OUTPUT\\'
ECOLOG_CSV_OUTPUT_PATH = '\\\\tommylab.ynu.ac.jp\\dfs\\work\\ECOLOG\\プログラム&ソースコード\\2022\\ECOLOG_OUTPUT\\'
#リンクマッチで必要なテーブル
LINKS_TABLE = '[' + database + '].[dbo].[LINKS]'
LINKS_LOOKUP_TABLE = '[' + database + '].[dbo].[LINKS_LOOKUP]'
SEMANTIC_LINKS_TABLE = '[' + database + '].[dbo].[SEMANTIC_LINKS]'

#ECOLOG計算で必要なテーブル
ALTITUDE_TABLE = '[' + database + '].[dbo].[ALTITUDE_10M_MESH_REGISTERED_FIXED]' #通勤ルート用に補正してる？
ALTITUDE_ALL_TABLE = '[' + database + '].[dbo].[ALTITUDE_10M_MESH]'
EFFICIENCY_TABLE = '[' + database + '].[dbo].[EFFICIENCY_MAP]'
EFFICIENCY_MAX_TABLE = '[' + database + '].[dbo].[EFFICIENCY_MAP_MAX]'


minTorque = 76  #EfficiencyMaxTableNameテーブルのTorqueカラムの最小値
maxTorque = 280 #EfficiencyMaxTableNameテーブルのTorqueカラムの最大値
maxRev = 9990   #EfficiencyMaxTableNameテーブルのRevカラムの最大値

def select_execute(con, sql):
    cursor = con.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    columnNames = [column[0] for column in cursor.description]
    cursor.close()
    return rows, columnNames


if __name__ == "__main__":
    print('このプログラムの単体動作は想定してません')



