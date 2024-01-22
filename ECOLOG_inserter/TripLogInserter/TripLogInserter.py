# import module

from . import SetTripId as sti
from . import TripIdMatchedtoLeafSpy as tl_ls
from . import LeafspyRawInserter as lsi
from . import LeafSpyTripInserter as ls_ti
from . import DirectionDetermination as jtd
from . import MapMatching as mm
from . import LinkMatcher as lm
from . import ConsumedEnergyCalculator as cec
from . import JudgementYorimichi as jy
from . import IlluminanceDataInserter as idi

from .InsertDataFunction import insert_data, update_trip_data
#from .DatabaseAccess_config import driver, server, database, uid, pwd, trusted_connection
from .DatabaseAccess_config import LEAFSPY_ALL_TABLE, GPS_RAW_TABLE, GPS_DOPPLER_RAW_TABLE, TRIPS_TABLE, TRIPS_DOPPLER_TABLE, TRIPS_DOPPLER_MM_TABLE, TRIPS_MM_TABLE#, TRIPS_RAW_TABLE, TRIPS_RAW_DOPPLER_TABLE, TRIPS_RAW_DOPPLER_MM_TABLE, TRIPS_RAW_MM_TABLE
from .DatabaseAccess_config import ECOLOG_TABLE, ECOLOG_DOPPLER_TABLE, ECOLOG_DOPPLER_MM_TABLE, ECOLOG_MM_TABLE, ECOLOG_CSV_OUTPUT_PATH
from .get_ids import get_ids, update_trip_time
from .MathUtil import altDao
from Manager_config import EndDate_result, StartDate_result

import sys
import datetime
import os
import psutil
import time
import pandas as pd
import re


def table_name_ANDROID_GPS_RAW():
    return GPS_RAW_TABLE

def table_name_ANDROID_GPS_RAW_Doppler():
    return GPS_DOPPLER_RAW_TABLE 

def column_list_ANDROID_GPS_RAW():
    return "(DRIVER_ID,CAR_ID,SENSOR_ID,JST,LATITUDE,LONGITUDE,ALTITUDE,ANDROID_TIME)"

def column_list_ANDROID_GPS_RAW_Doppler():
    return "(DRIVER_ID,CAR_ID,SENSOR_ID,JST,LATITUDE,LONGITUDE,ALTITUDE,ACCURACY,SPEED,BEARING,ANDROID_TIME)"
'''
def table_name_TRIPS_RAW():
    return TRIPS_RAW_TABLE

def table_name_TRIPS_RAW_DOPPLER():
    return TRIPS_RAW_DOPPLER_TABLE

def table_name_TRIPS_RAW_MM():
    return TRIPS_RAW_MM_TABLE

def table_name_TRIPS_RAW_DOPPLER_MM():
    return TRIPS_RAW_DOPPLER_MM_TABLE

def column_list_TRIPS_RAW():
    return "(DRIVER_ID,CAR_ID,SENSOR_ID,START_TIME,END_TIME,START_LATITUDE,START_LONGITUDE,END_LATITUDE,END_LONGITUDE)"
'''

def table_name_TRIPS():
    return TRIPS_TABLE

def table_name_TRIPS_DOPPLER():
    return TRIPS_DOPPLER_TABLE

def table_name_TRIPS_MM():
    return TRIPS_MM_TABLE

def table_name_TRIPS_DOPPLER_MM():
    return TRIPS_DOPPLER_MM_TABLE

def column_list_TRIPS():
    return "(TRIP_ID,DRIVER_ID,CAR_ID,START_TIME,END_TIME,START_LATITUDE,START_LONGITUDE,END_LATITUDE,END_LONGITUDE,CONSUMED_ENERGY,TRIP_DIRECTION,VALIDATION)"

def table_name_ECOLOG():
    return ECOLOG_TABLE

def table_name_ECOLOG_DOPPLER():
    return ECOLOG_DOPPLER_TABLE

def table_name_ECOLOG_MM():
    return ECOLOG_MM_TABLE

def table_name_ECOLOG_DOPPLER_MM():
    return ECOLOG_DOPPLER_MM_TABLE

def column_list_ECOLOG():
    return "(TRIP_ID,DRIVER_ID,CAR_ID,SENSOR_ID,JST,LATITUDE,LONGITUDE,SPEED,HEADING,DISTANCE_DIFFERENCE,TERRAIN_ALTITUDE,TERRAIN_ALTITUDE_DIFFERENCE,ENERGY_BY_AIR_RESISTANCE,ENERGY_BY_ROLLING_RESISTANCE,ENERGY_BY_CLIMBING_RESISTANCE,ENERGY_BY_ACC_RESISTANCE,CONVERT_LOSS,REGENE_LOSS,REGENE_ENERGY,LOST_ENERGY,EFFICIENCY,CONSUMED_ELECTRIC_ENERGY,TRIP_DIRECTION,MESH_ID,LINK_ID,ROAD_THETA)"

def grouping_temp_trip_id(df_trip_list, GPS_File_Path):
    list_trip_list = []
    df_trip_list = df_trip_list.reset_index()

    #print(max_temp_trip_id)
    i = 0
    list_trip_index = 0
    is_exist_dopplerSpeed = -1
    is_real_log = -1

    gps_data0 = pd.read_csv(GPS_File_Path + '\\' + df_trip_list.iat[0, -1])
    columns_num = len(gps_data0.columns)
    if columns_num == 8: #ドップラー車速を取得
        is_exist_dopplerSpeed = 1
        is_real_log = 1
        column_name = ['JST','ANDROID_TIME','LATITUDE','LONGITUDE','ALTITUDE','ACCURACY','SPEED','BEARING']
    elif columns_num == 4:   #仮想走行ログ
        #現時点では、仮想走行ログに対応できるようになってません
        is_exist_dopplerSpeed = 0
        is_real_log = 0
        column_name = ['JST','ANDROID_TIME','LATITUDE','LONGITUDE']
    elif columns_num == 5:   #ドップラー車速未取得
        is_exist_dopplerSpeed = 0
        is_real_log = 1
        column_name = ['JST','ANDROID_TIME','LATITUDE','LONGITUDE','ALTITUDE']
    else:
        print('GPSログカラム数エラー')
        #column_name = ['JST','ANDROID_TIME','LATITUDE','LONGITUDE','ALTITUDE','ACCURACY','SPEED','BEARING']
    if list_trip_index >= max(df_trip_list['TEMP_TRIP_ID']):
        print('想定外の分岐（多分ないと思うけど）')

    while i < len(df_trip_list):
        #print('TEMP_TRIP_ID:' + str(df_trip_list.iloc[i, 1]))

        gps_data = pd.read_csv(GPS_File_Path + '\\' + df_trip_list.iat[i, -1],names=column_name)
        if len(gps_data.columns) == columns_num:
            k = 1

            if i < len(df_trip_list) - 1:
                #print(df_trip_list.iat[i, 1])
                #print(df_trip_list.iat[i + k, 1])
                while df_trip_list.iat[i, 1] == df_trip_list.iat[i + k, 1]:
                    #print('TEMP_TRIP_IDが前のファイルと一致')
                    if i + k >= len(df_trip_list):
                        #print('結合なし')
                        break
                    elif i + k == len(df_trip_list) - 1:
                        gps_data2 = pd.read_csv(GPS_File_Path + '\\' + df_trip_list.iat[i + k, -1],names=column_name)
                        gps_data = pd.concat([gps_data, gps_data2], axis=0)
                        #print('last ' + str(i))
                        k += 1
                        break
                    else:
                        gps_data2 = pd.read_csv(GPS_File_Path + '\\' + df_trip_list.iat[i + k, -1],names=['JST','ANDROID_TIME','LATITUDE','LONGITUDE','ALTITUDE','ACCURACY','SPEED','BEARING'] )
                        #print('結合')
                        gps_data = pd.concat([gps_data, gps_data2], axis=0)
                    k += 1
            else:
                print('last trip')
            gps_data.reset_index(inplace=True, drop=True)
            list_trip_list.append(gps_data)
            #print(list_trip_list[list_trip_index])
        else:
            print('GPSファイルのカラム数が途中から変わりました\nプログラムを終了します\n')
            sys.exit()

        i += k
        list_trip_index += 1
        
    return list_trip_list, is_exist_dopplerSpeed, is_real_log

# https://www.lifewithpython.com/2021/01/python-main-function.html
def main(df_trip_list, MYLOG_Path, GPS_File_Path):

    Driver_id, Car_id, Sensor_id = get_ids(GPS_File_Path)
    
    writeLog(MYLOG_Path, '\nEXECUTE: GENERATE list_trip_list')
    list_trip_list, is_exist_dopplerSpeed, is_real_log = grouping_temp_trip_id(df_trip_list, GPS_File_Path) #GPSファイルをTEMP_TRIP_IDで集約

    writeLog(MYLOG_Path, '\nDONE: GENERATE list_trip_list')

    for index, gps_data in enumerate(list_trip_list):
        writeLog(MYLOG_Path, "\nBegin Insert New DATA:{}/{}\n".format(index + 1, len(list_trip_list)))
        #print('gps_data:')
        #pd.set_option('display.max_rows', None)
        gps_data.insert(loc=0, column='DRIVER_ID', value=Driver_id)
        gps_data.insert(loc=1, column='CAR_ID', value=Car_id)
        gps_data.insert(loc=2, column='SENSOR_ID', value=Sensor_id)
        #print(gps_data)

        f = open(MYLOG_Path, mode='a')

        ## JudgeTripDirection
        writeLog(MYLOG_Path, "\nEXECUTE: DirectionDetermination:{}".format(index))
        try:
            direction = jtd.DirectionDetermination(gps_data['LATITUDE'][0],gps_data['LONGITUDE'][0],gps_data['LATITUDE'][len(gps_data)-1],gps_data['LONGITUDE'][len(gps_data)-1],Driver_id)
            print('trip_direction:{}'.format(direction))
            gps_data['TRIP_DIRECTION'] = direction
            writeLog(MYLOG_Path, "\nDONE: DirectionDetermination:{}\n".format(index))
        except Exception as e:
            writeLog(MYLOG_Path, "\nERROR:  TripLogInserter.py at call DirectionDetermination.py\n")
            writeLog(MYLOG_Path, "\n    TRACEBACK: {}".format(e))
            sys.exit()

        ## SetTripId
        writeLog(MYLOG_Path, "\nEXECUTE: SetTripId:{}".format(index))
        try:
            #setTripId実行処理
            trip_record, Trip_id, is_new_trip_id, is_already_inserted = sti.all_func(gps_data)
            if is_new_trip_id == False:
                trip_record['EPOCH_TIME'] = 0
                for i in trip_record.itertuples():
                    trip_record.at[i.Index, 'EPOCH_TIME'] = datetime.datetime.strptime(re.sub('\.[0-9]{3}', '', i.JST), '%Y-%m-%d %H:%M:%S').timestamp()
            if is_already_inserted:
                writeLog(MYLOG_Path, "\nThis TRIP:{} already inserted as TRIP:{}\n".format(index, Trip_id))
                continue
            
            trip_start_time = trip_record.loc[0]['JST']
            trip_end_time = trip_record.loc[len(trip_record)-1]['JST']

            writeLog(MYLOG_Path, "\nDONE: SetTripId:{} -> {}\n".format(index, Trip_id))
            writeLog(MYLOG_Path, "\nIs_New_Trip:{}\n".format(is_new_trip_id))
        except Exception as e:  
            writeLog(MYLOG_Path, "\nERROR: TripLogInserter.py at call SetTripId.py")
            writeLog(MYLOG_Path, "\n    TRACEBACK: {}".format(e))
            sys.exit()

        #pd.set_option('display.max_columns', 20)
        print(trip_record)


        ## INSERT ANDROID_GPS_RAW
        writeLog(MYLOG_Path, '\nEXECUTE: INSERT ANDROID_GPS_RAW: ' + str(index))
        gps_data_not_doppler = gps_data[['DRIVER_ID','CAR_ID','SENSOR_ID','JST','LATITUDE','LONGITUDE','ALTITUDE','ANDROID_TIME']]
        insert_data(table_name_ANDROID_GPS_RAW(),column_list_ANDROID_GPS_RAW(), gps_data_not_doppler)
        #print(gps_data_not_doppler)
        writeLog(MYLOG_Path, '\nDONE: INSERT ANDROID_GPS_RAW: ' + str(index) + '\n')

        if is_exist_dopplerSpeed == 1:  #Doppler車速を取得している時
            writeLog(MYLOG_Path, '\nEXECUTE: INSERT ANDROID_GPS_RAW_Doppler: ' + str(index))
            gps_data_doppler = gps_data[['DRIVER_ID','CAR_ID','SENSOR_ID','JST','LATITUDE','LONGITUDE','ALTITUDE','ACCURACY','SPEED','BEARING','ANDROID_TIME']]
            insert_data(table_name_ANDROID_GPS_RAW_Doppler(),column_list_ANDROID_GPS_RAW_Doppler(), gps_data_doppler)
            #print(gps_data_doppler)
            writeLog(MYLOG_Path, "\nDONE: INSERT ANDROID_GPS_RAW_Doppler: " + str(index) + '\n')

        
        ## MapMatching
        try:
            writeLog(MYLOG_Path, "\nEXECUTE: MapMatching:{}".format(Trip_id))
            linkIdList_MM, neighbor_pointList, distanceList = mm.all_func(trip_record.loc[:, ['LATITUDE', 'LONGITUDE']], Driver_id)
            #print(distanceList)
            trip_record_MM = trip_record.copy()
            df_neighbor_point = pd.DataFrame(neighbor_pointList, columns = ['NEIGHBOR_LATITUDE', 'NEIGHBOR_LONGITUDE'])
            trip_record_MM['LATITUDE'] = df_neighbor_point['NEIGHBOR_LATITUDE']
            trip_record_MM['LONGITUDE'] = df_neighbor_point['NEIGHBOR_LONGITUDE']
            writeLog(MYLOG_Path, "\nDONE: MapMatching:{}".format(Trip_id))
        except Exception as e:            
            writeLog(MYLOG_Path, "\nERROR:  TripLogInserter.py at call MapMatching.py\n")
            writeLog(MYLOG_Path, "\n    TRACEBACK: {}".format(e))
            sys.exit()
        MapMatchingJudge = True #MMデータをインサートするか
        if min(distanceList) < 0:
            MapMatchingJudge = False
            print('ドライバー対象SL外への逸脱があったため、マップマッチングは適用しません')
        writeLog(MYLOG_Path, "\nMapMatching:{}\n".format(MapMatchingJudge))
        
        ## GetAltitude
        writeLog(MYLOG_Path, "\nEXECUTE: GetAltitude:{}".format(Trip_id))
        try:
            altitudeList, meshIdList = altDao(trip_record.loc[:, ['LATITUDE', 'LONGITUDE']].values.tolist(), trip_record['LATITUDE'].min(), trip_record['LATITUDE'].max(), trip_record['LONGITUDE'].min(), trip_record['LONGITUDE'].max())
            trip_record['TERRAIN_ALTITUDE'] = altitudeList
            trip_record['MESH_ID'] = meshIdList
            if MapMatchingJudge:
                altitudeList, meshIdList = altDao(trip_record_MM.loc[:, ['LATITUDE', 'LONGITUDE']].values.tolist(), trip_record_MM['LATITUDE'].min(), trip_record_MM['LATITUDE'].max(), trip_record_MM['LONGITUDE'].min(), trip_record_MM['LONGITUDE'].max())
                trip_record_MM['TERRAIN_ALTITUDE'] = altitudeList
                trip_record_MM['MESH_ID'] = meshIdList
            writeLog(MYLOG_Path, "\nDONE: GetAltitude:{}\n".format(Trip_id))
        except Exception as e:            
            writeLog(MYLOG_Path, "\nERROR:  TripLogInserter.py at call GetAltitude\n")
            writeLog(MYLOG_Path, "\n    TRACEBACK: {}".format(e))
            sys.exit()

        ## ConsumedEnergyCalculator
        writeLog(MYLOG_Path, "\nEXECUTE: ConsumedEnergyCalculator:{}".format(Trip_id))
        try:
            energy_record, sum_consumed_energy = cec.all_func(trip_record, 0)
            #print(energy_record)
            print('総消費エネルギー(LINKS_LOOKUP2)：' + str(sum_consumed_energy) + '[kWh]')
            if is_exist_dopplerSpeed == 1:
                energy_record_doppler, sum_consumed_energy_doppler = cec.all_func(trip_record, is_exist_dopplerSpeed)
                #print(energy_record_doppler)
                print('総消費エネルギー(Doppler_not_MM)：' + str(sum_consumed_energy_doppler) + '[kWh]')
            if MapMatchingJudge:
                energy_record_MM, sum_consumed_energy_MM = cec.all_func(trip_record_MM, 0)
                #print(energy_record_MM)
                print('総消費エネルギー(MM)：' + str(sum_consumed_energy_MM) + '[kWh]')
                if is_exist_dopplerSpeed == 1:
                    energy_record_doppler_MM, sum_consumed_energy_doppler_MM = cec.all_func(trip_record_MM, is_exist_dopplerSpeed)
                    #print(energy_record_doppler_MM)
                    print('総消費エネルギー(Doppler_MM)：' + str(sum_consumed_energy_doppler_MM) + '[kWh]')
            writeLog(MYLOG_Path, "\nDONE: ConsumedEnergyCalculator:{}\n".format(Trip_id))
        except Exception as e:            
            writeLog(MYLOG_Path, "\nERROR:  TripLogInserter.py at call ConsumedEnergyCalculator.py\n")
            writeLog(MYLOG_Path, "\n    TRACEBACK: {}".format(e))
            sys.exit()


        ## LinkMatcher
        writeLog(MYLOG_Path, "\nEXECUTE: LinkMatcher:{}".format(Trip_id))
        try:
            linkIdList = lm.all_func(energy_record.loc[:, ['LATITUDE', 'LONGITUDE', 'HEADING']], Driver_id)
            energy_record['LINK_ID'] = linkIdList
            #print(energy_record)
 
            if MapMatchingJudge:
                energy_record_MM['LINK_ID'] = linkIdList_MM
            if is_exist_dopplerSpeed == 1:
                energy_record_doppler['LINK_ID'] = linkIdList
                if MapMatchingJudge:
                    energy_record_doppler_MM['LINK_ID'] = linkIdList_MM
            writeLog(MYLOG_Path, "\nDONE: LinkMatcher:{}\n".format(Trip_id))
        except Exception as e:            
            writeLog(MYLOG_Path, "\nERROR:  TripLogInserter.py at call LinkMatcher.py\n")
            writeLog(MYLOG_Path, "\n    TRACEBACK: {}".format(e))
            sys.exit()


        ## INSERT TRIPS
        if is_new_trip_id:
            writeLog(MYLOG_Path, "\nEXECUTE: INSERT TRIPS:{}".format(Trip_id))
            NULL = "NULL"
            try:
                cols = ['TRIP_ID','DRIVER_ID','CAR_ID','START_TIME','END_TIME','START_LATITUDE','START_LONGITUDE','END_LATITUDE','END_LONGITUDE','CONSUMED_ENERGY','TRIP_DIRECTION','VALIDATION']

                trip_start_latitue, trip_end_latitue, trip_start_longitude, trip_end_longitude = get_trip_table_instance(trip_record)
                trip_data = pd.DataFrame(index=[], columns=cols)
                trip_data.loc[index]=[Trip_id,Driver_id,Car_id,trip_start_time,trip_end_time,trip_start_latitue,trip_start_longitude,trip_end_latitue,trip_end_longitude,sum_consumed_energy,direction,NULL]
                print(trip_data)
                if is_new_trip_id:
                    insert_data(table_name_TRIPS(), column_list_TRIPS(), trip_data)
                else:
                    update_trip_data(table_name_TRIPS(), trip_start_latitue,trip_start_longitude,trip_end_latitue,trip_end_longitude,sum_consumed_energy,direction,Trip_id)
                
                if is_exist_dopplerSpeed == 1:
                    trip_data_doppler = pd.DataFrame(index=[], columns=cols)
                    trip_data_doppler.loc[index]=[Trip_id,Driver_id,Car_id,trip_start_time,trip_end_time,trip_start_latitue,trip_start_longitude,trip_end_latitue,trip_end_longitude,sum_consumed_energy_doppler,direction,NULL]
                    insert_data(table_name_TRIPS_DOPPLER(), column_list_TRIPS(), trip_data_doppler)
                
                if MapMatchingJudge:
                    trip_start_latitue, trip_end_latitue, trip_start_longitude, trip_end_longitude = get_trip_table_instance(trip_record_MM)
                    trip_data_MM = pd.DataFrame(index=[], columns=cols)
                    trip_data_MM.loc[index]=[Trip_id,Driver_id,Car_id,trip_start_time,trip_end_time,trip_start_latitue,trip_start_longitude,trip_end_latitue,trip_end_longitude,sum_consumed_energy_MM,direction,NULL]
                    insert_data(table_name_TRIPS_MM(), column_list_TRIPS(), trip_data_MM)

                    if is_exist_dopplerSpeed == 1:
                        trip_data_doppler_MM = pd.DataFrame(index=[], columns=cols)
                        trip_data_doppler_MM.loc[index]=[Trip_id,Driver_id,Car_id,trip_start_time,trip_end_time,trip_start_latitue,trip_start_longitude,trip_end_latitue,trip_end_longitude,sum_consumed_energy_doppler_MM,direction,NULL]
                        insert_data(table_name_TRIPS_DOPPLER_MM(), column_list_TRIPS(), trip_data_doppler_MM)
                
                writeLog(MYLOG_Path, "\nDONE: INSERT TRIPS:{}\n".format(Trip_id))
            except Exception as e:
                writeLog(MYLOG_Path, "\nERROR:  TripLogInserter.py at INSERT TRIPS\n")
                writeLog(MYLOG_Path, "\n    TRACEBACK: {}".format(e))
                sys.exit()
        else:
            writeLog(MYLOG_Path, "\nEXECUTE: UPDATE TRIPS START_TIME and END_TIME:{}\n".format(Trip_id))
            is_update_triptime = update_trip_time(Trip_id, trip_record.loc[0]['JST'], trip_record.loc[len(trip_record)-1]['JST'], trip_record.loc[0]['EPOCH_TIME'], trip_record.loc[len(trip_record)-1]['EPOCH_TIME'])
            if is_update_triptime:
                writeLog(MYLOG_Path, "\nUPDATE: TRIPS START_TIME or END_TIME:{}\n".format(Trip_id))
            else:
                writeLog(MYLOG_Path, "\nNOT UPDATE: TRIPS START_TIME and END_TIME:{}\n".format(Trip_id))

        
        ## INSERT ECOLOG
        writeLog(MYLOG_Path, "\nEXECUTE: INSERT ECOLOG:{}".format(Trip_id))
        try:
            ecolog_data = energy_record[['TRIP_ID','DRIVER_ID','CAR_ID','SENSOR_ID','JST','LATITUDE','LONGITUDE','SPEED(km/h)','HEADING','DISTANCE_DIFFERENCE','TERRAIN_ALTITUDE','TERRAIN_ALTITUDE_DIFFERENCE','ENERGY_BY_AIR_RESISTANCE','ENERGY_BY_ROLLING_RESISTANCE','ENERGY_BY_CLIMBING_RESISTANCE','ENERGY_BY_ACC_RESISTANCE','CONVERT_LOSS','REGENE_LOSS','REGENE_ENERGY','LOST_ENERGY','EFFICIENCY','CONSUMED_ELECTRIC_ENERGY','TRIP_DIRECTION','MESH_ID','LINK_ID','ROAD_THETA']]
            print(ecolog_data)
            insert_data(table_name_ECOLOG(), column_list_ECOLOG(), ecolog_data)
            
            if is_exist_dopplerSpeed == 1:
                ecolog_data_doppler = energy_record_doppler[['TRIP_ID','DRIVER_ID','CAR_ID','SENSOR_ID','JST','LATITUDE','LONGITUDE','SPEED(km/h)','HEADING','DISTANCE_DIFFERENCE','TERRAIN_ALTITUDE','TERRAIN_ALTITUDE_DIFFERENCE','ENERGY_BY_AIR_RESISTANCE','ENERGY_BY_ROLLING_RESISTANCE','ENERGY_BY_CLIMBING_RESISTANCE','ENERGY_BY_ACC_RESISTANCE','CONVERT_LOSS','REGENE_LOSS','REGENE_ENERGY','LOST_ENERGY','EFFICIENCY','CONSUMED_ELECTRIC_ENERGY','TRIP_DIRECTION','MESH_ID','LINK_ID','ROAD_THETA']]
                insert_data(table_name_ECOLOG_DOPPLER(), column_list_ECOLOG(), ecolog_data_doppler)
                ecolog_data_doppler.to_csv(ECOLOG_CSV_OUTPUT_PATH + 'EDOLOG_Doppler_NotMM_' + str(Trip_id) + '.csv')
            
            if MapMatchingJudge:
                ecolog_data_MM = energy_record_MM[['TRIP_ID','DRIVER_ID','CAR_ID','SENSOR_ID','JST','LATITUDE','LONGITUDE','SPEED(km/h)','HEADING','DISTANCE_DIFFERENCE','TERRAIN_ALTITUDE','TERRAIN_ALTITUDE_DIFFERENCE','ENERGY_BY_AIR_RESISTANCE','ENERGY_BY_ROLLING_RESISTANCE','ENERGY_BY_CLIMBING_RESISTANCE','ENERGY_BY_ACC_RESISTANCE','CONVERT_LOSS','REGENE_LOSS','REGENE_ENERGY','LOST_ENERGY','EFFICIENCY','CONSUMED_ELECTRIC_ENERGY','TRIP_DIRECTION','MESH_ID','LINK_ID','ROAD_THETA']]
                insert_data(table_name_ECOLOG_MM(), column_list_ECOLOG(), ecolog_data_MM)

                if is_exist_dopplerSpeed == 1:
                    ecolog_data_doppler_MM = energy_record_doppler_MM[['TRIP_ID','DRIVER_ID','CAR_ID','SENSOR_ID','JST','LATITUDE','LONGITUDE','SPEED(km/h)','HEADING','DISTANCE_DIFFERENCE','TERRAIN_ALTITUDE','TERRAIN_ALTITUDE_DIFFERENCE','ENERGY_BY_AIR_RESISTANCE','ENERGY_BY_ROLLING_RESISTANCE','ENERGY_BY_CLIMBING_RESISTANCE','ENERGY_BY_ACC_RESISTANCE','CONVERT_LOSS','REGENE_LOSS','REGENE_ENERGY','LOST_ENERGY','EFFICIENCY','CONSUMED_ELECTRIC_ENERGY','TRIP_DIRECTION','MESH_ID','LINK_ID','ROAD_THETA']]
                    insert_data(table_name_ECOLOG_DOPPLER_MM(), column_list_ECOLOG(), ecolog_data_doppler_MM)
            
            writeLog(MYLOG_Path, "\nDONE: INSERT ECOLOG:{}\n".format(Trip_id))
        
        except Exception as e:
            writeLog(MYLOG_Path, "\nERROR:  TripLogInserter.py at INSERT ECOLOG\n")
            writeLog(MYLOG_Path, "\n    TRACEBACK: {}".format(e))
            sys.exit()
        

        
        ## INSERT LEAFSPY_PROJECTED_ALL
        leafspy_EndDate_result = EndDate_result + 7

        writeLog(MYLOG_Path, "\nEXECUTE: LeafspyRawInserter.py {} - {}".format(StartDate_result, leafspy_EndDate_result))
        try:
            lsi.all_func(StartDate_result, leafspy_EndDate_result)
            writeLog(MYLOG_Path, "\nDONE: LeafspyRawInserter.py {} - {}\n".format(StartDate_result, leafspy_EndDate_result))
        except Exception as e:            
            writeLog(MYLOG_Path, "\nERROR: SensorLogInserterManager.py at call LeafspyRawInserter.py")
            writeLog(MYLOG_Path, "\n    TRACEBACK: {}".format(e))
            sys.exit()
        

        ## INSERT LEAFSPY_TRIP
        if is_new_trip_id:
            if Car_id == 22: #LEAF2020以外のEVのLEAFSPY_TRIP_RECORDテーブルインサートは後回し
                writeLog(MYLOG_Path, "\nEXECUTE: INSERT LEAFSPY_TRIP:{}".format(Trip_id))
                try:
                    ls_ti.all_func(ecolog_data)
                    writeLog(MYLOG_Path, "\nDONE: INSERT LEAFSPY_TRIP:{}\n".format(Trip_id))
                except Exception as e:
                    writeLog(MYLOG_Path, "\nERROR:  TripLogInserter.py at INSERT LeafSpyTripInserter.py\n")
                    writeLog(MYLOG_Path, "\n    TRACEBACK: {}".format(e))
                    sys.exit()
        
        ## JUDGEMENT YORIMICHI
        if is_new_trip_id:
            try:
                writeLog(MYLOG_Path, "\nEXECUTE: JUDGEMENT YORIMICHI:{}".format(Trip_id))
                writeLog(MYLOG_Path, '\nJUDGEMENT YORIMICHI OFF MODE')
                #jy.main(trip_start_time, trip_end_time, TRIPS_DOPPLER_TABLE, ECOLOG_DOPPLER_TABLE)
                writeLog(MYLOG_Path, "\nDONE: JUDGEMENT YORIMICHI:{}\n".format(Trip_id))
            except Exception as e:
                writeLog(MYLOG_Path, "\nERROR:  TripLogInserter.py at JudgementYorimichi.py\n")
                writeLog(MYLOG_Path, "\n    TRACEBACK: {}".format(e))
                sys.exit()
            
        '''
        ## IlluminanceDataInserter
        writeLog(MYLOG_Path, "\nEXECUTE: IlluminanceDataInserter:{}".format(Trip_id))
        try:
            idi.all_func(Driver_id, Car_id, Sensor_id, trip_start_time)
            writeLog(MYLOG_Path, "\nDONE: IlluminanceDataInserter:{}".format(Trip_id))
        except Exception as e:            
            writeLog(MYLOG_Path, "\nERROR: TripLogInserter.py at call IlluminanceDataInserter.py\n" + str(e))
            writeLog(MYLOG_Path, "\n    TRACEBACK: {}".format(e))
            sys.exit()
        '''

        writeLog(MYLOG_Path, "\nFinish Insert:{}  {}/{}\n----------------------------------------------------------------\n".format(Trip_id, index + 1, len(list_trip_list)))
        f.close()


def get_trip_table_instance(trip_record):
    trip_start_latitue = trip_record.loc[0]['LATITUDE']
    trip_end_latitue = trip_record.loc[len(trip_record)-1]['LATITUDE']
    trip_start_longitude = trip_record.loc[0]['LONGITUDE']
    trip_end_longitude = trip_record.loc[len(trip_record)-1]['LONGITUDE']
    return trip_start_latitue, trip_end_latitue, trip_start_longitude, trip_end_longitude
        

#コマンドライン表示とログファイルへの出力を同時に行う関数
def writeLog(MYLOG_Path, str_log):
    print(str_log)
    f = open(MYLOG_Path, mode='a')
    f.write(str_log)
    f.close()


def all_func(df_trip_list, MYLOG_Path, GPS_File_Path):
    main(df_trip_list, MYLOG_Path, GPS_File_Path)
            
if __name__ == "__main__":
    MYLOG_Path = str(os.getcwd())+"/log/"+str(datetime.datetime.now().date())+".md"
    #これ要る？必要な人は自分でやってくれ
    main(df_trip_list, MYLOG_Path, GPS_File_Path)