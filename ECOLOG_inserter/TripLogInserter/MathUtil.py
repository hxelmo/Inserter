#import folium
import pandas as pd
import pprint
import pyodbc
import math
import sys
from decimal import Decimal, ROUND_HALF_UP, ROUND_HALF_EVEN
from .DatabaseAccess_config import driver,server,database,uid,pwd,trusted_connection,ALTITUDE_TABLE, ALTITUDE_ALL_TABLE, EFFICIENCY_TABLE, EFFICIENCY_MAX_TABLE
from .ConstantData import InverterEfficiency,MaxDrivingPower,minTorque,maxTorque,maxRev,get_max_driving_force

#座標の系列データを入力とし、高度の系列データを出力するプログラム
def altDao(gpsRows, low_Latitude, high_Latitude, low_Longitude, high_Longitude):
    connect= pyodbc.connect('DRIVER='+driver+';SERVER='+server+';UID='+uid+';PED='+pwd+';DATABASE='+database+';Trusted_Connection='+trusted_connection+';')
    altitudeList = []
    meshIdList = []

    sql = '''SELECT * from {}
             WHERE UPPER_LATITUDE >= {}
             AND LOWER_LATITUDE <= {}
             AND UPPER_LONGITUDE >= {}
            AND LOWER_LONGITUDE <= {}'''.format(ALTITUDE_TABLE, low_Latitude, high_Latitude, low_Longitude, high_Longitude)
    #print(sql)
    df_dbdata = pd.read_sql(sql, connect)

    #print(df_dbdata)

    for i in gpsRows:
        _lat = i[0]
        _long = i[1]
        #sql = 'SELECT MIN(ALTITUDE), MIN(MESH_ID) \nFROM ' + ALTITUDE_TABLE + ' \nWHERE LOWER_LATITUDE <= ' + _lat + ' AND UPPER_LATITUDE > ' + _lat + ' AND LOWER_LONGITUDE <= ' + _long + ' AND UPPER_LONGITUDE > ' + _long
        try:
            selected_df_dbdata = df_dbdata[(df_dbdata['LOWER_LATITUDE']<=_lat) & (df_dbdata['UPPER_LATITUDE']>_lat) \
                                & (df_dbdata['LOWER_LONGITUDE']<=_long) & (df_dbdata['UPPER_LONGITUDE']>_long)]
            altitude = selected_df_dbdata.ALTITUDE.iloc[0]
            mesh_id = selected_df_dbdata.MESH_ID.iloc[0]
            #print('ALTITUDE:' + str(altitude) + '  MESH_ID:' + str(mesh_id))
            '''rows = select_execute(connect, sql)
            alt_and_id = str(rows[0]).replace(')', '').replace('(', '').split(',')
            if len(rows) > 0:
                altitude = round(float(alt_and_id[0]), 3)
                mesh_id = int(alt_and_id[1])
            else:
                print('10MMesh標高データなし')
                altitude = -999
                #標高データがなかった場合は下のException処理に移るので、ここには来ない、来たら多分エラー出る'''
        except Exception as e:
            sql = 'SELECT ALTITUDE, LOWER_LATITUDE, LOWER_LONGITUDE, UPPER_LATITUDE, UPPER_LONGITUDE \nFROM ' + ALTITUDE_ALL_TABLE + ' \nWHERE LOWER_LATITUDE <= ' + str(_lat) + ' AND UPPER_LATITUDE > ' + str(_lat) + ' AND LOWER_LONGITUDE <= ' + str(_long) + ' AND UPPER_LONGITUDE > ' + str(_long)
            rows = select_execute(connect, sql)
            #print(rows)
            if len(rows) > 0:
                alt_table_values = str(rows[0]).replace(')', '').replace('(', '').split(',')
                altitude = Decimal(str(alt_table_values[0])).quantize(Decimal('0.001'), rounding=ROUND_HALF_UP)
                get_max_mesh_id_sql = 'SELECT MAX(MESH_ID) \nFROM ' + ALTITUDE_TABLE
                rows_max_mesh_id = select_execute(connect, get_max_mesh_id_sql)
                print('rows_max_mesh_id[0]:' + str(rows_max_mesh_id[0]))
                mesh_id = int(str(rows_max_mesh_id[0]).replace(')', '').replace('(', '').replace(',', '')) + 1
                
                #ALTITUDE_TABLEに新たに加える処理を入れたい
                insert_sql = 'INSERT INTO ' + ALTITUDE_TABLE + '([MESH_ID], [LOWER_LATITUDE], [LOWER_LONGITUDE], [UPPER_LATITUDE], [UPPER_LONGITUDE], [ALTITUDE]) VALUES ({}, {}, {}, {}, {}, {})'.format(mesh_id, float(alt_table_values[1]), float(alt_table_values[2]), float(alt_table_values[3]), float(alt_table_values[4]), altitude)
                print('insert_sql:' + insert_sql)
                cursor = connect.cursor()
                cursor.execute(insert_sql)
                connect.commit()

                cursor.close()
            else:
                print('(' + str(_lat) + ', ' + str(_long) + ') does not have 10MMesh Altitude Data')
                altitude = -999
                mesh_id = 'NULL'

        altitudeList.append(float(altitude))
        meshIdList.append(mesh_id)
    #print('--- altitude insert finished ---\n')
    connect.close()
    #print(altitudeList)
    return altitudeList, meshIdList

##まだ未完成
def ADASaltDao(gpsRows):
    connect= pyodbc.connect('DRIVER='+driver+';SERVER='+server+';UID='+uid+';PED='+pwd+';DATABASE='+database+';Trusted_Connection='+trusted_connection+';')
    altitude_ADAS_List = []
    is_ADAS_altitude = []
    target_ADAS_list = []
    for i in gpsRows:
        _lat = str(i[0])
        _long = str(i[1])
        sql = 'SELECT MIN(ALTITUDE), MIN(MESH_ID) \nFROM ' + ALTITUDE_TABLE + ' \nWHERE LOWER_LATITUDE <= ' + _lat + ' AND UPPER_LATITUDE > ' + _lat + ' AND LOWER_LONGITUDE <= ' + _long + ' AND UPPER_LONGITUDE > ' + _long
        rows = select_execute(connect, sql)
        alt_and_id = str(rows[0]).replace(')', '').replace('(', '').split(',')
        if len(rows) > 0:
            altitude = round(float(alt_and_id[0]), 3)
            mesh_id = int(alt_and_id[1])
        #print(str(i) + ' -> ' + str(altitude))
        altitude_ADAS_List.append(altitude)
        is_ADAS_altitude.append(mesh_id)
    #print('--- altitude insert finished ---\n')
    connect.close()
    #print(altitudeList)
    return altitude_ADAS_List, is_ADAS_altitude


def Calc_Heading(_lat1, _long1, _lat2, _long2):
    heading = 0
    lat1 = _lat1 * math.pi / 180
    long1 = _long1 * math.pi / 180
    lat2 = _lat2 * math.pi / 180
    long2 = _long2 * math.pi / 180

    y = math.cos(lat2) * math.sin(long2 - long1)
    x = math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(long2 - long1)
    heading = math.atan2(y, x) * 180 / math.pi
    if heading < 0:
        heading = heading + 360

    return heading

def select_execute(con, sql):
    cursor = con.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    cursor.close()
    return rows

def get_efficiency(speed, torque, dict_carModel):

    #print('---最大効率の処理を入れる必要あり---')
    efficiency = -1
    rpm = speed * 60 / (dict_carModel['TireRadius'] * 2 * math.pi) * dict_carModel['ReductionRatio']
    rev = math.floor(rpm / 10) * 10 #四捨五入ではなく切り捨て
    #print('rmp:', rpm, '  rev:', rev)
    connect= pyodbc.connect('DRIVER='+driver+';SERVER='+server+';UID='+uid+';PED='+pwd+';DATABASE='+database+';Trusted_Connection='+trusted_connection+';')
    
    if rpm <= maxRev and torque <= maxTorque and torque >= minTorque:   #C#のSensorLogInserterはここの条件が逆になってる？？
        sql = 'SELECT EFFICIENCY FROM ' + EFFICIENCY_MAX_TABLE + ' WHERE torque = ' + str(torque) + ' AND REV = ' + str(rev)
        rows = select_execute(connect, sql)
        if len(rows) > 0:
            efficiency = int(str(rows[0]).replace(', )', '').replace('(', ''))
        #print('MAX')
    if efficiency == -1:
        sql = 'SELECT EFFICIENCY FROM ' + EFFICIENCY_TABLE + ' WHERE torque = ' + str(torque) + ' AND REV = ' + str(rev)
        rows = select_execute(connect, sql)
        #print('not MAX')
        if len(rows) > 0:
            efficiency = int(str(rows[0]).replace(', )', '').replace('(', ''))
    if efficiency == -1:
        efficiency = 80
        print('変換効率取得で想定外の分岐が発生  ' + sql + '  ' + str(rows))
    #print(efficiency)
    connect.close()
    return efficiency

def Calc_RegeneEnergy(drivingPower, speed, efficiency, weight):
    regeneEnergy = 0
    if speed < 7 / 3.6:
        drivingForce = 0
    else:
        drivingForce = drivingPower * 1000 * 3600 / speed   #制動力[N]
    speedC = MaxDrivingPower * 1000 / get_max_driving_force(weight)   #限界回生力と限界回生エネルギーの時の回生力の低い方が変わるときの車速[m/s]
    if drivingPower < 0:
        if speed < 7 / 3.6:
            regeneEnergy = 0
        elif drivingPower * 3600 >= MaxDrivingPower and drivingForce >= get_max_driving_force(weight):
            regeneEnergy = drivingPower * efficiency
        elif speed <= speedC and drivingForce < get_max_driving_force(weight):
            regeneEnergy = get_max_driving_force(weight) * speed * efficiency / 3600 / 1000
        elif drivingPower * 3600 < MaxDrivingPower and speed > speedC:
            regeneEnergy = MaxDrivingPower / 3600 * efficiency
        else:
            regeneEnergy = 0
            print('回生制約で想定外の分岐が発生  speed:', str(speed), '  power:', str(drivingPower), '  efficiency:', str(efficiency))
        regeneEnergy = regeneEnergy / 100 * InverterEfficiency
    return regeneEnergy

#ヒュベニの公式
#https://butter-tiger.hatenablog.com/entry/2020/08/20/222650
POLE_RADIUS = 6356752    # 極半径(短半径)
EQUATOR_RADIUS = 6378137 # 赤道半径(長半径)
E = 0.081819191042815790 # 離心率
E2= 0.006694380022900788 # 離心率の２乗
def distance(_lat1, _long1, _lat2, _long2):
    lat1 = math.radians(_lat1)
    long1 = math.radians(_long1)
    lat2 = math.radians(_lat2)
    long2 = math.radians(_long2)
    m_lat = (lat1 + lat2) / 2 # 平均緯度
    d_lat = abs(lat1 - lat2) # 緯度差
    d_lon = abs(long1 - long2) # 経度差
    W = math.sqrt(1-E2*math.pow(math.sin(m_lat),2))
    M = EQUATOR_RADIUS*(1-E2) / math.pow(W, 3) # 子午線曲率半径
    N = EQUATOR_RADIUS / W # 卯酉線曲率半径
    # d = math.sqrt(math.pow(M*d_lat,2) + math.pow(N*d_lon*math.cos(m_lat),2) + math.pow(point_a.altitude-point_b.altitude,2))
    d = math.sqrt(math.pow(M*d_lat,2) + math.pow(N*d_lon*math.cos(m_lat),2))
    return d

gpsRows = [[35.47218581,139.586728],[35.47215704,139.5866673],[35.47213248,139.5866422]]
if __name__ == '__main__':altDao(gpsRows)
