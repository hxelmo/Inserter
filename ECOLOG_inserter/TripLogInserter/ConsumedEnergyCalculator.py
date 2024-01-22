
import folium
import pandas as pd
import pprint
import pyodbc
import os
import numpy as np
import math
import sys
import time
from datetime import datetime as dt
import shutil
from os import path
import re
from pathlib import Path
import webbrowser

#from config import car_id
from .MathUtil import distance,Calc_Heading,get_efficiency,Calc_RegeneEnergy
from .ConstantData import myu,rho,GravityResistanceCoefficient,InverterEfficiency
from . import ConstantData

'''
INPUT_FOLDER = rf'\input'
OUTPUT_FOLDER = rf'\output'
INSERTED_FOLDER = rf'\inserted'
FOLIUM_FOLDER = rf'\folium'
'''

def main(trip_record, is_exist_dopplerSpeed):

    car = ConstantData.CarModel(trip_record.loc[0]['CAR_ID'])
    start_time = trip_record.loc[0]['JST']
    dict_carModel = car.get_car_model()
    #print(dict_carModel['Weight'])

    #trip_record = trip_record.iloc[:400, :]#デバッグの簡略化のために一部の行に限定している

    row_data = trip_record.loc[:, ['TRIP_ID', 'DRIVER_ID', 'CAR_ID', 'SENSOR_ID', 'JST', 'LATITUDE', 'LONGITUDE', 'TERRAIN_ALTITUDE', 'MESH_ID', 'TRIP_DIRECTION']]

    #gpsRows = trip_record.loc[:,['LATITUDE', 'LONGITUDE']].values.tolist()
    #altitudeList, meshIdList = altDao(gpsRows)  #ここで高度取得
    #row_data['TERRAIN_ALTITUDE'] = altitudeList
    #row_data['MESH_ID'] = meshIdList

    row_data['SPEED'] = 0.0
    row_data['SPEED(km/h)'] = 0.0
    row_data['HEADING'] = 0.0
    row_data['DISTANCE_DIFFERENCE'] = 0.0
    row_data['TERRAIN_ALTITUDE_DIFFERENCE'] = 0.0
    row_data['ROAD_THETA'] = 0.0
    row_data['SECONDS_DIFFERENCE'] = 0
    
    if is_exist_dopplerSpeed == 1:
        row_data['SPEED'] = trip_record['SPEED']
        row_data['SPEED(km/h)'] = trip_record['SPEED'] * 3600 / 1000

    lastvar = 0
    secondsDiff = 0
    for i in row_data.itertuples():
        if '.' in start_time:   #JSTがマイクロ秒単位でデータを保持しているとき
            row_data.at[i.Index, 'JST'] = re.sub('\.[0-9]{3}', '', i.JST)
            row_data.at[i.Index, 'JST'] = row_data.at[i.Index, 'JST']
        if(hasattr(lastvar, 'JST')) and (i.Index + 1 < len(row_data)):
            altitudeDiff = round(i.TERRAIN_ALTITUDE - lastvar.TERRAIN_ALTITUDE, 3)
            secondsDiff = (dt.strptime(row_data.at[i.Index, 'JST'], '%Y-%m-%d %H:%M:%S') - dt.strptime(row_data.at[i.Index - 1, 'JST'], '%Y-%m-%d %H:%M:%S')).seconds
            dist = round(distance(i.LATITUDE, i.LONGITUDE, lastvar.LATITUDE, lastvar.LONGITUDE), 5)
            row_data.at[i.Index, 'SECONDS_DIFFERENCE'] = secondsDiff
            if secondsDiff == 1: #停車時など、ログが2秒以上途切れた時は初期値
                row_data.at[i.Index, 'TERRAIN_ALTITUDE_DIFFERENCE'] = altitudeDiff
                row_data.at[i.Index, 'DISTANCE_DIFFERENCE'] = dist
                if (is_exist_dopplerSpeed == 0 and (dt.strptime(re.sub('\.[0-9]{3}', '', row_data.at[i.Index + 1, 'JST']), '%Y-%m-%d %H:%M:%S') - dt.strptime(row_data.at[i.Index, 'JST'], '%Y-%m-%d %H:%M:%S')).seconds) == 1:
                    speed = (dist + round(distance(i.LATITUDE, i.LONGITUDE, row_data.at[i.Index + 1, 'LATITUDE'], row_data.at[i.Index + 1, 'LONGITUDE']), 5)) / 2
                    #print(speed)
                    row_data.at[i.Index, 'SPEED'] = speed
                    row_data.at[i.Index, 'SPEED(km/h)'] = speed * 3600 / 1000
                if dist > 0 and row_data.at[i.Index, 'SPEED'] > 1:  #速度が1km/h以上になったらHEADINGを更新する(停止時に1つ1つ計算するとHEADINDが暴れるため)
                    row_data.at[i.Index, 'ROAD_THETA'] = math.atan(altitudeDiff / dist)
                    #heading計算
                    row_data.at[i.Index, 'HEADING'] = round(Calc_Heading(i.LATITUDE, i.LONGITUDE, lastvar.LATITUDE, lastvar.LONGITUDE), 5)
                else:
                    #heading据え置き
                    row_data.at[i.Index, 'HEADING'] = lastvar.HEADING
            elif secondsDiff < 1 and i.Index > 2:
                print('secondsDiffが1未満のレコードが発生  secondsDiff:' + str(secondsDiff) + '  Index:' + str(i.Index) + '  ' + str(i.JST) + ', ' + str(lastvar.JST))
        lastvar = i

    #print(row_data)

    row_data['ENERGY_BY_AIR_RESISTANCE'] = 0.0
    row_data['ENERGY_BY_ROLLING_RESISTANCE'] = 0.0
    row_data['ENERGY_BY_CLIMBING_RESISTANCE'] = 0.0
    row_data['ENERGY_BY_ACC_RESISTANCE'] = 0.0
    row_data['DRIVING_RESISTANCE_POWER'] = 0.0
    row_data['CONVERT_LOSS'] = 0.0
    row_data['REGENE_LOSS'] = 0.0
    row_data['REGENE_ENERGY'] = 0.0
    row_data['LOST_ENERGY'] = 0.0
    row_data['EFFICIENCY'] = 80
    row_data['CONSUMED_ELECTRIC_ENERGY'] = 0.0
    row_data['TORQUE'] = 0.0
    for i in row_data.itertuples():
        if(hasattr(lastvar, 'JST')):
            airResistancePower = rho * dict_carModel['CdValue'] * dict_carModel['FrontalProjectedArea'] * i.SPEED * i.SPEED / 2 * i.SPEED / 3600 /1000
            row_data.at[i.Index, 'ENERGY_BY_AIR_RESISTANCE'] = airResistancePower
            rollingResistancePower = myu * dict_carModel['Weight'] * GravityResistanceCoefficient * math.cos(i.ROAD_THETA) * i.SPEED / 3600 /1000
            row_data.at[i.Index, 'ENERGY_BY_ROLLING_RESISTANCE'] = rollingResistancePower
            climbingResistancePower = dict_carModel['Weight'] * GravityResistanceCoefficient * math.sin(i.ROAD_THETA) * i.SPEED / 3600 /1000
            row_data.at[i.Index, 'ENERGY_BY_CLIMBING_RESISTANCE'] = climbingResistancePower
            accResistancePower = dict_carModel['Weight'] * (math.pow(i.SPEED, 2) - math.pow(lastvar.SPEED, 2)) / 2 / 3600 /1000
            row_data.at[i.Index, 'ENERGY_BY_ACC_RESISTANCE'] = accResistancePower
            drivingResistancePower = airResistancePower + rollingResistancePower + climbingResistancePower + accResistancePower
            row_data.at[i.Index, 'DRIVING_RESISTANCE_POWER'] = drivingResistancePower
            torque = 0
            if row_data.at[i.Index, 'SPEED'] > 0 and drivingResistancePower > 0:
                torque = int(drivingResistancePower * 1000 * 3600 / i.SPEED * dict_carModel['TireRadius'] / dict_carModel['ReductionRatio'])
                row_data.at[i.Index, 'TORQUE'] = torque
            efficiency = get_efficiency(i.SPEED, torque, dict_carModel)
            row_data.at[i.Index, 'EFFICIENCY'] = efficiency
            if drivingResistancePower >= 0:
                regeneEnergy = 0
                consumedEnergy = drivingResistancePower / efficiency * 100 / InverterEfficiency
            else:
                regeneEnergy = Calc_RegeneEnergy(drivingResistancePower, i.SPEED, efficiency, dict_carModel['Weight'])
                consumedEnergy = regeneEnergy
            row_data.at[i.Index, 'CONSUMED_ELECTRIC_ENERGY'] = consumedEnergy
            row_data.at[i.Index, 'REGENE_ENERGY'] = regeneEnergy
            if drivingResistancePower >= 0:
                convertLoss = consumedEnergy * (1 - (1 * efficiency / 100 * InverterEfficiency))
                regeneLoss = 0
            else:
                convertLoss = consumedEnergy * ((1 / efficiency * 100 / InverterEfficiency) - 1)
                regeneLoss = drivingResistancePower - regeneEnergy / efficiency * 100 / InverterEfficiency
            lostEnergy = abs(convertLoss) - regeneLoss + airResistancePower + rollingResistancePower
            row_data.at[i.Index, 'LOST_ENERGY'] = lostEnergy
            row_data.at[i.Index, 'CONVERT_LOSS'] = convertLoss
            row_data.at[i.Index, 'REGENE_LOSS'] = regeneLoss
            #row_data.at[i.Index, 'DRIVING_RESISTANCE_POWER'] = drivingResistancePower
        lastvar = i
    
    #print(row_data)
    output_data = row_data
    #output_data = row_data.iloc[1:, :]
    #output_data.to_csv(output_path)

    sum_consumed_electric_energy = round(output_data['CONSUMED_ELECTRIC_ENERGY'].sum(), 3)
    #print('総消費エネルギー：')
    #print(sum_consumed_electric_energy)
    #print(output_data)

    return output_data, sum_consumed_electric_energy

def all_func(trip_record, is_exist_dopplerSpeed):
    return main(trip_record, is_exist_dopplerSpeed)

if __name__ == "__main__":
    t1 = time.time()
    #main(trip_record, is_exist_dopplerSpeed)
    t2 = time.time()
    print('実行時間：', str(t2-t1))