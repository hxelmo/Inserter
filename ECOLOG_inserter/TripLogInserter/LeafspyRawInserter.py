"""
LeafspyRawInserter.py

LEAFSpy データを読み出して, データベースに書き込む.
"""

from Manager_config import EndDate_result, StartDate_result
from TripLogInserter.DatabaseAccess_config import server, database, LEAFSPY_ToinsertPath, LEAFSPY_InsertedPath, LEAFSPY_ALL_TABLE
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
Insert_Table = LEAFSPY_ALL_TABLE
SERVER = server
DATABASE = database

def all_func(StartDate, EndDate):
    for filename in search_filelist(StartDate, EndDate):
        insert(filename)
        copy_file(filename)

#ここから下は既存のプログラムから変更なし

# DB へのコネクション用文字列.
# git 追跡対象外の config.py に記述した DB サーバー名と DB 名を用いている.  
config = "DRIVER={{SQL Server}};SERVER={};DATABASE={}".format(SERVER, DATABASE)

class LeafspyRecord:
    """Leafspy のログ 1 行に対応するクラス.  

    Leafspy に存在するデータ項目名と, そのパース処理を記述している.
    """
    KEY_JST = 'Date/Time'
    KEY_LATITUDE = 'Lat'
    KEY_LONGITUDE = 'Long'
    KEY_ELEVATION = 'Elv'
    KEY_SPEED = 'Speed'
    KEY_EPOCH_TIME = 'epoch time'
    KEY_VEHICLE_IDENTIFICATION_NUMBER = 'VIN'
    KEY_AHR = 'AHr'
    KEY_PACK_T1_C = 'Pack T1 C'
    KEY_PACK_T2_C = 'Pack T2 C'
    KEY_PACK_T3_C = 'Pack T3 C'
    KEY_PACK_T4_C = 'Pack T4 C'
    KEY_PACK_T1_F = 'Pack T1 F'
    KEY_PACK_T2_F = 'Pack T2 F'
    KEY_PACK_T3_F = 'Pack T3 F'
    KEY_PACK_T4_F = 'Pack T4 F'
    KEY_ODO_KM = 'Odo(km)'
    KEY_GIDS = 'Gids'
    KEY_SOH = 'SOH'
    KEY_SOC = 'SOC'
    KEY_QC = 'QC'
    KEY_L1_L2 = 'L1/L2'
    KEY_TORQUE_NM = 'Torque Nm'
    KEY_PACK_VOLTS = 'Pack Volts'
    KEY_PACK_AMP = 'Pack Amps'
    KEY_REGEN_WH = 'RegenWh'
    KEY_MOTOR_POWER_W = 'Motor Pwr(w)'
    KEY_MOTOR_POWER_100W = 'Motor Pwr(100)'
    KEY_AUX_POWER_100W = 'Aux Pwr(100w)'
    KEY_AC_POWER_250W = 'A/C Pwr(250w)'
    KEY_AC_COMP_100KPA = 'A/C Comp(0.1MPa)'
    KEY_ESTIMATED_AC_POWER_50W = 'Est Pwr A/C(50w)'
    KEY_ESTIMATED_HEATER_POWER_250W = 'Est Pwr Htr(250w)'
    KEY_PLUG_STATE = 'Plug State'
    KEY_CHARGE_MODE = 'Charge Mode'
    KEY_OBC_OUT_POWER = 'OBC Out Pwr'
    KEY_GEAR = 'Gear'
    KEY_H_VOLT_1 = 'HVolt1'
    KEY_H_VOLT_2 = 'HVolt2'
    KEY_GPS_STATUS = 'GPS Status'
    KEY_SW_POWER = 'Power SW'
    KEY_BMS = 'BMS'
    KEY_OBC = 'OBC'
    KEY_DEBUG = 'Debug'
    KEY_MOTOR_TEMPERATURE_ADDED_40C = 'Motor Temp'
    KEY_INVERTER_2_TEMPERATURE_ADDED_40C = 'Inverter 2 Temp'
    KEY_INVERTER_4_TEMPERATURE_ADDED_40C = 'Inverter 4 Temp'
    KEY_SPEED_1 = 'Speed1'
    KEY_SPEED_2 = 'Speed2'
    KEY_WIPER_STATUS = 'Wiper Status'
    KEY_MAX_CP_MV = 'Max CP mV'
    KEY_MIN_CP_MV = 'Min CP mV'
    KEY_AVG_CP_MV = 'Avg CP mV'
    KEY_CP_MV_DIFFERENCE = 'CP mV Diff'
    KEY_JUDGEMENT_VALUE = 'Juggement Value'
    KEY_CP1 = 'CP1'
    KEY_CP2 = 'CP2'
    KEY_CP3 = 'CP3'
    KEY_CP4 = 'CP4'
    KEY_CP5 = 'CP5'
    KEY_CP6 = 'CP6'
    KEY_CP7 = 'CP7'
    KEY_CP8 = 'CP8'
    KEY_CP9 = 'CP9'
    KEY_CP10 = 'CP10'
    KEY_CP11 = 'CP11'
    KEY_CP12 = 'CP12'
    KEY_CP13 = 'CP13'
    KEY_CP14 = 'CP14'
    KEY_CP15 = 'CP15'
    KEY_CP16 = 'CP16'
    KEY_CP17 = 'CP17'
    KEY_CP18 = 'CP18'
    KEY_CP19 = 'CP19'
    KEY_CP20 = 'CP20'
    KEY_CP21 = 'CP21'
    KEY_CP22 = 'CP22'
    KEY_CP23 = 'CP23'
    KEY_CP24 = 'CP24'
    KEY_CP25 = 'CP25'
    KEY_CP26 = 'CP26'
    KEY_CP27 = 'CP27'
    KEY_CP28 = 'CP28'
    KEY_CP29 = 'CP29'
    KEY_CP30 = 'CP30'
    KEY_CP31 = 'CP31'
    KEY_CP32 = 'CP32'
    KEY_CP33 = 'CP33'
    KEY_CP34 = 'CP34'
    KEY_CP35 = 'CP35'
    KEY_CP36 = 'CP36'
    KEY_CP37 = 'CP37'
    KEY_CP38 = 'CP38'
    KEY_CP39 = 'CP39'
    KEY_CP40 = 'CP40'
    KEY_CP41 = 'CP41'
    KEY_CP42 = 'CP42'
    KEY_CP43 = 'CP43'
    KEY_CP44 = 'CP44'
    KEY_CP45 = 'CP45'
    KEY_CP46 = 'CP46'
    KEY_CP47 = 'CP47'
    KEY_CP48 = 'CP48'
    KEY_CP49 = 'CP49'
    KEY_CP50 = 'CP50'
    KEY_CP51 = 'CP51'
    KEY_CP52 = 'CP52'
    KEY_CP53 = 'CP53'
    KEY_CP54 = 'CP54'
    KEY_CP55 = 'CP55'
    KEY_CP56 = 'CP56'
    KEY_CP57 = 'CP57'
    KEY_CP58 = 'CP58'
    KEY_CP59 = 'CP59'
    KEY_CP60 = 'CP60'
    KEY_CP61 = 'CP61'
    KEY_CP62 = 'CP62'
    KEY_CP63 = 'CP63'
    KEY_CP64 = 'CP64'
    KEY_CP65 = 'CP65'
    KEY_CP66 = 'CP66'
    KEY_CP67 = 'CP67'
    KEY_CP68 = 'CP68'
    KEY_CP69 = 'CP69'
    KEY_CP70 = 'CP70'
    KEY_CP71 = 'CP71'
    KEY_CP72 = 'CP72'
    KEY_CP73 = 'CP73'
    KEY_CP74 = 'CP74'
    KEY_CP75 = 'CP75'
    KEY_CP76 = 'CP76'
    KEY_CP77 = 'CP77'
    KEY_CP78 = 'CP78'
    KEY_CP79 = 'CP79'
    KEY_CP80 = 'CP80'
    KEY_CP81 = 'CP81'
    KEY_CP82 = 'CP82'
    KEY_CP83 = 'CP83'
    KEY_CP84 = 'CP84'
    KEY_CP85 = 'CP85'
    KEY_CP86 = 'CP86'
    KEY_CP87 = 'CP87'
    KEY_CP88 = 'CP88'
    KEY_CP89 = 'CP89'
    KEY_CP90 = 'CP90'
    KEY_CP91 = 'CP91'
    KEY_CP92 = 'CP92'
    KEY_CP93 = 'CP93'
    KEY_CP94 = 'CP94'
    KEY_CP95 = 'CP95'
    KEY_CP96 = 'CP96'
    KEY_BATTERY_12V_AMP = '12v Bat Amps'
    KEY_HX = 'Hx'
    KEY_BATTERY_12V_VOLTS = '12v Bat Volts'
    KEY_TP_FL = 'TP-FL'
    KEY_TP_FR = 'TP-FR'
    KEY_TP_RR = 'TP-RR'
    KEY_TP_RL = 'TP-RL'
    KEY_AMBIENT = 'Ambient'
    KEY_B_LEVEL = 'BLevel'


    def __init__(self, record_dict):
        """
        Leafspy ログ 1 行に対応する辞書オブジェクトを引数にとるコンストラクタ.

        """
        if not isinstance(record_dict, dict):
            raise Exception("Argument of LeaspyRecord.__init__() must be dictionary.")

        record_keys = record_dict.keys()

        if self.KEY_JST in record_keys:
            self.jst = datetime.strptime(record_dict[self.KEY_JST], "%m/%d/%Y %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")
        else:
            raise Exception("Key \"{}\" must be required in dictionary.".format(self.KEY_JST))
        
        if self.KEY_LATITUDE in record_keys:
            self.lat = record_dict[self.KEY_LATITUDE]
        else:
            self.lat = None
        
        if self.KEY_LONGITUDE in record_keys:
            self.long = record_dict[self.KEY_LONGITUDE]
        else:
            self.long = None
        
        if self.KEY_ELEVATION in record_keys:
            self.elv = record_dict[self.KEY_ELEVATION]
        else:
            self.elv = None
        
        if self.KEY_SPEED in record_keys:
            self.speed = record_dict[self.KEY_SPEED]
        else:
            self.speed = None
        
        if self.KEY_GIDS in record_keys:
            self.gids = record_dict[self.KEY_GIDS]
        else:
            self.gids = None

        if self.KEY_SOC in record_keys:
            self.soc = record_dict[self.KEY_SOC]
        else:
            self.soc = None

        if self.KEY_AHR in record_keys:
            self.ahr = record_dict[self.KEY_AHR]
        else:
            self.ahr = None

        if self.KEY_PACK_VOLTS in record_keys:
            self.pack_volts = record_dict[self.KEY_PACK_VOLTS]
        else:
            self.pack_volts = "NULL"

        if self.KEY_PACK_AMP in record_keys:
            self.pack_amp = record_dict[self.KEY_PACK_AMP]
        else:
            self.pack_amp = None
        
        if self.KEY_MAX_CP_MV in record_keys:
            self.max_cp_mv = record_dict[self.KEY_MAX_CP_MV]
        else:
            self.max_cp_mv = None
        
        if self.KEY_MIN_CP_MV in record_keys:
            self.min_cp_mv = record_dict[self.KEY_MIN_CP_MV]
        else:
            self.min_cp_mv = None
        
        if self.KEY_AVG_CP_MV in record_keys:
            self.avg_cp_mv = record_dict[self.KEY_AVG_CP_MV]
        else:
            self.avg_cp_mv = None
        
        if self.KEY_CP_MV_DIFFERENCE in record_keys:
            self.cp_mv_difference = record_dict[self.KEY_CP_MV_DIFFERENCE]
        else:
            self.cp_mv_difference = None
        
        if self.KEY_JUDGEMENT_VALUE in record_keys:
            self.judgement_value = record_dict[self.KEY_JUDGEMENT_VALUE]
        else:
            self.judgement_value = None

        if self.KEY_PACK_T1_F in record_keys:
            self.pack_t1_f = record_dict[self.KEY_PACK_T1_F]
            if self.pack_t1_f == 'none':
                self.pack_t1_f = None
        else:
            self.pack_t1_f = None
        
        if self.KEY_PACK_T1_C in record_keys:
            self.pack_t1_c = record_dict[self.KEY_PACK_T1_C]
            if self.pack_t1_c == 'none':
                self.pack_t1_c = None
        else:
            self.pack_t1_c = None
        
        if self.KEY_PACK_T2_F in record_keys:
            self.pack_t2_f = record_dict[self.KEY_PACK_T2_F]
            if self.pack_t2_f == 'none':
                self.pack_t2_f = None
        else:
            self.pack_t2_f = None

        if self.KEY_PACK_T2_C in record_keys:
            self.pack_t2_c = record_dict[self.KEY_PACK_T2_C]
            if self.pack_t2_c == 'none':
                self.pack_t2_c = None
        else:
            self.pack_t2_c = None

        if self.KEY_PACK_T3_F in record_keys:
            self.pack_t3_f = record_dict[self.KEY_PACK_T3_F]
            if self.pack_t3_f == 'none':
                self.pack_t3_f = None
        else:
            self.pack_t3_f = None

        if self.KEY_PACK_T3_C in record_keys:
            self.pack_t3_c = record_dict[self.KEY_PACK_T3_C]
            if self.pack_t3_c == 'none':
                self.pack_t3_c = None
        else:
            self.pack_t3_c = None

        if self.KEY_PACK_T4_F in record_keys:
            self.pack_t4_f = record_dict[self.KEY_PACK_T4_F]
            if self.pack_t4_f == 'none':
                self.pack_t4_f = None
        else:
            self.pack_t4_f = None

        if self.KEY_PACK_T4_C in record_keys:
            self.pack_t4_c = record_dict[self.KEY_PACK_T4_C]
            if self.pack_t4_c == 'none':
                self.pack_t4_c = None
        else:
            self.pack_t4_c = None

        if self.KEY_CP1 in record_keys:
            self.cp1 = record_dict[self.KEY_CP1]
        else:
            self.cp1 = None
        
        if self.KEY_CP2 in record_keys:
            self.cp2 = record_dict[self.KEY_CP2]
        else:
            self.cp2 = None
        
        if self.KEY_CP3 in record_keys:
            self.cp3 = record_dict[self.KEY_CP3]
        else:
            self.cp3 = None
        
        if self.KEY_CP4 in record_keys:
            self.cp4 = record_dict[self.KEY_CP4]
        else:
            self.cp4 = None
        
        if self.KEY_CP5 in record_keys:
            self.cp5 = record_dict[self.KEY_CP5]
        else:
            self.cp5 = None
        
        if self.KEY_CP6 in record_keys:
            self.cp6 = record_dict[self.KEY_CP6]
        else:
            self.cp6 = None

        if self.KEY_CP7 in record_keys:
            self.cp7 = record_dict[self.KEY_CP7]
        else:
            self.cp7 = None
        
        if self.KEY_CP8 in record_keys:
            self.cp8 = record_dict[self.KEY_CP8]
        else:
            self.cp8 = None
        
        if self.KEY_CP9 in record_keys:
            self.cp9 = record_dict[self.KEY_CP9]
        else:
            self.cp9 = None
        
        if self.KEY_CP10 in record_keys:
            self.cp10 = record_dict[self.KEY_CP10]
        else:
            self.cp10 = None
        
        if self.KEY_CP11 in record_keys:
            self.cp11 = record_dict[self.KEY_CP11]
        else:
            self.cp11 = None
        
        if self.KEY_CP12 in record_keys:
            self.cp12 = record_dict[self.KEY_CP12]
        else:
            self.cp12 = None
        
        if self.KEY_CP13 in record_keys:
            self.cp13 = record_dict[self.KEY_CP13]
        else:
            self.cp13 = None
        
        if self.KEY_CP14 in record_keys:
            self.cp14 = record_dict[self.KEY_CP14]
        else:
            self.cp14 = None
        
        if self.KEY_CP15 in record_keys:
            self.cp15 = record_dict[self.KEY_CP15]
        else:
            self.cp15 = None
        
        if self.KEY_CP16 in record_keys:
            self.cp16 = record_dict[self.KEY_CP16]
        else:
            self.cp16 = None

        if self.KEY_CP17 in record_keys:
            self.cp17 = record_dict[self.KEY_CP17]
        else:
            self.cp17 = None
        
        if self.KEY_CP18 in record_keys:
            self.cp18 = record_dict[self.KEY_CP18]
        else:
            self.cp18 = None
        
        if self.KEY_CP19 in record_keys:
            self.cp19 = record_dict[self.KEY_CP19]
        else:
            self.cp19 = None
        
        if self.KEY_CP20 in record_keys:
            self.cp20 = record_dict[self.KEY_CP20]
        else:
            self.cp20 = None
        
        if self.KEY_CP21 in record_keys:
            self.cp21 = record_dict[self.KEY_CP21]
        else:
            self.cp21 = None
        
        if self.KEY_CP22 in record_keys:
            self.cp22 = record_dict[self.KEY_CP22]
        else:
            self.cp22 = None
        
        if self.KEY_CP23 in record_keys:
            self.cp23 = record_dict[self.KEY_CP23]
        else:
            self.cp23 = None
        
        if self.KEY_CP24 in record_keys:
            self.cp24 = record_dict[self.KEY_CP24]
        else:
            self.cp24 = None
        
        if self.KEY_CP25 in record_keys:
            self.cp25 = record_dict[self.KEY_CP25]
        else:
            self.cp25 = None
        
        if self.KEY_CP26 in record_keys:
            self.cp26 = record_dict[self.KEY_CP26]
        else:
            self.cp26 = None

        if self.KEY_CP27 in record_keys:
            self.cp27 = record_dict[self.KEY_CP27]
        else:
            self.cp27 = None
        
        if self.KEY_CP28 in record_keys:
            self.cp28 = record_dict[self.KEY_CP28]
        else:
            self.cp28 = None
        
        if self.KEY_CP29 in record_keys:
            self.cp29 = record_dict[self.KEY_CP29]
        else:
            self.cp29 = None
        
        if self.KEY_CP30 in record_keys:
            self.cp30 = record_dict[self.KEY_CP30]
        else:
            self.cp30 = None
        
        if self.KEY_CP31 in record_keys:
            self.cp31 = record_dict[self.KEY_CP31]
        else:
            self.cp31 = None
        
        if self.KEY_CP32 in record_keys:
            self.cp32 = record_dict[self.KEY_CP32]
        else:
            self.cp32 = None
        
        if self.KEY_CP33 in record_keys:
            self.cp33 = record_dict[self.KEY_CP33]
        else:
            self.cp33 = None
        
        if self.KEY_CP34 in record_keys:
            self.cp34 = record_dict[self.KEY_CP34]
        else:
            self.cp34 = None
        
        if self.KEY_CP35 in record_keys:
            self.cp35 = record_dict[self.KEY_CP35]
        else:
            self.cp35 = None
        
        if self.KEY_CP36 in record_keys:
            self.cp36 = record_dict[self.KEY_CP36]
        else:
            self.cp36 = None

        if self.KEY_CP37 in record_keys:
            self.cp37 = record_dict[self.KEY_CP37]
        else:
            self.cp37 = None
        
        if self.KEY_CP38 in record_keys:
            self.cp38 = record_dict[self.KEY_CP38]
        else:
            self.cp38 = None
        
        if self.KEY_CP39 in record_keys:
            self.cp39 = record_dict[self.KEY_CP39]
        else:
            self.cp39 = None
        
        if self.KEY_CP40 in record_keys:
            self.cp40 = record_dict[self.KEY_CP40]
        else:
            self.cp40 = None
        
        if self.KEY_CP41 in record_keys:
            self.cp41 = record_dict[self.KEY_CP41]
        else:
            self.cp41 = None
        
        if self.KEY_CP42 in record_keys:
            self.cp42 = record_dict[self.KEY_CP42]
        else:
            self.cp42 = None
        
        if self.KEY_CP43 in record_keys:
            self.cp43 = record_dict[self.KEY_CP43]
        else:
            self.cp43 = None
        
        if self.KEY_CP44 in record_keys:
            self.cp44 = record_dict[self.KEY_CP44]
        else:
            self.cp44 = None
        
        if self.KEY_CP45 in record_keys:
            self.cp45 = record_dict[self.KEY_CP45]
        else:
            self.cp45 = None
        
        if self.KEY_CP46 in record_keys:
            self.cp46 = record_dict[self.KEY_CP46]
        else:
            self.cp46 = None

        if self.KEY_CP47 in record_keys:
            self.cp47 = record_dict[self.KEY_CP47]
        else:
            self.cp47 = None
        
        if self.KEY_CP48 in record_keys:
            self.cp48 = record_dict[self.KEY_CP48]
        else:
            self.cp48 = None
        
        if self.KEY_CP49 in record_keys:
            self.cp49 = record_dict[self.KEY_CP49]
        else:
            self.cp49 = None
        
        if self.KEY_CP50 in record_keys:
            self.cp50 = record_dict[self.KEY_CP50]
        else:
            self.cp50 = None
        
        if self.KEY_CP51 in record_keys:
            self.cp51 = record_dict[self.KEY_CP51]
        else:
            self.cp51 = None
        
        if self.KEY_CP52 in record_keys:
            self.cp52 = record_dict[self.KEY_CP52]
        else:
            self.cp52 = None
        
        if self.KEY_CP53 in record_keys:
            self.cp53 = record_dict[self.KEY_CP53]
        else:
            self.cp53 = None
        
        if self.KEY_CP54 in record_keys:
            self.cp54 = record_dict[self.KEY_CP54]
        else:
            self.cp54 = None
        
        if self.KEY_CP55 in record_keys:
            self.cp55 = record_dict[self.KEY_CP55]
        else:
            self.cp55 = None
        
        if self.KEY_CP56 in record_keys:
            self.cp56 = record_dict[self.KEY_CP56]
        else:
            self.cp56 = None

        if self.KEY_CP57 in record_keys:
            self.cp57 = record_dict[self.KEY_CP57]
        else:
            self.cp57 = None
        
        if self.KEY_CP58 in record_keys:
            self.cp58 = record_dict[self.KEY_CP58]
        else:
            self.cp58 = None
        
        if self.KEY_CP59 in record_keys:
            self.cp59 = record_dict[self.KEY_CP59]
        else:
            self.cp59 = None
        
        if self.KEY_CP60 in record_keys:
            self.cp60 = record_dict[self.KEY_CP60]
        else:
            self.cp60 = None
        
        if self.KEY_CP61 in record_keys:
            self.cp61 = record_dict[self.KEY_CP61]
        else:
            self.cp61 = None
        
        if self.KEY_CP62 in record_keys:
            self.cp62 = record_dict[self.KEY_CP62]
        else:
            self.cp62 = None
        
        if self.KEY_CP63 in record_keys:
            self.cp63 = record_dict[self.KEY_CP63]
        else:
            self.cp63 = None
        
        if self.KEY_CP64 in record_keys:
            self.cp64 = record_dict[self.KEY_CP64]
        else:
            self.cp64 = None
        
        if self.KEY_CP65 in record_keys:
            self.cp65 = record_dict[self.KEY_CP65]
        else:
            self.cp65 = None
        
        if self.KEY_CP66 in record_keys:
            self.cp66 = record_dict[self.KEY_CP66]
        else:
            self.cp66 = None

        if self.KEY_CP67 in record_keys:
            self.cp67 = record_dict[self.KEY_CP67]
        else:
            self.cp67 = None
        
        if self.KEY_CP68 in record_keys:
            self.cp68 = record_dict[self.KEY_CP68]
        else:
            self.cp68 = None
        
        if self.KEY_CP69 in record_keys:
            self.cp69 = record_dict[self.KEY_CP69]
        else:
            self.cp69 = None
        
        if self.KEY_CP70 in record_keys:
            self.cp70 = record_dict[self.KEY_CP70]
        else:
            self.cp70 = None
        
        if self.KEY_CP71 in record_keys:
            self.cp71 = record_dict[self.KEY_CP71]
        else:
            self.cp71 = None
        
        if self.KEY_CP72 in record_keys:
            self.cp72 = record_dict[self.KEY_CP72]
        else:
            self.cp72 = None
        
        if self.KEY_CP73 in record_keys:
            self.cp73 = record_dict[self.KEY_CP73]
        else:
            self.cp73 = None
        
        if self.KEY_CP74 in record_keys:
            self.cp74 = record_dict[self.KEY_CP74]
        else:
            self.cp74 = None
        
        if self.KEY_CP75 in record_keys:
            self.cp75 = record_dict[self.KEY_CP75]
        else:
            self.cp75 = None
        
        if self.KEY_CP76 in record_keys:
            self.cp76 = record_dict[self.KEY_CP76]
        else:
            self.cp76 = None

        if self.KEY_CP77 in record_keys:
            self.cp77 = record_dict[self.KEY_CP77]
        else:
            self.cp77 = None
        
        if self.KEY_CP78 in record_keys:
            self.cp78 = record_dict[self.KEY_CP78]
        else:
            self.cp78 = None
        
        if self.KEY_CP79 in record_keys:
            self.cp79 = record_dict[self.KEY_CP79]
        else:
            self.cp79 = None
        
        if self.KEY_CP80 in record_keys:
            self.cp80 = record_dict[self.KEY_CP80]
        else:
            self.cp80 = None
        
        if self.KEY_CP81 in record_keys:
            self.cp81 = record_dict[self.KEY_CP81]
        else:
            self.cp81 = None
        
        if self.KEY_CP82 in record_keys:
            self.cp82 = record_dict[self.KEY_CP82]
        else:
            self.cp82 = None
        
        if self.KEY_CP83 in record_keys:
            self.cp83 = record_dict[self.KEY_CP83]
        else:
            self.cp83 = None
        
        if self.KEY_CP84 in record_keys:
            self.cp84 = record_dict[self.KEY_CP84]
        else:
            self.cp84 = None
        
        if self.KEY_CP85 in record_keys:
            self.cp85 = record_dict[self.KEY_CP85]
        else:
            self.cp85 = None
        
        if self.KEY_CP86 in record_keys:
            self.cp86 = record_dict[self.KEY_CP86]
        else:
            self.cp86 = None

        if self.KEY_CP87 in record_keys:
            self.cp87 = record_dict[self.KEY_CP87]
        else:
            self.cp87 = None
        
        if self.KEY_CP88 in record_keys:
            self.cp88 = record_dict[self.KEY_CP88]
        else:
            self.cp88 = None
        
        if self.KEY_CP89 in record_keys:
            self.cp89 = record_dict[self.KEY_CP89]
        else:
            self.cp89 = None
        
        if self.KEY_CP90 in record_keys:
            self.cp90 = record_dict[self.KEY_CP90]
        else:
            self.cp90 = None
        
        if self.KEY_CP91 in record_keys:
            self.cp91 = record_dict[self.KEY_CP91]
        else:
            self.cp91 = None
        
        if self.KEY_CP92 in record_keys:
            self.cp92 = record_dict[self.KEY_CP92]
        else:
            self.cp92 = None
        
        if self.KEY_CP93 in record_keys:
            self.cp93 = record_dict[self.KEY_CP93]
        else:
            self.cp93 = None
        
        if self.KEY_CP94 in record_keys:
            self.cp94 = record_dict[self.KEY_CP94]
        else:
            self.cp94 = None
        
        if self.KEY_CP95 in record_keys:
            self.cp95 = record_dict[self.KEY_CP95]
        else:
            self.cp95 = None
        
        if self.KEY_CP96 in record_keys:
            self.cp96 = record_dict[self.KEY_CP96]
        else:
            self.cp96 = None
        
        if self.KEY_BATTERY_12V_AMP in record_keys:
            self.battery_12v_amp = record_dict[self.KEY_BATTERY_12V_AMP]
        else:
            self.battery_12v_amp = None
        
        if self.KEY_VEHICLE_IDENTIFICATION_NUMBER in record_keys:
            self.vehicle_identification_number = record_dict[self.KEY_VEHICLE_IDENTIFICATION_NUMBER]
            self.vehicle_identification_number = self.vehicle_identification_number.replace('(', '')
            self.vehicle_identification_number = self.vehicle_identification_number.replace(')', '')
        else:
            raise Exception("Key \"{}\" must be required in dictionary.".format(self.KEY_VEHICLE_IDENTIFICATION_NUMBER))
        
        if self.KEY_HX in record_keys:
            self.hx = record_dict[self.KEY_HX]
        else:
            self.hx = None
        
        if self.KEY_BATTERY_12V_VOLTS in record_keys:
            self.battery_12v_volts = record_dict[self.KEY_BATTERY_12V_VOLTS]
        else:
            self.battery_12v_volts = None
        
        if self.KEY_ODO_KM in record_keys:
            self.odo_km = record_dict[self.KEY_ODO_KM]
        else:
            self.odo_km = None
        
        if self.KEY_QC in record_keys:
            self.qc = record_dict[self.KEY_QC]
        else:
            self.qc = None

        if self.KEY_L1_L2 in record_keys:
            self.l1_l2 = record_dict[self.KEY_L1_L2]
        else:
            self.l1_l2 = None

        if self.KEY_TP_FL in record_keys:
            self.tp_fl = record_dict[self.KEY_TP_FL]
        else:
            self.tp_fl = None
        
        if self.KEY_TP_FR in record_keys:
            self.tp_fr = record_dict[self.KEY_TP_FR]
        else:
            self.tp_fr = None
        
        if self.KEY_TP_RR in record_keys:
            self.tp_rr = record_dict[self.KEY_TP_RR]
        else:
            self.tp_rr = None
        
        if self.KEY_TP_RL in record_keys:
            self.tp_rl = record_dict[self.KEY_TP_RL]
        else:
            self.tp_rl = None
        
        if self.KEY_AMBIENT in record_keys:
            self.ambient = record_dict[self.KEY_AMBIENT]
        else:
            self.ambient = None
        
        if self.KEY_SOH in record_keys:
            self.soh = record_dict[self.KEY_SOH]
        else:
            self.soh = None
        
        if self.KEY_REGEN_WH in record_keys:
            self.regen_wh = record_dict[self.KEY_REGEN_WH]
        else:
            self.regen_wh = None
        
        if self.KEY_B_LEVEL in record_keys:
            self.b_level = record_dict[self.KEY_B_LEVEL]
        else:
            self.b_level = None

        if self.KEY_EPOCH_TIME in record_keys:
            self.epoch_time = record_dict[self.KEY_EPOCH_TIME]
        else:
            self.epoch_time = None
        
        if self.KEY_MOTOR_POWER_W in record_keys:
            self.motor_power_w = record_dict[self.KEY_MOTOR_POWER_W]
        else:
            self.motor_power_w = None
        
        if self.KEY_AUX_POWER_100W in record_keys:
            self.aux_power_100w = record_dict[self.KEY_AUX_POWER_100W]
        else:
            self.aux_power_100w = None

        if self.KEY_AC_POWER_250W in record_keys:
            self.ac_power_250w = record_dict[self.KEY_AC_POWER_250W]
        else:
            self.ac_power_250w = None
        
        if self.KEY_AC_COMP_100KPA in record_keys:
            self.ac_comp_100kpa = record_dict[self.KEY_AC_COMP_100KPA]
        else:
            self.ac_comp_100kpa = None
        
        if self.KEY_ESTIMATED_AC_POWER_50W in record_keys:
            self.estimated_ac_power_50w = record_dict[self.KEY_ESTIMATED_AC_POWER_50W]
        else:
            self.estimated_ac_power_50w = None

        if self.KEY_ESTIMATED_HEATER_POWER_250W in record_keys:
            self.estimated_heater_power_250w = record_dict[self.KEY_ESTIMATED_HEATER_POWER_250W]
        else:
            self.estimated_heater_power_250w = None

        if self.KEY_PLUG_STATE in record_keys:
            self.plug_state = record_dict[self.KEY_PLUG_STATE]
        else:
            self.plug_state = None

        if self.KEY_CHARGE_MODE in record_keys:
            self.charge_mode = record_dict[self.KEY_CHARGE_MODE]
        else:
            self.charge_mode = None
        
        if self.KEY_OBC_OUT_POWER in record_keys:
            self.obc_out_power = record_dict[self.KEY_OBC_OUT_POWER]
        else:
            self.obc_out_power = None

        if self.KEY_GEAR in record_keys:
            self.gear = record_dict[self.KEY_GEAR]
        else:
            self.gear = None

        if self.KEY_H_VOLT_1 in record_keys:
            self.h_volt_1 = record_dict[self.KEY_H_VOLT_1]
        else:
            self.h_volt_1 = None

        if self.KEY_H_VOLT_2 in record_keys:
            self.h_volt_2 = record_dict[self.KEY_H_VOLT_2]
        else:
            self.h_volt_2 = None

        if self.KEY_GPS_STATUS in record_keys:
            self.gps_status = int(record_dict[self.KEY_GPS_STATUS], 16)  #16進数をint型に変換
        else:
            self.gps_status = None

        if self.KEY_SW_POWER in record_keys:
            self.sw_power = record_dict[self.KEY_SW_POWER]
        else:
            self.sw_power = None

        if self.KEY_BMS in record_keys:
            self.bms = record_dict[self.KEY_BMS]
        else:
            self.bms = None

        if self.KEY_OBC in record_keys:
            self.obc = record_dict[self.KEY_OBC]
        else:
            self.obc = None
        
        if self.KEY_DEBUG in record_keys:
            self.debug = record_dict[self.KEY_DEBUG]
        else:
            self.debug = None
        
        if self.KEY_MOTOR_TEMPERATURE_ADDED_40C in record_keys:
            self.motor_temperature_added_40c = record_dict[self.KEY_MOTOR_TEMPERATURE_ADDED_40C]
        else:
            self.motor_temperature_added_40c = None

        if self.KEY_INVERTER_2_TEMPERATURE_ADDED_40C in record_keys:
            self.inverter_2_temperature_added_40c = record_dict[self.KEY_INVERTER_2_TEMPERATURE_ADDED_40C]
        else:
            self.inverter_2_temperature_added_40c = None
        
        if self.KEY_INVERTER_4_TEMPERATURE_ADDED_40C in record_keys:
            self.inverter_4_temperature_added_40c = record_dict[self.KEY_INVERTER_4_TEMPERATURE_ADDED_40C]
        else:
            self.inverter_4_temperature_added_40c = None
        
        if self.KEY_SPEED_1 in record_keys:
            self.speed_1 = record_dict[self.KEY_SPEED_1]
        else:
            self.speed_1 = None
        
        if self.KEY_SPEED_2 in record_keys:
            self.speed_2 = record_dict[self.KEY_SPEED_2]
        else:
            self.speed_2 = None
        
        if self.KEY_WIPER_STATUS in record_keys:
            self.wiper_status = "\'" + record_dict[self.KEY_WIPER_STATUS] + "\'"

        else:
            self.wiper_status = None

        if self.KEY_TORQUE_NM in record_keys:
            self.torque_nm = record_dict[self.KEY_TORQUE_NM]
        else:
            self.torque_nm = None

    @classmethod
    def column_list(cls):
        """LEAFSPY_RAW_PROJECTED_ALL と対応するカラムリストを生成する関数.
        """
        return "(JST, LATITUDE, LONGITUDE, ELEVATION, SPEED, GIDS, SOC, AHR, PACK_VOLTS, PACK_AMP, MAX_CP_MV, MIN_CP_MV, AVG_CP_MV, CP_MV_DIFFERENCE, JUDGEMENT_VALUE, PACK_T1_F, PACK_T1_C, PACK_T2_F, PACK_T2_C, PACK_T3_F, PACK_T3_C, PACK_T4_F, PACK_T4_C, CP1, CP2, CP3, CP4, CP5, CP6, CP7, CP8, CP9, CP10, CP11, CP12, CP13, CP14, CP15, CP16, CP17, CP18, CP19, CP20, CP21, CP22, CP23, CP24, CP25, CP26, CP27, CP28, CP29, CP30, CP31, CP32, CP33, CP34, CP35, CP36, CP37, CP38, CP39, CP40, CP41, CP42, CP43, CP44, CP45, CP46, CP47, CP48, CP49, CP50, CP51, CP52, CP53, CP54, CP55, CP56, CP57, CP58, CP59, CP60, CP61, CP62, CP63, CP64, CP65, CP66, CP67, CP68, CP69, CP70, CP71, CP72, CP73, CP74, CP75, CP76, CP77, CP78, CP79, CP80, CP81, CP82, CP83, CP84, CP85, CP86, CP87, CP88, CP89, CP90, CP91, CP92, CP93, CP94, CP95, CP96, BATTERY_12V_AMP, VEHICLE_IDENTIFICATION_NUMBER, HX, BATTERY_12V_VOLTS, ODO_KM, QC, L1_L2, TP_FL, TP_FR, TP_RR, TP_RL, AMBIENT, SOH, REGEN_WH, B_LEVEL, EPOCH_TIME, MOTOR_POWER_W, AUX_POWER_100W, AC_POWER_250W, AC_COMP_100KPA, ESTIMATED_AC_POWER_50W, ESTIMATED_HEATER_POWER_250W, PLUG_STATE, CHARGE_MODE, OBC_OUT_POWER, GEAR, H_VOLT_1, H_VOLT_2, GPS_STATUS, SW_POWER, BMS, OBC, DEBUG, MOTOR_TEMPERATURE_ADDED_40C, INVERTER_2_TEMPERATURE_ADDED_40C, INVERTER_4_TEMPERATURE_ADDED_40C, SPEED_1, SPEED_2, WIPER_STATUS, TORQUE_NM)"

    def __str__(self):
        """他言語における ToString() メソッド.  

        LEAFSPY_RAW_PROJECTED_ALL テーブルにデータ挿入する形式で文字列を出力する.  
        """
        ret_str = "('{jst}', {lat}, {long}, {elv}, {speed}, {gids}, {soc}, {ahr}, {pack_volts}, {pack_amp}, {max_cp_mv}, {min_cp_mv}, {avg_cp_mv}, {cp_mv_difference} ,{judgement_value}, {pack_t1_f}, {pack_t1_c}, {pack_t2_f}, {pack_t2_c}, {pack_t3_f}, {pack_t3_c}, {pack_t4_f}, {pack_t4_c}, {cp1}, {cp2}, {cp3}, {cp4}, {cp5}, {cp6}, {cp7}, {cp8}, {cp9}, {cp10}, {cp11}, {cp12}, {cp13}, {cp14}, {cp15}, {cp16}, {cp17}, {cp18}, {cp19}, {cp20}, {cp21}, {cp22}, {cp23}, {cp24}, {cp25}, {cp26}, {cp27}, {cp28}, {cp29}, {cp30}, {cp31}, {cp32}, {cp33}, {cp34}, {cp35}, {cp36}, {cp37}, {cp38}, {cp39}, {cp40}, {cp41}, {cp42}, {cp43}, {cp44}, {cp45}, {cp46}, {cp47}, {cp48}, {cp49}, {cp50}, {cp51}, {cp52}, {cp53}, {cp54}, {cp55}, {cp56}, {cp57}, {cp58}, {cp59}, {cp60}, {cp61}, {cp62}, {cp63}, {cp64}, {cp65}, {cp66}, {cp67}, {cp68}, {cp69}, {cp70}, {cp71}, {cp72}, {cp73}, {cp74}, {cp75}, {cp76}, {cp77}, {cp78}, {cp79}, {cp80}, {cp81}, {cp82}, {cp83}, {cp84}, {cp85}, {cp86}, {cp87}, {cp88}, {cp89}, {cp90}, {cp91}, {cp92}, {cp93}, {cp94}, {cp95}, {cp96}, {battery_12v_amp}, '{vehicle_identification_number}', {hx}, {battery_12v_volts}, {odo_km}, {qc}, {l1_l2}, {tp_fl}, {tp_fr}, {tp_rr}, {tp_rl}, {ambient}, {soh}, {regen_wh}, {b_level}, {epoch_time}, {motor_power_w}, {aux_power_100w}, {ac_power_250w}, {ac_comp_100kpa}, {estimated_ac_power_50w}, {estimated_heater_power_250w}, {plug_state}, {charge_mode}, {obc_out_power}, {gear}, {h_volt_1}, {h_volt_2}, {gps_status}, {sw_power}, {bms}, {obc}, {debug}, {motor_temperature_added_40c}, {inverter_2_temperature_added_40c}, {inverter_4_temperature_added_40c}, {speed_1}, {speed_2}, {wiper_status}, {torque_nm})".format(
            jst = self.jst,
            #lat = self.lat if self.lat is not None else "NULL",
            lat = "NULL",
            #long = self.long if self.long is not None else "NULL",
            long = "NULL",
            #elv = self.elv if self.elv is not None else "NULL",
            elv = "NULL",
            speed = self.speed if self.speed is not None else "NULL",
            gids = self.gids if self.gids is not None else "NULL",
            soc = self.soc if self.soc is not None else "NULL",
            ahr = self.ahr if self.ahr is not None else "NULL",
            pack_volts = self.pack_volts if self.pack_volts is not None else "NULL",
            pack_amp = self.pack_amp if self.pack_amp is not None else "NULL",
            max_cp_mv = self.max_cp_mv if self.max_cp_mv is not None else "NULL",
            min_cp_mv = self.min_cp_mv if self.min_cp_mv is not None else "NULL",
            avg_cp_mv = self.avg_cp_mv if self.avg_cp_mv is not None else "NULL",
            cp_mv_difference = self.cp_mv_difference if self.cp_mv_difference is not None else "NULL",
            judgement_value = self.judgement_value if self.judgement_value is not None else "NULL",
            pack_t1_f = self.pack_t1_f if self.pack_t1_f is not None else "NULL",
            pack_t1_c = self.pack_t1_c if self.pack_t1_c is not None else "NULL",
            pack_t2_f = self.pack_t2_f if self.pack_t2_f is not None else "NULL",
            pack_t2_c = self.pack_t2_c if self.pack_t2_c is not None else "NULL",
            pack_t3_f = self.pack_t3_f if self.pack_t3_f is not None else "NULL",
            pack_t3_c = self.pack_t3_c if self.pack_t3_c is not None else "NULL",
            pack_t4_f = self.pack_t4_f if self.pack_t4_f is not None else "NULL",
            pack_t4_c = self.pack_t4_c if self.pack_t4_c is not None else "NULL",
            cp1 = self.cp1 if self.cp1 is not None else "NULL",
            cp2 = self.cp2 if self.cp2 is not None else "NULL",
            cp3 = self.cp3 if self.cp3 is not None else "NULL",
            cp4 = self.cp4 if self.cp4 is not None else "NULL",
            cp5 = self.cp5 if self.cp5 is not None else "NULL",
            cp6 = self.cp6 if self.cp6 is not None else "NULL",
            cp7 = self.cp7 if self.cp7 is not None else "NULL",
            cp8 = self.cp8 if self.cp8 is not None else "NULL",
            cp9 = self.cp9 if self.cp9 is not None else "NULL",
            cp10 = self.cp10 if self.cp10 is not None else "NULL",
            cp11 = self.cp11 if self.cp11 is not None else "NULL",
            cp12 = self.cp12 if self.cp12 is not None else "NULL",
            cp13 = self.cp13 if self.cp13 is not None else "NULL",
            cp14 = self.cp14 if self.cp14 is not None else "NULL",
            cp15 = self.cp15 if self.cp15 is not None else "NULL",
            cp16 = self.cp16 if self.cp16 is not None else "NULL",
            cp17 = self.cp17 if self.cp17 is not None else "NULL",
            cp18 = self.cp18 if self.cp18 is not None else "NULL",
            cp19 = self.cp19 if self.cp19 is not None else "NULL",
            cp20 = self.cp20 if self.cp20 is not None else "NULL",
            cp21 = self.cp21 if self.cp21 is not None else "NULL",
            cp22 = self.cp22 if self.cp22 is not None else "NULL",
            cp23 = self.cp23 if self.cp23 is not None else "NULL",
            cp24 = self.cp24 if self.cp24 is not None else "NULL",
            cp25 = self.cp25 if self.cp25 is not None else "NULL",
            cp26 = self.cp26 if self.cp26 is not None else "NULL",
            cp27 = self.cp27 if self.cp27 is not None else "NULL",
            cp28 = self.cp28 if self.cp28 is not None else "NULL",
            cp29 = self.cp29 if self.cp29 is not None else "NULL",
            cp30 = self.cp30 if self.cp30 is not None else "NULL",
            cp31 = self.cp31 if self.cp31 is not None else "NULL",
            cp32 = self.cp32 if self.cp32 is not None else "NULL",
            cp33 = self.cp33 if self.cp33 is not None else "NULL",
            cp34 = self.cp34 if self.cp34 is not None else "NULL",
            cp35 = self.cp35 if self.cp35 is not None else "NULL",
            cp36 = self.cp36 if self.cp36 is not None else "NULL",
            cp37 = self.cp37 if self.cp37 is not None else "NULL",
            cp38 = self.cp38 if self.cp38 is not None else "NULL",
            cp39 = self.cp39 if self.cp39 is not None else "NULL",
            cp40 = self.cp40 if self.cp40 is not None else "NULL",
            cp41 = self.cp41 if self.cp41 is not None else "NULL",
            cp42 = self.cp42 if self.cp42 is not None else "NULL",
            cp43 = self.cp43 if self.cp43 is not None else "NULL",
            cp44 = self.cp44 if self.cp44 is not None else "NULL",
            cp45 = self.cp45 if self.cp45 is not None else "NULL",
            cp46 = self.cp46 if self.cp46 is not None else "NULL",
            cp47 = self.cp47 if self.cp47 is not None else "NULL",
            cp48 = self.cp48 if self.cp48 is not None else "NULL",
            cp49 = self.cp49 if self.cp49 is not None else "NULL",
            cp50 = self.cp50 if self.cp50 is not None else "NULL",
            cp51 = self.cp51 if self.cp51 is not None else "NULL",
            cp52 = self.cp52 if self.cp52 is not None else "NULL",
            cp53 = self.cp53 if self.cp53 is not None else "NULL",
            cp54 = self.cp54 if self.cp54 is not None else "NULL",
            cp55 = self.cp55 if self.cp55 is not None else "NULL",
            cp56 = self.cp56 if self.cp56 is not None else "NULL",
            cp57 = self.cp57 if self.cp57 is not None else "NULL",
            cp58 = self.cp58 if self.cp58 is not None else "NULL",
            cp59 = self.cp59 if self.cp59 is not None else "NULL",
            cp60 = self.cp60 if self.cp60 is not None else "NULL",
            cp61 = self.cp61 if self.cp61 is not None else "NULL",
            cp62 = self.cp62 if self.cp62 is not None else "NULL",
            cp63 = self.cp63 if self.cp63 is not None else "NULL",
            cp64 = self.cp64 if self.cp64 is not None else "NULL",
            cp65 = self.cp65 if self.cp65 is not None else "NULL",
            cp66 = self.cp66 if self.cp66 is not None else "NULL",
            cp67 = self.cp67 if self.cp67 is not None else "NULL",
            cp68 = self.cp68 if self.cp68 is not None else "NULL",
            cp69 = self.cp69 if self.cp69 is not None else "NULL",
            cp70 = self.cp70 if self.cp70 is not None else "NULL",
            cp71 = self.cp71 if self.cp71 is not None else "NULL",
            cp72 = self.cp72 if self.cp72 is not None else "NULL",
            cp73 = self.cp73 if self.cp73 is not None else "NULL",
            cp74 = self.cp74 if self.cp74 is not None else "NULL",
            cp75 = self.cp75 if self.cp75 is not None else "NULL",
            cp76 = self.cp76 if self.cp76 is not None else "NULL",
            cp77 = self.cp77 if self.cp77 is not None else "NULL",
            cp78 = self.cp78 if self.cp78 is not None else "NULL",
            cp79 = self.cp79 if self.cp79 is not None else "NULL",
            cp80 = self.cp80 if self.cp80 is not None else "NULL",
            cp81 = self.cp81 if self.cp81 is not None else "NULL",
            cp82 = self.cp82 if self.cp82 is not None else "NULL",
            cp83 = self.cp83 if self.cp83 is not None else "NULL",
            cp84 = self.cp84 if self.cp84 is not None else "NULL",
            cp85 = self.cp85 if self.cp85 is not None else "NULL",
            cp86 = self.cp86 if self.cp86 is not None else "NULL",
            cp87 = self.cp87 if self.cp87 is not None else "NULL",
            cp88 = self.cp88 if self.cp88 is not None else "NULL",
            cp89 = self.cp89 if self.cp89 is not None else "NULL",
            cp90 = self.cp90 if self.cp90 is not None else "NULL",
            cp91 = self.cp91 if self.cp91 is not None else "NULL",
            cp92 = self.cp92 if self.cp92 is not None else "NULL",
            cp93 = self.cp93 if self.cp93 is not None else "NULL",
            cp94 = self.cp94 if self.cp94 is not None else "NULL",
            cp95 = self.cp95 if self.cp95 is not None else "NULL",
            cp96 = self.cp96 if self.cp96 is not None else "NULL",
            battery_12v_amp = self.battery_12v_amp if self.battery_12v_amp is not None else "NULL",
            vehicle_identification_number = self.vehicle_identification_number,
            hx = self.hx if self.hx is not None else "NULL",
            battery_12v_volts = self.battery_12v_volts if self.battery_12v_volts is not None else "NULL",
            odo_km = self.odo_km if self.odo_km is not None else "NULL",
            qc = self.qc if self.qc is not None else "NULL",
            l1_l2 = self.l1_l2 if self.l1_l2 is not None else "NULL",
            tp_fl = self.tp_fl if self.tp_fl is not None else "NULL",
            tp_fr = self.tp_fr if self.tp_fr is not None else "NULL",
            tp_rr = self.tp_rr if self.tp_rr is not None else "NULL",
            tp_rl = self.tp_rl if self.tp_rl is not None else "NULL",
            ambient = self.ambient if self.ambient is not None else "NULL",
            soh = self.soh if self.soh is not None else "NULL",
            regen_wh = self.regen_wh if self.regen_wh is not None else "NULL",
            b_level = self.b_level if self.b_level is not None else "NULL",
            epoch_time = self.epoch_time if self.epoch_time is not None else "NULL",
            motor_power_w = self.motor_power_w if self.motor_power_w is not None else "NULL",
            aux_power_100w = self.aux_power_100w if self.aux_power_100w is not None else "NULL",
            ac_power_250w = self.ac_power_250w if self.ac_power_250w is not None else "NULL",
            ac_comp_100kpa = self.ac_comp_100kpa if self.ac_comp_100kpa is not None else "NULL",
            estimated_ac_power_50w = self.estimated_ac_power_50w if self.estimated_ac_power_50w is not None else "NULL",
            estimated_heater_power_250w = self.estimated_heater_power_250w if self.estimated_heater_power_250w is not None else "NULL",
            plug_state = self.plug_state if self.plug_state is not None else "NULL",
            charge_mode = self.charge_mode if self.charge_mode is not None else "NULL",
            obc_out_power = self.obc_out_power if self.obc_out_power is not None else "NULL",
            gear = self.gear if self.gear is not None else "NULL",
            h_volt_1 = self.h_volt_1 if self.h_volt_1 is not None else "NULL",
            h_volt_2 = self.h_volt_2 if self.h_volt_2 is not None else "NULL",
            gps_status = self.gps_status if self.gps_status is not None else "NULL",
            sw_power = self.sw_power if self.sw_power is not None else "NULL",
            bms = self.bms if self.bms is not None else "NULL",
            obc = self.obc if self.obc is not None else "NULL",
            debug = self.debug if self.debug is not None else "NULL",
            motor_temperature_added_40c = self.motor_temperature_added_40c if self.motor_temperature_added_40c is not None else "NULL",
            inverter_2_temperature_added_40c = self.inverter_2_temperature_added_40c if self.inverter_2_temperature_added_40c is not None else "NULL",
            inverter_4_temperature_added_40c = self.inverter_4_temperature_added_40c if self.inverter_4_temperature_added_40c is not None else "NULL",
            speed_1 = self.speed_1 if self.speed_1 is not None else "NULL",
            speed_2 = self.speed_2 if self.speed_2 is not None else "NULL",
            wiper_status = self.wiper_status if self.wiper_status is not None else "NULL",
            torque_nm = self.torque_nm if self.torque_nm is not None else "NULL"       
        )

        return ret_str

def insert(filename):
    """Leafspy ファイルに含まれるレコードを DB に挿入する関数.  

    Args:
        filename (string): 挿入対象ファイルパス.
    """
    print("try insert {}.".format(filename))
    with open(filename, newline='') as csvfile:
        reader = csv.DictReader(csvfile)

        with pyodbc.connect(config) as connection:
            with connection.cursor() as cursor:
                
                for row in reader:
                    leafspy_record = LeafspyRecord(row)                    
                    stmt = """
                        INSERT INTO {} {} 
                        VALUES {}
                        """.format(Insert_Table,LeafspyRecord.column_list(), str(leafspy_record))

                    try:
                        cursor.execute(stmt)
                        cursor.commit()
                    except pyodbc.IntegrityError as err:
                        # 主キー違反の場合には読み飛ばす
                        continue
                    except Exception as e:
                        print("---")
                        print(e)
                        print("HINT: トリップの最初のレコードだけエラーが出る様子.")
                        print("Excecuted SQL below.")
                        print(stmt)
                        print("---")

def search_filelist(StartDate, EndDate):
    """toinsert/ ディレクトリに含まれるファイルリストを返す関数.

    Returns:
        [string]: toinsert/ ディレクトリに含まれるファイルパスのリスト.
    TODO:
        Leafspy ファイル以外のファイルを弾く処理.  
    """
    p = Path(path.join(path.dirname(__file__), LEAFSPY_ToinsertPath))

    filenames = []

    for file in p.iterdir():
        if file.is_dir():
            continue

        # pathstrings = str(file).split('/')
        # filename = file.name
        # print(pathstrings)
        if re.match('(Log_).+[0-9]{8}(.csv)', file.name):
            file_date = file.name.split('-')[-1]
            file_date_1 = file_date.split('_')[-1]

            file_date_2 = int(file_date_1.split('.')[0])
            #print(file_date_2)

            if (StartDate < file_date_2) & (EndDate > file_date_2):
                filenames.append(LEAFSPY_ToinsertPath+'\\{}'.format(file.name))
                print('対象ファイル： '+file.name+'  OK')
            #else :
                #print('no')

        # filenames.append(file.name)
    return filenames

def move_file(filename):
    shutil.move(filename, LEAFSPY_InsertedPath)

def copy_file(filename):
    shutil.copy2(filename, LEAFSPY_InsertedPath)

if __name__ == "__main__":

    for filename in search_filelist(StartDate_result, EndDate_result):
        insert(filename)
        copy_file(filename)
        

