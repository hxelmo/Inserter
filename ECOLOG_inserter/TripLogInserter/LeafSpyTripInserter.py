# TODO：
## TripIdの割り当て→DopplerNotMMとLinksLookUp2用それぞれ用を作る（テーブルをそれぞれ作成）
## LEAFSPY_TRIP_RECORD_Doppler_NotMMテーブルとLEAFSPY_TRIP_RECORD_LNKS_LOOKUP2テーブル


# LeafSpyのデータをDBにインサートするプログラム
from .DatabaseAccess_config import driver, server, database, LEAFSPY_ALL_TABLE, LEAFSPY_TRIP_RECORD, DRIVER1_VEHICLE_IDENTIFICATION_NUMBER#, trusted_connection, ToinsertPath, InsertedPath
from . import InsertDataFunction
import pyodbc
import pandas as pd
from .InsertDataFunction import insert_data


# DB へのコネクション用文字列.
connection = "DRIVER={};SERVER={};DATABASE={}".format(driver, server, database)

# insert先テーブル名定義
def table_name_LEAFSPY_TRIP_RECORD():
    return LEAFSPY_TRIP_RECORD

# LEAFSPY_TRIP_RECORDテーブルのカラム名定義
def column_list_LEAFSPY_TRIP_RECORD():
    return "(TRIP_ID, JST, LATITUDE, LONGITUDE, ELEVATION, SPEED, GIDS, SOC, AHR, PACK_VOLTS, PACK_AMP, MAX_CP_MV, MIN_CP_MV, AVG_CP_MV, CP_MV_DIFFERENCE, JUDGEMENT_VALUE, PACK_T1_F, PACK_T1_C, PACK_T2_F, PACK_T2_C, PACK_T3_F, PACK_T3_C, PACK_T4_F, PACK_T4_C, CP1, CP2, CP3, CP4, CP5, CP6, CP7, CP8, CP9, CP10, CP11, CP12, CP13, CP14, CP15, CP16, CP17, CP18, CP19, CP20, CP21, CP22, CP23, CP24, CP25, CP26, CP27, CP28, CP29, CP30, CP31, CP32, CP33, CP34, CP35, CP36, CP37, CP38, CP39, CP40, CP41, CP42, CP43, CP44, CP45, CP46, CP47, CP48, CP49, CP50, CP51, CP52, CP53, CP54, CP55, CP56, CP57, CP58, CP59, CP60, CP61, CP62, CP63, CP64, CP65, CP66, CP67, CP68, CP69, CP70, CP71, CP72, CP73, CP74, CP75, CP76, CP77, CP78, CP79, CP80, CP81, CP82, CP83, CP84, CP85, CP86, CP87, CP88, CP89, CP90, CP91, CP92, CP93, CP94, CP95, CP96, BATTERY_12V_AMP, VEHICLE_IDENTIFICATION_NUMBER, HX, BATTERY_12V_VOLTS, ODO_KM, QC, L1_L2, TP_FL, TP_FR, TP_RR, TP_RL, AMBIENT, SOH, REGEN_WH, B_LEVEL, EPOCH_TIME, MOTOR_POWER_W, AUX_POWER_100W, AC_POWER_250W, AC_COMP_100KPA, ESTIMATED_AC_POWER_50W, ESTIMATED_HEATER_POWER_250W, PLUG_STATE, CHARGE_MODE, OBC_OUT_POWER, GEAR, H_VOLT_1, H_VOLT_2, GPS_STATUS, SW_POWER, BMS, OBC, DEBUG, MOTOR_TEMPERATURE_ADDED_40C, INVERTER_2_TEMPERATURE_ADDED_40C, INVERTER_4_TEMPERATURE_ADDED_40C, SPEED_1, SPEED_2, WIPER_STATUS, TORQUE_NM)"

# LEAFSPY_RAW_PROJECTED_ALLテーブルから、対象期間のレコードとtripidを取得して、LEAFSPY_TRIP_RECORDインサート用データを作成
def insertLeafSpyRawData(ECOLOG_df):
    # 対象期間を探す
    starttime = ECOLOG_df['JST'].min()
    endtime = ECOLOG_df['JST'].max()

    # LEAFSPY_RAW_PROJCTED_ALLテーブルから対象期間のデータを取ってくる
    con = pyodbc.connect(connection)
    query = '''SELECT * 
    FROM {} 
    WHERE JST BETWEEN '{}' AND '{}' AND VEHICLE_IDENTIFICATION_NUMBER = '{}'
    '''.format(LEAFSPY_ALL_TABLE, starttime, endtime, DRIVER1_VEHICLE_IDENTIFICATION_NUMBER)
    #print(query)
    LeafSpy_raw_df = pd.read_sql(query, con)
    
    # TripIdを取得してLeafSpyのカラムに追加
    TripId = ECOLOG_df['TRIP_ID'].min()
    LeafSpy_raw_df['TRIP_ID'] = TripId
    # カラムリスト作成（TRIP_IDカラムを先頭に移動させるため）
    LeafSpy_trip_columns_list = column_list_LEAFSPY_TRIP_RECORD().strip('()').split(', ')
    # カラムの順番を変更
    LeafSpy_trip_df = LeafSpy_raw_df.reindex(columns=LeafSpy_trip_columns_list)
    LeafSpy_trip_df['JST'] = LeafSpy_trip_df['JST'].astype(str)
    print(LeafSpy_trip_df)
    NULL = "NULL"
    LeafSpy_trip_df = LeafSpy_trip_df.fillna(NULL)

    print(LeafSpy_trip_df)
    insert_data(table_name_LEAFSPY_TRIP_RECORD(), column_list_LEAFSPY_TRIP_RECORD(), LeafSpy_trip_df)

    #return LeafSpy_trip_df


def all_func(ECOLOG_df):

    # Doppler_NotMMのTRIP_IDと対応付けて、insertまで
    # ECOLOG_Doppler_NotMM_dfに、インサートしたECOLOG_Doppler_NotMMのデータを入れる（dataframe）
    insertLeafSpyRawData(ECOLOG_df)
    #InsertDataFunction.insert_data(table_name_LEAFSPY_TRIP_RECORD() , column_list_LEAFSPY_TRIP_RECORD() , LeafSpy_trip_Doppler_NotMM_df)

    #ECOLOG_df = pd.read_csv('C:\prog\leafspyinserter/ecolog_data.csv')
    #LS_df = pd.read_csv('C:\prog\leafspyinserter/ls_data.csv')
    #TripId = getLeafSpyRawData(ECOLOG_df)
    #LS_insert_data = TripIdMatchedtoLeafSpy(LS_df,ECOLOG_df)
    #print(LS_insert_data)
    #LS_insert_data.to_csv('C:\prog\leafspyinserter/ls.csv')

if __name__ == "__main__":
    print('このプログラムの単体動作は想定してません')
    all_func(ECOLOG_df)