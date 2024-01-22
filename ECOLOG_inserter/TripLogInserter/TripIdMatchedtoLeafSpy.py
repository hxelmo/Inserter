# LeafSpyレコードにTripIdを対応付けるプログラム
# ECOLOGのJSTとCarIdをキーに、ECOLOGから対応TripIdを探す

from .DatabaseAccess_config import driver, server, database, trusted_connection
import pyodbc
import pandas as pd

def TripIdMatchedtoLeafSpy(LS_df,ECOLOG_df):
    # [ECOLOGDBver3].[dbo].[ELECTRIC_VEHICLES]テーブルを用いて、LeafSpyのVEHICLE_IDENTIFICATION_NUMBERとCAR_IDを紐づけ
    user = ""
    password = ""

    #接続文字列の組み立て
    connection = "DRIVER={SQL Server};SERVER=" + driver + \
         ";uid=" + user + \
         ";pwd=" + password + \
         ";DATABASE=" + database
    #データベースへ接続
    con = pyodbc.connect(connection)
    query = '''SELECT [CAR_ID],[VEHICLE_IDENTIFICATION_NUMBER] FROM [ECOLOGDBver3].[dbo].[ELECTRIC_VEHICLES]'''
    car_data = pd.read_sql(query, con)

    # leafspyのカラムリストを作っておく
    list_LS_columns = ['TRIP_ID'] + LS_df.columns.tolist()

    # leafspyとELECTRIC_VEHICLESテーブルをjoin
    LS_car_df = pd.merge(LS_df, car_data, on='VEHICLE_IDENTIFICATION_NUMBER', how='left')
    # ECOLOGのカラムをJST, CAR_ID, TRIP_IDのみにする
    ECOLOG_df = ECOLOG_df[['TRIP_ID','CAR_ID','JST']]
    # leafspyとecologをjoin
    LS_ECOLOG_df = pd.merge(LS_car_df, ECOLOG_df, on=['JST','CAR_ID'], how = 'left')
    # 不必要なカラムを削除して、インサート用データのdataframeを作成
    LS_insert_data = LS_ECOLOG_df.drop('CAR_ID', axis=1)
    # カラムの順番を整理（TRIP_IDが先頭に来るように）
    LS_insert_data = LS_insert_data.reindex(columns=list_LS_columns)
    # TRIP_IDがnanの時に前のレコードの値で埋める（leafspyよりも先にdrivingloggerの動作が終わった時）
    LS_insert_data = LS_insert_data.fillna(method='ffill')

    return LS_insert_data
