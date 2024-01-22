from config_weather import SERVER, DATABASE, Insert_Table,downloadPath_name,start_year,start_month,start_day,end_year,end_month,end_day,start,end
import sys
import csv
import pyodbc
import shutil
import re
import pandas as pd
import numpy as np
from pathlib import Path
from os import path
import datetime as dt


# start_year = int(input('start year: '))
# start_month = int(input('start month: '))
# start_day = int(input('start day: '))
# end_year = int(input('end year: '))
# end_month = int(input('end month: '))
# end_day = int(input('end day: '))

# start = dt.date(start_year, start_month, start_day)
# end = dt.date(end_year, end_month, end_day)
d = start
delta = dt.timedelta(days=1)
df = pd.read_html('https://www.data.jma.go.jp/obd/stats/etrn/view/10min_s1.php?prec_no=46&block_no=47670&year='+str(d.year)+'&month='+str(d.month)+'&day='+str(d.day)+'&view=')
df[0].insert(loc=0, column='JST', value=d)
df2 = df[0]
d += delta

while d <= end:
    df = pd.read_html('https://www.data.jma.go.jp/obd/stats/etrn/view/10min_s1.php?prec_no=46&block_no=47670&year='+str(d.year)+'&month='+str(d.month)+'&day='+str(d.day)+'&view=')
    df[0].insert(loc=0, column='JST', value=d)
    df1 = df[0]
    df2 = pd.concat([df2, df1], axis=0, ignore_index=True)
    d += delta


DATETIME_1 = []
PLACE =[]
LAST_10MIN_DATETIME_1 = []
for i in range(len(df2['JST'])):
    PLACE.append(47670)
    if(df2[('時分','時分')][i] != '24:00'):
        DATETIME_1.append(np.array(str(df2['JST'][i]) + ' '+ df2[('時分','時分')][i]+':00'))
        LAST_10MIN_DATETIME_1.append(np.array(str(df2['JST'][i]) + ' '+ df2[('時分','時分')][i]+':00'))
    else:
        DATETIME_1.append(np.array(str(df2['JST'][i]) + ' '+'23:59:00'))
        LAST_10MIN_DATETIME_1.append(np.array(str(df2['JST'][i]+delta) + ' '+'00:00:00'))


BAROMETRIC_VALUE = np.array(df2[('気圧(hPa)','現地')])
ATMOSPHERIC_PRESSURE = np.array(df2[('気圧(hPa)','海面')])
PRECIPITATION = np.array(df2[('降水量(mm)','降水量(mm)')])
TEMPERATURE = np.array(df2[('気温(℃)','気温(℃)')])
HUMIDITY = np.array(df2[('相対湿度(％)','相対湿度(％)')])
WIND_SPEED = np.array(df2[('風向・風速(m/s)','平均')])
WIND_DIRECTION = np.array(df2[('風向・風速(m/s)','風向')])
MAX_WIND_SPEED = np.array(df2[('風向・風速(m/s)','最大瞬間')])
MAX_WIND_DIRECTION = np.array(df2[('風向・風速(m/s)','風向.1')])
SUNLIGHT = np.array(df2[('日照時間(分)','日照時間(分)')])

weather_data = pd.DataFrame()


weather_data.insert(loc=0, column='PLACE', value=PLACE)
weather_data.insert(loc=1, column='DATETIME_1', value=DATETIME_1)
weather_data.insert(loc=2, column='LAST_10MIN_DATETIME_1', value=LAST_10MIN_DATETIME_1)
weather_data.insert(loc=3, column='BAROMETRIC_VALUE', value=BAROMETRIC_VALUE)
weather_data.insert(loc=4, column='ATMOSPHERIC_PRESSURE', value=ATMOSPHERIC_PRESSURE)
weather_data.insert(loc=5, column='PRECIPITATION', value=PRECIPITATION)
weather_data.insert(loc=6, column='TEMPERATURE', value=TEMPERATURE)
weather_data.insert(loc=7, column='HUMIDITY', value=HUMIDITY)
weather_data.insert(loc=8, column='WIND_SPEED', value=WIND_SPEED)
weather_data.insert(loc=9, column='WIND_DIRECTION', value=WIND_DIRECTION)
weather_data.insert(loc=10, column='MAX_WIND_SPEED', value=MAX_WIND_SPEED)
weather_data.insert(loc=11, column='MAX_WIND_DIRECTION', value=MAX_WIND_DIRECTION)
weather_data.insert(loc=12, column='SUNLIGHT', value=SUNLIGHT)


DATETIME = []
LAST_10MIN_DATETIME = []
last_10 = dt.timedelta(minutes=10)

for i in range(len(weather_data)):
    d1 = pd.to_datetime(weather_data.iloc[i]['DATETIME_1'])
    d2 = pd.to_datetime(weather_data.iloc[i]['LAST_10MIN_DATETIME_1'])
    DATETIME.append(d1)
    LAST_10MIN_DATETIME.append(d2 - last_10)

del weather_data['DATETIME_1']
del weather_data['LAST_10MIN_DATETIME_1']
weather_data.insert(loc=1, column='DATETIME', value=DATETIME)
weather_data.insert(loc=2, column='LAST_10MIN_DATETIME', value=LAST_10MIN_DATETIME)


config = "DRIVER={{SQL Server}};SERVER={};DATABASE={}".format(SERVER, DATABASE)



def column_list(cls):
        """WEATHER と対応するカラムリストを生成する関数.
        """
        return "(PLACE, DATETIME, LAST_10MIN_DATETIME, BAROMETRIC_VALUE, ATMOSPHERIC_PRESSURE, PRECIPITATION, TEMPERATURE, HUMIDITY, WIND_SPEED, WIND_DIRECTION, MAX_WIND_SPEED, MAX_WIND_DIRECTION, SUNLIGHT)"

def __str__(self):
        """他言語における ToString() メソッド.  

        WEATHER テーブルにデータ挿入する形式で文字列を出力する.  
        """
        ret_str = "({place}, {datetime}, {last_datetime}, {barometric}, {atm_pressure}, {precipitation}, {temperature}, {humidity}, {wind_speed}, {wind_direction}, {max_wind_speed}, {max_wind_direction}, {sunlight})".format(

            place = self[0] if self[0] is not None else "NULL",
            datetime = self[1] if self[1] is not None else "NULL",
            last_datetime = self[2] if self[2] is not None else "NULL",
            barometric = self[3] if self[3] is not None else "NULL",
            atm_pressure = self[4] if self[4] is not None else "NULL",
            precipitation = self[5] if self[5] is not None else "NULL",
            temperature = self[6] if self[6] is not None else "NULL",
            humidity = self[7] if self[7] is not None else "NULL",
            wind_speed = self[8] if self[8] is not None else "NULL",
            wind_direction = self[9] if self[9] is not None else "NULL",
            max_wind_speed = self[10] if self[10] is not None else "NULL",
            max_wind_direction = self[11] if self[11] is not None else "NULL",
            sunlight = self[12] if self[12] is not None else "NULL",
                
        )

        return ret_str

with pyodbc.connect(config) as connection:
            with connection.cursor() as cursor:
                

                for i in range(len(weather_data)):
                        list1  = []
                        for j in range(len(list(weather_data.columns))):
                            
                            list1.append(weather_data.iloc[i,j])
                            
                        list1[1] = list1[1].strftime('%Y-%m-%d %H:%M:%S')
                        list1[2] = list1[2].strftime('%Y-%m-%d %H:%M:%S')
                        list1[5] = -1
                        list1[-1] = -1
                        stmt = """
                        INSERT INTO {} {} 
                        VALUES {}
                        """.format(Insert_Table,column_list(weather_data),tuple(list1))
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
                            

weather_data.to_csv(downloadPath_name)
