import os
import sys
import pyodbc

import pandas as pd
import datetime
import numpy as np

Insert_Table = '**************'
EndDate = datetime.date.today()
EndDate_result = int(EndDate.strftime('%Y%m%d'))

StartDate = EndDate - pd.Timedelta('7 days') #デフォルトは今日から一週間前からを設定している
StartDate_result = int(StartDate.strftime('%Y%m%d'))
StartDate_result = 20200729 #期間を指定　先ずは上一行のコードを先頭に＃して無効化してから、この行の先頭の＃を削除する、そしてint型の数字を入力する：2022年㋃15日を指定したい場合：20220415

# EndDate_result = 20231014
# StartDate_result = 20230927
#ファイルパス指定は、
ITSSERVER_File_Path = '\\\\**************'

Driver_id = 1
list_Car_id = [22] #LEAF2020
list_Sensor_id = [39] #Xperia1 3
#list_Sensor_id = [33]
#list_Sensor_id = [37]``
judge_same_trip_seconds = 3600
