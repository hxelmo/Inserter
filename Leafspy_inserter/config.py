import pandas as pd
import datetime
import numpy as np

SERVER = "ecologdb2020"
DATABASE = "ECOLOGDBver3"
Insert_Table = 'LEAFSPY_RAW_PROJECTED_ALL'
EndDate = datetime.date.today()
EndDate_result = int(EndDate.strftime('%Y%m%d'))

StartDate = EndDate - pd.Timedelta('14 days') #デフォルトは今日から一週間前からを設定している
StartDate_result = int(StartDate.strftime('%Y%m%d'))
# StartDate_result = 20230617 #期間を指定　先ずは上一行のコードを先頭に＃して無効化してから、この行の先頭の＃を削除する、そしてint型の数字を入力する：2022年㋃15日を指定したい場合：20220415
ToinsertPath = '\\\\********************'
InsertedPath = '\\\\********************'