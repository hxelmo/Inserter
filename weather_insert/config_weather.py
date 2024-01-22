import pandas as pd
import datetime as dt
import numpy as np

SERVER = "ecologdb2020"
DATABASE = "ECOLOGDBver3"
Insert_Table = 'WEATHER'
EndDate = datetime.date.today()
EndDate_result = int(EndDate.strftime('%Y%m%d'))

StartDate = EndDate - pd.Timedelta('7 days') #デフォルトは今日から一週間前からを設定している
StartDate_result = int(StartDate.strftime('%Y%m%d'))

# start_year = 2023
# start_month = 11
# start_day = 8
# end_year = 2023
# end_month = 11
# end_day = 13

# start = dt.date(start_year, start_month, start_day)
# end = dt.date(end_year, end_month, end_day)
# StartDate_result = int(start.strftime('%Y%m%d'))
# EndDate_result = int(end.strftime('%Y%m%d'))

