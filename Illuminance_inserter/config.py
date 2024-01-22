import datetime
import pandas as pd

SERVER = "ecologdb2020"
DATABASE = "ECOLOGDBver3"
TABLE_NAME = "ILLUMINANCE_RAW_ver2"

EndDate0 = datetime.date.today()
EndDate = int(EndDate0.strftime('%Y%m%d'))

StartDate0 = EndDate0 - pd.Timedelta('7 days') #デフォルトは今日から一週間前からを設定している
StartDate = int(StartDate0.strftime('%Y%m%d'))
# StartDate = 20230721   #期間を直接指定する場合はこちら

#InsertedPathに合わせて適宜変更
driverID = 1
carID = 22
sensorID = 34