"""

照度 データを読み出して, データベースに書き込む.
照度データはITSサーバにある．

"""

from .DatabaseAccess_config import driver, server, database, trusted_connection, ILLUMINANCE_TABLE, ITSServerPath, IlluminanceInsertedPath#, StartDate, EndDate
from .DatabaseAccess_config import DRIVER_TABLE, CAR_TABLE, SENSOR_TABLE
import sys
import csv
import pyodbc
import re
import shutil
import pandas as pd
from pathlib import Path
from os import path
import datetime as dt1
from datetime import datetime as dt2

# DB へのコネクション用文字列.
# git 追跡対象外の config.py に記述した DB サーバー名と DB 名を用いている.  
config = "DRIVER={};SERVER={};DATABASE={}".format(driver, server, database)

class IlluminanceRecord:
    """Illuminance のログ 1 行に対応するクラス.  

    Illuminance に存在するデータ項目名と, そのパース処理を記述している.
    """
    KEY_DRIVER_ID = 'Driver_id'
    KEY_CAR_ID = 'Car_id'
    KEY_SENSOR_ID = 'Sensor_id'
    KEY_DATETIME = 'Date/Time'
    KEY_ILLUMINANCE = 'Illuminance'

    def __init__(self, record_dict, DriverId, CarId, SensorId):
        """
        Illuminance ログ 1 行に対応する辞書オブジェクトを引数にとるコンストラクタ.

        """
        if not isinstance(record_dict, dict):   #rowが辞書形式じゃない場合
            raise Exception("Argument of LeaspyRecord.__init__() must be dictionary.")

        record_keys = record_dict.keys()

        if self.KEY_DATETIME in record_keys:
            self.datetime = dt2.strptime(record_dict[self.KEY_DATETIME].replace("=", '').replace("\"", ''), "%Y-%m-%d %H:%M:%S.%f").strftime("%Y-%m-%d %H:%M:%S")    #strftime()はdatetime->str変換
            #self.datetime = record_dict[self.KEY_DATETIME]
        else:
            raise Exception("Key \"{}\" must be required in dictionary.".format(self.KEY_DATETIME))

        if self.KEY_ILLUMINANCE in record_keys:
            self.illuminance = record_dict[self.KEY_ILLUMINANCE]
        else:
            self.illuminance = None
        self.driver_id = DriverId
        self.car_id = CarId
        self.sensor_id = SensorId


    @classmethod
    def column_list(cls):
        """ILLUMINANCE_RAW_ver2 と対応するカラムリストを生成する関数.
        """
        return "(DRIVER_ID, CAR_ID, SENSOR_ID, DATETIME, ILLUMINANCE)"

    def __str__(self):
        """他言語における ToString() メソッド.  

        ILLUMINANCE_RAW_ver2 テーブルにデータ挿入する形式で文字列を出力する.  
        """
        ret_str = "('{driver_id}','{car_id}','{sensor_id}','{datetime}', {illuminance})".format(
            datetime = self.datetime,
            illuminance = self.illuminance,# if self.illuminance is not None else "NULL",
            driver_id = self.driver_id,
            car_id = self.car_id,
            sensor_id = self.sensor_id,
        )

        return ret_str

def insert(filename, DriverId, CarId, SensorId):
    """Illuminance ファイルに含まれるレコードを DB に挿入する関数.  

    Args:
        filename (string): 挿入対象ファイルパス.
    """
    print("--- try insert {} ---.".format(filename))
    #print(IlluminanceRecord.column_list())
    with open(filename, newline='') as csvfile:
        #reader = csv.DictReader(csvfile)
        csv_header = ['Date/Time', 'Illuminance']
        #reader = pd.read_csv(csvfile, names=['Date/Time', 'Illuminance'])

        with pyodbc.connect(config) as connection:
            with connection.cursor() as cursor:
                last_time = dt1.datetime(year=1970, month=10, day=10, hour=10)
                last_illuminance = 0
                for row in csv.DictReader(csvfile, csv_header):  #rowは辞書(dict)形式
                    #print('row2:' + str(row))
                    insert_bool = 0
                    illuminance_record = IlluminanceRecord(row, DriverId, CarId, SensorId) #これをカンマで分割して値をとろう
                    illuminance_record_split = str(illuminance_record).split(',')
                    #print('  -> 対象レコード：', str(illuminance_record)) 
                    #レコードが飛んだ時の処理
                    datetime_jst = dt2.strptime(row['Date/Time'].replace("=", '').replace("\"", ''), "%Y-%m-%d %H:%M:%S.%f")#.datetime("%Y-%m-%d %H:%M:%S")
                    #print(datetime_jst)
                    seconds_diff = (datetime_jst - last_time).seconds
                    #print(seconds_diff)
                    if seconds_diff == 1 or last_time.year == 1970:
                        #print('☆レコード挿入：', str(row['Date/Time']))
                        insert_bool = 1
                    elif seconds_diff == 0:
                        insert_bool = 0
                        #print('☆重複レコードのため挿入なし')
                    elif seconds_diff > 1 and last_time.year > 1970:
                        #print('☆レコード挿入（レコード抜けあり）', str(row['Date/Time']))
                        insert_bool = 1
                    else:
                        print('☆この処理が出るのはおかしいから今すぐ見直せ')

                    stmt = """
                        INSERT INTO {} {} 
                        VALUES {}
                        """.format(ILLUMINANCE_TABLE, IlluminanceRecord.column_list(), str(illuminance_record))
                    
                    last_time = dt2.strptime(illuminance_record_split[3].replace('\'',''), "%Y-%m-%d %H:%M:%S")
                    last_illuminance = illuminance_record_split[4].replace('\'','')
                    if insert_bool == 1:
                        #print('  insert:', str(illuminance_record))
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
                    

def search_filelist(DriverId, CarId, SensorId, StartDate):
    """toinsert/ ディレクトリに含まれるファイルリストを返す関数.

    Returns:
        [string]: toinsert/ ディレクトリに含まれるファイルパスのリスト.
    TODO:
        Illuminance ファイル以外のファイルを弾く処理.  
    """
    EndDate = StartDate + pd.Timedelta('1 days')
    driver_path = get_pathname('PATH_NAME', 'DRIVER_ID', DRIVER_TABLE, DriverId)
    car_path = get_pathname('PATH_NAME', 'CAR_ID', CAR_TABLE, CarId)
    sensor_path = get_pathname('PATH_NAME', 'SENSOR_ID', SENSOR_TABLE, SensorId)
    ToinsertPath = ITSServerPath + '//' + driver_path + '//' + car_path + '//' + sensor_path

    p = Path(ToinsertPath)

    filenames = []
    for file in p.iterdir():
        #print(file.name)
        if file.is_dir():
            continue
        #if re.match('[1970-2030][0-9]{4}[0-9]{6}(UnsentIlluminance.csv)', file.name):
        if re.match('[0-9]{14}(UnsentIlluminance.csv)', file.name): #日射量データのみ処理
            file_datetime = file.name.split('Unsent')[0]
            file_date = int(file_datetime[0:8])
            #print(file_date)

            if (StartDate < file_date) & (EndDate + 1 > file_date):
                filenames.append(ToinsertPath+'\\{}'.format(file.name))
                print('Illuminance 対象ファイル： '+file.name+'  OK')

    return filenames

def get_pathname(TARGET_COLUMN_NAME, ID_COLUMN_NAME, TABLE_NAME, ID):

    connect= pyodbc.connect('DRIVER='+driver+';SERVER='+server+';DATABASE='+database+';Trusted_Connection='+trusted_connection+';')
    cursor = connect.cursor()
    sql = """SELECT {} FROM {} {} = {}""".format(TARGET_COLUMN_NAME, TABLE_NAME, ID_COLUMN_NAME, ID)
    cursor.execute(sql)
    rows = cursor.fetchall()
    if len(rows) > 0:
        pathname = str(rows[0]).replace(', )', '').replace('(', '').replace('\'', '')
        #print('pathname:', pathname)
    else:
        print('---パス取得エラー---')
    return pathname

#def move_file(filename):
#    shutil.move(filename, '.\\inserted\\')

def copy_file(filename):
    shutil.copy2(filename, IlluminanceInsertedPath)

def all_func(DriverId, CarId, SensorId, StartDate):
    for filename in search_filelist(DriverId, CarId, SensorId, StartDate):
        insert(filename, DriverId, CarId, SensorId)
        copy_file(filename)

if __name__ == "__main__":

    for filename in search_filelist():
        insert(filename)
        copy_file(filename)
        

