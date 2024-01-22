import os
import sys
import pyodbc
from .DatabaseAccess_config import driver, server, database, uid, pwd, trusted_connection, CAR_TABLE

minTorque = 76  #EfficiencyMaxTableNameテーブルのTorqueカラムの最小値
maxTorque = 280 #EfficiencyMaxTableNameテーブルのTorqueカラムの最大値
maxRev = 9990   #EfficiencyMaxTableNameテーブルのRevカラムの最大値

def select_execute(con, sql):
    cursor = con.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    columnNames = [column[0] for column in cursor.description]
    cursor.close()
    return rows, columnNames

#https://www.codegrepper.com/code-examples/python/pyodbc.row+to+dictionary  sql -> dict変換の参考に
class CarModel():
    def __init__(self, car_id):
        connect= pyodbc.connect('DRIVER='+driver+';SERVER='+server+';UID='+uid+';PED='+pwd+';DATABASE='+database+';Trusted_Connection='+trusted_connection+';')

        sql = (
            'SELECT CAR_ID, \
            MODEL, \
            WEIGHT, \
            TIRE_RADIUS, \
            REDUCTION_RATIO, \
            CD_VALUE, \
            FRONTAL_PROJECTED_AREA \
            FROM ' + CAR_TABLE
            + 'WHERE CAR_ID = ' + str(car_id)
        )
        self.rows, self.column_Names = select_execute(connect, sql)
        connect.close()

    def get_car_model(self):
        
        dict_carModel = {}
        column_Names2 = ['CarID', 'Model', 'Weight', 'TireRadius', 'ReductionRatio', 'CdValue', 'FrontalProjectedArea']
        #column_Names2 = ['CarID', 'Model', 'CarWeight', 'TireRadius', 'ReductionRatio', 'CdValue', 'FrontalProjectedArea']

        if len(self.rows) == 0:
            print('CAR_IDが違います')
            sys.exit()
        elif len(self.rows) == 1:
            #print(self.rows[0])
            #print(type(self.rows[0]))
            dict_carModel.update( dict( zip( column_Names2 , self.rows[0] ) ) )
            #dict_carModel['Weight'] = dict_carModel['CarWeight'] + CarOtherWeight
        else:
            print('CarID重複エラー？？')
        return dict_carModel

if __name__ == "__main__":
    print('このプログラムの単体動作は想定してません')

#環境データ
myu = 0.015
rho = 1.22
GravityResistanceCoefficient = 9.80665
windSpeed = 0.0

InverterEfficiency = 0.95
MaxDrivingPower = -30
#MaxDrivingForce = -0.15 * GravityResistanceCoefficient * Weight
def get_max_driving_force(weight):
    return -0.15 * GravityResistanceCoefficient * weight

#車両重量と乗車重量は分けるのが望ましいが、現状のCARテーブルには合計の値が入っている
#修正はしたいがひとまずこのままで
CarOtherWeight = 0

