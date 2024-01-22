import sys
import pyodbc
from .DatabaseAccess_config import driver, server, database, uid, pwd, trusted_connection, PLACE_TABLE

#PLACEテーブルのT
'''
ynuStartLatitude = 35.47
ynuEndLatitude = 35.476
ynuStartLongitude = 139.58
ynuEndLongitude = 139.6

homeStartLatitude = 35.43
homeEndLatitude = 35.435
homeStartLongitude = 139.404
homeEndLongitude = 139.42
'''
def select_execute(con, sql):
    cursor = con.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    columnNames = [column[0] for column in cursor.description]
    cursor.close()
    return rows, columnNames

#https://www.codegrepper.com/code-examples/python/pyodbc.row+to+dictionary  sql -> dict変換の参考に

def get_place_dict(place_id):
    connect= pyodbc.connect('DRIVER='+driver+';SERVER='+server+';UID='+uid+';PED='+pwd+';DATABASE='+database+';Trusted_Connection='+trusted_connection+';')

    sql = (
        'SELECT PLACE_ID, \
        PLACE_NAME, \
        START_LATITUDE, \
        END_LATITUDE, \
        START_LONGITUDE, \
        END_LONGITUDE, \
        PROPERTY \
        FROM ' + PLACE_TABLE
        + 'WHERE PLACE_ID = ' + str(place_id)
    )
    #print('GET PLACE INFO')
    #print(sql)
    rows, column_Names = select_execute(connect, sql)
    connect.close()
    return rows, column_Names

def get_place(Driver_id):
        
    dict_place = {}
    column_Names = ['PLACE_ID', 'PLACE_NAME', 'START_LATITUDE', 'END_LATITUDE', 'START_LONGITUDE', 'END_LONGITUDE', 'PROPERTY']

    out_rows, column_Names = get_place_dict(get_out_place_id(Driver_id))
    home_rows, column_Names = get_place_dict(get_home_place_id(Driver_id))

    if len(out_rows) == 0 or len(home_rows) == 0:
        print('DRIVER_IDにhomeward地点またはoutward地点が登録されていません')
        sys.exit()
    elif len(out_rows) == 1:
        dict_place.update( dict( zip( column_Names , out_rows[0] ) ) )
        outStartLatitude = dict_place['START_LATITUDE']
        outEndLatitude = dict_place['END_LATITUDE']
        outStartLongitude = dict_place['START_LONGITUDE']
        outEndLongitude = dict_place['END_LONGITUDE']
    else:
        print('PLACE_ID重複エラー？？')
    
    if len(home_rows) == 1:
        #print(home_rows[0])
        #print(type(home_rows[0]))
        dict_place.update( dict( zip( column_Names , home_rows[0] ) ) )
        homeStartLatitude = dict_place['START_LATITUDE']
        homeEndLatitude = dict_place['END_LATITUDE']
        homeStartLongitude = dict_place['START_LONGITUDE']
        homeEndLongitude = dict_place['END_LONGITUDE']
    elif len(home_rows) != 0:
        print('PLACE_ID重複エラー？？')
        
    return outStartLatitude, outEndLatitude, outStartLongitude, outEndLongitude, homeStartLatitude, homeEndLatitude, homeStartLongitude, homeEndLongitude

def get_home_place_id(Driver_id):
    if Driver_id == 1:
        return 2
    elif Driver_id == 4:
        return 3
    elif Driver_id == 9:
        return 5
    elif Driver_id == 17:
        return 7
    elif Driver_id == 28:
        return 29
    else:
        return 1

def get_out_place_id(Driver_id):
    if Driver_id == 1:
        return 1
    elif Driver_id == 17:
        return 1
    elif Driver_id == 28:
        return 1
    else:
        return 1


def get_MM_SL(Driver_id):
    if Driver_id == 1:
        return '220, 224, 221, 225, 469'
    elif Driver_id == 17:
        return '328, 329, 330, 331'
    elif Driver_id == 28:
        return '365, 366, 369, 370'
    elif Driver_id == 0:    #highwayのSL
        return '183, 184'
    else:
        #その他ドライバーの場合はMMしない
        return '9999'


