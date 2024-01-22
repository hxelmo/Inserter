# import module

import FileSearcher as fs
from TripLogInserter import TripLogInserter as tli
from TripLogInserter.DatabaseAccess_config import LOG_PATH
from TripLogInserter import InsertDataFunction as idf
#from TripLogInserter import SetTripId as sti
#from TripLogInserter import TripIdMatchedtoLeafSpy as tl_ls

import sys
import datetime
import os
import psutil
import time
from Manager_config import Driver_id, list_Car_id, list_Sensor_id
from Manager_config import EndDate_result, StartDate_result


# https://www.lifewithpython.com/2021/01/python-main-function.html
def main():
    try:
        argvs = sys.argv
        MYLOG_Path = argvs[1]
    except Exception as e:
    ## ログファイルがなければ新規作成
        MYLOG_Path = LOG_PATH
        print('LOG_PATH:' + MYLOG_Path)
        with open(MYLOG_Path, "a") as f:
            f.write("\nWARNING: cannot import bat variables")
            f.write("\n    TRACEBACK: {}".format(e))

    writeLog(MYLOG_Path, "\n\n\n----------------------------------------------------------------\n" + str(datetime.datetime.now()) + "\nEXECUTE: SensorLogInserterManager.py")
    writeLog(MYLOG_Path, "\nTARGET DATE:{}-{}".format(StartDate_result, EndDate_result))
    writeLog(MYLOG_Path, "\nTARGET CARS:{}".format(list_Car_id))
    writeLog(MYLOG_Path, "\nTARGET SENSORS:{}".format(list_Sensor_id))
    for Car_id in list_Car_id:
        writeLog(MYLOG_Path, "\nSTART INSERT Car_id:{} DATA".format(Car_id))

        for Sensor_id in list_Sensor_id:
            writeLog(MYLOG_Path, "\nSTART INSERT Sensor_id:{} DATA".format(Sensor_id))

            ## FileSearcher
            writeLog(MYLOG_Path, "\nEXECUTE: FileSearcher.py Sensor_id:{}".format(Sensor_id))
            try:
                df_trip_list, GPS_File_Path = fs.all_func(Driver_id, Car_id, Sensor_id)
                writeLog(MYLOG_Path, "\nDONE: FileSearcher.py Sensor_id:{}\n".format(Sensor_id))
            except Exception as e:            
                writeLog(MYLOG_Path, "\nERROR: SensorLogInserterManager.py at call FileSearcher.py")
                writeLog(MYLOG_Path, "\n    TRACEBACK: {}".format(e))
                sys.exit()
            #print(df_trip_list)
            

            ## TripLogInserter
            writeLog(MYLOG_Path, "\nEXECUTE: TripLogInserter.py Sensor_id:{}".format(Sensor_id))
            #print('GPS_File_Path:', GPS_File_Path)
            try:
                tli.all_func( df_trip_list, MYLOG_Path, GPS_File_Path)
                writeLog(MYLOG_Path, "\nDONE: TripLogInserter.py\n")
            
            except Exception as e:
                writeLog(MYLOG_Path, "\nERROR: SensorLogInserterManager.py at call TripLogInserter.py")
                writeLog(MYLOG_Path, "\n    TRACEBACK: {}".format(e))
                sys.exit()

            
            ## 道路セグメント対応付け（これは処理に時間がかかるので、一旦後回し）

            writeLog(MYLOG_Path, "\nFINISH INSERT Sensor_id:{} DATA".format(Sensor_id))
        writeLog(MYLOG_Path, "\nFINISH INSERT Car_id:{} DATA".format(Car_id))

    writeLog(MYLOG_Path, "\nDONE: SensorLogInserterManager.py Car_id:{}  Sensor_id:{}\n".format(list_Car_id, list_Sensor_id))

def writeLog(MYLOG_Path, str_log):
    print(str_log)
    f = open(MYLOG_Path, mode='a')
    f.write(str_log)
    f.close()

            
if __name__ == "__main__":
    main()