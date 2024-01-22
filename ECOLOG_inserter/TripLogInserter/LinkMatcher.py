import sys
import csv
import pyodbc
import shutil
import re
import pandas as pd
import numpy as np
from pathlib import Path
from os import path
from datetime import datetime
from numpy.linalg import norm

#このプログラムを単体実行する場合はピリオドを抜く
from .DatabaseAccess_config import driver, server, database, trusted_connection, LINKS_TABLE, SEMANTIC_LINKS_TABLE, LINKS_LOOKUP_TABLE
#from MapMatching import get_driver_link
from .Place_config import get_MM_SL
from . import MathUtil as mu

extra_LAT_long = 0.003
SEARCH_HIGHWAY_LINKS_EXTRA = 0.002  #C#のプログラムからそのまま
SEARCH_SEMANTIC_LINKS_EXTRA = 0.0001
SEARCH_LINKS_LOOKUP_EXTRA = 20  #C#のプログラムからそのまま
SEARCH_ALL_LINKS_BASE = 0.00012

#点と線分の距離を求める
#https://qiita.com/deltaMASH/items/e7ffcca78c9b75710d09
def calc_distance_and_neighbor_point(a, b, p):
    ap = p - a
    ab = b - a
    ba = a - b
    bp = p - b
    if np.dot(ap, ab) < 0:
        distance = norm(ap)
        neighbor_point = a
    elif np.dot(bp, ba) < 0:
        distance = norm(p - b)
        neighbor_point = b
    else:
        ai_norm = np.dot(ap, ab)/norm(ab)
        neighbor_point = a + (ab)/norm(ab)*ai_norm
        distance = mu.distance(p[0], p[1], neighbor_point[0], neighbor_point[1])
        distance = norm(p - neighbor_point)
    return neighbor_point, distance

def get_semanticLinkTable(str_semantic_link_ids):
    #print(str_semantic_link_ids)
    select_semantic_link_ids =   'where SEMANTIC_LINK_ID in (' + str_semantic_link_ids.replace('\'', '') + ')'
    if str_semantic_link_ids == '0':
        select_semantic_link_ids = ''
        
    query = '''\
with LINKS_TABLE as (
select *
from {}
)
, SEMANTIC_LINKS_TABLE as (
select *
from {}
)
, DIRECTION as ( 
select LINK_ID, LAT1,LON1,LAT2,LON2, 
	case 
		when ATN2(Y,X) * 180 / PI() >= 0 then ATN2(Y,X) * 180 / PI() 
		when ATN2(Y,X) * 180 / PI() < 0 then ATN2(Y,X) * 180 / PI() + 180 
	end as HEADING,A,B,C 
from ( 
	select *,ROUND((COS(RAD_LAT2) * SIN(RAD_LON2 - RAD_LON1)* 1000000),2) as Y, ROUND((COS(RAD_LAT1) * SIN(RAD_LAT2) - SIN(RAD_LAT1) * COS(RAD_LAT2) * COS(RAD_LON2 - RAD_LON1))*1000000,2) as X, LAT2-LAT1 as A,LON1-LON2 as B,LON2+LAT1-LON1*LAT2 as C  
	from( 
		select node1.LINK_ID, node1.LATITUDE as LAT1, node1. LONGITUDE as LON1, node2.LATITUDE as LAT2, node2.LONGITUDE as LON2, node1.LATITUDE * PI() / 180 as RAD_LAT1, node1.LONGITUDE * PI() / 180 as RAD_LON1, node2.LATITUDE * PI() / 180 as RAD_LAT2, node2.LONGITUDE * PI() / 180 as RAD_LON2 
		from ( 
			select * 
			from LINKS_TABLE 
			where NODE_ID is not null 
			and DIRECTION = 1 
		) as node1,( 
			select * 
			from LINKS_TABLE 
			where NODE_ID is not null 
			and DIRECTION = 2 
		) as node2 
		where node1.LINK_ID = node2.LINK_ID 
	) as rad 
) as XY 
where (X != 0 
or Y != 0) 
) 

select SEMANTIC_LINKS_TABLE.SEMANTIC_LINK_ID,SEMANTIC_LINKS_TABLE.LINK_ID,LINKS_TABLE.LATITUDE,LINKS_TABLE.LONGITUDE,LAT1,LON1,LAT2,LON2,DIRECTION.HEADING,A,B,C 
from SEMANTIC_LINKS_TABLE 
	left join LINKS_TABLE on LINKS_TABLE.LINK_ID = SEMANTIC_LINKS_TABLE.LINK_ID 
	left join DIRECTION on LINKS_TABLE.LINK_ID = DIRECTION.LINK_ID 
--where SEMANTIC_LINK_ID = semanticLinkId
order by SEMANTIC_LINK_ID 
    '''.format(LINKS_TABLE, SEMANTIC_LINKS_TABLE, select_semantic_link_ids)
    #print(query)

    connect= pyodbc.connect('DRIVER='+driver+';SERVER='+server+';DATABASE='+database+';PORT=1433;Trusted_Connection='+trusted_connection+';')
    cursor = connect.cursor()

    dataframe_link = pd.read_sql(query, connect)

    cursor.close()
    connect.close() 
    return dataframe_link

#1レコードごとに最近傍リンクを求める
def searchNearestLink(selected_dataframe_link, gps_LATitude, gps_longitude, gps_heading):
    if gps_heading > 180:
        car_heading = gps_heading - 180
    else:
        car_heading = gps_heading
    tmp_min_dist = 11
    tmp_link_dist = 11
    tmp_min_angle = 90
    ndarray_gps = np.array([gps_LATitude, gps_longitude])
    tmp_nearest_index = 0
    tmp_nearest_linkId = 'RB000000000'
    flag = True
    #neighbor_point = np.array([selected_dataframe_link[0, 'START_LAT'], selected_dataframe_link[0, 'START_LONG']])

    for row in selected_dataframe_link.itertuples():
        ndaary_start_linkpoint = np.array([row.LAT1, row.LON1])
        ndaary_end_linkpoint = np.array([row.LAT2, row.LON2])
        tmp_neighbor_point, dist = calc_distance_and_neighbor_point(ndaary_start_linkpoint, ndaary_end_linkpoint, ndarray_gps)
        #print(str(row[0]) + ':' + str(dist))

        if row.HEADING is not None:
            if tmp_link_dist >= dist:
                angle = abs(car_heading - row.HEADING)
                if angle > 90:
                    angle = 180 - angle
                if dist < 10 and (tmp_min_angle - angle > 3 or \
                        (abs(tmp_min_angle - angle) <= 3) and tmp_link_dist > dist):
                    tmp_link_dist = dist
                    tmp_nearest_index = row[0]
                    tmp_nearest_linkId = row.LINK_ID
                    neighbor_point = tmp_neighbor_point
                    flag = False
                elif (tmp_min_dist >= dist) and flag:   #10m以下のリンクがない時は距離が短いものをマッチング
                    tmp_min_dist = dist
                    tmp_nearest_linkId = row.LINK_ID
                    neighbor_point = tmp_neighbor_point
        else:   #headingが計算できない時は距離が短いものをマッチング
            if(tmp_min_dist >= dist) and flag:
                tmp_min_dist = dist
                tmp_nearest_linkId = row.LINK_ID
                neighbor_point = tmp_neighbor_point
    if flag:
        link_dist = tmp_min_dist
    else:
        link_dist = tmp_link_dist
    
    return tmp_nearest_linkId, neighbor_point, link_dist

#どのSL上の道路リンクにも該当しない場合はこの関数で取得
def get_link_id(gps_LATitude, gps_longitude):
    minLATitude = int(gps_LATitude * 10000 - SEARCH_LINKS_LOOKUP_EXTRA)
    maxLATitude = int(gps_LATitude * 10000 + SEARCH_LINKS_LOOKUP_EXTRA)
    minLongitude = int(gps_longitude * 10000 - SEARCH_LINKS_LOOKUP_EXTRA)
    maxLongitude = int(gps_longitude * 10000 + SEARCH_LINKS_LOOKUP_EXTRA)
    query = '''\
with LINKS_TABLE0 as ( SELECT * FROM {} )
, LINKS_LOOKUP_TABLE0 as ( SELECT * FROM {} )
, LINKS_TABLE as (
SELECT LINKS_TABLE0.* 
FROM LINKS_LOOKUP_TABLE0 ,LINKS_TABLE0
WHERE key_LATitude >= '{}' AND key_longitude >= '{}' AND key_LATitude <= '{}' AND key_longitude <= '{}' AND
    LINKS_LOOKUP_TABLE0.NUM = LINKS_TABLE0.NUM AND LINKS_LOOKUP_TABLE0.LINK_ID = LINKS_TABLE0.LINK_ID )
, DIRECTION as ( 
select LINK_ID, LAT1,LON1,LAT2,LON2, 
	case 
		when ATN2(Y,X) * 180 / PI() >= 0 then ATN2(Y,X) * 180 / PI() 
		when ATN2(Y,X) * 180 / PI() < 0 then ATN2(Y,X) * 180 / PI() + 180 
	end as HEADING,A,B,C 
from ( 
	select *,ROUND((COS(RAD_LAT2) * SIN(RAD_LON2 - RAD_LON1)* 1000000),2) as Y, ROUND((COS(RAD_LAT1) * SIN(RAD_LAT2) - SIN(RAD_LAT1) * COS(RAD_LAT2) * COS(RAD_LON2 - RAD_LON1))*1000000,2) as X, LAT2-LAT1 as A,LON1-LON2 as B,LON2+LAT1-LON1*LAT2 as C  
	from( 
		select node1.LINK_ID, node1.LATITUDE as LAT1, node1. LONGITUDE as LON1, node2.LATITUDE as LAT2, node2.LONGITUDE as LON2, node1.LATITUDE * PI() / 180 as RAD_LAT1, node1.LONGITUDE * PI() / 180 as RAD_LON1, node2.LATITUDE * PI() / 180 as RAD_LAT2, node2.LONGITUDE * PI() / 180 as RAD_LON2 
		from ( 
			select * 
			from LINKS_TABLE0
			where NODE_ID is not null 
				and DIRECTION = 1 
		) as node1,( 
			select * 
			from LINKS_TABLE0
			where NODE_ID is not null 
				and DIRECTION = 2 
		) as node2 
		where node1.LINK_ID = node2.LINK_ID 
	) as rad 
) as XY 
where (X != 0 or Y != 0) 
) 
select LINKS_TABLE.LINK_ID,LINKS_TABLE.LATITUDE,LINKS_TABLE.LONGITUDE,LAT1,LON1,LAT2,LON2,DIRECTION.HEADING,A,B,C 
from LINKS_TABLE left join DIRECTION on LINKS_TABLE.LINK_ID = DIRECTION.LINK_ID 
    '''.format(LINKS_TABLE, LINKS_LOOKUP_TABLE, minLATitude, minLongitude, maxLATitude, maxLongitude)
    #print(query)

    connect= pyodbc.connect('DRIVER='+driver+';SERVER='+server+';DATABASE='+database+';PORT=1433;Trusted_Connection='+trusted_connection+';')
    cursor = connect.cursor()

    #cursor.execute(query)
    dataframe_link = pd.read_sql(query, connect)

    cursor.close()
    connect.close()

    return dataframe_link


def all_func(gps_record, Driver_id):    #gps_recordは['LATITUDE, LONGITUDE', 'HEADING']のdf
    #ドライバーごとにMM対象SLを設定、適宜追加してください
    #これはいずれはDB接続で対処したい
    #dataframe_highway_link = get_driver_link(get_MM_SL(0))
    dataframe_driver_link = get_semanticLinkTable(get_MM_SL(Driver_id)) #ドライバーごとの対象SL内の道路リンク
    dataframe_driver_link = dataframe_driver_link[(gps_record['LATITUDE'].min() < dataframe_driver_link['LATITUDE']) \
            & (gps_record['LATITUDE'].max() > dataframe_driver_link['LATITUDE']) \
            & (gps_record['LONGITUDE'].min() < dataframe_driver_link['LONGITUDE']) \
            & (gps_record['LONGITUDE'].max() > dataframe_driver_link['LONGITUDE'])]

    data_frame_sl_link = get_semanticLinkTable('0') #全SL内の道路リンク
    data_frame_sl_link = data_frame_sl_link[(gps_record['LATITUDE'].min() < data_frame_sl_link['LATITUDE']) \
            & (gps_record['LATITUDE'].max() > data_frame_sl_link['LATITUDE']) \
            & (gps_record['LONGITUDE'].min() < data_frame_sl_link['LONGITUDE']) \
            & (gps_record['LONGITUDE'].max() > data_frame_sl_link['LONGITUDE'])]
    
    #print(dataframe_driver_link)
    linkIdList = []
    neighbor_pointList = []
    distanceList = []
    sum_dist = 0

    for row in gps_record.itertuples():

        #対象道路リンク全体だと多いので、予め近いリンクのみを選択しておく
        dataframe_selectedRows = dataframe_driver_link[(abs(dataframe_driver_link['LATITUDE'] - row.LATITUDE) < SEARCH_SEMANTIC_LINKS_EXTRA) \
                                                        & (abs(dataframe_driver_link['LONGITUDE'] - row.LONGITUDE) < SEARCH_SEMANTIC_LINKS_EXTRA)]
        if len(dataframe_selectedRows) != 0:  #対象ドライバーのSL内の道路リンクに該当
            linkId, neighbor_point, dist = searchNearestLink(dataframe_selectedRows, row.LATITUDE, row.LONGITUDE, row.HEADING)
        else:
            dataframe_selectedRows = data_frame_sl_link[(abs(data_frame_sl_link['LATITUDE'] - row.LATITUDE) < SEARCH_SEMANTIC_LINKS_EXTRA) \
                                        & (abs(data_frame_sl_link['LONGITUDE'] - row.LONGITUDE) < SEARCH_SEMANTIC_LINKS_EXTRA)]
            if len(dataframe_selectedRows) != 0:  #全SL内の道路リンクに該当
                linkId, neighbor_point, dist = searchNearestLink(dataframe_selectedRows, row.LATITUDE, row.LONGITUDE, row.HEADING)
            else:
                dataframe_selectedRows = get_link_id(row.LATITUDE, row.LONGITUDE)
                if len(dataframe_selectedRows) != 0:  #全道路リンクの中のどれかに該当
                    linkId, neighbor_point, dist = searchNearestLink(dataframe_selectedRows, row.LATITUDE, row.LONGITUDE, row.HEADING)
                else:
                    linkId = 'NULL'
                    dist = -1
                    neighbor_point = np.array(['NULL', 'NULL'])
        
        #print('レコード' + str(row[0]) + ' LINK_ID:' + linkId + '  dist:' + str(dist) + '  neighbor_point:' + str(neighbor_point))
        linkIdList.append(linkId)
        neighbor_pointList.append(neighbor_point)
        distanceList.append(dist)
        sum_dist = sum_dist + dist

    #print(linkIdList)
    print('sum_dist:' + str(sum_dist))
    
    return linkIdList
    #return linkIdList, neighbor_pointList, distanceList


if __name__ == "__main__":

    Driver_id = 1
    filepath = "\\\\tommylab.ynu.ac.jp\\dfs\\home\\shichiry\\ECOLOG\\tmp\\"
    filename = "20221116214542UnsentGPS.csv"
    gps_record = pd.read_csv(filepath_or_buffer = filepath + filename, encoding = "ms932", sep = ",")
    #gps_record = gps_record[gps_record['LATITUDE'] > 35.43656994]
    print(gps_record)
    for i in gps_record.itertuples():




        lastvar = i

    linkIdList, neighbor_pointList, distanceList = all_func(gps_record, Driver_id)

    df_neighbor_point = pd.DataFrame(neighbor_pointList, columns = ['NEIGHBOR_LATITUDE', 'NEIGHBOR_LONGITUDE'])

    gps_record['LINK_ID'] = linkIdList
    gps_record['NEIGHBOR_LATITUDE'] = df_neighbor_point['NEIGHBOR_LATITUDE']
    gps_record['NEIGHBOR_LONGITUDE'] = df_neighbor_point['NEIGHBOR_LONGITUDE']
    gps_record['MATCHED_DISTANCE'] = distanceList

    gps_record.to_csv(filepath + filename.replace('UnsentGPS', 'UnsentGPS_LinkMatched'))
