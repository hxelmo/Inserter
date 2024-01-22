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
from .DatabaseAccess_config import  driver, server, database, trusted_connection, LINKS_TABLE, SEMANTIC_LINKS_TABLE
from .Place_config import  get_MM_SL

extra_lat_long = 0.005
MAX_MM_CORRECTION = 0.001   #対象SL上の道路リンクでこの値より近いものがなかったら、MMしない

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
        distance = norm(p - neighbor_point)
    return neighbor_point, distance

#1レコード(gps_latitude, gps_longitude)ごとに、dataframe_linkの中から最近傍リンクを求める
def searchNearestLinkMM(dataframe_link, gps_latitude, gps_longitude):
    tmp_min_dist = 1
    ndarray_gps = np.array([gps_latitude, gps_longitude])
    tmp_nearest_index = 0
    tmp_nearest_linkId = 'RB000000000'
    #neighbor_point = np.array([dataframe_link[0, 'START_LAT'], dataframe_link[0, 'START_LONG']])

    for row in dataframe_link.itertuples():
        ndaary_start_linkpoint = np.array([row.START_LAT, row.START_LONG])
        ndaary_end_linkpoint = np.array([row.END_LAT, row.END_LONG])

        tmp_neighbor_point, dist = calc_distance_and_neighbor_point(ndaary_start_linkpoint, ndaary_end_linkpoint, ndarray_gps)
        #print(str(row[0]) + ':' + str(dist))
        if dist < tmp_min_dist:
            tmp_min_dist = dist
            tmp_nearest_index = row[0]
            tmp_nearest_linkId = row.LINK_ID
            neighbor_point = tmp_neighbor_point
    
    #最近傍道路リンクまでの距離がMAX_MM_CORRECTION以上なら、MMしない
    if tmp_min_dist > MAX_MM_CORRECTION:
        tmp_nearest_linkId = 'NULL'
        neighbor_point = np.array(['NULL', 'NULL'])
        tmp_min_dist = -1

    return tmp_nearest_linkId, neighbor_point, tmp_min_dist


def get_driver_link(str_semantic_link_ids):
    #print(str_semantic_link_ids)
    select_semantic_link_ids =   'and SEMANTIC_LINK_ID in (' + str_semantic_link_ids.replace('\'', '') + ')'
    if str_semantic_link_ids == '0':
        select_semantic_link_ids = ''
        
    query = '''\
with LINKS_TABLE as(
select *
from {}
)
select l1.LINK_ID as LINK_ID 
	, l1.NUM
	, l1.LATITUDE as START_LAT
	, l1.LONGITUDE as START_LONG
	,l2.LATITUDE as END_LAT
	, l2.LONGITUDE as END_LONG 
	,SQRT((l1.LATITUDE - l2.LATITUDE) * (l1.LATITUDE - l2.LATITUDE) + (l1.LONGITUDE - l2.LONGITUDE) * (l1.LONGITUDE - l2.LONGITUDE)) as DISTANCE  
from LINKS_TABLE as l1
	,LINKS_TABLE as l2
	,(
        select l1.NUM
		    ,MIN(l2.NUM - l1.NUM) as diff 
		from LINKS_TABLE as l1
			,LINKS_TABLE as l2
			,{}
        where l1.NUM < l2.NUM  and l1.LINK_ID = l2.LINK_ID  and l1.LINK_ID = {}.LINK_ID {}
		group by l1.NUM
	) as Corres 
where l1.NUM = Corres.NUM and l2.NUM = l1.NUM + Corres.diff 
order by NUM 
    '''.format(LINKS_TABLE, SEMANTIC_LINKS_TABLE, SEMANTIC_LINKS_TABLE, select_semantic_link_ids)
    #print(query)

    connect= pyodbc.connect('DRIVER='+driver+';SERVER='+server+';DATABASE='+database+';PORT=1433;Trusted_Connection='+trusted_connection+';')
    cursor = connect.cursor()

    dataframe_link = pd.read_sql(query, connect)

    cursor.close()
    connect.close() 
    return dataframe_link


def all_func(gps_record, Driver_id):

    #ドライバーごとにMM対象SLを設定、適宜追加してください
    #これはいずれはDB接続で対処したい
    dataframe_link = get_driver_link(get_MM_SL(Driver_id))
    
    #print(dataframe_link)

    #MMは対象SL以外は絶対にマッチングしないので、ここはコメントアウト
    '''
    dataframe_all_link = get_all_link(gps_record['LATITUDE'].min(), gps_record['LATITUDE'].max(), gps_record['LONGITUDE'].min(), gps_record['LONGITUDE'].max())
    print(dataframe_all_link)
    '''

    linkIdList = []
    neighbor_pointList = []
    distanceList = []
    sum_dist = 0

    for row in gps_record.itertuples():

        #対象SL全体だと多いので、予め近いリンクのみを選択しておく
        select_dataframe_link = dataframe_link[(dataframe_link['START_LAT'] > row.LATITUDE - extra_lat_long) \
            & (dataframe_link['START_LAT'] < row.LATITUDE + extra_lat_long) \
            & (dataframe_link['START_LONG'] > row.LONGITUDE - extra_lat_long) \
            & (dataframe_link['START_LONG'] < row.LONGITUDE + extra_lat_long)]

        linkId, neighbor_point, dist = searchNearestLinkMM(select_dataframe_link, row.LATITUDE, row.LONGITUDE)
        #print('レコード' + str(row[0]) + ' LINK_ID:' + linkId + '  dist:' + str(dist) + '  neighbor_point:' + str(neighbor_point))
        linkIdList.append(linkId)
        neighbor_pointList.append(neighbor_point)
        distanceList.append(dist)
        if dist > 0:
            sum_dist = sum_dist + dist

    #print(linkIdList)
    print('マップマッチングによる総補正距離:' + str(sum_dist))
    
    return linkIdList, neighbor_pointList, distanceList


if __name__ == "__main__":

    Driver_id = 1
    filepath = "\\\\tommylab.ynu.ac.jp\\dfs\\home\\shichiry\\ECOLOG\\tmp\\" #テストフォルダ
    filename = "20221116214542UnsentGPS.csv"    #テストファイル
    gps_record = pd.read_csv(filepath_or_buffer = filepath + filename, encoding = "ms932", sep = ",")
    #gps_record = gps_record[gps_record['LATITUDE'] > 35.43656994]
    print(gps_record)

    #これが本体
    linkIdList, neighbor_pointList, distanceList = all_func(gps_record, Driver_id)

    df_neighbor_point = pd.DataFrame(neighbor_pointList, columns = ['NEIGHBOR_LATITUDE', 'NEIGHBOR_LONGITUDE'])

    gps_record['LINK_ID'] = linkIdList
    gps_record['NEIGHBOR_LATITUDE'] = df_neighbor_point['NEIGHBOR_LATITUDE']
    gps_record['NEIGHBOR_LONGITUDE'] = df_neighbor_point['NEIGHBOR_LONGITUDE']
    gps_record['MATCHED_DISTANCE'] = distanceList

    gps_record.to_csv(filepath + filename.replace('UnsentGPS', 'UnsentGPS_MapMatched'))
