#作成：nomura, \\tommylab.ynu.ac.jp\dfs\home_old\home20211214\nomura2\寄り道検出
#更新：shichiri, インサートがうまくいかなくなったのでpyodbc導入

import sys

import numpy as np
import pandas as pd

import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

import pyodbc

from geopy.distance import geodesic

import time
import datetime

from .DatabaseAccess_config import driver, server, database, ECOLOG_DOPPLER_YORIMICHI_TABLE

#FROM_TABLE_NAME = "[" + database + "].[dbo].[ECOLOG_Doppler_NotMM]"
#TO_TABLE_NAME = "[" + database + "].[dbo].[ECOLOG_Doppler_NotMM_Yorimichi]"
#TRIPS_TABLE = "[" + database + "].[dbo].[TRIPS_Doppler_NotMM]"

# https://docs.sqlalchemy.org/en/14/dialects/mssql.html#dialect-mssql-pyodbc-connect
CONNECTION_STRING = "DRIVER=" + driver + ";SERVER=" + server + ";DATABASE=" + database
CONNECTION_URL = URL.create("mssql+pyodbc", query={"odbc_connect": CONNECTION_STRING})
engine = create_engine(CONNECTION_URL)

# 最近傍リンクセグメントとの距離の閾値(これを超えると寄り道として検出される) (単位 : m)
YORIMICHI_LIMIT_DISTANCE = 30

# 最近傍リンクセグメント候補を探索する正方形の範囲 (m)
LINK_SEGMENT_SEARCH_AREA = 180

TARGET_SEMANTIC_LINK_IDS = [220, 221, 224, 225, 469]

# 走行開始、終了地点付近の寄り道開始判定をしない矩形領域の頂点座標
# (総合研究棟駐車場を含む矩形領域)
TRIP_START_AND_END_AREA = {
    "min_latitude": 35.472023,
    "min_longitude": 139.586433,
    "max_latitude": 35.472674,
    "max_longitude": 139.587401,
}

def all_func(trip_start_time, trip_end_time, TRIPS_TABLE, ECOLOG_TABLE):
    main(trip_start_time, trip_end_time, TRIPS_TABLE, ECOLOG_TABLE)

def main(trip_start_time, trip_end_time, TRIPS_TABLE, ECOLOG_TABLE):
    
    query = f"""
            SELECT 
                TRIP_ID
            FROM 
                {TRIPS_TABLE}
            WHERE 
                END_TIME >= '{trip_start_time}'
                AND START_TIME <= '{trip_end_time}'
            """
    target_trip_ids = pd.read_sql(query, engine)["TRIP_ID"].tolist()
    print(query)
    print(target_trip_ids)

    # 分析対象経路 (通勤路) の往路と復路は区別しない
    # direction = df_trip_records.at[df_trip_records.index[0], "TRIP_DIRECTION"]
    # if direction == "homeward":
    #     # 復路
    #     semantic_link_id = 220
    # elif direction == "outward":
    #     # 往路
    #     semantic_link_id = 221
    # else:
    #     print("directionがhomewardかoutwardではありません")
    #     sys.exit()

    query = f"""
            SELECT  LINKS1.LINK_ID AS LINKID,
                LINKS1.NUM 
                AS NUM1,
                LINKS2.NUM AS NUM2,
                LINKS1.LATITUDE AS LATITUDE1,
                LINKS1.LONGITUDE AS LONGITUDE1,
                LINKS2.LATITUDE AS LATITUDE2,
                LINKS2.LONGITUDE AS LONGITUDE2,
                LINKS1.NODE_ID AS NODE1,
                LINKS2.NODE_ID AS NODE2
            FROM LINKS AS LINKS1
                INNER JOIN LINKS AS LINKS2 ON LINKS1.LINK_ID = LINKS2.LINK_ID
            WHERE   LINKS1.NUM - LINKS2.NUM = 1
            AND   LINKS1.NUM > LINKS2.NUM          
            AND   LINKS1.LINK_ID IN (
                    SELECT LINK_ID
                    FROM SEMANTIC_LINKS
                    WHERE SEMANTIC_LINK_ID IN ({",".join(map(str,TARGET_SEMANTIC_LINK_IDS))}))
            ORDER BY LINKS1.LINK_ID
            """

    df_links = pd.read_sql(query, engine)

    print(df_links)

    # 寄り道回数
    yorimichi_count = 0
    for target_trip_id in target_trip_ids:
        query = f"""
            SELECT 
                *
            FROM 
                {ECOLOG_TABLE}
            WHERE 
                TRIP_ID ={target_trip_id}
            ORDER BY 
                JST
            """
        df_trip_records = pd.read_sql(query, engine)
        print(df_trip_records)

        # 通過した道路リンクIDのリスト(重複しない)
        passed_through_unique_link_id_list = []

        # 寄り道中か示すフラグ
        is_on_yorimichi = False
        for index, trip_record in df_trip_records.iterrows():
            # リンクセグメントの探索範囲を指定する
            df_near_links = df_links.query(
                f"""LATITUDE1 > {trip_record['LATITUDE']} - {m_to_latitude(LINK_SEGMENT_SEARCH_AREA)} \
                and LATITUDE1 < {trip_record['LATITUDE']} + {m_to_latitude(LINK_SEGMENT_SEARCH_AREA)} \
                and LONGITUDE1 > {trip_record['LONGITUDE']} - {m_to_longitude(LINK_SEGMENT_SEARCH_AREA, trip_record['LATITUDE'])} \
                and LONGITUDE1 < {trip_record['LONGITUDE']} + {m_to_longitude(LINK_SEGMENT_SEARCH_AREA, trip_record['LATITUDE'])}"""
            )
            # 最近傍リンクセグメントとの距離
            min_distance_to_link = np.inf
            for ix, link in df_near_links.iterrows():
                # 道路リンクとの距離
                distance_to_link = distance_between_point_and_line_segment(
                    trip_record["LATITUDE"],
                    trip_record["LONGITUDE"],
                    link["LATITUDE2"],
                    link["LONGITUDE2"],
                    link["LATITUDE1"],
                    link["LONGITUDE1"],
                )
                if distance_to_link < min_distance_to_link:
                    min_distance_to_link = distance_to_link
                    # 最近傍の道路リンクIDを更新
                    nearest_link_id = link["LINKID"]
                    """  distance_to_node1 = hubeny_distance(trip_record['LATITUDE'], trip_record['LONGITUDE'],
                                                        link['LATITUDE1'],
                                                        link['LONGITUDE1'])
                    distance_to_node2 = hubeny_distance(trip_record['LATITUDE'], trip_record['LONGITUDE'],
                                                        link['LATITUDE2'],
                                                        link['LONGITUDE2'])"""

            if nearest_link_id not in passed_through_unique_link_id_list:
                passed_through_unique_link_id_list.append(nearest_link_id)

            # ドライバーの近くにセマンティックリンク内のリンクセグメントがなく、寄り道中ではない場合
            # print(f"--最短距離--")
            # print(f"TRIP_ID:{trip_record['TRIP_ID']}")
            # print(f"jst:{trip_record['JST']}")
            # print(f"LATITUDE:{trip_record['LATITUDE']}")
            # print(f"LONGITUDE:{trip_record['LONGITUDE']}")
            # print(f"min_distance_to_link:{min_distance_to_link}")
            # print("\n")
            # --寄り道開始--
            if min_distance_to_link > YORIMICHI_LIMIT_DISTANCE and not is_on_yorimichi:
                # 走行開始、終了地点の駐車領域の範囲外のとき寄り道開始の判定をする
                if not (
                    trip_record["LATITUDE"] > TRIP_START_AND_END_AREA["min_latitude"]
                    and trip_record["LATITUDE"]
                    < TRIP_START_AND_END_AREA["max_latitude"]
                    and trip_record["LONGITUDE"]
                    > TRIP_START_AND_END_AREA["min_longitude"]
                    and trip_record["LONGITUDE"]
                    < TRIP_START_AND_END_AREA["max_longitude"]
                ):
                    is_on_yorimichi = True
                    start_time = trip_record["JST"]

                # print(f"--寄り道開始--")
                # print(f"TRIP_ID:{trip_record['TRIP_ID']}")
                # print(f"START_TIME:{start_time}")
                # print(f"is_on_yorimichi:{is_on_yorimichi}")
                # print(f"min_distance_to_link:{min_distance_to_link}")
            # ドライバーの近くにセマンティックリンク内のリンクセグメントがあり、寄り道中の場合
            # --寄り道終了--
            elif min_distance_to_link <= YORIMICHI_LIMIT_DISTANCE and is_on_yorimichi:
                # 最近傍の道路リンクIDが最後に通過した道路リンクIDと一致している場合
                # ト
                end_time = trip_record["JST"]
                if nearest_link_id == passed_through_unique_link_id_list[-1]:
                    yorimichi_type_id = 1
                # 最近傍の道路リンクIDが通過した道路リンクIDに含まれていない場合
                # コ
                elif nearest_link_id not in passed_through_unique_link_id_list:
                    yorimichi_type_id = 2
                # 最近傍の道路リンクIDが最後に通過した道路リンクID以外の場合
                # ロ
                else:
                    yorimichi_type_id = 3
                print(f"--寄り道検出--")
                print(f"TRIP_ID:{trip_record['TRIP_ID']}")
                print(f"START_TIME:{start_time}")
                print(f"END_TIME:{end_time}")
                print(f"YORIMICHI_TYPE_ID:{yorimichi_type_id}\n")
                # print(f"min_distance_to_link:{min_distance_to_link}\n")

                # print(f"is_on_yorimichi:{is_on_yorimichi}")
                # print(f"min_distance_to_link:{min_distance_to_link}")

                # 寄り道をインサート
                # 重複データのインサートを試みて例外が発生しても実行が中断されないように例外をキャッチする
                try:
                    insert_yorimichi(trip_record["TRIP_ID"], start_time, end_time, yorimichi_type_id)
                except pyodbc.IntegrityError as e:
                    print(e)
                # 寄り道を終了する
                is_on_yorimichi = False
                yorimichi_count += 1

    #elapsed_time = time.time() - start
    #print(f"実行時間:{elapsed_time}秒")
    print(f"寄り道回数:{yorimichi_count}")


# 点Aと点Bのヒュベニ距離を求める
def hubeny_distance(lat_a, lon_a, lat_b, lon_b):
    pa = (lat_a, lon_a)
    pb = (lat_b, lon_b)
    return geodesic(pa, pb).m


# 点pと線分abの距離を求める
def distance_between_point_and_line_segment(lat_p, lon_p, lat_a, lon_a, lat_b, lon_b):
    ap = np.array([lat_p - lat_a, lon_p - lon_a])
    ab = np.array([lat_b - lat_a, lon_b - lon_a])
    ap_distance = hubeny_distance(lat_a, lon_a, lat_p, lon_p)
    ab_distance = hubeny_distance(lat_a, lon_a, lat_b, lon_b)
    bp_distance = hubeny_distance(lat_b, lon_b, lat_p, lon_p)

    cos_pab = np.inner(ap, ab) / (np.linalg.norm(ap) * np.linalg.norm(ab))
    # 点pから直線abにおろした垂線の足をhとしたときの、ahの長さ
    ah_distance = ap_distance * cos_pab

    if cos_pab < 0:
        distance = ap_distance
    elif ah_distance <= ab_distance:
        sin_pab = np.sqrt(1 - cos_pab ** 2)
        distance = ap_distance * sin_pab
    else:
        distance = bp_distance
    return distance


# 地球を球体としてmを緯度に変換する
def m_to_latitude(distance_m):
    # 極半径
    polar_radius = 6356775
    return 360 * distance_m / (2 * np.pi * polar_radius)


# 地球を球体として、緯度を指定しmを経度に変換する
def m_to_longitude(distance_m, latitude):
    # 赤道半径
    equatorial_radius = 6378137
    return (
        360
        * distance_m
        / (2 * np.pi * equatorial_radius * np.cos(np.deg2rad(latitude)))
    )


def insert_yorimichi(trip_id, start_time, end_time, yorimichi_type_id):
    df_yorimichi = pd.DataFrame(
        [
            {
                "TRIP_ID": trip_id,
                "START_TIME": start_time,
                "END_TIME": end_time,
                "YORIMICHI_TYPE_ID": yorimichi_type_id,
            }
        ]
    )
    print('INSERT:')
    #df_yorimichi.to_sql(TO_TABLE_NAME, engine, if_exists="append", index=False)

    #以下、pyodbcで書き換えた
    query = f"""
            INSERT INTO {ECOLOG_DOPPLER_YORIMICHI_TABLE}(TRIP_ID, START_TIME, END_TIME, YORIMICHI_TYPE_ID)
                VALUES({trip_id}, '{start_time}', '{end_time}', {yorimichi_type_id})
            """
    print(query)
    con = pyodbc.connect(CONNECTION_STRING)
    cur = con.cursor()
    #cur.execute(query)
    con.commit()
    cur.close()
    con.close()
    

# def parse_in_multiple_format(str):
#     for format in ("%Y-%m-%d", "%Y/%m/%d"):
#         try:
#             return datetime.datetime.strptime(str, format)
#         except ValueError:
#             pass
#     raise ValueError("日付のフォーマットが正しくありません")


if __name__ == "__main__":
    start = time.time()
    if len(sys.argv) < 3:
        print("開始日と終了日が正しく指定されていないため、直近2週間のデータを対象とします")
        end_jst = datetime.datetime.today()
        start_jst = end_date - pd.Timedelta('14 days')
    else:
        start_jst = sys.argv[1]
        end_jst = sys.argv[2]
    #main(start_jst, end_jst)
    #ここも後で（余力があれば）書く
