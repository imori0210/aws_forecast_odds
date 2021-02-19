# JRA datalabで出力できるファイルからAWS forecastの仕様に合ったファイルを生成

from urllib.parse import urlparse
import mysql.connector
import csv
import io
import codecs
import glob
import os
import pandas as pd

YEAR = '2021'
SEC = '00'

# 指定されたディレクトリ内の全てのファイルを取得
def get_all_file(dir):
    path_list = glob.glob(dir + '/*')
    return path_list

# ファイル情報を取得する関数を実行
path_list = get_all_file('../data/origin')

for i in path_list:
    # 日付フォーマット変換
    f = open(i, "r")
    csv_data = csv.reader(f)
    list = [ e for e in csv_data]
    f.close()
    for k in range(len(list)):
        if k == 0: continue
        list[k][2] = YEAR + '-' + str(list[k][2])[0:2] + '-' + str(list[k][2])[2:4] + ' ' + str(list[k][2])[4:6] + ':' + str(list[k][2])[6:8] + ':' + SEC
    # 日付のフォーマット変更し上書き
    with open(i, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(list)
    #CSVファイルの読み込み
    df = pd.read_csv(i)
    df=df.rename(columns={'race_id': 'item_id','date': 'timestamp'})
    # 各馬ごとの時系列オッズをCSVに出力
    for k in range(df['horse_cnt'][1]):
        # AWS Forecastのデータ仕様に変換
        df=df.rename(columns={str(k+1) : 'target_value'})
        # [race_id]_[馬番].scvに出力
        df[['item_id','timestamp','target_value']].to_csv( '../data/gen/' + str(df['item_id'][1]) + '_' + str(k+1) + '.csv', index=False)