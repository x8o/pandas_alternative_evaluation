import numpy as np
import time
import sys
import re
from eval_set import exec_time, get_tasks

# コマンドライン引数をチェック
if len(sys.argv) > 1:
    # 第2の引数（ここでは "hoge"）を eval_lib に割り当てる
    eval_lib = sys.argv[1]
else:
    # 引数が指定されていない場合は、デフォルト値かエラーメッセージを設定
    eval_lib = None  # または適切なデフォルト値
    print("Usage: python3 main.py <library_name>")

if eval_lib:
    print("Library specified:", eval_lib)

subset = 100000
tasks = get_tasks()


# 各タスクの実行
for task in tasks:
    if eval_lib == 'pandas':
        import pandas as pd
    elif eval_lib == 'fireducks':
        import fireducks.pandas as pd
    elif eval_lib == 'modin':
        import modin.pandas as pd
    elif eval_lib == 'vaex':
        import vaex as vx
    elif eval_lib == 'polars':
        import polars as pl

    start = time.time()
    
    if task == 'read_csv':
        if eval_lib == 'vaex':
            df = vx.from_csv('posts.csv')
        elif eval_lib == 'polars':
            df = pl.read_csv('posts.csv')
        elif eval_lib == 'fireducks':
            df = pd.read_csv('posts.csv')
            df._evaluate()
        else:
            df = pd.read_csv('posts.csv')
    
    elif task == 'prep':
        if eval_lib == 'polars':    
            df2 = df
            df2 = df2.with_columns((pl.col('latitude') * 2).alias('SomeOtherColumn'))
            df2 = df2[['ParentPostID','SomeOtherColumn']]
            df2 = df2[:40]
        else :
            df2 = df.copy()
            df2['SomeOtherColumn'] = df2['latitude'] * 2
            df2 = df2[['ParentPostID','SomeOtherColumn']]
            df2 = df2[:40]            

    elif task == 'to_csv':
        if eval_lib == 'vaex':
            df.export_csv('output.csv')
        elif eval_lib == 'polars':
            df.write_csv('output.csv')
        else:
            df.to_csv('output.csv')

    elif task == 'gcols':
        if eval_lib == 'fireducks':
            df_gcol = df[['PostID', 'ProfileID']] # 特定のカラムを選択
            df_gcol._evaluate()
        else:
            df_gcol = df[['PostID', 'ProfileID']]
            df_gcol.tail()

    elif task == 'isna':
        if eval_lib == 'fireducks':
            sum = df.isna().sum()
            sum._evaluate()
        elif eval_lib == 'vaex':
            sum = {col: df[col].ismissing().sum() for col in df.get_column_names()}
            print(sum)
        elif eval_lib == 'polars':
            sum = df.null_count()
            print(sum)
        else:
            sum = df.isna().sum()
            print(sum)

    elif task == 'sort':
        if eval_lib == 'fireducks':
            df = df.sort_values(by=['PostedOn', 'latitude', 'longitude'], ascending=[True, False, True])
            df._evaluate()
        elif eval_lib == 'vaex':
            df = df.sort(['PostedOn', 'latitude', 'longitude'])
            df.head()
        elif eval_lib == 'polars':
            df = df.sort(['PostedOn', 'latitude', 'longitude'], descending=[False, True, False])
            df.head()
        else:
            df = df.sort_values(by=['PostedOn', 'latitude', 'longitude'], ascending=[True, False, True])
            df.head()
            
    elif task == 'query':
        if eval_lib == 'fireducks':
            filtered_df = df.query("latitude > 50 and -5 < longitude < 5 and PostText != -1") # クエリによるフィルタリング
            filtered_df._evaluate()
        elif eval_lib == 'vaex':
            filtered_df = df[(df.latitude > 50) & (df.longitude > -5) & (df.longitude < 5) & (df.PostText != -1)]
            filtered_df.head()
        elif eval_lib == 'polars':
            filtered_df = df.filter((df['latitude'] > 50) & (df['longitude'] > -5) & (df['longitude'] < 5) & (df['PostText'] != -1))
            filtered_df.head()
        else:
            filtered_df = df.query("latitude > 50 and -5 < longitude < 5 and PostText != -1") # クエリによるフィルタリング
            filtered_df.head()

    elif task == 'dtypes':
        if eval_lib == 'fireducks':
            df.dtypes
            df._evaluate()
        else:
            df.dtypes

    elif task == 'outlier':
        if eval_lib == 'fireducks':
            df_outlier = df[df['latitude'] > df['latitude'].mean() + 3 * df['latitude'].std()]
            df_outlier._evaluate()
        elif eval_lib == 'vaex':
            df_outlier = df[(df.latitude > df.latitude.mean() + 3 * df.latitude.std())]
        elif eval_lib == 'polars':
            df_outlier = df.filter(df['latitude'] > df['latitude'].mean() + 3 * df['latitude'].std())
        else:
            df_outlier = df[df['latitude'] > df['latitude'].mean() + 3 * df['latitude'].std()] # 外れ値の検出

    elif task == 'join':
        if eval_lib == 'fireducks':
            complex_join = df.merge(df2, on='ParentPostID', how='inner')
            complex_join._evaluate()
        elif eval_lib == 'vaex':
            complex_join = df.join(df2, on='ParentPostID', how='inner',allow_duplication=True)
            complex_join.head()
        elif eval_lib == 'polars':
            complex_join = df.join(df2, on='ParentPostID', how='inner', suffix='_right')
            complex_join.head()
        else:
            complex_join = df.merge(df2, on='ParentPostID', how='inner')
            complex_join.head()
    
    elif task == 'drop':
        if eval_lib == 'fireducks':
            df.drop(columns=['PostText', 'latitude', 'longitude'])
            df._evaluate()
        else:
            df.drop(columns=['PostText', 'latitude', 'longitude'])
            df.head()

    elif task == 'rename':
        if eval_lib == 'fireducks':
            df.rename(columns={'PostID': 'ID'}, inplace=True)
            df._evaluate()
        elif eval_lib == 'modin':
            df.rename(columns={'PostID': 'ID'})
            df.head()
        elif eval_lib == 'polars':
            df.rename({'PostID':'ID'})
            df.head()
        elif eval_lib == 'vaex':
            df.rename('PostID','ID')
            df.head()
        else:
            df.rename(columns={'PostID': 'ID'})
            df.head()

    elif task == 'apply':
        if eval_lib == 'fireducks':
            df['latitude'] = df['latitude'].apply(lambda x: x + 1)
            df._evaluate()
        elif eval_lib == 'polars':
            df = df.with_columns((df['latitude'] + 1).alias('latitude'))
        else:
            df['latitude'] = df['latitude'].apply(lambda x: x + 1)

    elif task == 'astype':
        #fireducks
        if eval_lib == 'fireducks':
            df.astype({'latitude': 'string'}) # データ型の変更
            df._evaluate()
        elif eval_lib == 'modin':
            df.astype({'latitude': 'string'}) # データ型の変更
        elif eval_lib == 'polars':
            df = df.with_columns([
                df['latitude'].cast(pl.Utf8)
            ])
        elif eval_lib == 'vaex':
            df['latitude'].astype('string')
        else:
            df.astype({'latitude': 'string'}) # データ型の変更
    
    elif task == 'pivot':
        df_subset = df[:subset]  # 例えば、最初の100,000行のみを使用
        if eval_lib == 'fireducks':
            df_subset.pivot_table(index='ProfileID', columns='longitude', values='latitude', aggfunc='first')  # ピボットテーブルの作成
            df_subset._evaluate()
        elif eval_lib == 'modin':
            df_subset.pivot_table(index='ProfileID', columns='longitude', values='latitude', aggfunc='first')
            df_subset.head()
        elif eval_lib == 'polars':
            df_subset.pivot(index='ProfileID', 
                     columns='longitude', 
                     values='latitude', 
                     aggregate_function='first')
            df_subset.head()
        elif eval_lib == 'vaex':
            pass
        else:
            df_subset.pivot_table(index='ProfileID', columns='longitude', values='latitude', aggfunc='first')
            df_subset.head()

    elif task == 'group':
        if eval_lib == 'fireducks':
            aggregated_df = df.groupby('ProfileID').agg({
            'latitude': 'mean',        # 緯度の平均
            'longitude': 'max',        # 経度の最大値
            'PostedOn': 'max'          # 最新の投稿日時
            })        
            aggregated_df._evaluate()
        elif eval_lib == 'modin':
            aggregated_df = df.groupby('ProfileID').agg({
            'latitude': 'mean',        # 緯度の平均
            'longitude': 'max',        # 経度の最大値
            'PostedOn': 'max'          # 最新の投稿日時
            })
            aggregated_df.head()
        elif eval_lib == 'polars':
            aggregated_df = df.group_by('ProfileID').agg([
            pl.col('latitude').mean().alias('latitude_mean'),
            pl.col('longitude').max().alias('longitude_max'),
            pl.col('PostedOn').max().alias('PostedOn_max')
            ])
            aggregated_df.head()
        elif eval_lib == 'vaex':
            aggregated_df = df.groupby('ProfileID', agg={
            'mean_latitude': vx.agg.mean('latitude'),
            'max_longitude': vx.agg.max('longitude'),
            'first_posted_on': vx.agg.count('PostedOn')
            })
            aggregated_df.head()
        else:
            aggregated_df = df.groupby('ProfileID').agg({
            'latitude': 'mean',        # 緯度の平均
            'longitude': 'max',        # 経度の最大値
            'PostedOn': 'max'          # 最新の投稿日時
            })
            aggregated_df.head()

    elif task == 'dedup':
        if eval_lib == 'fireducks':
            df = df.drop_duplicates() # 重複の削除
            df._evaluate()
        elif eval_lib == 'modin':
            #modin 
            df = df.drop_duplicates() # 重複の削除
            df.head()
        elif eval_lib == 'polars':
            #polars
            df = df.unique()# 重複の削除
            df.head()
        elif eval_lib == 'vaex':
            #vaex
            df_ = df.unique('ID') # 重複の削除
            df_[0]
        else:
            df = df.drop_duplicates()
            df.head()

    elif task == 'fillna':
        if eval_lib == 'fireducks':
            df['ParentPostID'] = df['ParentPostID'].fillna(' ')
            df._evaluate()
        elif eval_lib == 'polars':
            df = df.with_columns(df['ParentPostID'].fill_null(' ').alias('ProfileID'))
            df.head() 
        elif eval_lib == 'vaex' :
            df = df.fillna(value= ' ', column_names=['ParentPostID'])
            df.head()
        else:    
            df['ParentPostID'] = df['ParentPostID'].fillna(' ')
            df.head()

    elif task == 'replc':
        if eval_lib == 'fireducks':
            df['ProfileID'] = df['ProfileID'].replace(to_replace=r'^ABC.*', value='Unknown', regex=True)
            df._evaluate()
        
        elif eval_lib == 'modin':
            df['ProfileID'] = df['ProfileID'].replace(to_replace=r'^ABC.*', value='Unknown', regex=True)
            df.head()
        elif eval_lib == 'polars':
            df = df.with_columns(
                pl.col('ProfileID').apply(lambda x: 'Unknown' if re.match(r'^ABC.*', x) else x).alias('ProfileID')
            )
            df.head()
        elif eval_lib == 'vaex':
            df['ProfileID'] = df['ProfileID'].str.replace(pat='^ABC.*', repl='Unknown', regex=True)
            df.head()
        else:
            df['ProfileID'] = df['ProfileID'].replace(to_replace=r'^ABC.*', value='Unknown', regex=True)
            df.head()

    end = time.time()
    exec_time(eval_lib, task, start, end)