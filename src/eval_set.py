import time
import numpy as np

def exec_time(lib, task, start, end, filename='execution_times.csv'):
    print('task : {} is started'.format(task))
    diff_time = (end - start) * 1000
    h, remainder = divmod(diff_time, 3600000)  # 時間の計算
    m, remainder = divmod(remainder, 60000)    # 分の計算
    s, ms = divmod(remainder, 1000)            # 秒とミリ秒の計算
    h, m, s, ms = int(h), int(m), int(s), int(ms)

    # 現在の時刻を取得
    current_time = time.strftime("%Y-%m-%d %H:%M:%S")

    # 結果をCSV形式でフォーマット
    result = f"{current_time},{lib},{task},{h:02d}:{m:02d}:{s:02d}.{ms:03d},{(end - start)}\n"
    print(diff_time)

    # 結果をファイルに書き込む
    with open(filename, 'a') as file:
        file.write(result)

def get_tasks():
    tasks = ['read_csv','prep','join','to_csv','gcols', 'isna', 'sort', 'query', 'dtypes', 'outlier', 
          'rename', 'apply', 'astype', 'pivot', 'group', 'dedup', 'fillna', 'replc','drop']
    return tasks