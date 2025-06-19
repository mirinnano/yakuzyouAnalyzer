import pandas as pd
import hashlib
import os
import json
from datetime import datetime
from openpyxl import load_workbook
import time

def generate_hash(row):
    """行の内容からハッシュ値を生成"""
    row_str = ''.join(map(str, row.values))
    return hashlib.sha256(row_str.encode()).hexdigest()

def check_for_new_data():
    # JSONファイルのパス（以前のデータを保持）
    json_file_path = 'c:/ayumi/data.json'
    
    # Excelファイルとシートのパス（読み込み用）
    file_path = 'c:/ayumi/ayumi.xlsm'
    sheet_name = 'Sheet2'
    
    # Excelファイルからデータを読み込む
    while True:
        try:
            df = pd.read_excel(file_path, sheet_name=sheet_name)
            break
        except PermissionError:
            print("ファイルにアクセスできません。5秒後に再試行します...")
            time.sleep(5)
    
    # 最初の300行を選択（2行目から301行目に相当）
    df = df.iloc[0:300]
    
    # ハッシュ値を計算して新しい列に追加
    df['hash'] = df.apply(generate_hash, axis=1)
    
    # 既存のJSONファイルからデータを読み込む
    if os.path.exists(json_file_path):
        with open(json_file_path, 'r', encoding='utf-8') as file:
            previous_data = json.load(file)
        df_previous = pd.DataFrame(previous_data)
        if 'hash' not in df_previous.columns:
            df_previous['hash'] = [None] * len(df_previous)  # 'hash'列がない場合は追加
    else:
        df_previous = pd.DataFrame(columns=['hash'])  # 空のDataFrameに'hash'列を設定
    
    # 新しいデータのハッシュ値が以前のデータに存在しない場合、そのデータを新規追加として扱う
    df_new_unique = df[~df['hash'].isin(df_previous['hash'])]
    
    # 新規データがあれば、それを処理
    if not df_new_unique.empty:
        print("新しいユニークなデータが検出されました。以下のデータが追加されました:")
        print(df_new_unique.drop(columns=['hash']).to_string(index=False))
        
        # エクセルファイルのパス
        excel_output_path = 'c:/ayumi/updated_data.xlsx'
        
        # 新しいシート名（例：現在の日時を使用）
        new_sheet_name = datetime.now().strftime('Data_%Y-%m-%d_%H-%M-%S')
        
        # 無効な日時値をフィルタリング
        df_new_unique = df_new_unique[~df_new_unique['時刻'].astype(str).str.contains('--------')]
        
        # エクセルファイルに新しいシートを追加
        with pd.ExcelWriter(excel_output_path, engine='openpyxl', mode='a' if os.path.exists(excel_output_path) else 'w') as writer:
            df_new_unique.drop(columns=['hash']).iloc[::-1].to_excel(writer, sheet_name=new_sheet_name, index=False)
        
        # 全データ（新旧）を統合してJSONファイルに保存
        df_full = pd.concat([df_previous, df_new_unique[['hash']]])
        df_full.to_json(json_file_path, orient='records', force_ascii=False)
        print("データの処理が完了しました。")
    else:
        print("新しいユニークなデータは検出されませんでした。")

# 定期的にデータチェックを行うループ
while True:
    check_for_new_data()
    time.sleep(5)  # 5秒待機