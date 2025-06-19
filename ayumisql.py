import pandas as pd
import os
import sqlite3
from datetime import datetime, time
import time as sleep_timer

# --- 設定項目 ---
# Excelファイルのパス（環境に合わせて変更してください）
EXCEL_FILE_PATH = 'c:/ayumi/ayumi.xlsm'
# 歩み値データが読み込まれるシート名
SHEET_NAME_DATA = 'Sheet2'
# 銘柄コードが書き込まれているシート名
SHEET_NAME_TICKER = "Sheet1"
# 銘柄コードが入力されているExcelのセル番地
TICKER_CODE_CELL_ADDRESS = 'E4'
# データベースファイルのパス
DB_PATH = 'c:/ayumi/market_data.db'

def setup_database(conn):
    """
    データベースとテーブルをセットアップする関数。
    'ayumi'テーブルが存在しない場合は作成します。
    """
    cursor = conn.cursor()
    create_table_query = """
    CREATE TABLE IF NOT EXISTS ayumi (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker_code TEXT NOT NULL,
        jikoku TEXT NOT NULL,
        price REAL NOT NULL,
        dekidaka INTEGER NOT NULL,
        baibai TEXT NOT NULL,
        UNIQUE(ticker_code, jikoku, price, dekidaka)
    )
    """
    cursor.execute(create_table_query)
    
    # パフォーマンス向上のためのインデックスを作成
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_ticker_jikoku ON ayumi (ticker_code, jikoku)")
    
    conn.commit()

def check_for_new_data():
    """
    Excelファイルを監視し、新しい約定データを読み込んでDBに保存する関数。
    """
    try:
        # 正しく'E4'セルを読み込むためのロジック
        df_ticker = pd.read_excel(
            EXCEL_FILE_PATH,
            sheet_name=SHEET_NAME_TICKER,
            header=None,
            usecols="E", # E列のみ対象
            skiprows=3,  # E1, E2, E3をスキップ
            nrows=1      # E4の1行だけ読み込む
        )
        ticker_code = str(df_ticker.iloc[0, 0])
        
        # Excelから歩み値データを読み込む
        df_data = pd.read_excel(EXCEL_FILE_PATH, sheet_name=SHEET_NAME_DATA, header=None)

    except Exception as e:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Excel読み込みエラー: {e}")
        return

    # --- ★★★ここからが売買方向の高度な判定ロジック★★★ ---
    df_cleaned = df_data.iloc[:, [0, 1, 2]].copy()
    df_cleaned.columns = ['時刻', '価格', '出来高']
    df_cleaned['価格'] = pd.to_numeric(df_cleaned['価格'], errors='coerce')
    df_cleaned['出来高'] = pd.to_numeric(df_cleaned['出来高'], errors='coerce')
    df_cleaned.dropna(subset=['価格', '出来高'], inplace=True)
    df_cleaned = df_cleaned[df_cleaned['出来高'] > 0].reset_index(drop=True)
    if df_cleaned.empty:
        return

    # 1. 証券会社からの公式な売買方向データがあれば最優先で利用
    if len(df_data.columns) >= 4:
        df_cleaned['方向'] = df_data.iloc[:, 3].astype(str).map({'2': '買い', '1': '売り', '02': '買い', '01': '売り'})

    # 2. 売買方向データがない、または不完全な場合、高度な推測ロジックで補完
    if '方向' not in df_cleaned.columns or df_cleaned['方向'].isnull().any():
        
        # 価格差を計算
        price_diff = df_cleaned['価格'].diff()
        
        # 推定方向を格納する新しい列を準備
        inferred_direction = pd.Series(index=df_cleaned.index, dtype=object)
        
        # Uptickルール: 価格が上昇したら「買い」
        inferred_direction[price_diff > 0] = '買い'
        
        # Downtickルール: 価格が下落したら「売り」
        inferred_direction[price_diff < 0] = '売り'
        
        # 直近の価格変動があった方向で、同値約定（NaN）を埋める (ティックテスト)
        inferred_direction.ffill(inplace=True)
        
        # それでも残る先頭のNaNは、ひとまず「買い」と見なす
        inferred_direction.fillna('買い', inplace=True)
        
        # '方向'列に、まだ方向が定まっていない行があれば、推定した方向で埋める
        if '方向' in df_cleaned.columns:
            df_cleaned['方向'] = df_cleaned['方向'].combine_first(inferred_direction)
        else:
            df_cleaned['方向'] = inferred_direction

    df_final = df_cleaned[['時刻', '価格', '出来高', '方向']]
    # --- ★★★判定ロジックここまで★★★ ---

    # --- データベースへの挿入 ---
    try:
        conn = sqlite3.connect(DB_PATH, timeout=10.0)
        setup_database(conn)
        cursor = conn.cursor()
        new_data_count = 0

        for _, row in df_final.iterrows():
            try:
                time_val, price_val, volume_val, direction_val = row['時刻'], row['価格'], row['出来高'], row['方向']
                if pd.isna(time_val) or pd.isna(direction_val): continue
                
                jikoku_str = time_val.strftime('%H:%M:%S') if isinstance(time_val, (datetime, time)) else str(time_val)
                
                insert_query = "INSERT OR IGNORE INTO ayumi (ticker_code, jikoku, price, dekidaka, baibai) VALUES (?, ?, ?, ?, ?)"
                values = (ticker_code, jikoku_str, float(price_val), int(volume_val), str(direction_val))
                cursor.execute(insert_query, values)
                
                if cursor.rowcount > 0:
                    new_data_count += 1
            except (ValueError, TypeError, IndexError):
                continue

        conn.commit()
        conn.close()
    
        if new_data_count > 0:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] [{ticker_code}] 新規データ {new_data_count} 件をDBに保存。")

    except sqlite3.Error as e:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] データベースエラー: {e}")


if __name__ == "__main__":
    print("データ収集スクリプトを開始します。")
    print(f"監視対象Excel: {EXCEL_FILE_PATH}")
    print(f"保存先DB: {DB_PATH}")
    print("Ctrl+Cで終了します。")
    
    while True:
        check_for_new_data()
        sleep_timer.sleep(5)
