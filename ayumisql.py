# coding: utf-8
import pandas as pd
import sqlite3
from datetime import datetime, time
import time as sleep_timer
import os

# --- 設定項目 ---
EXCEL_FILE_PATH = 'c:/ayumi/ayumi.xlsm'
SHEET_NAME_DATA = 'Sheet2'
SHEET_NAME_TICKER = "Sheet1"
TICKER_CODE_CELL_ADDRESS = 'E4'
DB_PATH = 'c:/ayumi/market_data.db'

def setup_database(conn):
    """データベースとテーブル、インデックスをセットアップする。"""
    with conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("""
        CREATE TABLE IF NOT EXISTS ayumi (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker_code TEXT NOT NULL,
            jikoku TEXT NOT NULL,
            price REAL NOT NULL,
            dekidaka INTEGER NOT NULL,
            baibai TEXT NOT NULL,
            UNIQUE(ticker_code, jikoku, price, dekidaka, baibai)
        )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ticker_id ON ayumi (ticker_code, id)")

def check_for_new_data(conn):
    """Excelファイルを監視し、新しい約定データを読み込んでDBに保存する。"""
    try:
        if not os.path.exists(EXCEL_FILE_PATH):
            print(f"[{datetime.now().strftime('%H:%M:%S')}] エラー: Excelファイルが見つかりません: {EXCEL_FILE_PATH}")
            sleep_timer.sleep(10) # 10秒待ってリトライ
            return

        df_ticker = pd.read_excel(
            EXCEL_FILE_PATH,
            sheet_name=SHEET_NAME_TICKER,
            header=None,
            usecols="E",
            skiprows=3,
            nrows=1
        )
        ticker_code = str(df_ticker.iloc[0, 0])
        
        df_data = pd.read_excel(EXCEL_FILE_PATH, sheet_name=SHEET_NAME_DATA, header=None)

    except Exception as e:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Excel読み込みエラー: {e}")
        return

    # --- 売買方向の高度な判定ロジック ---
    if df_data.empty or len(df_data.columns) < 3:
        return # 必要な列がない場合はスキップ
        
    df_cleaned = df_data.iloc[:, [0, 1, 2]].copy()
    df_cleaned.columns = ['時刻', '価格', '出来高']
    df_cleaned['価格'] = pd.to_numeric(df_cleaned['価格'], errors='coerce')
    df_cleaned['出来高'] = pd.to_numeric(df_cleaned['出来高'], errors='coerce')
    df_cleaned.dropna(subset=['価格', '出来高'], inplace=True)
    df_cleaned = df_cleaned[df_cleaned['出来高'] > 0].reset_index(drop=True)
    if df_cleaned.empty:
        return

    if len(df_data.columns) >= 4:
        df_cleaned['方向'] = df_data.iloc[:, 3].astype(str).map({'2': '買い', '1': '売り', '02': '買い', '01': '売り'})

    if '方向' not in df_cleaned.columns or df_cleaned['方向'].isnull().any():
        price_diff = df_cleaned['価格'].diff()
        inferred_direction = pd.Series(index=df_cleaned.index, dtype=object)
        inferred_direction[price_diff > 0] = '買い'
        inferred_direction[price_diff < 0] = '売り'
        inferred_direction.ffill(inplace=True)
        inferred_direction.fillna('買い', inplace=True)
        
        if '方向' in df_cleaned.columns:
            df_cleaned['方向'] = df_cleaned['方向'].combine_first(inferred_direction)
        else:
            df_cleaned['方向'] = inferred_direction

    df_final = df_cleaned[['時刻', '価格', '出来高', '方向']].dropna()
    
    # --- データベースへの挿入 ---
    if df_final.empty:
        return
        
    try:
        records_to_insert = []
        for _, row in df_final.iterrows():
            time_val = row['時刻']
            jikoku_str = time_val.strftime('%H:%M:%S') if isinstance(time_val, (datetime, time)) else str(time_val)
            records_to_insert.append(
                (ticker_code, jikoku_str, float(row['価格']), int(row['出来高']), str(row['方向']))
            )

        with conn:
            cursor = conn.cursor()
            cursor.executemany(
                "INSERT OR IGNORE INTO ayumi (ticker_code, jikoku, price, dekidaka, baibai) VALUES (?, ?, ?, ?, ?)",
                records_to_insert
            )
            new_data_count = cursor.rowcount
    
        if new_data_count > 0:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] [{ticker_code}] 新規データ {new_data_count} 件をDBに保存。")

    except sqlite3.Error as e:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] データベースエラー: {e}")
    except Exception as e:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] データ処理エラー: {e}")

if __name__ == "__main__":
    print("データ収集スクリプトを開始します。")
    print(f"監視対象Excel: {EXCEL_FILE_PATH}")
    print(f"保存先DB: {DB_PATH}")
    print("Ctrl+Cで終了します。")
    
    conn = None
    try:
        # スクリプト開始時に一度だけ接続
        conn = sqlite3.connect(DB_PATH, timeout=10.0)
        setup_database(conn)
        
        while True:
            check_for_new_data(conn)
            sleep_timer.sleep(2) # ポーリング間隔を短縮

    except KeyboardInterrupt:
        print("\nスクリプトを終了します。")
    except Exception as e:
        print(f"致命的なエラーが発生しました: {e}")
    finally:
        # スクリプト終了時に接続を閉じる
        if conn:
            conn.close()
            print("データベース接続を解放しました。")