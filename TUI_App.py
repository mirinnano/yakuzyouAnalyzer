import pandas as pd
import os
import sqlite3
import time as sleep_timer
import subprocess
import re
from collections import deque

from textual.app import App, ComposeResult
from textual.widgets import Header, Footer, Static, DataTable
from textual.binding import Binding
from rich.text import Text
from rich.table import Table
from rich.layout import Layout
from rich.panel import Panel
import numpy as np

# Windows連携のために、pywin32をインポート
try:
    import win32com.client
except ImportError:
    print("エラー: pywin32がインストールされていません。")
    print("コマンドプロンプトで 'pip install pywin32' を実行してください。")
    exit()

# --- 設定項目 ---
EXCEL_WORKBOOK_PATH = r"C:\ayumi\ayumi.xlsm"
EXCEL_SHEET_NAME_DATA = 'Sheet2'
EXCEL_SHEET_NAME_TICKER = 'Sheet1'
EXCEL_TICKER_CELL = 'E4'
EXCEL_ADDIN_PATH    = r"C:\Users\ren-k\AppData\Local\MarketSpeed2\Bin\rss\MarketSpeed2_RSS_64bit.xll"
DATA_IMPORTER_SCRIPT_PATH = r"C:\Users\ren-k\Desktop\karauri\ayumisql.py"
DB_PATH             = r"c:/ayumi/market_data.db"

# --- 金額フォーマット用ヘルパー関数 (変更なし) ---
def format_yen(value: float) -> str:
    if value >= 1_0000_0000: return f"{value / 1_0000_0000:,.1f}億円"
    if value >= 1_0000: return f"{value / 1_0000:,.0f}万円"
    return f"{value:,.0f}円"

# --- トレード解析クラス (変更なし) ---
class TradeAnalyzer:
    def __init__(self, window_size: int = 5000, time_window_sec: int = 300):
        self.window_size, self.time_window_sec, self.history = window_size, time_window_sec, pd.DataFrame()
    def _calculate_metrics(self, df: pd.DataFrame) -> dict:
        if df.empty or '時刻' not in df.columns or df['時刻'].iloc[-1] is pd.NaT: return {}
        now = pd.to_datetime(df['時刻'].iloc[-1]); window_df = df[df['時刻'] > now - pd.Timedelta(seconds=self.time_window_sec)]
        if window_df.empty or len(window_df) < 2: return {}
        vwap = (window_df['価格'] * window_df['出来高']).sum() / window_df['出来高'].sum(); volatility = window_df['価格'].std(ddof=0)
        time_span_min = (window_df['時刻'].iloc[-1] - window_df['時刻'].iloc[0]).total_seconds() / 60
        trade_density = len(window_df) / time_span_min if time_span_min > 0 else 0; avg_volume_per_trade = window_df['出来高'].mean()
        price_open, price_close = window_df['価格'].iloc[0], window_df['価格'].iloc[-1]
        return {'vwap': vwap, 'volatility': volatility, 'trade_density_per_min': trade_density, 'avg_volume_per_trade': avg_volume_per_trade, 'price_open': price_open, 'price_close': price_close}
    def _get_dynamic_thresholds(self, metrics: dict) -> tuple:
        if not metrics or 'avg_volume_per_trade' not in metrics or metrics['avg_volume_per_trade'] == 0: return (1_000_000, 10_000_000, 50_000_000)
        base_vol, vwap = metrics['avg_volume_per_trade'], metrics['vwap']; return (base_vol * 5 * vwap, base_vol * 20 * vwap, base_vol * 100 * vwap)
    def analyze(self, df: pd.DataFrame) -> dict | None:
        if df.empty or len(df) < 2: return None
        df = df.copy(); df.columns = ['id', '時刻', '価格', '出来高', '方向']
        df['時刻'] = pd.to_datetime(df['時刻'], errors='coerce'); df['価格'] = pd.to_numeric(df['価格'], errors='coerce'); df['出来高'] = pd.to_numeric(df['出来高'], errors='coerce')
        df.dropna(inplace=True); df = df[df['出来高'] > 0]
        if df.empty: return None
        metrics = self._calculate_metrics(df)
        if not metrics: return None
        df['約定代金'] = df['価格'] * df['出来高']; m_th, l_th, s_th = self._get_dynamic_thresholds(metrics)
        bins, code_labels = [0, m_th, l_th, s_th, float('inf')], ['小口', '中口', '大口', '超大口']
        df['ロット'] = pd.cut(df['約定代金'], bins=bins, labels=code_labels, right=False, include_lowest=True)
        pivot = df.groupby(['ロット', '方向'], observed=True)['出来高'].sum().unstack(fill_value=0)
        for col in ['買い', '売り']:
            if col not in pivot.columns: pivot[col] = 0
        pivot['差引'] = pivot['買い'] - pivot['売り']; pivot = pivot.reindex(code_labels, fill_value=0)
        signal, confidence, condition = "中立", 0, "様子見"
        large_trades = df[df['ロット'].isin(['大口', '超大口'])]; large_buys = large_trades[large_trades['方向'] == '買い']; large_sells = large_trades[large_trades['方向'] == '売り']
        large_buy_volume = large_buys['出来高'].sum(); large_sell_volume = large_sells['出来高'].sum(); large_net_volume = large_buy_volume - large_sell_volume
        confidence_score = abs(large_net_volume) / (metrics.get('avg_volume_per_trade', 1) * 10)
        buy_vwap = (large_buys['価格'] * large_buys['出来高']).sum() / large_buy_volume if large_buy_volume > 0 else 0
        sell_vwap = (large_sells['価格'] * large_sells['出来高']).sum() / large_sell_volume if large_sell_volume > 0 else 0
        is_price_up = metrics['price_close'] >= metrics['price_open']; is_price_down = metrics['price_close'] < metrics['price_open']
        if large_sell_volume > large_buy_volume * 1.5 and is_price_up: signal, confidence, condition = "強い買い", min(10, int(confidence_score * 1.5) + 3), "売り吸収の可能性"
        elif large_buy_volume > large_sell_volume * 1.5 and is_price_down: signal, confidence, condition = "強い売り", min(10, int(confidence_score * 1.5) + 3), "買い疲れの兆候"
        elif large_net_volume > 0 and confidence_score > 1:
            signal, confidence = "強い買い", min(10, int(confidence_score) + 1)
            condition = "VWAP越えの積極買い" if buy_vwap > metrics['vwap'] else "大口による買い集め"
        elif large_net_volume < 0 and confidence_score > 1:
            signal, confidence = "強い売り", min(10, int(confidence_score) + 1)
            condition = "VWAP下での売り" if sell_vwap < metrics['vwap'] else "大口による売り"
        elif pivot['差引'].sum() > 0: signal, confidence, condition = "買い優勢", 3, "小口中心の買い"
        elif pivot['差引'].sum() < 0: signal, confidence, condition = "売り優勢", 3, "小口中心の売り"
        summary = {'total_volume': int(df['出来高'].sum()), 'breakdown': pivot, 'signal': signal, 'confidence': confidence, 'condition': condition, 'metrics': metrics, 'thresholds_yen': {'medium': m_th, 'large': l_th, 'super_large': s_th}}
        return {'summary': summary, 'detail_df': df.tail(self.window_size)}

# --- TUIウィジェット定義 (変更なし) ---
class TradeLogWidget(Static):
    def compose(self) -> ComposeResult: yield DataTable()
    def on_mount(self) -> None:
        self.border_title = "リアルタイム約定ログ"; tbl = self.query_one(DataTable); tbl.cursor_type = "row"
        tbl.add_columns("時刻","価格","出来高","方向","ロット")
    def update_log(self, df: pd.DataFrame|None) -> None:
        tbl = self.query_one(DataTable); tbl.clear()
        if df is None or df.empty: return
        for _, r in df.tail(500).iloc[::-1].iterrows():
            base_style = 'red' if r['方向'] == '買い' else 'yellowgreen'
            if r['ロット'] == '大口': final_style = f"bold {base_style}"
            elif r['ロット'] == '超大口': final_style = 'bold bright_magenta' if r['方向'] == '買い' else 'bold yellow'
            else: final_style = base_style
            tbl.add_row(
                Text(r['時刻'].strftime('%H:%M:%S'), style=final_style), Text(f"{r['価格']:,}", style=final_style),
                Text(f"{int(r['出来高']):,}", style=final_style), Text(r['方向'], style=final_style),
                Text(str(r['ロット']), style=final_style), key=str(r['id'])
            )
        tbl.scroll_home(animate=False)

class TradeAnalysisWidget(Static):
    def on_mount(self) -> None:
        self.border_title = "インテリジェント約定分析"; self.analysis_layout = Layout(); self.analysis_layout.split_column(Layout(name="header", size=5), Layout(name="main"))
        self.analysis_layout["main"].split_row(Layout(name="metrics"), Layout(name="breakdown")); self.update(self.analysis_layout)
    def update_analysis(self, analysis: dict|None) -> None:
        layout = self.analysis_layout
        if not analysis: self.update(Panel("分析データを待っています...", style="bold dim")); return
        summary, metrics = analysis, analysis['metrics']; sig, conf, cond = summary['signal'], summary['confidence'], summary['condition']
        style = 'bold green' if '買い' in sig else 'bold red' if '売り' in sig else 'bold white'
        header_table = Table.grid(expand=True); header_table.add_column(justify="left"); header_table.add_column(justify="right")
        header_table.add_row(f"[bold]推奨シグナル: [{style}]{sig}[/{style}][/]", f"信頼度: {'★'*conf}{'☆'*(10-conf)}"); header_table.add_row(f"[bold]市場コンディション: [cyan]{cond}[/]", f"総出来高: {summary['total_volume']:,}株")
        layout["header"].update(Panel(header_table, title="判定", border_style="blue"))
        metrics_table = Table.grid(padding=(0, 1)); metrics_table.add_column(); metrics_table.add_column(justify="right")
        metrics_table.add_row("[bold]VWAP:", f"[yellow]{metrics['vwap']:,.2f}[/]"); metrics_table.add_row("[bold]ボラティリティ:", f"[cyan]{metrics['volatility']:,.2f}[/]"); metrics_table.add_row("[bold]取引密度/分:", f"[magenta]{metrics['trade_density_per_min']:.1f}回[/]"); metrics_table.add_row("[bold]平均出来高/約定:", f"[green]{metrics['avg_volume_per_trade']:,.0f}株[/]")
        layout["metrics"].update(Panel(metrics_table, title="市場指標", border_style="green"))
        breakdown_table = Table(title="ロット別出来高", header_style="bold magenta"); breakdown_table.add_column("ロット", justify="left", style="cyan"); breakdown_table.add_column("約定代金レンジ", justify="left", style="dim white"); breakdown_table.add_column("買い", justify="right", style="green"); breakdown_table.add_column("売り", justify="right", style="red"); breakdown_table.add_column("差引", justify="right")
        thresholds = summary['thresholds_yen']; m_th, l_th, s_th = thresholds['medium'], thresholds['large'], thresholds['super_large']
        ranges = {'小口': f"~ {format_yen(m_th)}", '中口': f"{format_yen(m_th)} ~ {format_yen(l_th)}", '大口': f"{format_yen(l_th)} ~ {format_yen(s_th)}", '超大口': f"{format_yen(s_th)} ~"}
        for lot_name, row in summary['breakdown'].iterrows():
            b, s, n = int(row['買い']), int(row['売り']), int(row['差引']); ns = 'bold green' if n > 0 else 'bold red' if n < 0 else 'white'; range_str = ranges.get(lot_name, "N/A")
            breakdown_table.add_row(lot_name, range_str, f"{b:,}", f"{s:,}", f"[{ns}]{n:+,}[/{ns}]")
        layout["breakdown"].update(Panel(breakdown_table, border_style="yellow")); self.update(layout)

# --- メインアプリ (変更なし) ---
class TraderApp(App):
    BINDINGS = [Binding("q", "quit", "終了"), Binding("p", "toggle_pause", "一時停止/再開"),]
    def __init__(self, ticker_code: str):
        super().__init__(); self.target_ticker = ticker_code; self.analyzer = TradeAnalyzer(); self.last_id = 0; self.df_history = pd.DataFrame(); self.is_paused = False; self.update_timer = None
        self.footer_message_timer = None; self.trade_counts = deque(maxlen=30)
    CSS = ("Screen{layout:grid;grid-size:2;grid-columns:1fr 2fr;grid-gutter:1;padding:1;background:#1e1f22;} #trade-log,#trade-analysis{border:round #4a4a4a;background:#2f3136;padding:1;overflow:auto;height:100%;} #trade-analysis{padding:0;}")
    def compose(self) -> ComposeResult: yield Header(show_clock=True); yield TradeLogWidget(id="trade-log"); yield TradeAnalysisWidget(id="trade-analysis"); yield Footer()
    def on_mount(self) -> None: self.update_panels(); self.update_timer = self.set_interval(2, self.update_panels)
    async def on_ready(self) -> None:
        try: self.query_one("#header-title", Static).update(f"統合トレーディング環境 - [{self.target_ticker}]")
        except Exception: pass
    def show_flash_message(self, message: str, duration: float = 5.0):
        footer = self.query_one(Footer); footer.show_bindings = False; self.sub_title = message
        if self.footer_message_timer is not None: self.footer_message_timer.stop()
        self.footer_message_timer = self.set_timer(duration, self.clear_flash_message)
    def clear_flash_message(self): self.sub_title = ""; self.query_one(Footer).show_bindings = True; self.footer_message_timer = None
    def analyze_latest_ticks(self, new_df: pd.DataFrame, last_summary: dict | None):
        if new_df.empty or last_summary is None: return
        self.trade_counts.append(len(new_df))
        avg_trade_count = sum(self.trade_counts) / len(self.trade_counts) if self.trade_counts else 0
        if len(new_df) > avg_trade_count * 5 and len(new_df) > 5:
            buy_ratio = (new_df['方向'] == '買い').sum() / len(new_df)
            if buy_ratio > 0.8: self.show_flash_message(f"[bold green]!![/] [white]高密度な[red]買いバースト[/red]を検知 ({len(new_df)}件)[/white]")
            elif buy_ratio < 0.2: self.show_flash_message(f"[bold red]!![/] [white]高密度な[yellowgreen]売りバースト[/yellowgreen]を検知 ({len(new_df)}件)[/white]")
    def update_panels(self) -> None:
        if self.is_paused: return
        if not os.path.exists(DB_PATH): return
        last_summary = self.analyzer.analyze(self.df_history) if not self.df_history.empty else None
        try:
            with sqlite3.connect(DB_PATH, timeout=10.0) as conn:
                query = "SELECT id, jikoku AS 時刻, price AS 価格, dekidaka AS 出来高, baibai AS 方向 FROM ayumi WHERE ticker_code = ? AND id > ? ORDER BY id"
                new_df = pd.read_sql_query(query, conn, params=(self.target_ticker, self.last_id))
        except sqlite3.Error as e: self.log(f"!!! データベースエラー: {e}"); return
        self.analyze_latest_ticks(new_df, last_summary)
        log_widget = self.query_one(TradeLogWidget); analysis_widget = self.query_one(TradeAnalysisWidget)
        log_widget.border_title = f"リアルタイム約定ログ [{self.target_ticker}]"; analysis_widget.border_title = f"インテリジェント約定分析 [{self.target_ticker}]"
        if new_df.empty and self.df_history.empty: analysis_widget.update_analysis(None); return
        elif not new_df.empty: self.df_history = pd.concat([self.df_history, new_df]).tail(10000); self.last_id = self.df_history['id'].max()
        if not self.df_history.empty:
            res = self.analyzer.analyze(self.df_history)
            if res:
                log_widget.update_log(res['detail_df']); analysis_widget.update_analysis(res['summary'])
                summary = res['summary']
                if summary['confidence'] >= 7 and "強い" in summary['signal']:
                    self.app.bell(); original_style = analysis_widget.styles.border; alert_color = "green" if "買い" in summary['signal'] else "red"
                    analysis_widget.styles.border = (alert_color, alert_color); self.set_timer(1.0, lambda: self.reset_border_style(analysis_widget, original_style))
    def reset_border_style(self, widget: Static, original_style) -> None: widget.styles.border = original_style
    def action_quit(self) -> None: self.exit()
    def action_toggle_pause(self) -> None:
        self.is_paused = not self.is_paused
        if self.is_paused: self.show_flash_message("[yellow]一時停止中... (Pキーで再開)", duration=9999); self.update_timer.pause()
        else: self.clear_flash_message(); self.update_timer.resume()

# --- ★★★ここからが修正ブロック★★★ ---
# --- 環境起動 司令塔 ---
def launch_environment(ticker_code_to_set: str):
    """
    Excelを起動し、指定された銘柄コードをセルに書き込んでから、
    データ収集スクリプトを起動する。
    """
    print(f">>> ステップ1: Excelを起動し、銘柄コードを {ticker_code_to_set} に更新します...")
    try:
        excel = win32com.client.Dispatch("Excel.Application")
        excel.Visible = True
        
        # --- アドインの堅牢な処理ロジック ---
        target_addin_name = os.path.basename(EXCEL_ADDIN_PATH)
        target_addin = None
        
        # 1. 既に登録済みのアドインを探す
        for addin in excel.AddIns:
            if os.path.basename(addin.FullName) == target_addin_name:
                target_addin = addin
                print(f">>> アドイン '{target_addin_name}' は既に登録されています。")
                break
        
        # 2. 登録されていなければ、新規に登録する
        if target_addin is None:
            print(f">>> アドイン '{target_addin_name}' が未登録です。新規登録を試みます...")
            target_addin = excel.AddIns.Add(EXCEL_ADDIN_PATH, CopyFile=True)
            print(">>> アドインの登録に成功しました。")

        # 3. アドインが有効(Installed)でなければ、有効化する
        if not target_addin.Installed:
            print(">>> アドインを有効化します...")
            target_addin.Installed = True
            print(">>> アドインが有効になりました。")
        else:
            print(">>> アドインは既に有効です。")
            
        # --- ワークブックの処理 ---
        target_wb = None
        for wb in excel.Workbooks:
            if wb.FullName == EXCEL_WORKBOOK_PATH:
                target_wb = wb
                break
        if not target_wb:
            target_wb = excel.Workbooks.Open(EXCEL_WORKBOOK_PATH)

        # シートを選択し、指定セルに銘柄コードを書き込む
        ws = target_wb.Sheets(EXCEL_SHEET_NAME_TICKER)
        ws.Range(EXCEL_TICKER_CELL).Value = ticker_code_to_set
        target_wb.Save()
        print(f">>> Excelシート '{EXCEL_SHEET_NAME_TICKER}' のセル {EXCEL_TICKER_CELL} を {ticker_code_to_set} に更新し、保存しました。")

    except Exception as e:
        print(f"XXX Excel操作中にエラーが発生しました: {e}")
        print("XXX 続行しますが、データが正しく取得できない可能性があります。")
        print("XXX Excelのパス、アドインのパスが正しいか、Excelが手動で開けるか確認してください。")
    
    # ステップ2: データ収集スクリプトの起動
    print(">>> ステップ2: データ収集スクリプトをバックグラウンドで起動します...")
    try:
        subprocess.Popen(['pythonw', DATA_IMPORTER_SCRIPT_PATH])
        print(">>> データ収集スクリプトを起動しました。")
    except Exception as e:
        print(f"XXX スクリプト起動失敗: {e}")
# ★★★修正ブロックここまで★★★

# --- メイン実行ブロック (変更なし) ---
if __name__ == "__main__":
    ticker_pattern = re.compile(r"^\d{4}(\.(JNX|CIX))?$", re.IGNORECASE)
    while True:
        ticker_code_input = input("監視したい銘柄コードを入力してください (例: 3350, 3350.JNX, 3350.CIX): ")
        cleaned_input = ticker_code_input.strip()
        if ticker_pattern.match(cleaned_input):
            ticker_to_run = cleaned_input.upper()
            break
        else:
            print("エラー: 「4桁の数字」または「4桁の数字.JNX/CIX」の形式で入力してください。")
            
    launch_environment(ticker_code_to_set=ticker_to_run)
    
    print(f">>> 監視対象銘柄: {ticker_to_run}")
    print(">>> 5秒後にTUI起動")
    sleep_timer.sleep(5)
    
    app = TraderApp(ticker_code=ticker_to_run)
    app.run()
