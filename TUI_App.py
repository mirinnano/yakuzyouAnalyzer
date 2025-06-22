# coding: utf-8
import pandas as pd
import os
import sqlite3
import time as sleep_timer
import subprocess
import re
from collections import deque
import sys
import win32com.client
from textual.app import App, ComposeResult
from textual.widgets import Header, Footer, Static, DataTable, Input, Button
from textual.containers import VerticalScroll, Horizontal
from textual.screen import ModalScreen
from textual.binding import Binding
from rich.text import Text
from rich.table import Table
from rich.layout import Layout
from rich.panel import Panel
from rich.console import Group
import numpy as np

# --- 基本設定 ---
AYUMI_BASE_DIR = r"C:\ayumi"

# --- 必要なディレクトリを起動時に自動作成 ---
try:
    os.makedirs(AYUMI_BASE_DIR, exist_ok=True)
    print(f"INFO: データディレクトリ '{AYUMI_BASE_DIR}' を確認・作成しました。")
except OSError as e:
    print(f"エラー: ディレクトリの作成に失敗しました: {AYUMI_BASE_DIR}")
    print(f"詳細: {e}")
    input("Enterキーを押して終了します...")
    sys.exit(1)

# --- ヘルパー関数: リソースパスの解決 ---
def resource_path(relative_path: str) -> str:
    """
    実行可能ファイル(.exe)にパッケージされたリソースへのパスを取得する。
    開発時と実行時でパスの解決方法を切り替える。
    """
    try:
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)

# --- パス設定 (AYUMI_BASE_DIRを基準に) ---
EXCEL_WORKBOOK_PATH = os.path.join(AYUMI_BASE_DIR, "ayumi.xlsm")
DB_PATH = os.path.join(AYUMI_BASE_DIR, "market_data.db")
DATA_IMPORTER_SCRIPT_PATH = resource_path("ayumisql.py")
EXCEL_ADDIN_PATH = os.path.expandvars(r"%LOCALAPPDATA%\MarketSpeed2\Bin\rss\MarketSpeed2_RSS_64bit.xll")

# --- 定数 ---
EXCEL_SHEET_NAME_TICKER = 'Sheet1'
EXCEL_TICKER_CELL = 'E4'
VERIFICATION_CELL = 'F4'

def format_yen(value: float) -> str:
    if value >= 1_0000_0000: return f"{value / 1_0000_0000:,.1f}億円"
    if value >= 1_0000: return f"{value / 1_0000:,.0f}万円"
    return f"{value:,.0f}円"

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
        total_buy_volume = pivot['買い'].sum()
        total_sell_volume = pivot['売り'].sum()
        total_volume_for_ratio = total_buy_volume + total_sell_volume
        buy_ratio = total_buy_volume / total_volume_for_ratio if total_volume_for_ratio > 0 else 0
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
        summary = {'total_volume': int(df['出来高'].sum()), 'breakdown': pivot, 'signal': signal, 'confidence': confidence, 'condition': condition, 'metrics': metrics, 'thresholds_yen': {'medium': m_th, 'large': l_th, 'super_large': s_th}, 'buy_ratio': buy_ratio}
        return {'summary': summary, 'detail_df': df.tail(self.window_size)}

class TradeLogWidget(Static):
    def compose(self) -> ComposeResult: yield DataTable()
    def on_mount(self) -> None:
        self.border_title = "リアルタイム約定ログ"; tbl = self.query_one(DataTable); tbl.cursor_type = "row"
        tbl.add_columns("時刻","価格","出来高","方向","ロット")

    def update_log(self, df: pd.DataFrame|None) -> None:
        tbl = self.query_one(DataTable); tbl.clear()
        if df is None or df.empty: return
        df_display = df.tail(500).iloc[::-1]
        rows = []
        for _, r in df_display.iterrows():
            time_str = r['時刻'].strftime('%H:%M:%S') if pd.notnull(r['時刻']) else "N/A"
            base_style = 'red' if r['方向'] == '買い' else 'yellowgreen'
            if r['ロット'] == '大口': final_style = f"bold {base_style}"
            elif r['ロット'] == '超大口': final_style = 'bold bright_magenta' if r['方向'] == '買い' else 'bold yellow'
            else: final_style = base_style
            rows.append((
                Text(time_str, style=final_style), Text(f"{r['価格']:,}", style=final_style),
                Text(f"{int(r['出来高']):,}", style=final_style), Text(r['方向'], style=final_style),
                Text(str(r['ロット']), style=final_style),
            ))
        tbl.add_rows(rows)
        tbl.scroll_home(animate=False)

    def clear_log(self) -> None:
        """ログテーブルをクリアする"""
        self.query_one(DataTable).clear()

class TradeAnalysisWidget(Static):
    def on_mount(self) -> None:
        self.border_title = "インテリジェント約定分析"; self.analysis_layout = Layout(); self.analysis_layout.split_column(Layout(name="header", size=5), Layout(name="main"))
        self.analysis_layout["main"].split_row(Layout(name="metrics"), Layout(name="breakdown")); self.update(self.analysis_layout)
    def _create_ratio_bar(self, buy_ratio: float, width: int = 40) -> Table:
        buy_width = int(buy_ratio * width)
        sell_width = width - buy_width
        bar_text = Text().append("█" * buy_width, style="bold green").append("█" * sell_width, style="bold red")
        grid = Table.grid(expand=True)
        grid.add_column(justify="right", no_wrap=True); grid.add_column(width=width, justify="center"); grid.add_column(justify="left", no_wrap=True)
        grid.add_row(Text(f"買い {buy_ratio:.1%}", style="bold green"), bar_text, Text(f"{1 - buy_ratio:.1%} 売り", style="bold red"))
        return grid
    def update_analysis(self, analysis: dict|None) -> None:
        layout = self.analysis_layout
        if not analysis:
            self.update(Panel("分析データを待っています...", style="bold dim"))
            return
        summary, metrics = analysis, analysis['metrics']; sig, conf, cond = summary['signal'], summary['confidence'], summary['condition']
        style = 'bold green' if '買い' in sig else 'bold red' if '売り' in sig else 'bold white'
        header_table = Table.grid(expand=True); header_table.add_column(justify="left"); header_table.add_column(justify="right")
        header_table.add_row(f"[bold]推奨シグナル: [{style}]{sig}[/{style}][/]", f"信頼度: {'★'*conf}{'☆'*(10-conf)}"); header_table.add_row(f"[bold]市場コンディション: [cyan]{cond}[/]", f"総出来高: {summary['total_volume']:,}株")
        layout["header"].update(Panel(header_table, title="判定", border_style="blue"))
        metrics_table = Table.grid(padding=(0, 1)); metrics_table.add_column(); metrics_table.add_column(justify="right")
        metrics_table.add_row("[bold]VWAP:", f"[yellow]{metrics['vwap']:,.2f}[/]"); metrics_table.add_row("[bold]ボラティリティ:", f"[cyan]{metrics['volatility']:,.2f}[/]"); metrics_table.add_row("[bold]取引密度/分:", f"[magenta]{metrics['trade_density_per_min']:.1f}回[/]"); metrics_table.add_row("[bold]平均出来高/約定:", f"[green]{metrics['avg_volume_per_trade']:,.0f}株[/]")
        layout["metrics"].update(Panel(metrics_table, title="市場指標", border_style="green"))
        buy_ratio = summary.get('buy_ratio', 0)
        ratio_bar_table = self._create_ratio_bar(buy_ratio)
        breakdown_table = Table(title="ロット別出来高", header_style="bold magenta", show_header=True, expand=True)
        breakdown_table.add_column("ロット", justify="left", style="cyan"); breakdown_table.add_column("約定代金レンジ", justify="left", style="dim white", max_width=25); breakdown_table.add_column("買い", justify="right", style="green"); breakdown_table.add_column("売り", justify="right", style="red"); breakdown_table.add_column("差引", justify="right")
        thresholds = summary['thresholds_yen']; m_th, l_th, s_th = thresholds['medium'], thresholds['large'], thresholds['super_large']
        ranges = {'小口': f"~ {format_yen(m_th)}", '中口': f"{format_yen(m_th)} ~ {format_yen(l_th)}", '大口': f"{format_yen(l_th)} ~ {format_yen(s_th)}", '超大口': f"{format_yen(s_th)} ~"}
        for lot_name, row in summary['breakdown'].iterrows():
            b, s, n = int(row['買い']), int(row['売り']), int(row['差引']); ns = 'bold green' if n > 0 else 'bold red' if n < 0 else 'white'; range_str = ranges.get(lot_name, "N/A")
            breakdown_table.add_row(lot_name, range_str, f"{b:,}", f"{s:,}", f"[{ns}]{n:+,}[/{ns}]")
        breakdown_content = Group(Panel(ratio_bar_table, title="全体出来高比率", border_style="cyan"), breakdown_table)
        layout["breakdown"].update(Panel(breakdown_content, border_style="yellow", title="売買分析"))
        self.update(layout)

    def clear_analysis(self) -> None:
        """分析パネルを初期状態に戻す"""
        self.update_analysis(None)

class ChangeTickerScreen(ModalScreen):
    """銘柄コードを変更するためのモーダル画面"""
    def compose(self) -> ComposeResult:
        with VerticalScroll(id="change_ticker_dialog"):
            yield Static("新しい銘柄コードを入力してください (例: 3350, 5721.JNX)", id="change_ticker_title")
            yield Input(placeholder="4桁の数字 or 4桁.JNX/CIX", id="ticker_input")
            with Horizontal(id="change_ticker_buttons"):
                yield Button("変更", variant="primary", id="apply_change")
                yield Button("キャンセル", id="cancel_change")

    def on_mount(self) -> None:
        """マウント時にインプットウィジェットにフォーカスする"""
        self.query_one(Input).focus()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "apply_change":
            ticker_input = self.query_one("#ticker_input", Input)
            new_ticker = ticker_input.value.strip().upper() # 大文字に統一
            # ★★ ここが修正箇所 1/2 ★★
            if re.match(r"^\d{4}(\.(JNX|CIX))?$", new_ticker, re.IGNORECASE):
                self.dismiss(new_ticker)
            else:
                ticker_input.border_title = "[bold red]無効な形式です[/]"
                ticker_input.styles.border = ("round", "red")
        else:
            self.dismiss(None)


class TraderApp(App):
    BINDINGS = [
        Binding("q", "quit", "終了"),
        Binding("p", "toggle_pause", "一時停止/再開"),
        Binding("c", "change_ticker", "銘柄変更"),
    ]
    def __init__(self, ticker_code: str, background_process=None, excel_instance=None):
        super().__init__()
        self.target_ticker = ticker_code
        self.analyzer = TradeAnalyzer()
        self.last_id = 0
        self.df_history = pd.DataFrame()
        self.is_paused = False
        self.update_timer = None
        self.footer_message_timer = None
        self.trade_counts = deque(maxlen=30)
        self.background_process = background_process
        self.excel_instance = excel_instance
        self.db_connection = None
    CSS = ("Screen{layout:grid;grid-size:2;grid-columns:1fr 2fr;grid-gutter:1;padding:1;background:#1e1f22;} #trade-log,#trade-analysis{border:round #4a4a4a;background:#2f3136;padding:1;overflow:auto;height:100%;} #trade-analysis{padding:0;}")
    def compose(self) -> ComposeResult: yield Header(show_clock=True); yield TradeLogWidget(id="trade-log"); yield TradeAnalysisWidget(id="trade-analysis"); yield Footer()
    def on_mount(self) -> None:
        try:
            self.db_connection = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=10.0, check_same_thread=False)
            self.db_connection.execute("PRAGMA journal_mode=WAL;")
            self.log(">>> データベース接続をWALモード(Read-Only)で確立しました。")
        except sqlite3.Error as e:
            self.show_flash_message(f"[bold red]!!! DB接続エラー: {e}[/]", duration=9999); return
        self.update_panels(); self.update_timer = self.set_interval(2, self.update_panels)
    def on_unmount(self) -> None:
        if self.db_connection: self.db_connection.close(); self.log(">>> データベース接続を解放しました。")
    async def on_ready(self) -> None:
        try:
            header = self.query_one(Header)
            header.tall = True
            header.header_title = f"統合トレーディング環境\n銘柄: [{self.target_ticker}]"
        except Exception:
            pass

    def update_status(self, message: str, color: str = "gray"):
        """フッターの左側に通常メッセージを表示する。フラッシュメッセージ表示中は更新しない。"""
        if self.footer_message_timer is not None:
            return
        self.query_one(Footer).show_bindings = True; self.sub_title = f"[{color}]{message}[/{color}]"

    def show_flash_message(self, message: str, duration: float = 8.0):
        """フッターの左側に一時的なメッセージ（フラッシュメッセージ）を表示する。"""
        self.sub_title = message
        if self.footer_message_timer is not None:
            self.footer_message_timer.stop()
        self.footer_message_timer = self.set_timer(duration, self.clear_flash_message)

    def clear_flash_message(self):
        """フラッシュメッセージをクリアする。"""
        self.sub_title = ""
        self.footer_message_timer = None
        self.update_panels()

    def analyze_latest_ticks(self, new_df: pd.DataFrame, last_summary: dict | None):
        if new_df.empty or last_summary is None: return
        self.trade_counts.append(len(new_df))
        avg_trade_count = sum(self.trade_counts) / len(self.trade_counts) if self.trade_counts else 0
        if len(new_df) > avg_trade_count * 5 and len(new_df) > 5:
            buy_ratio = (new_df['方向'] == '買い').sum() / len(new_df)
            if buy_ratio > 0.8:
                self.show_flash_message(f"[bold green]!![/bold green] [white]高密度な[red]買いバースト[/red]を検知 ({len(new_df)}件)[/white]")
            elif buy_ratio < 0.2:
                self.show_flash_message(f"[bold red]!![/bold red] [white]高密度な[yellowgreen]売りバースト[/yellowgreen]を検知 ({len(new_df)}件)[/white]")
    def update_panels(self) -> None:
        if self.is_paused or not self.db_connection: return
        last_summary = self.analyzer.analyze(self.df_history) if not self.df_history.empty else None
        new_df = pd.DataFrame()
        try:
            query = "SELECT id, jikoku AS 時刻, price AS 価格, dekidaka AS 出来高, baibai AS 方向 FROM ayumi WHERE ticker_code = ? AND id > ? ORDER BY id"
            new_df = pd.read_sql_query(query, self.db_connection, params=(self.target_ticker, self.last_id))
            status_message = f"最終確認: {pd.Timestamp.now().strftime('%H:%M:%S')} | 新規約定: {len(new_df)}件"
            self.update_status(status_message, color="white" if not new_df.empty else "gray")
        except sqlite3.Error as e:
            self.show_flash_message(f"[bold red]!!! データベースエラー: {e}[/]"); self.log(f"!!! データベースエラー: {e}"); return
        self.analyze_latest_ticks(new_df, last_summary)
        log_widget = self.query_one(TradeLogWidget); analysis_widget = self.query_one(TradeAnalysisWidget)
        log_widget.border_title = f"リアルタイム約定ログ [{self.target_ticker}]"; analysis_widget.border_title = f"インテリジェント約定分析 [{self.target_ticker}]"
        if new_df.empty and self.df_history.empty:
            analysis_widget.update_analysis(None)
            return
        elif not new_df.empty:
            self.df_history = pd.concat([self.df_history, new_df]).tail(10000)
            self.last_id = int(self.df_history['id'].max())
        if not self.df_history.empty:
            res = self.analyzer.analyze(self.df_history)
            if res:
                log_widget.update_log(res['detail_df']); analysis_widget.update_analysis(res['summary'])
                summary = res['summary']
                if summary['confidence'] >= 7 and "強い" in summary['signal']:
                    self.app.bell(); original_style = analysis_widget.styles.border; alert_color = "green" if "買い" in summary['signal'] else "red"
                    analysis_widget.styles.border = (alert_color, alert_color); self.set_timer(1.0, lambda: self.reset_border_style(analysis_widget, original_style))
    def reset_border_style(self, widget: Static, original_style) -> None: widget.styles.border = original_style
    def action_toggle_pause(self) -> None:
        self.is_paused = not self.is_paused
        if self.is_paused: self.show_flash_message("[yellow]一時停止中...[/]", duration=9999); self.update_timer.pause(); self.update_status("一時停止中", color="yellow")
        else: self.clear_flash_message(); self.update_timer.resume(); self.update_panels()

    def action_change_ticker(self) -> None:
        """銘柄変更モーダルを表示する"""
        def handle_new_ticker(new_ticker: str | None):
            if new_ticker:
                # バックグラウンドで変更処理を実行
                self.run_worker(self.process_ticker_change(new_ticker), exclusive=True)
        self.push_screen(ChangeTickerScreen(), handle_new_ticker)

    async def process_ticker_change(self, new_ticker: str):
        """銘柄変更の全プロセスを管理する"""
        # 1. 現在の処理を一時停止
        self.show_flash_message(f"[yellow]銘柄を {new_ticker} に変更中...[/]", duration=9999)
        if self.update_timer: self.update_timer.pause()

        # 2. 既存のプロセスと接続をクリーンアップ
        self.log(f">>> 銘柄変更開始: {self.target_ticker} -> {new_ticker}")
        if self.background_process:
            self.log(">>> 古いデータ収集スクリプトを終了します...")
            self.background_process.terminate()
            self.background_process.wait(timeout=3)
        if self.db_connection:
            self.log(">>> データベース接続を解放します...")
            self.db_connection.close()
            self.db_connection = None

        # 3. TUIの状態をリセット
        self.log(">>> UIの状態をリセットします...")
        self.target_ticker = new_ticker
        self.df_history = pd.DataFrame()
        self.last_id = 0
        self.query_one(TradeLogWidget).clear_log()
        self.query_one(TradeAnalysisWidget).clear_analysis()
        self.query_one(Header).header_title = f"統合トレーディング環境\n銘柄: [{self.target_ticker}]"

        # 4. 新しい銘柄で環境を再起動
        self.log(f">>> {new_ticker} で環境を再起動します...")
        _, self.background_process = launch_environment(
            ticker_code_to_set=new_ticker,
            excel_instance=self.excel_instance
        )

        # 5. DBに再接続し、ポーリングを再開
        sleep_timer.sleep(3) # 新しいプロセスがDBを準備するのを少し待つ
        try:
            self.db_connection = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=10.0, check_same_thread=False)
            self.db_connection.execute("PRAGMA journal_mode=WAL;")
            self.log(">>> 新しいデータベース接続を確立しました。")
            if self.update_timer: self.update_timer.resume()
            self.clear_flash_message()
            self.show_flash_message(f"[bold green]銘柄が {new_ticker} に変更されました。[/]", duration=5)
        except sqlite3.Error as e:
            self.show_flash_message(f"[bold red]!!! DB再接続エラー: {e}[/]", duration=9999)

    def action_quit(self) -> None:
        self.log("\n>>> アプリケーションを終了しています...")
        if self.background_process:
            try:
                self.log(">>> データ収集スクリプトを終了します...")
                self.background_process.terminate()
                self.background_process.wait(timeout=3)
                self.log(">>> スクリプトを終了しました。")
            except Exception as e:
                self.log(f"XXX スクリプトの終了中にエラー: {e}")
        if self.excel_instance:
            try:
                self.log(">>> Excelへの接続を解放します...")
                # 参照を解放
                self.excel_instance = None
            except Exception as e:
                self.log(f"XXX Excelの解放中にエラー: {e}")
        self.exit("ユーザー操作により終了しました。")


def launch_environment(ticker_code_to_set: str, excel_instance=None):
    """
    Excelを起動または再利用し、指定された銘柄コードをセルに書き込んでから、
    データ収集スクリプトを起動し、それらのプロセス情報を返す。
    """
    print(f">>> ステップ1: Excelを操作し、銘柄コードを {ticker_code_to_set} に更新します...")
    excel_app = excel_instance
    background_proc = None
    try:
        if excel_app is None:
            excel_app = win32com.client.Dispatch("Excel.Application")
            excel_app.Visible = True

        target_addin_name = os.path.basename(EXCEL_ADDIN_PATH)
        target_addin = None
        for addin in excel_app.AddIns:
            if os.path.basename(addin.FullName) == target_addin_name:
                target_addin = addin
                break
        if target_addin is None:
            target_addin = excel_app.AddIns.Add(EXCEL_ADDIN_PATH, CopyFile=True)

        if not target_addin.Installed:
            target_addin.Installed = True
        
        target_wb = None
        for wb in excel_app.Workbooks:
            try:
                if wb.FullName == EXCEL_WORKBOOK_PATH:
                    target_wb = wb
                    break
            except Exception:
                continue

        if not target_wb:
            target_wb = excel_app.Workbooks.Open(EXCEL_WORKBOOK_PATH)
        
        ws = target_wb.Sheets(EXCEL_SHEET_NAME_TICKER)
        ws.Range(EXCEL_TICKER_CELL).Value = ticker_code_to_set
        # target_wb.Save() # 頻繁な変更で問題を起こす可能性があるためコメントアウト
        print(f">>> Excelシート '{EXCEL_SHEET_NAME_TICKER}' のセル {EXCEL_TICKER_CELL} を {ticker_code_to_set} に更新しました。")
    except Exception as e:
        print(f"XXX Excel操作中にエラーが発生しました: {e}")

    print(">>> ステップ2: データ収集スクリプトをバックグラウンドで起動します...")
    try:
        background_proc = subprocess.Popen(['pythonw', DATA_IMPORTER_SCRIPT_PATH])
        print(">>> データ収集スクリプトを起動しました。")
    except Exception as e:
        print(f"XXX スクリプト起動失敗: {e}")
        
    return excel_app, background_proc

# --- メイン実行ブロック ---
if __name__ == "__main__":
    # ★★ ここが修正箇所 2/2 ★★
    ticker_pattern = re.compile(r"^\d{4}(\.(JNX|CIX))?$", re.IGNORECASE)
    while True:
        ticker_code_input = input("監視したい銘柄コードを入力してください (例: 3350, 5721.JNX): ")
        cleaned_input = ticker_code_input.strip().upper() # 大文字に統一
        if ticker_pattern.match(cleaned_input):
            ticker_to_run = cleaned_input
            break
        else:
            print("エラー: 「4桁の数字」または「4桁の数字.JNX/CIX」の形式で入力してください。")
    
    excel_instance, bg_process = launch_environment(ticker_code_to_set=ticker_to_run)
    
    print(f">>> 監視対象銘柄: {ticker_to_run}")
    print(">>> 5秒後にTUI起動")
    sleep_timer.sleep(5)
    
    app = TraderApp(
        ticker_code=ticker_to_run,
        background_process=bg_process,
        excel_instance=excel_instance
    )
    app.run()
    print(">>> TUIアプリケーションが終了しました。")