import pandas as pd
from openpyxl import Workbook

# Excelファイルのパスを指定
input_excel_path = 'c:/ayumi/updated_data.xlsx'  # 入力ファイルのパスに変更してください
output_excel_path = 'c:/ayumi/consolidated_data.xlsx'  # 新しい出力ファイルのパス

# 新しいワークブックとシートを作成
wb = Workbook()
ws_consolidated = wb.active
ws_consolidated.title = "Consolidated"

# パンダスでExcelファイルを読み込む
xls = pd.ExcelFile(input_excel_path)

# すべてのシートをループ処理
first_sheet_processed = False
for sheet_name in xls.sheet_names:
    # シートごとにDataFrameを読み込む
    df = pd.read_excel(xls, sheet_name=sheet_name)
    if not first_sheet_processed:
        # 最初のシートからヘッダーを追加
        ws_consolidated.append(df.columns.tolist())
        first_sheet_processed = True
    # データ行を追加
    for row in df.itertuples(index=False):
        ws_consolidated.append(row)

# 新しいExcelファイルに保存
wb.save(output_excel_path)
print(f'データが結合され、{output_excel_path} に保存されました。')
