"""
excel_db.py  –  ฟังก์ชันกลางสำหรับอ่าน / เขียน Excel Workbook
"""
import os
import pandas as pd
from openpyxl import load_workbook, Workbook
from config import DB_PATH, ALL_SHEETS


# ──────────────────────────────────────────────
# สร้าง Workbook ถ้ายังไม่มีไฟล์
# ──────────────────────────────────────────────
def init_workbook():
    if os.path.exists(DB_PATH):
        # เพิ่ม sheet ที่ขาดหายไปเท่านั้น ห้ามลบข้อมูลเดิม
        wb = load_workbook(DB_PATH)
        changed = False
        for sheet in ALL_SHEETS:
            if sheet not in wb.sheetnames:
                wb.create_sheet(sheet)
                changed = True
        if changed:
            wb.save(DB_PATH)
        wb.close()
    else:
        wb = Workbook()
        # ลบ default sheet
        if "Sheet" in wb.sheetnames:
            del wb["Sheet"]
        for sheet in ALL_SHEETS:
            wb.create_sheet(sheet)
        wb.save(DB_PATH)
        wb.close()


# ──────────────────────────────────────────────
# อ่าน Sheet → DataFrame
# ──────────────────────────────────────────────
def read_sheet(sheet_name: str) -> pd.DataFrame:
    init_workbook()
    try:
        df = pd.read_excel(DB_PATH, sheet_name=sheet_name, dtype=str)
        df = df.fillna("")
        return df
    except Exception:
        return pd.DataFrame()


# ──────────────────────────────────────────────
# เขียน DataFrame → Sheet (overwrite sheet เดิม)
# ──────────────────────────────────────────────
def write_sheet(sheet_name: str, df: pd.DataFrame):
    init_workbook()
    wb = load_workbook(DB_PATH)
    if sheet_name in wb.sheetnames:
        del wb[sheet_name]
    ws = wb.create_sheet(sheet_name)

    # Header
    for col_idx, col_name in enumerate(df.columns, 1):
        ws.cell(row=1, column=col_idx, value=col_name)

    # Data rows
    for row_idx, row in enumerate(df.itertuples(index=False), 2):
        for col_idx, val in enumerate(row, 1):
            ws.cell(row=row_idx, column=col_idx, value=val)

    wb.save(DB_PATH)
    wb.close()


# ──────────────────────────────────────────────
# เพิ่มแถว (append)
# ──────────────────────────────────────────────
def append_row(sheet_name: str, row_dict: dict):
    df = read_sheet(sheet_name)
    new_row = pd.DataFrame([row_dict])
    df = pd.concat([df, new_row], ignore_index=True)
    write_sheet(sheet_name, df)


# ──────────────────────────────────────────────
# อัปเดตแถวตาม id_col + id_value
# ──────────────────────────────────────────────
def update_row(sheet_name: str, id_col: str, id_value: str, updated_dict: dict):
    df = read_sheet(sheet_name)
    mask = df[id_col].astype(str) == str(id_value)
    for key, val in updated_dict.items():
        df.loc[mask, key] = str(val) if val is not None else ""
    write_sheet(sheet_name, df)


# ──────────────────────────────────────────────
# ลบแถวตาม id_col + id_value
# ──────────────────────────────────────────────
def delete_row(sheet_name: str, id_col: str, id_value: str):
    df = read_sheet(sheet_name)
    df = df[df[id_col].astype(str) != str(id_value)]
    write_sheet(sheet_name, df)
