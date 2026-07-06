"""
gsheets_db.py  –  Google Sheets Database Layer
แทน excel_db.py เมื่อใช้ Google Sheets เป็น Database กลาง
"""
import json
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import streamlit as st

SPREADSHEET_ID = "1yNGEsrhdlwJ4rB88hYjNghitmro8MfPc4_9XJKATn1E"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# ── ดึง credentials จาก Streamlit Secrets หรือ local file ──
@st.cache_resource
def _get_client():
    try:
        # บน Streamlit Cloud → ดึงจาก st.secrets
        creds_dict = dict(st.secrets["gcp_service_account"])
        creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    except Exception:
        # Local → ดึงจากไฟล์ JSON
        import os
        json_path = os.path.join(os.path.dirname(__file__), "credentials.json")
        creds = Credentials.from_service_account_file(json_path, scopes=SCOPES)
    return gspread.authorize(creds)


def _get_spreadsheet():
    client = _get_client()
    return client.open_by_key(SPREADSHEET_ID)


def _get_or_create_sheet(sheet_name: str):
    """เปิด Sheet หรือสร้างใหม่ถ้ายังไม่มี"""
    ss = _get_spreadsheet()
    try:
        return ss.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        ws = ss.add_worksheet(title=sheet_name, rows=1000, cols=50)
        return ws


# ── READ ────────────────────────────────────────────────────
def read_sheet(sheet_name: str) -> pd.DataFrame:
    try:
        ws = _get_or_create_sheet(sheet_name)
        data = ws.get_all_records(default_blank="")
        if not data:
            # Sheet ว่าง — ดึง header แถวแรก
            header = ws.row_values(1)
            if header:
                return pd.DataFrame(columns=header)
            return pd.DataFrame()
        df = pd.DataFrame(data)
        return df.fillna("")
    except Exception as e:
        return pd.DataFrame()


# ── WRITE (overwrite sheet) ──────────────────────────────────
def write_sheet(sheet_name: str, df: pd.DataFrame):
    try:
        ws = _get_or_create_sheet(sheet_name)
        ws.clear()
        if df.empty:
            if len(df.columns) > 0:
                ws.append_row(list(df.columns))
            return
        # เขียน header + data
        data = [list(df.columns)] + df.astype(str).values.tolist()
        ws.update(range_name="A1", values=data)
    except Exception as e:
        raise e


# ── APPEND ROW ───────────────────────────────────────────────
def append_row(sheet_name: str, row_dict: dict):
    try:
        ws = _get_or_create_sheet(sheet_name)
        # ตรวจ header
        header = ws.row_values(1)
        if not header:
            # ยังไม่มี header — เพิ่มก่อน
            ws.append_row(list(row_dict.keys()))
            header = list(row_dict.keys())
        # เรียงค่าตาม header
        row_values = [str(row_dict.get(col, "")) for col in header]
        ws.append_row(row_values)
    except Exception as e:
        raise e


# ── UPDATE ROW ───────────────────────────────────────────────
def update_row(sheet_name: str, id_col: str, id_value: str, updated_dict: dict):
    df = read_sheet(sheet_name)
    if df.empty:
        return
    mask = df[id_col].astype(str) == str(id_value)
    for key, val in updated_dict.items():
        df.loc[mask, key] = str(val) if val is not None else ""
    write_sheet(sheet_name, df)


# ── DELETE ROW ───────────────────────────────────────────────
def delete_row(sheet_name: str, id_col: str, id_value: str):
    df = read_sheet(sheet_name)
    if df.empty:
        return
    df = df[df[id_col].astype(str) != str(id_value)]
    write_sheet(sheet_name, df)


# ── INIT WORKBOOK ────────────────────────────────────────────
def init_workbook():
    """
    สร้าง Sheet ที่ยังไม่มีเท่านั้น — ไม่ตรวจ header ทุก Sheet
    เพื่อลด API calls ป้องกัน quota exceeded
    """
    from config import ALL_SHEETS
    import time

    try:
        ss       = _get_spreadsheet()
        existing = set(ws.title for ws in ss.worksheets())

        # สร้างเฉพาะ Sheet ที่ขาด — ทีละ Sheet พักเล็กน้อย
        missing = [s for s in ALL_SHEETS if s not in existing]
        for i, sheet_name in enumerate(missing):
            try:
                ss.add_worksheet(title=sheet_name, rows=1000, cols=50)
                if i > 0 and i % 5 == 0:
                    time.sleep(1)  # พัก 1 วินาทีทุก 5 sheets
            except Exception:
                pass
    except Exception as e:
        # quota exceeded หรือ network error — ข้ามไปก่อน
        pass


def init_sheet_headers(sheet_name: str, columns: list):
    """
    สร้าง header ใน Sheet ถ้ายังว่างอยู่
    เรียกเฉพาะตอนที่จะใช้ Sheet นั้นจริงๆ (lazy init)
    """
    try:
        ws     = _get_or_create_sheet(sheet_name)
        header = ws.row_values(1)
        if not header and columns:
            ws.append_row(columns)
    except Exception:
        pass
