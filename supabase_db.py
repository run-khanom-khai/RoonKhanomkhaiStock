"""
supabase_db.py  –  Supabase (PostgreSQL) Database Layer
เร็วกว่า Google Sheets 10-20 เท่า ไม่มี quota limit
"""
import pandas as pd
import streamlit as st
from supabase import create_client, Client

SUPABASE_URL = "https://yyodvbzuabcwyytxeyng.supabase.co"
SUPABASE_KEY = "sb_publishable_tKTUMX116_h4arRjWpGNvQ_HQAiU01z"


@st.cache_resource
def _get_client() -> Client:
    try:
        url = st.secrets["supabase"]["url"]
        key = st.secrets["supabase"]["key"]
    except Exception:
        url = SUPABASE_URL
        key = SUPABASE_KEY
    return create_client(url, key)


def read_sheet(table_name: str) -> pd.DataFrame:
    """อ่านข้อมูลจาก Supabase table"""
    try:
        client = _get_client()
        res = client.table(table_name).select("*").execute()
        if res.data:
            return pd.DataFrame(res.data).fillna("")
        return pd.DataFrame()
    except Exception:
        return pd.DataFrame()


def write_sheet(table_name: str, df: pd.DataFrame):
    """ลบข้อมูลเดิมแล้วเขียนใหม่ทั้งหมด"""
    try:
        client = _get_client()
        client.table(table_name).delete().neq("id", -1).execute()
        if not df.empty:
            records = df.to_dict(orient="records")
            # แบ่งเป็น batch 100 rows
            for i in range(0, len(records), 100):
                client.table(table_name).insert(records[i:i+100]).execute()
    except Exception as e:
        raise e


def append_row(table_name: str, row_dict: dict):
    """เพิ่มแถวใหม่"""
    try:
        client = _get_client()
        client.table(table_name).insert(row_dict).execute()
    except Exception as e:
        raise e


def update_row(table_name: str, id_col: str, id_value: str, updated_dict: dict):
    """อัปเดตแถวตาม id"""
    try:
        client = _get_client()
        client.table(table_name).update(updated_dict).eq(id_col, id_value).execute()
    except Exception as e:
        raise e


def delete_row(table_name: str, id_col: str, id_value: str):
    """ลบแถวตาม id"""
    try:
        client = _get_client()
        client.table(table_name).delete().eq(id_col, id_value).execute()
    except Exception as e:
        raise e


def init_workbook():
    """Supabase ไม่ต้อง init — tables สร้างผ่าน SQL Editor"""
    pass
