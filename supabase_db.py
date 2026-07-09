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
        # ลบด้วย truncate แทน (ไม่ต้องพึ่ง id column)
        try:
            client.rpc("truncate_table", {"tbl": table_name}).execute()
        except Exception:
            # fallback: ลบทีละแถว
            try:
                existing = client.table(table_name).select("*").execute()
                if existing.data:
                    # หา primary key จาก column แรก
                    first_col = list(existing.data[0].keys())[0]
                    for row in existing.data:
                        try:
                            client.table(table_name).delete().eq(
                                first_col, row[first_col]
                            ).execute()
                        except Exception:
                            pass
            except Exception:
                pass

        if not df.empty:
            records = df.to_dict(orient="records")
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
    """Supabase — ตรวจ connection เท่านั้น"""
    try:
        client = _get_client()
        # ทดสอบ connection
        client.table("branches").select("branch_id").limit(1).execute()
    except Exception:
        pass
