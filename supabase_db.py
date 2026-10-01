"""
supabase_db.py  –  Supabase (PostgreSQL) Database Layer
เร็วกว่า Google Sheets 10-20 เท่า ไม่มี quota limit
"""
import pandas as pd
import streamlit as st
from supabase import create_client, Client

# ⚠️ ความปลอดภัย (14/8/2026):
#   - ห้าม hardcode คีย์ Supabase ในโค้ดอีก (repo เป็นสาธารณะ = คีย์รั่ว)
#   - ใช้ "service_role secret key" เก็บใน Streamlit Secrets เท่านั้น
#     (แอปรันฝั่งเซิร์ฟเวอร์ คีย์ไม่หลุดไปเบราว์เซอร์ + service_role ข้าม RLS
#      ทำให้เปิด RLS แบบ deny-all ได้โดยแอปยังทำงานปกติ)
#   - ตั้งค่าใน Streamlit: Settings → Secrets
#         [supabase]
#         url = "https://xxxx.supabase.co"
#         key = "<service_role secret key>"


@st.cache_resource
def _get_client() -> Client:
    try:
        url = st.secrets["supabase"]["url"]
        key = st.secrets["supabase"]["key"]
    except Exception:
        # ไม่มีคีย์ใน Secrets → หยุดอย่างชัดเจน (fail-closed) ไม่ fallback ไปคีย์ในโค้ด
        st.error(
            "❌ ยังไม่ได้ตั้งค่าการเชื่อมต่อฐานข้อมูล (Supabase) อย่างปลอดภัย\n\n"
            "กรุณาไปที่ Streamlit → Settings → Secrets แล้วใส่:\n\n"
            "[supabase]\n"
            'url = \"https://<project>.supabase.co\"\n'
            'key = \"<service_role secret key>\"'
        )
        st.stop()
    return create_client(url, key)


@st.cache_data(ttl=60, show_spinner=False)
def read_sheet(table_name: str) -> pd.DataFrame:
    """อ่านข้อมูลจาก Supabase table — ดึงครบ 'ทุกแถว' (มีแคช 60 วินาที)

    เพิ่มแคช (st.cache_data) เพื่อความเร็ว: ไม่ต้องดึงข้อมูลใหม่ทุกคลิก
    - จำข้อมูลไว้ 60 วินาที → เมนู/รายงานเด้งเร็วขึ้นมาก
    - ทุกครั้งที่ 'บันทึก/แก้ไข/ลบ' ระบบจะล้างแคชทันที (ดู excel_db._bust_cache)
      จึงไม่มีปัญหาข้อมูลเก่าหลังบันทึก

    ⚠️ สำคัญมาก (แก้บั๊ก DV1397 duplicate key):
    PostgREST/Supabase จำกัดการอ่าน select("*") ไว้สูงสุด ~1000 แถวต่อครั้ง
    ตารางที่มีมากกว่า 1000 แถว (เช่น branch_sales_delivery) จะอ่านได้ไม่ครบ
    ทำให้ระบบมองไม่เห็นเลขล่าสุด → สร้างรหัสซ้ำ (เช่น DV1397 ซ้ำ)
    จึงต้องวนอ่านทีละหน้า (pagination) ด้วย .range() จนครบทุกแถว
    """
    try:
        client = _get_client()
        page_size = 1000
        start = 0
        all_rows = []
        while True:
            res = (
                client.table(table_name)
                .select("*")
                .range(start, start + page_size - 1)
                .execute()
            )
            batch = res.data or []
            all_rows.extend(batch)
            # ถ้าได้น้อยกว่าเต็มหน้า แปลว่าหมดแล้ว
            if len(batch) < page_size:
                break
            start += page_size
        if all_rows:
            return pd.DataFrame(all_rows).fillna("")
        return pd.DataFrame()
    except Exception:
        return pd.DataFrame()


def write_sheet(table_name: str, df: pd.DataFrame):
    """ลบข้อมูลเดิมแล้วเขียนใหม่ทั้งหมด
    ⚠️ กันข้อมูลหาย: ถ้า df ว่าง (เช่น การ init ตาราง/ตอน read ผิดพลาดแล้วได้ค่าว่าง)
    จะ 'ไม่ลบ' ข้อมูลเดิมในตาราง — ตารางบน Supabase สร้างด้วย SQL อยู่แล้ว
    ไม่ต้องให้แอปเขียน header ว่างทับ (เดิม bug นี้ทำให้ข้อมูลหายทุกครั้งที่เปิดหน้า)
    """
    if df is None or df.empty:
        return
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
