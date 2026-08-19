"""
record_stock.py  –  เมนู "บันทึกสต๊อก" (รอบที่ 2)
ผู้พัฒนา: ดร.วรรณ (ดร.อภิวรรณ์ ดำแสงสวัสดิ์)

โครงสร้างตามที่ ดร.วรรณ กำหนด:
  - พนักงานสาขากรอก "ยอดบรรจุภัณฑ์คงเหลือ" หลังปิดร้าน (~21:00-22:00)
  - บันทึกบรรจุภัณฑ์คงเหลือ + วัตถุดิบคงเหลือ + รูปเนยที่แกะแล้ว (ได้ถึง 3 รูป)
  - แก้ไข/ลบ ได้ (ลบยืนยันก่อน) — วันที่ซ้ำ แสดงข้อมูลเดิมแบบอ่านอย่างเดียว
  - หมายเหตุ: ตัด "ระบบคำนวณ เมื่อวาน+เข้า−คงเหลือ=ใช้ไป" ออกแล้ว (ตามที่สั่ง)
"""
import io
import base64
import datetime
import pandas as pd
import streamlit as st

from config import SHEET_BRANCH_STOCK_DAILY
from modules.excel_db import (
    read_sheet, write_sheet, init_workbook, append_row, update_row, delete_row,
)
from utils.id_generator import next_id


# 12 รายการบรรจุภัณฑ์: (key, label, unit)
STOCK_FIELDS = [
    ("paper_bag_qty",        "ถุงกระดาษ",                          "ถุง"),
    ("plastic_box_qty",      "กล่องพลาสติก (รวมกล่องที่ใส่ขนมเหลือ)", "กล่อง"),
    ("band_qty",             "สายคาด",                             "เส้น"),
    ("skewer_pack_qty",      "ไม้เสียบ",                           "แพ็ค"),
    ("hot_bag_pack_qty",     "ถุงร้อน",                            "แพ็ค"),
    ("printed_carry_bag_qty","ถุงหูหิ้วกระดาษพิมพ์ลาย",            "ใบ"),
    ("carry_bag_7x15_qty",   'ถุงหูหิ้ว 7"×15"',                   "แพ็ค"),
    ("carry_bag_8x16_qty",   'ถุงหูหิ้วใหญ่ 8"×16"',               "แพ็ค"),
    ("water_cup_qty",        "แก้วน้ำ",                            "ใบ"),
    ("cup_lid_qty",          "ฝาแก้วน้ำ",                          "ฝา"),
    ("ice_cream_cup_qty",    "แก้วไอศครีม",                        "ใบ"),
    ("ice_cream_ring_qty",   "วงแหวนรองถ้วยไอศครีม",              "แผ่น"),
    ("dip_cup_qty",          "ถ้วยดิป",                            "ถ้วย"),
    ("ice_cream_cup_ring_qty", "วงแหวนรองแก้วไอศกรีม",            "แผ่น"),
]
STOCK_KEYS = [k for k, _l, _u in STOCK_FIELDS]

# รูป "เนยที่แกะแล้ว" — รับได้ 3 รูป
BUTTER_IMAGE_KEYS = ["butter_used_image", "butter_used_image_2", "butter_used_image_3"]

# ── วัตถุดิบคงเหลือ (รอบ 2 เพิ่ม) : (key, label, unit) ──
MATERIAL_FIELDS = [
    ("egg_remaining",        "ไข่เหลือ",              "ฟอง"),
    ("flour_finished_big",   "แป้งสำเร็จรูปถุงใหญ่",  "ถุง"),
    ("flour_finished_small", "แป้งสำเร็จรูปถุงเล็ก",  "ถุง"),
    ("mix_big",              "ส่วนผสมถุงใหญ่",        "ถุง"),
    ("mix_small",            "ส่วนผสมถุงเล็ก",        "ถุง"),
    ("butter_unopened_qty",  "เนยยังไม่แกะ",          "ก้อน"),
]
MATERIAL_KEYS = [k for k, _l, _u in MATERIAL_FIELDS]


def _encode_image(uploaded_file) -> str:
    """แปลงรูปเป็น base64 (ย่อขนาด)"""
    raw = uploaded_file.getvalue()
    try:
        from PIL import Image
        img = Image.open(io.BytesIO(raw)).convert("RGB")
        w, h = img.size
        if max(w, h) > 1000:
            if w >= h:
                img = img.resize((1000, int(h * 1000 / w)))
            else:
                img = img.resize((int(w * 1000 / h), 1000))
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=70)
        raw = buf.getvalue()
    except Exception:
        pass
    return base64.b64encode(raw).decode()


def _material_inputs(prefix, defaults=None):
    """ช่องกรอก 'วัตถุดิบคงเหลือ' — คืน (values_dict, butter_files)
    values_dict: egg_remaining, flour_finished_big/small, mix_big/small, butter_unopened_qty
    butter_files: list ไฟล์รูป 'เนยที่แกะแล้ว' (รับได้ถึง 3 รูป — [] ถ้าไม่แนบ)
    """
    d = defaults or {}
    vals = {}
    # 5 วัตถุดิบแรก (ไข่ + แป้ง + ส่วนผสม) — แถวละ 3
    first5 = MATERIAL_FIELDS[:5]
    for i in range(0, len(first5), 3):
        chunk = first5[i:i + 3]
        cols = st.columns(len(chunk))
        for col, (k, label, unit) in zip(cols, chunk):
            vals[k] = col.number_input(f"{label} ({unit})", min_value=0, step=1,
                                       value=_int(d.get(k, 0)), key=f"{prefix}_{k}")
    # เนยยังไม่แกะ (ก้อน)
    vals["butter_unopened_qty"] = st.number_input(
        "เนยยังไม่แกะ (ก้อน)", min_value=0, step=1,
        value=_int(d.get("butter_unopened_qty", 0)),
        key=f"{prefix}_butter_unopened")

    # เนยที่แกะแล้ว (แนบรูป — รับได้ 3 รูป)
    st.markdown("**🧈 เนยที่แกะแล้ว** (แนบรูป — ได้ถึง 3 รูป)")
    old_imgs = [str(d.get(k, "")).strip() for k in BUTTER_IMAGE_KEYS]
    old_imgs = [im for im in old_imgs if im]
    if old_imgs:
        icols = st.columns(len(old_imgs))
        for ic, im in zip(icols, old_imgs):
            try:
                ic.image(base64.b64decode(im), width=140, caption="รูปที่แนบไว้")
            except Exception:
                pass
    butter_files = st.file_uploader(
        "แนบรูปเนยที่แกะแล้ว (เลือกได้สูงสุด 3 รูป)",
        type=["png", "jpg", "jpeg"], accept_multiple_files=True,
        key=f"{prefix}_butter_img")
    butter_files = list(butter_files or [])[:3]
    return vals, butter_files


# ══════════════════════════════════════════════════════════════
# INIT
# ══════════════════════════════════════════════════════════════
def _schema():
    return ["stock_id", "stock_date", "branch_id"] + STOCK_KEYS + \
           MATERIAL_KEYS + BUTTER_IMAGE_KEYS + \
           ["recorded_by", "remark", "created_at", "updated_at"]


def _init_sheet():
    if st.session_state.get("_init_rst"):
        return
    init_workbook()
    try:
        df = read_sheet(SHEET_BRANCH_STOCK_DAILY)
    except Exception:
        df = pd.DataFrame()
    if df is None or df.empty:
        try:
            write_sheet(SHEET_BRANCH_STOCK_DAILY, pd.DataFrame(columns=_schema()))
        except Exception:
            pass
    st.session_state["_init_rst"] = True


def _int(v, default=0):
    try:
        return int(float(str(v).replace(",", "").strip() or 0))
    except Exception:
        return default


# ══════════════════════════════════════════════════════════════
# MAIN RENDER
# ══════════════════════════════════════════════════════════════
def render():
    _init_sheet()

    st.markdown(
        "<h1 style='text-align:center;color:#1565C0;font-size:2rem;"
        "font-weight:900;margin-bottom:2px;'>📦 บันทึกสต๊อก</h1>"
        "<p style='text-align:center;color:#999;margin-top:0;'>"
        "บันทึกยอดบรรจุภัณฑ์คงเหลือประจำวัน (กรอกหลังปิดร้าน)</p>",
        unsafe_allow_html=True,
    )
    st.divider()

    branch_id = str(st.session_state.get("locked_branch_id", "")).strip()
    if not branch_id:
        st.error("❌ ไม่พบรหัสสาขา กรุณาเข้าสู่ระบบใหม่")
        return

    tab_new, tab_list = st.tabs(["📝 บันทึกใหม่", "📋 รายการที่บันทึกไว้ (แก้ไข/ลบ)"])
    with tab_new:
        _render_new(branch_id)
    with tab_list:
        _render_list(branch_id)


# ══════════════════════════════════════════════════════════════
# บันทึกใหม่
# ══════════════════════════════════════════════════════════════
def _render_new(branch_id):
    st.subheader("① วันที่")
    stock_date = st.date_input("📅 วันที่ (วันนี้)", value=datetime.date.today(),
                               key="rst_new_date")

    # ตรวจว่ามี record วันนี้แล้วหรือยัง — ถ้าซ้ำ แสดงข้อมูลที่บันทึกแล้ว (อ่านอย่างเดียว)
    existing = read_sheet(SHEET_BRANCH_STOCK_DAILY)
    if not existing.empty and "branch_id" in existing.columns:
        mask = ((existing["branch_id"].astype(str).str.strip() == branch_id) &
                (existing["stock_date"].astype(str) == str(stock_date)))
        if mask.any():
            st.warning("⚠️ สาขานี้ **บันทึกสต๊อกของวันที่นี้ไว้แล้ว** — "
                       "ด้านล่างคือข้อมูลที่บันทึกไว้ (ดูอย่างเดียว)")
            st.info("ℹ️ หากต้องการ **แก้ไข / ลบ** ให้ไปที่แท็บ "
                    "\"📋 รายการที่บันทึกไว้ (แก้ไข/ลบ)\"")
            st.divider()
            _render_stock_data(existing[mask].iloc[-1])
            return

    st.divider()
    st.subheader("② ยอดบรรจุภัณฑ์คงเหลือ (นับหลังปิดร้าน)")
    st.caption("กรอกจำนวนที่นับได้จริง ณ ตอนปิดร้าน")

    remaining = {}
    for i in range(0, len(STOCK_FIELDS), 3):
        cols = st.columns(3)
        for col, (key, label, unit) in zip(cols, STOCK_FIELDS[i:i + 3]):
            with col:
                remaining[key] = st.number_input(
                    f"{label} ({unit})", min_value=0, step=1,
                    key=f"rst_new_{key}")

    st.divider()
    st.subheader("③ วัตถุดิบคงเหลือ")
    materials, butter_files = _material_inputs(prefix="rst_new")

    remark = st.text_input("📝 หมายเหตุ (ถ้ามี)", key="rst_new_remark")

    st.divider()
    if st.button("💾 บันทึกสต๊อก", type="primary", use_container_width=True,
                 key="rst_new_save"):
        _save_new(branch_id, str(stock_date), remaining, remark,
                  materials, butter_files)


def _save_new(branch_id, stock_date, remaining, remark,
              materials=None, butter_files=None):
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    materials = materials or {}
    butter_files = list(butter_files or [])[:3]
    try:
        df = read_sheet(SHEET_BRANCH_STOCK_DAILY)
        stock_id = next_id(df, "stock_id", "STK")
        row = {"stock_id": stock_id, "stock_date": stock_date, "branch_id": branch_id,
               "recorded_by": branch_id,
               "remark": remark, "created_at": now, "updated_at": now}
        for k in STOCK_KEYS:
            row[k] = _int(remaining.get(k, 0))
        for k in MATERIAL_KEYS:
            row[k] = _int(materials.get(k, 0))
        for idx, k in enumerate(BUTTER_IMAGE_KEYS):
            row[k] = (_encode_image(butter_files[idx])
                      if idx < len(butter_files) else "")
        append_row(SHEET_BRANCH_STOCK_DAILY, row)
    except Exception as e:
        msg = str(e)
        st.error(f"❌ บันทึกไม่สำเร็จ: {msg}")
        if ("does not exist" in msg or "relation" in msg or "42P01" in msg
                or "Could not find the table" in msg or "PGRST" in msg):
            st.warning(
                "⚠️ ดูเหมือนตาราง **branch_stock_daily** ยังไม่มีใน Supabase — "
                "กรุณาเปิด Supabase → SQL Editor → รันไฟล์ **roon_new_tables.sql** "
                "(ตัวล่าสุด) หนึ่งครั้งก่อน แล้วลองบันทึกอีกครั้งค่ะ"
            )
        else:
            st.info("ถ้ายังบันทึกไม่ได้ กรุณาแคปหน้าจอ error นี้ส่งให้ทีมพัฒนา")
        return
    st.success(f"✅ บันทึกสต๊อกสำเร็จ! เลขที่ {stock_id} | วันที่ {stock_date}")
    st.balloons()


# ══════════════════════════════════════════════════════════════
# รายการที่บันทึกไว้ (แก้ไข/ลบ)
# ══════════════════════════════════════════════════════════════
def _render_list(branch_id):
    st.subheader("📋 บันทึกสต๊อกของสาขา")
    df = read_sheet(SHEET_BRANCH_STOCK_DAILY)
    if df.empty or "branch_id" not in df.columns:
        st.info("ยังไม่มีบันทึกสต๊อก")
        return
    df = df[df["branch_id"].astype(str).str.strip() == branch_id].copy()
    if df.empty:
        st.info("ยังไม่มีบันทึกสต๊อกของสาขานี้")
        return

    sel_date = st.date_input("กรองตามวันที่ (ว่าง = ทั้งหมด)", value=None,
                             key="rst_list_date")
    if sel_date:
        df = df[df["stock_date"].astype(str) == str(sel_date)]
    if df.empty:
        st.info("ไม่พบบันทึกตามวันที่เลือก")
        return

    df = df.sort_values("stock_date", ascending=False)
    st.caption(f"พบ {len(df)} รายการ")

    for _, row in df.iterrows():
        stock_id = str(row["stock_id"])
        with st.expander(f"📦 {stock_id} | วันที่ {row.get('stock_date','')}"):
            if st.session_state.get(f"rst_edit_{stock_id}"):
                _render_edit(branch_id, row)
            else:
                _render_view(branch_id, row)


def _render_stock_data(row):
    """แสดงข้อมูลสต๊อกแบบอ่านอย่างเดียว (บรรจุภัณฑ์ + วัตถุดิบ + รูปเนย) — ไม่มีปุ่ม"""
    # บรรจุภัณฑ์คงเหลือ
    pkg_rows = [{"รายการ": f"{label} ({unit})", "คงเหลือ": _int(row.get(key, 0))}
                for key, label, unit in STOCK_FIELDS]
    st.markdown("**📦 บรรจุภัณฑ์คงเหลือ**")
    st.dataframe(pd.DataFrame(pkg_rows), use_container_width=True, hide_index=True)
    # วัตถุดิบคงเหลือ
    mat_rows = [{"รายการ": f"{label} ({unit})", "จำนวน": _int(row.get(key, 0))}
                for key, label, unit in MATERIAL_FIELDS]
    st.markdown("**🧺 วัตถุดิบคงเหลือ**")
    st.dataframe(pd.DataFrame(mat_rows), use_container_width=True, hide_index=True)
    # รูปเนยที่แกะแล้ว (ได้ถึง 3 รูป)
    bimgs = [str(row.get(k, "")).strip() for k in BUTTER_IMAGE_KEYS]
    bimgs = [im for im in bimgs if im]
    if bimgs:
        st.markdown("**🧈 เนยที่แกะแล้ว**")
        icols = st.columns(len(bimgs))
        for ic, im in zip(icols, bimgs):
            try:
                ic.image(base64.b64decode(im), width=160)
            except Exception:
                pass
    if str(row.get("remark", "")).strip():
        st.caption(f"📝 {row.get('remark')}")


def _render_view(branch_id, row):
    stock_id = str(row["stock_id"])
    _render_stock_data(row)

    b1, b2 = st.columns(2)
    with b1:
        if st.button("✏️ แก้ไข", key=f"rst_editbtn_{stock_id}",
                     use_container_width=True):
            st.session_state[f"rst_edit_{stock_id}"] = True
            st.rerun()
    with b2:
        if st.session_state.get(f"rst_del_{stock_id}"):
            st.warning("⚠️ ยืนยันลบรายการนี้?")
            cc1, cc2 = st.columns(2)
            if cc1.button("✅ ยืนยันลบ", key=f"rst_delyes_{stock_id}",
                          type="primary", use_container_width=True):
                delete_row(SHEET_BRANCH_STOCK_DAILY, "stock_id", stock_id)
                st.session_state.pop(f"rst_del_{stock_id}", None)
                st.success(f"ลบ {stock_id} แล้ว")
                st.rerun()
            if cc2.button("ยกเลิก", key=f"rst_delno_{stock_id}",
                          use_container_width=True):
                st.session_state.pop(f"rst_del_{stock_id}", None)
                st.rerun()
        else:
            if st.button("🗑️ ลบ", key=f"rst_delbtn_{stock_id}",
                         use_container_width=True):
                st.session_state[f"rst_del_{stock_id}"] = True
                st.rerun()


def _render_edit(branch_id, row):
    stock_id = str(row["stock_id"])
    st.markdown(f"### ✏️ แก้ไขบันทึกสต๊อก {stock_id}")
    stock_date = st.date_input("📅 วันที่",
                               value=_parse_date(row.get("stock_date")),
                               key=f"rst_e_date_{stock_id}")

    remaining = {}
    for i in range(0, len(STOCK_FIELDS), 3):
        cols = st.columns(3)
        for col, (key, label, unit) in zip(cols, STOCK_FIELDS[i:i + 3]):
            with col:
                remaining[key] = st.number_input(
                    f"{label} ({unit})", min_value=0, step=1,
                    value=_int(row.get(key, 0)), key=f"rst_e_{key}_{stock_id}")
    st.markdown("**🧺 วัตถุดิบคงเหลือ**")
    materials, butter_files = _material_inputs(prefix=f"rst_e_{stock_id}",
                                               defaults=row.to_dict())

    remark = st.text_input("📝 หมายเหตุ", value=str(row.get("remark", "")),
                           key=f"rst_e_remark_{stock_id}")

    b1, b2 = st.columns(2)
    with b1:
        if st.button("💾 บันทึกการแก้ไข", type="primary",
                     use_container_width=True, key=f"rst_e_save_{stock_id}"):
            now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            upd = {"stock_date": str(stock_date), "remark": remark,
                   "updated_at": now}
            for k in STOCK_KEYS:
                upd[k] = _int(remaining.get(k, 0))
            for k in MATERIAL_KEYS:
                upd[k] = _int(materials.get(k, 0))
            if butter_files:   # แนบรูปใหม่เท่านั้นถึงจะเปลี่ยน (แทนที่ทั้งชุด)
                for idx, k in enumerate(BUTTER_IMAGE_KEYS):
                    upd[k] = (_encode_image(butter_files[idx])
                              if idx < len(butter_files) else "")
            update_row(SHEET_BRANCH_STOCK_DAILY, "stock_id", stock_id, upd)
            st.session_state.pop(f"rst_edit_{stock_id}", None)
            st.success("✅ แก้ไขสำเร็จ")
            st.rerun()
    with b2:
        if st.button("ยกเลิก", use_container_width=True,
                     key=f"rst_e_cancel_{stock_id}"):
            st.session_state.pop(f"rst_edit_{stock_id}", None)
            st.rerun()


def _parse_date(v):
    try:
        return datetime.datetime.strptime(str(v)[:10], "%Y-%m-%d").date()
    except Exception:
        return datetime.date.today()
