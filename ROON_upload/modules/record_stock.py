"""
record_stock.py  –  เมนู "บันทึกสต๊อก" (รอบที่ 2)
ผู้พัฒนา: ดร.วรรณ (ดร.อภิวรรณ์ ดำแสงสวัสดิ์)

โครงสร้างตามที่ ดร.วรรณ กำหนด:
  - พนักงานสาขากรอก "ยอดบรรจุภัณฑ์คงเหลือ" หลังปิดร้าน (~21:00-22:00)
  - 12 รายการ (วันที่ + 12 ชนิดบรรจุภัณฑ์)
  - ระบบคำนวณและแสดงต่อรายการ:
      ยอดเมื่อวันก่อนหน้า 1 วัน + Stock เข้า(ดึงอัตโนมัติ) − คงเหลือวันนี้ = จำนวนที่ใช้ไปวันนี้ ✅
    (ทิศทาง: กรอกคงเหลือ → ระบบคำนวณยอดที่ใช้ไป)
  - Stock เข้า: ดึงอัตโนมัติจากระบบรับสต็อก (stock_in_to_branch)
  - แก้ไข/ลบ ได้ (ลบยืนยันก่อน)
"""
import io
import base64
import datetime
import pandas as pd
import streamlit as st

from config import (
    SHEET_BRANCH_STOCK_DAILY,
    SHEET_STOCK_IN_TO_BRANCH,
    SHEET_ITEMS,
)
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
]
STOCK_KEYS = [k for k, _l, _u in STOCK_FIELDS]

# คำค้นสำหรับจับคู่ชื่อสินค้าในระบบรับสต็อก → ช่องบรรจุภัณฑ์ (auto stock-in)
STOCK_IN_MATCH = {
    "paper_bag_qty":         ["ถุงกระดาษ"],
    "plastic_box_qty":       ["กล่องพลาสติก", "กล่องใส"],
    "band_qty":              ["สายคาด", "ยางรัด"],
    "skewer_pack_qty":       ["ไม้เสียบ"],
    "hot_bag_pack_qty":      ["ถุงร้อน"],
    "printed_carry_bag_qty": ["พิมพ์ลาย"],
    "carry_bag_7x15_qty":    ["7×15", "7x15", '7"×15', "7 x 15"],
    "carry_bag_8x16_qty":    ["8×16", "8x16", '8"×16', "8 x 16"],
    "water_cup_qty":         ["แก้วน้ำ"],
    "cup_lid_qty":           ["ฝาแก้ว"],
    "ice_cream_cup_qty":     ["แก้วไอศครีม", "ไอศครีม", "ไอติม"],
    "ice_cream_ring_qty":    ["วงแหวน", "รองถ้วย"],
}

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
    """ช่องกรอก 'วัตถุดิบคงเหลือ' — คืน (values_dict, butter_image_file)
    values_dict: egg_remaining, flour_finished_big/small, mix_big/small, butter_unopened_qty
    butter_image_file: ไฟล์รูป 'เนยที่ใช้แล้ว' (หรือ None)
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
    # เนยที่ใช้แล้ว (แนบรูป) + เนยยังไม่แกะ (ก้อน)
    bc1, bc2 = st.columns(2)
    with bc1:
        st.markdown("**🧈 เนยที่ใช้แล้ว** (แนบรูป)")
        old_img = str(d.get("butter_used_image", "")).strip()
        if old_img:
            try:
                st.image(base64.b64decode(old_img), width=140,
                         caption="รูปที่แนบไว้")
            except Exception:
                pass
        butter_file = st.file_uploader("แนบรูปเนยที่ใช้แล้ว",
                                       type=["png", "jpg", "jpeg"],
                                       key=f"{prefix}_butter_img")
    with bc2:
        vals["butter_unopened_qty"] = st.number_input(
            "เนยยังไม่แกะ (ก้อน)", min_value=0, step=1,
            value=_int(d.get("butter_unopened_qty", 0)),
            key=f"{prefix}_butter_unopened")
    return vals, butter_file


# ══════════════════════════════════════════════════════════════
# INIT
# ══════════════════════════════════════════════════════════════
def _schema():
    return ["stock_id", "stock_date", "branch_id"] + STOCK_KEYS + \
           MATERIAL_KEYS + ["butter_used_image",
                            "recorded_by", "remark", "created_at", "updated_at"]


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
# HELPERS: prev balance & auto stock-in
# ══════════════════════════════════════════════════════════════
def _prev_balance(branch_id, date_str):
    """ยอดคงเหลือของ 'วันก่อนหน้า' (record ล่าสุดที่ก่อนวันที่นี้) ของสาขานี้
    คืน (dict{field:qty}, prev_date) — ถ้าไม่มี คืน ({}, None)
    """
    try:
        df = read_sheet(SHEET_BRANCH_STOCK_DAILY)
    except Exception:
        return {}, None
    if df is None or df.empty or "branch_id" not in df.columns:
        return {}, None
    df = df[(df["branch_id"].astype(str).str.strip() == str(branch_id)) &
            (df["stock_date"].astype(str) < str(date_str))]
    if df.empty:
        return {}, None
    df = df.sort_values("stock_date")
    last = df.iloc[-1]
    return {k: _int(last.get(k, 0)) for k in STOCK_KEYS}, str(last.get("stock_date", ""))


def _auto_stock_in(branch_id, date_str):
    """ดึง Stock เข้าอัตโนมัติจาก stock_in_to_branch (สาขา+วันที่) จับคู่ด้วยชื่อสินค้า
    คืน dict{field:qty}
    """
    res = {k: 0 for k in STOCK_KEYS}
    try:
        sdf = read_sheet(SHEET_STOCK_IN_TO_BRANCH)
    except Exception:
        return res
    if sdf is None or sdf.empty or "branch_id" not in sdf.columns:
        return res
    sdf = sdf[(sdf["branch_id"].astype(str).str.strip() == str(branch_id)) &
              (sdf["stock_in_date"].astype(str).str[:10] == str(date_str)[:10])]
    if sdf.empty:
        return res

    # map item_id -> item_name
    id2name = {}
    try:
        idf = read_sheet(SHEET_ITEMS)
        if not idf.empty and "item_id" in idf.columns:
            id2name = dict(zip(idf["item_id"].astype(str),
                               idf["item_name"].astype(str)))
    except Exception:
        pass

    for _, r in sdf.iterrows():
        name = ""
        if "item_id" in r:
            name = id2name.get(str(r["item_id"]), "")
        if not name:
            name = str(r.get("item_name", ""))
        qty = _int(r.get("qty_in", 0))
        for field, kws in STOCK_IN_MATCH.items():
            if any(kw in name for kw in kws):
                res[field] += qty
                break
    return res


def _calc_rows(prev, stockin, remaining):
    """สร้างข้อมูลตารางคำนวณ: เมื่อวาน + เข้า − คงเหลือ = ใช้ไป"""
    rows = []
    for key, label, unit in STOCK_FIELDS:
        p = _int(prev.get(key, 0))
        s = _int(stockin.get(key, 0))
        r = _int(remaining.get(key, 0))
        used = p + s - r
        rows.append({
            "รายการ": f"{label} ({unit})",
            "เมื่อวาน": p,
            "+ เข้า": s,
            "คงเหลือวันนี้": r,
            "ใช้ไปวันนี้": used,
        })
    return rows


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

    # ตรวจว่ามี record วันนี้แล้วหรือยัง
    existing = read_sheet(SHEET_BRANCH_STOCK_DAILY)
    dup = False
    if not existing.empty and "branch_id" in existing.columns:
        mask = ((existing["branch_id"].astype(str).str.strip() == branch_id) &
                (existing["stock_date"].astype(str) == str(stock_date)))
        if mask.any():
            dup = True
            st.warning("⚠️ สาขานี้มีบันทึกสต๊อกของวันที่นี้แล้ว — ไปที่แท็บ "
                       "'รายการที่บันทึกไว้' เพื่อแก้ไข (กันบันทึกซ้ำ)")

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
    materials, butter_file = _material_inputs(prefix="rst_new")

    remark = st.text_input("📝 หมายเหตุ (ถ้ามี)", key="rst_new_remark")

    st.divider()
    if st.button("💾 บันทึกสต๊อก", type="primary", use_container_width=True,
                 key="rst_new_save", disabled=dup):
        _save_new(branch_id, str(stock_date), remaining, remark,
                  materials, butter_file)


def _show_calc_table(calc):
    df = pd.DataFrame(calc)
    def _hl(v):
        try:
            v = float(v)
        except Exception:
            return ""
        if v < 0:
            return "color:#C62828;font-weight:700;"   # ใช้ไปติดลบ = ผิดปกติ
        return ""
    try:
        styled = df.style.map(_hl, subset=["ใช้ไปวันนี้"])
        st.dataframe(styled, use_container_width=True, hide_index=True)
    except Exception:
        st.dataframe(df, use_container_width=True, hide_index=True)
    if any(r["ใช้ไปวันนี้"] < 0 for r in calc):
        st.warning("⚠️ มีบางรายการ 'ใช้ไปวันนี้' ติดลบ — แปลว่าคงเหลือที่กรอก "
                   "มากกว่ายอดเมื่อวาน+ของเข้า กรุณาตรวจสอบตัวเลข")


def _save_new(branch_id, stock_date, remaining, remark,
              materials=None, butter_file=None):
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    materials = materials or {}
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
        row["butter_used_image"] = _encode_image(butter_file) if butter_file else ""
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


def _render_view(branch_id, row):
    stock_id = str(row["stock_id"])
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
    _bimg = str(row.get("butter_used_image", "")).strip()
    if _bimg:
        try:
            st.image(base64.b64decode(_bimg), width=160,
                     caption="🧈 เนยที่ใช้แล้ว")
        except Exception:
            pass
    if str(row.get("remark", "")).strip():
        st.caption(f"📝 {row.get('remark')}")

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
    materials, butter_file = _material_inputs(prefix=f"rst_e_{stock_id}",
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
            if butter_file:   # แนบรูปใหม่เท่านั้นถึงจะเปลี่ยน
                upd["butter_used_image"] = _encode_image(butter_file)
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
