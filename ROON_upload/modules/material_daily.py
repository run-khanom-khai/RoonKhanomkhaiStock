"""
material_daily.py  –  ฝ่ายจัดซื้อ: วัตถุดิบรายวัน + คำนวณต้นทุน (แอปรวมระบบใหญ่)
ผู้พัฒนา: ดร.วรรณ (ดร.อภิวรรณ์ ดำแสงสวัสดิ์)

โครงสร้างตามที่ ดร.วรรณ กำหนด:
  - 12 วัตถุดิบ: ยกมา (auto จากคงเหลือเมื่อวาน) + ซื้อเข้า − ใช้ไป(กรอกตรง) = คงเหลือ
  - "ใช้ไปวันนี้" กรอกตรง ๆ → เก็บเป็นข้อมูลเพื่อคำนวณต้นทุนที่ใช้ไปต่อวัน
  - ต้นทุน/หน่วย: มีตารางราคาต้นทุนวัตถุดิบให้ตั้ง/แก้ได้ (แท็บ 'ราคาต้นทุน')
  - ต้นทุนที่ใช้วันนี้ = ใช้ไป × ต้นทุน/หน่วย ; รวมทั้งวัน = Σ
  - แก้ไข/ลบ ได้ (ลบยืนยันก่อน)
เก็บข้อมูลแบบ long format: 1 แถว/วัตถุดิบ/วัน (ตาราง material_daily)
"""
import datetime
import pandas as pd
import streamlit as st

from config import SHEET_MATERIAL_DAILY, SHEET_MATERIAL_COST
from modules.excel_db import (
    read_sheet, write_sheet, init_workbook, append_row,
)
from utils.id_generator import next_id


# 12 วัตถุดิบ: (key, label, unit)
MATERIAL_FIELDS = [
    ("flour_big",       "ถุงแป้ง (ใหญ่)",       "ถุง"),
    ("flour_small",     "ถุงแป้ง (เล็ก)",       "ถุง"),
    ("sugar_big",       "ถุงน้ำตาล (ใหญ่)",     "ถุง"),
    ("sugar_small",     "ถุงน้ำตาล (เล็ก)",     "ถุง"),
    ("egg",             "ไข่ไก่",               "ฟอง"),
    ("butter",          "เนย (แกะแล้ว)",        "ก้อน"),
    ("mixed_tea",       "ชาที่ผสมแล้ว",         "แกลลอน"),
    ("unmixed_tea",     "ชาที่ยังไม่ผสม",       "แกลลอน"),
    ("tea_base",        "เบสชา",                "ml"),
    ("condensed_milk",  "นมข้นหวาน",            "ถุง"),
    ("evaporated_milk", "นมข้นจืด",             "ถุง"),
    ("honey",           "น้ำผึ้ง",              "ml"),
]
MAT_KEYS   = [k for k, _l, _u in MATERIAL_FIELDS]
MAT_LABEL  = {k: l for k, l, _u in MATERIAL_FIELDS}
MAT_UNIT   = {k: u for k, _l, u in MATERIAL_FIELDS}


def _num(v, default=0.0):
    try:
        return float(str(v).replace(",", "").strip() or 0)
    except Exception:
        return default


def _daily_schema():
    return ["id", "entry_date", "material_key", "material_label",
            "opening_qty", "purchased_qty", "used_qty", "remaining_qty",
            "unit_cost", "used_cost", "created_at", "updated_at"]


def _init_sheets():
    if st.session_state.get("_init_md"):
        return
    init_workbook()
    # material_daily
    try:
        df = read_sheet(SHEET_MATERIAL_DAILY)
    except Exception:
        df = pd.DataFrame()
    if df is None or df.empty:
        try:
            write_sheet(SHEET_MATERIAL_DAILY, pd.DataFrame(columns=_daily_schema()))
        except Exception:
            pass
    # material_cost (seed 12 วัตถุดิบ ราคา 0)
    try:
        cdf = read_sheet(SHEET_MATERIAL_COST)
    except Exception:
        cdf = pd.DataFrame()
    if cdf is None or cdf.empty or "material_key" not in cdf.columns:
        rows = [{"material_key": k, "material_label": l, "unit": u, "unit_cost": 0}
                for k, l, u in MATERIAL_FIELDS]
        try:
            write_sheet(SHEET_MATERIAL_COST, pd.DataFrame(rows))
        except Exception:
            pass
    st.session_state["_init_md"] = True


def _cost_map():
    """ราคาต้นทุนต่อหน่วย {material_key: unit_cost}"""
    m = {k: 0.0 for k in MAT_KEYS}
    try:
        cdf = read_sheet(SHEET_MATERIAL_COST)
        if not cdf.empty and "material_key" in cdf.columns:
            for _, r in cdf.iterrows():
                m[str(r["material_key"])] = _num(r.get("unit_cost", 0))
    except Exception:
        pass
    return m


def _prev_remaining(entry_date):
    """คงเหลือของวันก่อนหน้า (record ล่าสุดก่อนวันนี้) → {key: remaining}"""
    res = {k: 0.0 for k in MAT_KEYS}
    try:
        df = read_sheet(SHEET_MATERIAL_DAILY)
    except Exception:
        return res
    if df is None or df.empty or "entry_date" not in df.columns:
        return res
    prior = df[df["entry_date"].astype(str) < str(entry_date)]
    if prior.empty:
        return res
    last_date = prior["entry_date"].astype(str).max()
    last = prior[prior["entry_date"].astype(str) == last_date]
    for _, r in last.iterrows():
        res[str(r["material_key"])] = _num(r.get("remaining_qty", 0))
    return res


# ══════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════
def render():
    _init_sheets()
    st.markdown(
        "<h1 style='color:#6A1B9A;font-size:1.9rem;font-weight:900;"
        "border-left:6px solid #6A1B9A;padding-left:12px;'>"
        "🧺 วัตถุดิบรายวัน (ฝ่ายจัดซื้อ) — คำนวณต้นทุน</h1>",
        unsafe_allow_html=True,
    )

    tab_new, tab_list, tab_cost = st.tabs(
        ["📝 บันทึกรายวัน", "📋 ประวัติ (แก้ไข/ลบ)", "💲 ราคาต้นทุนวัตถุดิบ"])
    with tab_new:
        _render_new()
    with tab_list:
        _render_list()
    with tab_cost:
        _render_cost_master()


# ══════════════════════════════════════════════════════════════
# บันทึกรายวัน
# ══════════════════════════════════════════════════════════════
def _render_new():
    st.subheader("① วันที่")
    entry_date = st.date_input("📅 วันที่", value=datetime.date.today(),
                               key="md_new_date")

    existing = read_sheet(SHEET_MATERIAL_DAILY)
    dup = False
    if not existing.empty and "entry_date" in existing.columns:
        if (existing["entry_date"].astype(str) == str(entry_date)).any():
            dup = True
            st.warning("⚠️ มีบันทึกของวันที่นี้แล้ว — แก้ไขที่แท็บ 'ประวัติ'")

    prev = _prev_remaining(str(entry_date))
    costs = _cost_map()

    st.divider()
    st.subheader("② กรอกข้อมูลวัตถุดิบ")
    st.caption("ยกมา = คงเหลือเมื่อวาน (อัตโนมัติ แก้ได้) | ซื้อเข้า และ ใช้ไปวันนี้ = กรอกเอง")

    rows = []
    total_used_cost = 0.0
    for idx, (key, label, unit) in enumerate(MATERIAL_FIELDS, 1):
        with st.container(border=True):
            st.markdown(f"**{idx}. {label} ({unit})** — ต้นทุน/หน่วย ฿{costs.get(key,0):,.2f}")
            c1, c2, c3, c4, c5 = st.columns(5)
            with c1:
                opening = st.number_input("ยกมา", min_value=0.0, step=1.0,
                                          value=float(prev.get(key, 0)),
                                          key=f"md_new_open_{key}")
            with c2:
                purchased = st.number_input("ซื้อเข้า", min_value=0.0, step=1.0,
                                            key=f"md_new_buy_{key}")
            with c3:
                used = st.number_input("ใช้ไปวันนี้", min_value=0.0, step=1.0,
                                       key=f"md_new_use_{key}")
            remaining = opening + purchased - used
            used_cost = used * costs.get(key, 0)
            total_used_cost += used_cost
            with c4:
                st.metric("คงเหลือ", f"{remaining:,.0f}")
            with c5:
                st.metric("ต้นทุนที่ใช้", f"฿{used_cost:,.2f}")
            rows.append({
                "material_key": key, "material_label": label,
                "opening_qty": opening, "purchased_qty": purchased,
                "used_qty": used, "remaining_qty": remaining,
                "unit_cost": costs.get(key, 0), "used_cost": used_cost,
            })

    st.divider()
    st.metric("💰 ต้นทุนวัตถุดิบที่ใช้ไปรวมทั้งวัน", f"฿{total_used_cost:,.2f}")

    if st.button("💾 บันทึกวัตถุดิบรายวัน", type="primary",
                 use_container_width=True, key="md_new_save", disabled=dup):
        _save_day(str(entry_date), rows)


def _save_day(entry_date, rows):
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df = read_sheet(SHEET_MATERIAL_DAILY)
    for r in rows:
        rid = next_id(read_sheet(SHEET_MATERIAL_DAILY), "id", "MTD")
        append_row(SHEET_MATERIAL_DAILY, {
            "id": rid, "entry_date": entry_date,
            "material_key": r["material_key"], "material_label": r["material_label"],
            "opening_qty": r["opening_qty"], "purchased_qty": r["purchased_qty"],
            "used_qty": r["used_qty"], "remaining_qty": r["remaining_qty"],
            "unit_cost": r["unit_cost"], "used_cost": r["used_cost"],
            "created_at": now, "updated_at": now,
        })
    st.success(f"✅ บันทึกวัตถุดิบวันที่ {entry_date} สำเร็จ ({len(rows)} รายการ)")
    st.balloons()


# ══════════════════════════════════════════════════════════════
# ประวัติ (แก้ไข/ลบ ทั้งวัน)
# ══════════════════════════════════════════════════════════════
def _render_list():
    st.subheader("📋 ประวัติวัตถุดิบรายวัน")
    df = read_sheet(SHEET_MATERIAL_DAILY)
    if df.empty or "entry_date" not in df.columns:
        st.info("ยังไม่มีข้อมูล")
        return

    dates = sorted(df["entry_date"].astype(str).unique(), reverse=True)
    st.caption(f"พบ {len(dates)} วัน")
    for d in dates:
        day = df[df["entry_date"].astype(str) == d]
        total = day["used_cost"].apply(_num).sum()
        with st.expander(f"📅 {d} | ต้นทุนใช้ไปรวม ฿{total:,.2f} ({len(day)} รายการ)"):
            if st.session_state.get(f"md_edit_{d}"):
                _render_edit(d, day)
            else:
                _render_view(d, day)


def _render_view(d, day):
    show = day[["material_label", "opening_qty", "purchased_qty", "used_qty",
                "remaining_qty", "unit_cost", "used_cost"]].rename(columns={
        "material_label": "วัตถุดิบ", "opening_qty": "ยกมา",
        "purchased_qty": "ซื้อเข้า", "used_qty": "ใช้ไป",
        "remaining_qty": "คงเหลือ", "unit_cost": "ต้นทุน/หน่วย",
        "used_cost": "ต้นทุนที่ใช้"})
    st.dataframe(show, use_container_width=True, hide_index=True)

    b1, b2 = st.columns(2)
    with b1:
        if st.button("✏️ แก้ไขทั้งวัน", key=f"md_editbtn_{d}",
                     use_container_width=True):
            st.session_state[f"md_edit_{d}"] = True
            st.rerun()
    with b2:
        if st.session_state.get(f"md_del_{d}"):
            st.warning("⚠️ ยืนยันลบข้อมูลทั้งวันนี้?")
            cc1, cc2 = st.columns(2)
            if cc1.button("✅ ยืนยันลบ", key=f"md_delyes_{d}",
                          type="primary", use_container_width=True):
                _delete_day(d)
                st.session_state.pop(f"md_del_{d}", None)
                st.success(f"ลบข้อมูลวันที่ {d} แล้ว")
                st.rerun()
            if cc2.button("ยกเลิก", key=f"md_delno_{d}", use_container_width=True):
                st.session_state.pop(f"md_del_{d}", None)
                st.rerun()
        else:
            if st.button("🗑️ ลบทั้งวัน", key=f"md_delbtn_{d}",
                         use_container_width=True):
                st.session_state[f"md_del_{d}"] = True
                st.rerun()


def _render_edit(d, day):
    st.markdown(f"### ✏️ แก้ไขวัตถุดิบวันที่ {d}")
    costs = _cost_map()
    cur = {str(r["material_key"]): r for _, r in day.iterrows()}
    rows = []
    total = 0.0
    for idx, (key, label, unit) in enumerate(MATERIAL_FIELDS, 1):
        c = cur.get(key, {})
        with st.container(border=True):
            st.markdown(f"**{idx}. {label} ({unit})** — ต้นทุน/หน่วย ฿{costs.get(key,0):,.2f}")
            c1, c2, c3, c4, c5 = st.columns(5)
            with c1:
                opening = st.number_input("ยกมา", min_value=0.0, step=1.0,
                                          value=_num(c.get("opening_qty", 0)),
                                          key=f"md_e_open_{d}_{key}")
            with c2:
                purchased = st.number_input("ซื้อเข้า", min_value=0.0, step=1.0,
                                            value=_num(c.get("purchased_qty", 0)),
                                            key=f"md_e_buy_{d}_{key}")
            with c3:
                used = st.number_input("ใช้ไปวันนี้", min_value=0.0, step=1.0,
                                       value=_num(c.get("used_qty", 0)),
                                       key=f"md_e_use_{d}_{key}")
            remaining = opening + purchased - used
            used_cost = used * costs.get(key, 0)
            total += used_cost
            c4.metric("คงเหลือ", f"{remaining:,.0f}")
            c5.metric("ต้นทุนที่ใช้", f"฿{used_cost:,.2f}")
            rows.append({"material_key": key, "material_label": label,
                         "opening_qty": opening, "purchased_qty": purchased,
                         "used_qty": used, "remaining_qty": remaining,
                         "unit_cost": costs.get(key, 0), "used_cost": used_cost})
    st.metric("💰 ต้นทุนรวมทั้งวัน", f"฿{total:,.2f}")

    b1, b2 = st.columns(2)
    with b1:
        if st.button("💾 บันทึกการแก้ไข", type="primary",
                     use_container_width=True, key=f"md_e_save_{d}"):
            _delete_day(d)
            _save_day(d, rows)
            st.session_state.pop(f"md_edit_{d}", None)
            st.rerun()
    with b2:
        if st.button("ยกเลิก", use_container_width=True, key=f"md_e_cancel_{d}"):
            st.session_state.pop(f"md_edit_{d}", None)
            st.rerun()


def _delete_day(d):
    try:
        df = read_sheet(SHEET_MATERIAL_DAILY)
        if not df.empty and "entry_date" in df.columns:
            write_sheet(SHEET_MATERIAL_DAILY,
                        df[df["entry_date"].astype(str) != str(d)])
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════
# ราคาต้นทุนวัตถุดิบ (ตั้ง/แก้ได้)
# ══════════════════════════════════════════════════════════════
def _render_cost_master():
    st.subheader("💲 ตารางราคาต้นทุนวัตถุดิบ (ต่อหน่วย)")
    st.caption("ตั้ง/แก้ราคาต้นทุนต่อหน่วยของแต่ละวัตถุดิบ — ใช้คำนวณต้นทุนที่ใช้ไปต่อวัน")

    costs = _cost_map()
    with st.form("md_cost_form"):
        new_costs = {}
        for idx, (key, label, unit) in enumerate(MATERIAL_FIELDS, 1):
            c1, c2 = st.columns([3, 2])
            c1.markdown(f"**{idx}. {label}** ({unit})")
            new_costs[key] = c2.number_input(
                f"ต้นทุน/หน่วย ({unit})", min_value=0.0, step=0.5,
                value=float(costs.get(key, 0)), key=f"md_cost_{key}",
                label_visibility="collapsed")
        saved = st.form_submit_button("💾 บันทึกราคาต้นทุน", type="primary")

    if saved:
        rows = [{"material_key": k, "material_label": MAT_LABEL[k],
                 "unit": MAT_UNIT[k], "unit_cost": new_costs[k]}
                for k in MAT_KEYS]
        write_sheet(SHEET_MATERIAL_COST, pd.DataFrame(rows))
        st.success("✅ บันทึกราคาต้นทุนสำเร็จ")
        st.rerun()
