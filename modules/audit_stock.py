"""
audit_stock.py  –  เมนู "ตรวจนับสต๊อกบรรจุภัณฑ์" (รอบที่ 3 – ฝ่าย Audit)
ผู้พัฒนา: ดร.วรรณ (ดร.อภิวรรณ์ ดำแสงสวัสดิ์)

โครงสร้างตามที่ ดร.วรรณ กำหนด:
  - ฝ่าย Audit นับสต๊อกจริงก่อนเปิดร้าน (~07:30-09:00) ปกติ 1 ครั้ง/สัปดาห์
  - Input เหมือนของสาขา (หัวข้อ "บันทึกสต๊อก") ครบ 12 รายการ
  - หัวข้อ "บรรจุภัณฑ์คงเหลือ" ประจำวันที่ = วันที่ตรวจ − 1 วัน
    (เพราะนับตอนเช้าก่อนเปิด = ยอดปิดของเมื่อวาน)
  - แต่ละรายการมีหมายเลข กด Tab ไปช่องถัดไปได้
  - เมื่อบันทึก → เทียบกับ "ยอดที่สาขาบันทึก" ของวันเดียวกัน (วันที่ตรวจ − 1)
    * ยอดสต็อกจริง = ยอดที่ฝ่าย Audit นับ
    * แสดง "ส่วนต่าง" เฉพาะรายการที่ตัวเลขไม่ตรงกัน
  - แก้ไข/ลบ ได้ (ลบยืนยันก่อน)
"""
import datetime
import pandas as pd
import streamlit as st

from config import (
    SHEET_AUDIT_STOCK_BALANCE,
    SHEET_BRANCH_STOCK_DAILY,
    SHEET_BRANCHES,
    SHEET_EMPLOYEES,
)
from modules.excel_db import (
    read_sheet, write_sheet, init_workbook, append_row, update_row, delete_row,
)
from modules.record_stock import STOCK_FIELDS, STOCK_KEYS, _int
from utils.id_generator import next_id


# บรรจุภัณฑ์ที่ให้กรอก "จำนวนเสียหาย/แตกหัก" (ใช้ในแอป Sale Audit เมนู 3.2)
DMG_FIELDS = [
    ("dmg_plastic_box_qty",       "กล่องพลาสติก"),
    ("dmg_paper_bag_qty",         "ถุงกระดาษ"),
    ("dmg_printed_carry_bag_qty", "ถุงหูหิ้วพิมพ์ลาย"),
    ("dmg_water_cup_qty",         "แก้วน้ำ"),
    ("dmg_ice_cream_cup_qty",     "แก้วไอศกรีม"),
]
DMG_KEYS = [k for k, _ in DMG_FIELDS]


def _schema():
    return ["audit_id", "audit_date", "audit_time", "compare_date", "branch_id"] + \
           STOCK_KEYS + DMG_KEYS + ["damage_photo"] + \
           ["auditor", "auditor_id", "auditor_name",
            "remark", "created_at", "updated_at"]


def _auditor_options():
    """คืน {employee_id: 'ชื่อ นามสกุล'} ของผู้ตรวจสอบ (ตำแหน่งฝ่ายตรวจสอบ) จากตาราง employees
    - เฉพาะที่ยังไม่ลาออก
    - ถ้าไม่มีข้อมูลผู้ตรวจสอบเลย → คืน {} (จะบันทึกไม่ได้)
    """
    try:
        edf = read_sheet(SHEET_EMPLOYEES)
    except Exception:
        return {}
    if edf is None or edf.empty or "employee_id" not in edf.columns:
        return {}
    df = edf.copy()
    if "position" in df.columns:
        df = df[df["position"].astype(str).str.contains("ตรวจสอบ", na=False)]
    if "status" in df.columns:
        df = df[df["status"].astype(str).str.strip().str.lower() != "resigned"]
    opts = {}
    for _, r in df.iterrows():
        code = str(r.get("employee_id", "")).strip()
        name = (str(r.get("first_name", "")).strip() + " " +
                str(r.get("last_name", "")).strip()).strip()
        if code:
            opts[code] = name
    return opts


def _init_sheet():
    if st.session_state.get("_init_au"):
        return
    init_workbook()
    try:
        df = read_sheet(SHEET_AUDIT_STOCK_BALANCE)
    except Exception:
        df = pd.DataFrame()
    if df is None or df.empty:
        try:
            write_sheet(SHEET_AUDIT_STOCK_BALANCE, pd.DataFrame(columns=_schema()))
        except Exception:
            pass
    st.session_state["_init_au"] = True


def _branch_options():
    """รายชื่อสาขา — ใช้รายชื่อจาก branch_auth (20 สาขาต้นฉบับ) เป็นหลัก
    แล้วเสริมด้วยตาราง branches (กันกรณีตาราง branches ว่าง/ข้อมูลเก่า)"""
    result = {}
    try:
        from modules.branch_auth import BRANCH_NAMES, BRANCH_LOGIN_SEED
        for b in BRANCH_LOGIN_SEED.keys():
            result[b] = BRANCH_NAMES.get(b, b)
    except Exception:
        pass
    try:
        bdf = read_sheet(SHEET_BRANCHES)
        if bdf is not None and not bdf.empty and "branch_id" in bdf.columns:
            for _, r in bdf.iterrows():
                bid = str(r.get("branch_id", "")).strip()
                if bid and bid not in result:
                    result[bid] = str(r.get("branch_name", "")).strip() or bid
    except Exception:
        pass
    return result


def _branch_stock_for(branch_id, date_str):
    """ยอดที่สาขาบันทึกของวันที่นั้น — คืน dict{field:qty} หรือ None ถ้าไม่มี"""
    try:
        df = read_sheet(SHEET_BRANCH_STOCK_DAILY)
    except Exception:
        return None
    if df is None or df.empty or "branch_id" not in df.columns:
        return None
    m = df[(df["branch_id"].astype(str).str.strip() == str(branch_id)) &
           (df["stock_date"].astype(str) == str(date_str))]
    if m.empty:
        return None
    last = m.iloc[-1]
    return {k: _int(last.get(k, 0)) for k in STOCK_KEYS}


def _diff_rows(branch_stock, audit_count):
    """สร้างรายการส่วนต่าง — เฉพาะรายการที่ไม่ตรงกัน
    ส่วนต่าง = สาขาบันทึก − ตรวจนับจริง(Audit)
    """
    rows = []
    for key, label, unit in STOCK_FIELDS:
        b = _int(branch_stock.get(key, 0))
        a = _int(audit_count.get(key, 0))
        if b != a:
            rows.append({
                "รายการ": f"{label} ({unit})",
                "สาขาบันทึก": b,
                "ตรวจนับจริง (Audit)": a,
                "ส่วนต่าง (สาขา−จริง)": b - a,
            })
    return rows


# ══════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════
def render():
    _init_sheet()

    st.markdown(
        "<h1 style='text-align:center;color:#1565C0;font-size:2rem;"
        "font-weight:900;margin-bottom:2px;'>🔎 ตรวจนับสต๊อกบรรจุภัณฑ์</h1>"
        "<p style='text-align:center;color:#999;margin-top:0;'>"
        "ฝ่ายตรวจสอบนับสต๊อกจริงก่อนเปิดร้าน แล้วเทียบกับยอดที่สาขาบันทึก</p>",
        unsafe_allow_html=True,
    )
    st.divider()

    tab_new, tab_list = st.tabs(["📝 ตรวจนับใหม่", "📋 ประวัติการตรวจ (แก้ไข/ลบ)"])
    with tab_new:
        _render_new()
    with tab_list:
        _render_list()


def _render_new():
    opts = _branch_options()
    if not opts:
        st.warning("⚠️ ยังไม่มีข้อมูลสาขาในระบบ")
        return

    # ── 1.1 รหัสสาขา / เลือกสาขา + 1.2 วันที่ + 1.3 เวลา ──
    c1, c2, c3 = st.columns([2, 1, 1])
    with c1:
        branch_id = st.selectbox(
            "🏪 รหัสสาขา / เลือกสาขาที่ตรวจ", options=list(opts.keys()),
            format_func=lambda k: f"{k} – {opts[k]}", key="au_new_branch")
    with c2:
        audit_date = st.date_input("📅 วันที่เข้าตรวจสอบ",
                                   value=datetime.date.today(), key="au_new_date")
    with c3:
        audit_time = st.time_input("⏰ เวลาที่ตรวจ",
                                   value=datetime.datetime.now().time(),
                                   key="au_new_time")

    # ── 1.4 ชื่อผู้ตรวจสอบสาขา (เลือกจาก HR ถ้ามี หรือพิมพ์ชื่อเองได้) ──
    auditor_opts = _auditor_options()
    auditor_id, auditor_name = "", ""
    if auditor_opts:
        _choices = list(auditor_opts.keys()) + ["__type__"]
        pick = st.selectbox(
            "🧑‍💼 ชื่อผู้ตรวจสอบสาขา", options=_choices,
            format_func=lambda k: ("✍️ พิมพ์ชื่อเอง..." if k == "__type__"
                                   else f"{k} – {auditor_opts[k]}"),
            key="au_new_auditor")
        if pick == "__type__":
            auditor_name = st.text_input("พิมพ์ชื่อผู้ตรวจสอบ",
                                         key="au_new_auditor_txt").strip()
        else:
            auditor_id = pick
            auditor_name = auditor_opts.get(pick, "")
    else:
        st.caption("ℹ️ ยังไม่มีรายชื่อผู้ตรวจสอบใน HR — พิมพ์ชื่อผู้ตรวจสอบได้เลย")
        auditor_name = st.text_input("🧑‍💼 ชื่อผู้ตรวจสอบสาขา (พิมพ์ชื่อ)",
                                     key="au_new_auditor_txt").strip()

    compare_date = audit_date - datetime.timedelta(days=1)

    # กันบันทึกซ้ำ (สาขา+วันที่ตรวจเดียวกัน)
    existing = read_sheet(SHEET_AUDIT_STOCK_BALANCE)
    dup = False
    if not existing.empty and "branch_id" in existing.columns:
        mask = ((existing["branch_id"].astype(str).str.strip() == str(branch_id)) &
                (existing["audit_date"].astype(str) == str(audit_date)))
        if mask.any():
            dup = True
            st.warning("⚠️ มีการตรวจของสาขานี้ในวันที่นี้แล้ว — แก้ไขที่แท็บ 'ประวัติการตรวจ'")

    st.divider()
    st.markdown(
        f"<h3 style='color:#0D47A1;'>📦 บรรจุภัณฑ์คงเหลือ "
        f"ประจำวันที่ {compare_date.strftime('%d/%m/%Y')}</h3>"
        f"<p style='color:#888;margin-top:-6px;'>(= วันที่ตรวจ − 1 วัน "
        f"คือยอดปิดร้านของเมื่อวาน) — กรอกแล้วกด Tab ไปช่องถัดไปได้</p>",
        unsafe_allow_html=True,
    )

    audit_count = {}
    for idx, (key, label, unit) in enumerate(STOCK_FIELDS, 1):
        audit_count[key] = st.number_input(
            f"{idx}. {label} ({unit})", min_value=0, step=1, key=f"au_new_{key}")

    # ── บรรจุภัณฑ์เสียหาย/แตกหัก + แนบรูป (สำหรับ Sale Audit เมนู 3.2) ──
    st.divider()
    st.markdown("#### 💔 บรรจุภัณฑ์เสียหาย/แตกหัก (ใช้ไม่ได้) — ถ้ามี")
    st.caption("จำนวนที่เสียหายจะถูกหักออกตอนคำนวณยอดเงินจากบรรจุภัณฑ์ (Sale Audit)")
    dmg_count = {}
    dcols = st.columns(len(DMG_FIELDS))
    for i, (key, label) in enumerate(DMG_FIELDS):
        dmg_count[key] = dcols[i].number_input(label, min_value=0, step=1, key=f"au_dmg_{key}")
    dmg_photo_file = st.file_uploader("📷 แนบรูปบรรจุภัณฑ์เสียหาย (ถ้ามี)",
                                      type=["png", "jpg", "jpeg"], key="au_dmg_photo")

    remark = st.text_input("📝 หมายเหตุ (ถ้ามี)", key="au_new_remark")

    st.divider()
    # บันทึกได้เลย (บล็อกเฉพาะกรณีบันทึกซ้ำวันเดิม) — ชื่อผู้ตรวจสอบไม่บังคับ
    if st.button("💾 บันทึกผลตรวจนับ", type="primary", use_container_width=True,
                 key="au_new_save", disabled=dup):
        dmg_photo_b64 = ""
        if dmg_photo_file is not None:
            try:
                import base64 as _b64
                dmg_photo_b64 = _b64.b64encode(dmg_photo_file.read()).decode()
            except Exception:
                dmg_photo_b64 = ""
        _save_new(branch_id, str(audit_date), str(audit_time)[:5],
                  str(compare_date), audit_count, remark,
                  auditor_id, auditor_name, dmg_count, dmg_photo_b64)
        # แสดงผลเทียบทันที
        _show_comparison(branch_id, str(compare_date), audit_count, opts)


def _save_new(branch_id, audit_date, audit_time, compare_date, audit_count, remark,
              auditor_id, auditor_name, dmg_count=None, dmg_photo_b64=""):
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df = read_sheet(SHEET_AUDIT_STOCK_BALANCE)
    audit_id = next_id(df, "audit_id", "AUD")
    row = {"audit_id": audit_id, "audit_date": audit_date, "audit_time": audit_time,
           "compare_date": compare_date, "branch_id": branch_id,
           "auditor": auditor_id, "auditor_id": auditor_id,
           "auditor_name": auditor_name,
           "remark": remark, "created_at": now, "updated_at": now,
           "damage_photo": dmg_photo_b64 or ""}
    for k in STOCK_KEYS:
        row[k] = _int(audit_count.get(k, 0))
    for k in DMG_KEYS:
        row[k] = _int((dmg_count or {}).get(k, 0))
    append_row(SHEET_AUDIT_STOCK_BALANCE, row)
    st.success(f"✅ บันทึกผลตรวจสำเร็จ! เลขที่ {audit_id} | "
               f"ผู้ตรวจสอบ: {auditor_id} – {auditor_name}")


def _show_comparison(branch_id, compare_date, audit_count, opts=None):
    opts = opts or _branch_options()
    st.markdown("### 🔍 ผลการเทียบกับยอดที่สาขาบันทึก")
    st.caption(f"สาขา {branch_id} – {opts.get(branch_id,'')} | "
               f"เทียบยอดคงเหลือวันที่ {compare_date}")

    branch_stock = _branch_stock_for(branch_id, compare_date)
    if branch_stock is None:
        st.warning(f"⚠️ สาขายังไม่ได้บันทึกสต๊อกของวันที่ {compare_date} — "
                   f"ยังเทียบส่วนต่างไม่ได้ (ระบบเก็บผลตรวจของ Audit ไว้แล้ว)")
        return

    diffs = _diff_rows(branch_stock, audit_count)
    if not diffs:
        st.success("✅ ตรงกันทุกรายการ — ไม่มีส่วนต่าง")
        return

    st.error(f"⚠️ พบส่วนต่าง {len(diffs)} รายการ (แสดงเฉพาะที่ไม่ตรง)")
    df = pd.DataFrame(diffs)

    def _hl(v):
        try:
            v = float(v)
        except Exception:
            return ""
        if v > 0:
            return "color:#C62828;font-weight:700;"   # สาขามากกว่าจริง
        if v < 0:
            return "color:#1565C0;font-weight:700;"   # สาขาน้อยกว่าจริง
        return ""
    try:
        styled = df.style.map(_hl, subset=["ส่วนต่าง (สาขา−จริง)"])
        st.dataframe(styled, use_container_width=True, hide_index=True)
    except Exception:
        st.dataframe(df, use_container_width=True, hide_index=True)
    st.caption("🔴 บวก = สาขาบันทึกมากกว่าของจริง | 🔵 ลบ = สาขาบันทึกน้อยกว่าของจริง")


# ══════════════════════════════════════════════════════════════
# ประวัติการตรวจ (แก้ไข/ลบ)
# ══════════════════════════════════════════════════════════════
def _render_list():
    st.subheader("📋 ประวัติการตรวจนับ")
    df = read_sheet(SHEET_AUDIT_STOCK_BALANCE)
    if df.empty or "audit_id" not in df.columns:
        st.info("ยังไม่มีประวัติการตรวจ")
        return

    opts = _branch_options()
    c1, c2 = st.columns(2)
    with c1:
        br_filter = st.selectbox(
            "กรองตามสาขา", ["ทั้งหมด"] + list(opts.keys()),
            format_func=lambda k: k if k == "ทั้งหมด" else f"{k} – {opts.get(k,'')}",
            key="au_list_branch")
    with c2:
        d_filter = st.date_input("กรองวันที่ตรวจ (ว่าง = ทั้งหมด)", value=None,
                                 key="au_list_date")

    show = df.copy()
    if br_filter != "ทั้งหมด":
        show = show[show["branch_id"].astype(str).str.strip() == br_filter]
    if d_filter:
        show = show[show["audit_date"].astype(str) == str(d_filter)]
    if show.empty:
        st.info("ไม่พบข้อมูลตามเงื่อนไข")
        return

    show = show.sort_values("audit_date", ascending=False)
    for _, row in show.iterrows():
        aid = str(row["audit_id"])
        title = (f"🔎 {aid} | สาขา {row.get('branch_id','')} | "
                 f"ตรวจ {row.get('audit_date','')}")
        with st.expander(title):
            if st.session_state.get(f"au_edit_{aid}"):
                _render_edit(row, opts)
            else:
                _render_view(row, opts)


def _render_view(row, opts):
    aid = str(row["audit_id"])
    audit_count = {k: _int(row.get(k, 0)) for k in STOCK_KEYS}
    compare_date = str(row.get("compare_date", ""))
    # ข้อมูลหัวการตรวจ: วันที่ / เวลา / ผู้ตรวจสอบ
    _auditor = str(row.get("auditor_name", "")).strip() or str(row.get("auditor", "")).strip()
    _aid = str(row.get("auditor_id", "")).strip()
    _atime = str(row.get("audit_time", "")).strip()
    st.caption(
        f"📅 วันที่ตรวจ {row.get('audit_date','')}"
        + (f" ⏰ {_atime}" if _atime else "")
        + (f" | 🧑‍💼 ผู้ตรวจสอบ: {(_aid + ' – ') if _aid else ''}{_auditor}"
           if _auditor else "")
    )
    _show_comparison(str(row.get("branch_id", "")), compare_date, audit_count, opts)
    if str(row.get("remark", "")).strip():
        st.caption(f"📝 {row.get('remark')}")

    b1, b2 = st.columns(2)
    with b1:
        if st.button("✏️ แก้ไข", key=f"au_editbtn_{aid}", use_container_width=True):
            st.session_state[f"au_edit_{aid}"] = True
            st.rerun()
    with b2:
        if st.session_state.get(f"au_del_{aid}"):
            st.warning("⚠️ ยืนยันลบ?")
            cc1, cc2 = st.columns(2)
            if cc1.button("✅ ยืนยันลบ", key=f"au_delyes_{aid}",
                          type="primary", use_container_width=True):
                delete_row(SHEET_AUDIT_STOCK_BALANCE, "audit_id", aid)
                st.session_state.pop(f"au_del_{aid}", None)
                st.success("ลบแล้ว")
                st.rerun()
            if cc2.button("ยกเลิก", key=f"au_delno_{aid}", use_container_width=True):
                st.session_state.pop(f"au_del_{aid}", None)
                st.rerun()
        else:
            if st.button("🗑️ ลบ", key=f"au_delbtn_{aid}", use_container_width=True):
                st.session_state[f"au_del_{aid}"] = True
                st.rerun()


def _render_edit(row, opts):
    aid = str(row["audit_id"])
    st.markdown(f"### ✏️ แก้ไขผลตรวจ {aid}")
    e1, e2 = st.columns(2)
    with e1:
        audit_date = st.date_input("📅 วันที่เข้าตรวจสอบ",
                                   value=_parse_date(row.get("audit_date")),
                                   key=f"au_e_date_{aid}")
    with e2:
        audit_time = st.time_input("⏰ เวลาที่ตรวจ",
                                   value=_parse_time(row.get("audit_time")),
                                   key=f"au_e_time_{aid}")
    compare_date = audit_date - datetime.timedelta(days=1)
    st.caption(f"เทียบยอดคงเหลือประจำวันที่ {compare_date.strftime('%d/%m/%Y')}")

    # ผู้ตรวจสอบ (แก้ไขได้ — เลือกจาก HR ถ้ามี หรือพิมพ์ชื่อเอง; ไม่บังคับ)
    auditor_opts = _auditor_options()
    _cur_id   = str(row.get("auditor_id", "")).strip()
    _cur_name = str(row.get("auditor_name", "")).strip() or str(row.get("auditor", "")).strip()
    auditor_id, auditor_name = "", ""
    if auditor_opts:
        _keys = list(auditor_opts.keys()) + ["__type__"]
        _idx = _keys.index(_cur_id) if _cur_id in _keys else len(_keys) - 1
        pick = st.selectbox(
            "🧑‍💼 ชื่อผู้ตรวจสอบสาขา", options=_keys, index=_idx,
            format_func=lambda k: ("✍️ พิมพ์ชื่อเอง..." if k == "__type__"
                                   else f"{k} – {auditor_opts[k]}"),
            key=f"au_e_auditor_{aid}")
        if pick == "__type__":
            auditor_name = st.text_input("พิมพ์ชื่อผู้ตรวจสอบ", value=_cur_name,
                                         key=f"au_e_auditor_txt_{aid}").strip()
        else:
            auditor_id = pick
            auditor_name = auditor_opts.get(pick, "")
    else:
        auditor_name = st.text_input("🧑‍💼 ชื่อผู้ตรวจสอบสาขา (พิมพ์ชื่อ)",
                                     value=_cur_name, key=f"au_e_auditor_txt_{aid}").strip()

    audit_count = {}
    for idx, (key, label, unit) in enumerate(STOCK_FIELDS, 1):
        audit_count[key] = st.number_input(
            f"{idx}. {label} ({unit})", min_value=0, step=1,
            value=_int(row.get(key, 0)), key=f"au_e_{key}_{aid}")
    remark = st.text_input("📝 หมายเหตุ", value=str(row.get("remark", "")),
                           key=f"au_e_remark_{aid}")

    b1, b2 = st.columns(2)
    with b1:
        if st.button("💾 บันทึกการแก้ไข", type="primary",
                     use_container_width=True, key=f"au_e_save_{aid}"):
            now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            upd = {"audit_date": str(audit_date), "audit_time": str(audit_time)[:5],
                   "compare_date": str(compare_date), "remark": remark,
                   "auditor": auditor_id, "auditor_id": auditor_id,
                   "auditor_name": auditor_name, "updated_at": now}
            for k in STOCK_KEYS:
                upd[k] = _int(audit_count.get(k, 0))
            update_row(SHEET_AUDIT_STOCK_BALANCE, "audit_id", aid, upd)
            st.session_state.pop(f"au_edit_{aid}", None)
            st.success("✅ แก้ไขสำเร็จ")
            st.rerun()
    with b2:
        if st.button("ยกเลิก", use_container_width=True, key=f"au_e_cancel_{aid}"):
            st.session_state.pop(f"au_edit_{aid}", None)
            st.rerun()


def _parse_date(v):
    try:
        return datetime.datetime.strptime(str(v)[:10], "%Y-%m-%d").date()
    except Exception:
        return datetime.date.today()


def _parse_time(v):
    s = str(v).strip()
    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            return datetime.datetime.strptime(s[:len(fmt) + 2], fmt).time()
        except Exception:
            continue
    return datetime.datetime.now().time()
