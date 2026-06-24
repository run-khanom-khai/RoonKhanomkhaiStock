"""
asset_management.py  –  ระบบทรัพย์สินและการซ่อมแซม
เพิ่มใหม่ ไม่แตะ logic เดิมของ purchase.py
"""
import io
import datetime
import streamlit as st
import pandas as pd

from config import (
    SHEET_BRANCHES, SHEET_ASSETS, SHEET_ASSET_REPAIRS,
    ASSET_CATEGORIES, ASSET_STATUSES, ASSET_LOCATIONS, REPAIR_STATUSES,
)
from modules.excel_db import read_sheet, write_sheet, append_row, update_row, init_workbook
from utils.id_generator import next_id

# ── Schema ──────────────────────────────────────────────────
ASSET_COLS = [
    "asset_code","asset_name","asset_category","brand","model","spec",
    "serial_number","purchase_date","purchase_price","supplier_name",
    "invoice_no","warranty_expire_date","branch_name","location_detail",
    "responsible_person","asset_status","note","is_deleted","created_at","updated_at",
]
REPAIR_COLS = [
    "repair_id","asset_code","asset_name","asset_detail","branch_name",
    "repair_start_date","repair_end_date","problem_detail","technician_report",
    "repair_vendor","repair_cost","repair_days","repair_status","note",
    "created_at","updated_at",
]


def _init_asset_sheets():
    init_workbook()
    for sheet, cols in [(SHEET_ASSETS, ASSET_COLS), (SHEET_ASSET_REPAIRS, REPAIR_COLS)]:
        df = read_sheet(sheet)
        if df.empty or list(df.columns) != cols:
            write_sheet(sheet, pd.DataFrame(columns=cols))


def _branches_list() -> list:
    df = read_sheet(SHEET_BRANCHES)
    if df.empty or "branch_name" not in df.columns:
        return ["สาขาทดสอบ"]
    return df["branch_name"].dropna().tolist() or ["สาขาทดสอบ"]


def _now() -> str:
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _active_assets() -> pd.DataFrame:
    df = read_sheet(SHEET_ASSETS)
    if df.empty:
        return pd.DataFrame(columns=ASSET_COLS)
    return df[df.get("is_deleted", pd.Series(["FALSE"]*len(df))).astype(str).str.upper() != "TRUE"]


# ══════════════════════════════════════════════════════════════
# ① การซื้อทรัพย์สินเข้า
# ══════════════════════════════════════════════════════════════
def render_asset_purchase():
    _init_asset_sheets()
    st.markdown("<h1 style='color:#1976D2;font-size:1.6rem;font-weight:800;"
                "border-left:6px solid #1976D2;padding-left:12px;'>🏢 การซื้อทรัพย์สินเข้า</h1>",
                unsafe_allow_html=True)

    tab_list, tab_add, tab_edit = st.tabs(["📋 รายการทรัพย์สิน","➕ เพิ่มทรัพย์สิน","✏️ แก้ไข / ลบ"])
    with tab_list: _render_asset_list()
    with tab_add:  _render_asset_add()
    with tab_edit: _render_asset_edit()


def _render_asset_list():
    st.subheader("📋 รายการทรัพย์สินทั้งหมด")
    df = _active_assets()
    branches = ["ทั้งหมด"] + _branches_list()

    # ── Filter ──────────────────────────────────────────────
    c1,c2,c3 = st.columns(3)
    with c1:
        f_branch = st.selectbox("สาขา", branches, key="al_branch")
        f_cat    = st.selectbox("ประเภท", ["ทั้งหมด"]+ASSET_CATEGORIES, key="al_cat")
    with c2:
        f_status = st.selectbox("สถานะ", ["ทั้งหมด"]+ASSET_STATUSES[:5], key="al_status")
        f_search = st.text_input("🔍 ค้นหา (รหัส/ชื่อ)", key="al_search")
    with c3:
        f_date_from = st.date_input("วันที่ซื้อ จาก", value=None, key="al_dfrom")
        f_date_to   = st.date_input("วันที่ซื้อ ถึง", value=None, key="al_dto")

    if df.empty:
        st.info("ยังไม่มีข้อมูลทรัพย์สิน")
        return

    dfs = df.copy()
    if f_branch != "ทั้งหมด":
        dfs = dfs[dfs["branch_name"].astype(str) == f_branch]
    if f_cat != "ทั้งหมด":
        dfs = dfs[dfs["asset_category"].astype(str) == f_cat]
    if f_status != "ทั้งหมด":
        dfs = dfs[dfs["asset_status"].astype(str) == f_status]
    if f_search:
        mask = dfs.apply(lambda r: f_search.lower() in
                         (str(r.get("asset_code",""))+str(r.get("asset_name",""))).lower(), axis=1)
        dfs = dfs[mask]
    if f_date_from:
        dfs = dfs[dfs["purchase_date"].astype(str) >= str(f_date_from)]
    if f_date_to:
        dfs = dfs[dfs["purchase_date"].astype(str) <= str(f_date_to)]

    show_cols = ["asset_code","asset_name","asset_category","brand","model",
                 "purchase_date","purchase_price","branch_name","asset_status","warranty_expire_date"]
    st.dataframe(dfs[[c for c in show_cols if c in dfs.columns]], use_container_width=True)
    st.caption(f"พบ {len(dfs)} รายการ")

    # Export
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        dfs.to_excel(w, index=False, sheet_name="assets")
    st.download_button("⬇️ Export Excel", data=buf.getvalue(),
                       file_name=f"assets_{datetime.date.today()}.xlsx",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    # Print
    if st.button("🖨️ พิมพ์รายงาน"):
        _print_asset_report(dfs)


def _print_asset_report(df: pd.DataFrame):
    rows_html = ""
    for _, r in df.iterrows():
        rows_html += "<tr>" + "".join(
            f"<td style='border:1px solid #ccc;padding:4px;font-size:11px;'>{r.get(c,'')}</td>"
            for c in ["asset_code","asset_name","asset_category","branch_name",
                      "purchase_date","purchase_price","asset_status"]
        ) + "</tr>"
    html = f"""<html><body>
    <h2>รายงานทรัพย์สิน — {datetime.date.today()}</h2>
    <table style='border-collapse:collapse;width:100%;'>
    <tr>{''.join(f"<th style='border:1px solid #ccc;padding:4px;background:#1976D2;color:white;'>{h}</th>"
        for h in ['รหัส','ชื่อ','ประเภท','สาขา','วันที่ซื้อ','ราคา','สถานะ'])}</tr>
    {rows_html}</table></body></html>"""
    st.markdown(html, unsafe_allow_html=True)
    st.info("กด Ctrl+P เพื่อพิมพ์หน้านี้ครับ")


def _render_asset_add():
    st.subheader("➕ เพิ่มทรัพย์สินใหม่")
    branches = _branches_list()

    with st.form("form_add_asset"):
        c1,c2 = st.columns(2)
        with c1:
            asset_code   = st.text_input("รหัสทรัพย์สิน * (unique)")
            asset_name   = st.text_input("ชื่อทรัพย์สิน *")
            asset_cat    = st.selectbox("ประเภท *", ASSET_CATEGORIES)
            brand        = st.text_input("แบรนด์")
            model        = st.text_input("รุ่น")
            spec         = st.text_area("Spec / รายละเอียด", height=80)
            serial_no    = st.text_input("Serial Number")
        with c2:
            purchase_date    = st.date_input("วันที่ซื้อ *", value=datetime.date.today())
            purchase_price   = st.number_input("ราคาซื้อ (บาท) *", min_value=0.0, step=100.0)
            supplier_name    = st.text_input("ผู้ขาย / ร้านค้า")
            invoice_no       = st.text_input("เลขที่ใบเสร็จ / ใบกำกับ")
            warranty_date    = st.date_input("วันหมดประกัน", value=None)
            branch_name      = st.selectbox("สาขา *", branches)
            location_detail  = st.selectbox("ตำแหน่งที่ใช้งาน", ASSET_LOCATIONS)
            responsible      = st.text_input("ผู้รับผิดชอบ")
            asset_status     = st.selectbox("สถานะ", ASSET_STATUSES[:5])
            note             = st.text_input("หมายเหตุ")

        saved = st.form_submit_button("💾 บันทึกทรัพย์สิน", type="primary")

    if saved:
        # Validation
        errors = []
        if not asset_code.strip():    errors.append("กรุณากรอกรหัสทรัพย์สิน")
        if not asset_name.strip():    errors.append("กรุณากรอกชื่อทรัพย์สิน")
        if not branch_name:           errors.append("กรุณาเลือกสาขา")
        # ตรวจรหัสซ้ำ
        df = read_sheet(SHEET_ASSETS)
        if not df.empty and asset_code.strip() in df["asset_code"].astype(str).tolist():
            errors.append(f"รหัสทรัพย์สิน '{asset_code}' มีอยู่แล้วในระบบ")
        # warranty >= purchase_date
        if warranty_date and warranty_date < purchase_date:
            errors.append("วันหมดประกันต้องไม่ก่อนวันที่ซื้อ")
        if errors:
            for e in errors: st.error(e)
            return

        now = _now()
        append_row(SHEET_ASSETS, {
            "asset_code": asset_code.strip(), "asset_name": asset_name.strip(),
            "asset_category": asset_cat, "brand": brand, "model": model,
            "spec": spec, "serial_number": serial_no,
            "purchase_date": str(purchase_date), "purchase_price": purchase_price,
            "supplier_name": supplier_name, "invoice_no": invoice_no,
            "warranty_expire_date": str(warranty_date) if warranty_date else "",
            "branch_name": branch_name, "location_detail": location_detail,
            "responsible_person": responsible, "asset_status": asset_status,
            "note": note, "is_deleted": "FALSE", "created_at": now, "updated_at": now,
        })
        st.success(f"✅ เพิ่มทรัพย์สิน '{asset_name}' รหัส '{asset_code}' สำเร็จ")


def _render_asset_edit():
    st.subheader("✏️ แก้ไข / ลบทรัพย์สิน")
    df = _active_assets()
    if df.empty:
        st.info("ยังไม่มีทรัพย์สิน")
        return

    branches = _branches_list()
    search = st.text_input("🔍 พิมพ์รหัสหรือชื่อเพื่อค้นหา", key="edit_search")
    dfs = df.copy()
    if search:
        mask = dfs.apply(lambda r: search.lower() in
                         (str(r.get("asset_code",""))+str(r.get("asset_name",""))).lower(), axis=1)
        dfs = dfs[mask]

    if dfs.empty:
        st.info("ไม่พบทรัพย์สิน")
        return

    opts = dfs["asset_code"].tolist()
    sel_code = st.selectbox("เลือกทรัพย์สิน", opts,
                             format_func=lambda c: f"{c} — {dfs[dfs['asset_code']==c]['asset_name'].values[0]}")
    row = dfs[dfs["asset_code"] == sel_code].iloc[0]

    with st.form("form_edit_asset"):
        c1,c2 = st.columns(2)
        with c1:
            asset_name   = st.text_input("ชื่อทรัพย์สิน *", value=row.get("asset_name",""))
            asset_cat    = st.selectbox("ประเภท", ASSET_CATEGORIES,
                                         index=ASSET_CATEGORIES.index(row.get("asset_category",ASSET_CATEGORIES[0]))
                                         if row.get("asset_category") in ASSET_CATEGORIES else 0)
            brand        = st.text_input("แบรนด์", value=row.get("brand",""))
            model        = st.text_input("รุ่น", value=row.get("model",""))
            spec         = st.text_area("Spec", value=row.get("spec",""), height=80)
            serial_no    = st.text_input("Serial Number", value=row.get("serial_number",""))
        with c2:
            try:    pd_val = datetime.date.fromisoformat(row.get("purchase_date","2024-01-01"))
            except: pd_val = datetime.date.today()
            purchase_date  = st.date_input("วันที่ซื้อ", value=pd_val)
            try:    pp_val = float(row.get("purchase_price",0))
            except: pp_val = 0.0
            purchase_price = st.number_input("ราคาซื้อ", min_value=0.0, step=100.0, value=pp_val)
            supplier_name  = st.text_input("ผู้ขาย", value=row.get("supplier_name",""))
            invoice_no     = st.text_input("เลขที่ใบเสร็จ", value=row.get("invoice_no",""))
            br_list = branches
            br_idx  = br_list.index(row.get("branch_name")) if row.get("branch_name") in br_list else 0
            branch_name = st.selectbox("สาขา", br_list, index=br_idx)
            loc_idx = ASSET_LOCATIONS.index(row.get("location_detail",ASSET_LOCATIONS[0])) \
                      if row.get("location_detail") in ASSET_LOCATIONS else 0
            location_detail = st.selectbox("ตำแหน่ง", ASSET_LOCATIONS, index=loc_idx)
            responsible     = st.text_input("ผู้รับผิดชอบ", value=row.get("responsible_person",""))
            st_list = ASSET_STATUSES[:5]
            st_idx  = st_list.index(row.get("asset_status",st_list[0])) \
                      if row.get("asset_status") in st_list else 0
            asset_status = st.selectbox("สถานะ", st_list, index=st_idx)
            note         = st.text_input("หมายเหตุ", value=row.get("note",""))

        cs, cd = st.columns(2)
        with cs: save   = st.form_submit_button("💾 บันทึกการแก้ไข", type="primary")
        with cd: delete = st.form_submit_button("🗑️ ลบทรัพย์สิน")

    if save:
        if not asset_name.strip():
            st.error("กรุณากรอกชื่อทรัพย์สิน"); return
        update_row(SHEET_ASSETS, "asset_code", sel_code, {
            "asset_name": asset_name, "asset_category": asset_cat,
            "brand": brand, "model": model, "spec": spec, "serial_number": serial_no,
            "purchase_date": str(purchase_date), "purchase_price": purchase_price,
            "supplier_name": supplier_name, "invoice_no": invoice_no,
            "branch_name": branch_name, "location_detail": location_detail,
            "responsible_person": responsible, "asset_status": asset_status,
            "note": note, "updated_at": _now(),
        })
        st.success(f"✅ แก้ไขทรัพย์สิน '{sel_code}' สำเร็จ")
        st.rerun()

    if delete:
        # ตรวจว่ามีประวัติซ่อมหรือไม่ → Soft Delete
        rep_df = read_sheet(SHEET_ASSET_REPAIRS)
        has_repair = not rep_df.empty and sel_code in rep_df.get("asset_code", pd.Series()).tolist()
        if has_repair:
            st.warning("⚠️ ทรัพย์สินนี้มีประวัติการซ่อม — เปลี่ยนสถานะเป็น 'ลบแล้ว' แทนการลบจริง")
            update_row(SHEET_ASSETS, "asset_code", sel_code,
                       {"asset_status": "ลบแล้ว", "is_deleted": "TRUE", "updated_at": _now()})
        else:
            update_row(SHEET_ASSETS, "asset_code", sel_code,
                       {"is_deleted": "TRUE", "updated_at": _now()})
        st.success(f"🗑️ ลบทรัพย์สิน '{sel_code}' สำเร็จ")
        st.rerun()


# ══════════════════════════════════════════════════════════════
# ② การซ่อมแซมทรัพย์สิน
# ══════════════════════════════════════════════════════════════
def render_asset_repair():
    _init_asset_sheets()
    st.markdown("<h1 style='color:#E65100;font-size:1.6rem;font-weight:800;"
                "border-left:6px solid #E65100;padding-left:12px;'>🛠️ การซ่อมแซมทรัพย์สิน</h1>",
                unsafe_allow_html=True)

    tab_add, tab_history, tab_report = st.tabs([
        "➕ บันทึกการส่งซ่อม",
        "📋 ประวัติการซ่อม",
        "📊 รายงานค่าใช้จ่าย",
    ])
    with tab_add:     _render_repair_add()
    with tab_history: _render_repair_history()
    with tab_report:  _render_repair_cost_report()


def _render_repair_add():
    st.subheader("➕ บันทึกการส่งซ่อม")
    asset_df = _active_assets()
    branches = _branches_list()

    if asset_df.empty:
        st.warning("ยังไม่มีทรัพย์สินในระบบ — กรุณาเพิ่มทรัพย์สินก่อน")
        return

    # ── เลือกทรัพย์สิน ──────────────────────────────────────
    search_code = st.text_input("🔍 พิมพ์รหัสหรือชื่อทรัพย์สิน", key="rep_search")
    asset_list  = asset_df.copy()
    if search_code:
        mask = asset_list.apply(lambda r: search_code.lower() in
                                (str(r.get("asset_code",""))+str(r.get("asset_name",""))).lower(), axis=1)
        asset_list = asset_list[mask]

    if asset_list.empty:
        st.info("ไม่พบทรัพย์สิน")
        return

    opts = asset_list["asset_code"].tolist()
    sel_code = st.selectbox("เลือกทรัพย์สิน", opts,
                             format_func=lambda c: f"{c} — {asset_list[asset_list['asset_code']==c]['asset_name'].values[0]}")
    arow = asset_list[asset_list["asset_code"] == sel_code].iloc[0]

    # แสดงรายละเอียดทรัพย์สิน
    asset_detail = f"{arow.get('brand','')} {arow.get('model','')} | S/N: {arow.get('serial_number','')} | {arow.get('spec','')}"
    st.info(f"🏢 **{arow.get('asset_name','')}** | {arow.get('asset_category','')} | สาขา: {arow.get('branch_name','')} | {asset_detail}")

    # ตรวจรายการซ่อมที่ยังไม่ปิด
    rep_df = read_sheet(SHEET_ASSET_REPAIRS)
    open_repairs = pd.DataFrame()
    if not rep_df.empty:
        open_repairs = rep_df[
            (rep_df["asset_code"].astype(str) == sel_code) &
            (rep_df["repair_status"].astype(str) == "กำลังซ่อม")
        ]
    if not open_repairs.empty:
        st.warning(f"⚠️ ทรัพย์สินนี้มีรายการซ่อมที่ยังไม่ปิดงาน {len(open_repairs)} รายการ — คุณต้องการสร้างรายการใหม่หรือปิดรายการเดิม?")
        if st.checkbox("ฉันต้องการสร้างรายการซ่อมใหม่แม้มีรายการเปิดอยู่", key="force_new"):
            pass
        else:
            # แสดงรายการเปิดให้ปิดงาน
            _render_close_repair(open_repairs)
            return

    with st.form("form_add_repair"):
        c1,c2 = st.columns(2)
        with c1:
            repair_start = st.date_input("วันที่ส่งซ่อม *", value=datetime.date.today())
            repair_end   = st.date_input("วันที่ซ่อมเสร็จ (ว่าง = ยังซ่อมอยู่)", value=None)
            problem      = st.text_area("อาการเสีย / สาเหตุ *", height=100)
            tech_report  = st.text_area("อาการที่ช่างแจ้ง", height=80)
        with c2:
            br_list = branches
            br_idx  = br_list.index(arow.get("branch_name")) if arow.get("branch_name") in br_list else 0
            branch_name   = st.selectbox("สาขา", br_list, index=br_idx)
            repair_vendor = st.text_input("ร้านซ่อม / ช่างซ่อม")
            try:    cost_val = float(arow.get("repair_cost",0))
            except: cost_val = 0.0
            repair_cost = st.number_input("ค่าใช้จ่าย (บาท)", min_value=0.0, step=100.0)
            note        = st.text_input("หมายเหตุ")

        # preview days
        if repair_end:
            days = (repair_end - repair_start).days
            st.info(f"จำนวนวันซ่อม: **{days} วัน**")

        saved = st.form_submit_button("💾 บันทึกการส่งซ่อม", type="primary")

    if saved:
        errors = []
        if not problem.strip(): errors.append("กรุณากรอกอาการเสีย")
        if repair_end and repair_end < repair_start: errors.append("วันที่ซ่อมเสร็จต้องไม่ก่อนวันส่งซ่อม")
        # ตรวจรายการซ้ำ
        if not rep_df.empty:
            dup = rep_df[
                (rep_df["asset_code"].astype(str) == sel_code) &
                (rep_df["repair_start_date"].astype(str) == str(repair_start))
            ]
            if not dup.empty:
                errors.append(f"มีรายการซ่อมของ '{sel_code}' วันที่ {repair_start} อยู่แล้ว")
        if errors:
            for e in errors: st.error(e)
            return

        repair_days  = (repair_end - repair_start).days if repair_end else 0
        repair_status = "ซ่อมเสร็จแล้ว" if repair_end else "กำลังซ่อม"
        rep_df2 = read_sheet(SHEET_ASSET_REPAIRS)
        repair_id = next_id(rep_df2, "repair_id", "REP")
        now = _now()
        append_row(SHEET_ASSET_REPAIRS, {
            "repair_id": repair_id,
            "asset_code": sel_code,
            "asset_name": arow.get("asset_name",""),
            "asset_detail": asset_detail,
            "branch_name": branch_name,
            "repair_start_date": str(repair_start),
            "repair_end_date": str(repair_end) if repair_end else "",
            "problem_detail": problem,
            "technician_report": tech_report,
            "repair_vendor": repair_vendor,
            "repair_cost": repair_cost,
            "repair_days": repair_days,
            "repair_status": repair_status,
            "note": note,
            "created_at": now, "updated_at": now,
        })
        # อัปเดตสถานะทรัพย์สิน
        new_asset_status = "ใช้งานอยู่" if repair_end else "ส่งซ่อม"
        update_row(SHEET_ASSETS, "asset_code", sel_code,
                   {"asset_status": new_asset_status, "updated_at": now})
        st.success(f"✅ บันทึกการส่งซ่อม {repair_id} สำเร็จ | สถานะ: {repair_status}")
        st.rerun()


def _render_close_repair(open_df: pd.DataFrame):
    """ปิดงานซ่อมที่ยังค้างอยู่"""
    st.subheader("🔒 ปิดงานซ่อม")
    for _, r in open_df.iterrows():
        with st.expander(f"REP: {r.get('repair_id','')} | ส่งซ่อม: {r.get('repair_start_date','')} | {r.get('problem_detail','')}"):
            with st.form(f"close_{r.get('repair_id','')}"):
                end_date    = st.date_input("วันที่ซ่อมเสร็จ", value=datetime.date.today())
                tech_report = st.text_area("อาการที่ช่างแจ้ง", value=r.get("technician_report",""))
                try:    cost_v = float(r.get("repair_cost",0))
                except: cost_v = 0.0
                repair_cost = st.number_input("ค่าใช้จ่าย", min_value=0.0, value=cost_v)
                note        = st.text_input("หมายเหตุ", value=r.get("note",""))
                close = st.form_submit_button("✅ ปิดงานซ่อม", type="primary")
            if close:
                try: start = datetime.date.fromisoformat(str(r.get("repair_start_date","")))
                except: start = datetime.date.today()
                days = (end_date - start).days
                update_row(SHEET_ASSET_REPAIRS, "repair_id", str(r.get("repair_id","")), {
                    "repair_end_date": str(end_date),
                    "repair_days": days,
                    "repair_cost": repair_cost,
                    "technician_report": tech_report,
                    "repair_status": "ซ่อมเสร็จแล้ว",
                    "note": note,
                    "updated_at": _now(),
                })
                update_row(SHEET_ASSETS, "asset_code", str(r.get("asset_code","")),
                           {"asset_status": "ใช้งานอยู่", "updated_at": _now()})
                st.success("✅ ปิดงานซ่อมสำเร็จ")
                st.rerun()


def _render_repair_history():
    st.subheader("📋 ประวัติการซ่อมทั้งหมด")
    rep_df = read_sheet(SHEET_ASSET_REPAIRS)
    branches = ["ทั้งหมด"] + _branches_list()

    c1,c2,c3 = st.columns(3)
    with c1:
        f_branch = st.selectbox("สาขา", branches, key="rh_branch")
        f_status = st.selectbox("สถานะ", ["ทั้งหมด"]+REPAIR_STATUSES, key="rh_status")
    with c2:
        f_code   = st.text_input("รหัสทรัพย์สิน", key="rh_code")
        f_name   = st.text_input("ชื่อทรัพย์สิน", key="rh_name")
    with c3:
        f_from   = st.date_input("วันส่งซ่อม จาก", value=None, key="rh_from")
        f_to     = st.date_input("วันส่งซ่อม ถึง", value=None, key="rh_to")

    if rep_df.empty:
        st.info("ยังไม่มีประวัติการซ่อม")
        return

    dfs = rep_df.copy()
    if f_branch != "ทั้งหมด":
        dfs = dfs[dfs["branch_name"].astype(str) == f_branch]
    if f_status != "ทั้งหมด":
        dfs = dfs[dfs["repair_status"].astype(str) == f_status]
    if f_code:
        dfs = dfs[dfs["asset_code"].astype(str).str.contains(f_code, case=False)]
    if f_name:
        dfs = dfs[dfs["asset_name"].astype(str).str.contains(f_name, case=False)]
    if f_from:
        dfs = dfs[dfs["repair_start_date"].astype(str) >= str(f_from)]
    if f_to:
        dfs = dfs[dfs["repair_start_date"].astype(str) <= str(f_to)]

    # เรียงล่าสุดก่อน
    dfs = dfs.sort_values("repair_start_date", ascending=False)

    # สรุป
    try:
        total_cost = pd.to_numeric(dfs["repair_cost"], errors="coerce").fillna(0).sum()
        avg_days   = pd.to_numeric(dfs["repair_days"], errors="coerce").fillna(0).mean()
    except: total_cost, avg_days = 0, 0

    c1,c2,c3 = st.columns(3)
    c1.metric("รายการทั้งหมด", len(dfs))
    c2.metric("ค่าใช้จ่ายรวม", f"฿{total_cost:,.2f}")
    c3.metric("เฉลี่ยวันซ่อม", f"{avg_days:.1f} วัน")

    st.dataframe(dfs, use_container_width=True)

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        dfs.to_excel(w, index=False, sheet_name="repair_history")
    st.download_button("⬇️ Export Excel", data=buf.getvalue(),
                       file_name=f"repair_history_{datetime.date.today()}.xlsx",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    if st.button("🖨️ พิมพ์รายงาน", key="print_repair"):
        rows_html = ""
        for _, r in dfs.iterrows():
            rows_html += "<tr>" + "".join(
                f"<td style='border:1px solid #ccc;padding:4px;font-size:11px;'>{r.get(c,'')}</td>"
                for c in ["asset_code","asset_name","branch_name","repair_start_date",
                           "repair_end_date","repair_days","repair_cost","repair_status"]
            ) + "</tr>"
        html = f"""<html><body>
        <h2>รายงานประวัติการซ่อม — {datetime.date.today()}</h2>
        <table style='border-collapse:collapse;width:100%;'>
        <tr>{''.join(f"<th style='border:1px solid #ccc;padding:4px;background:#E65100;color:white;'>{h}</th>"
            for h in ['รหัส','ชื่อ','สาขา','ส่งซ่อม','เสร็จ','วัน','ค่าใช้จ่าย','สถานะ'])}</tr>
        {rows_html}</table></body></html>"""
        st.markdown(html, unsafe_allow_html=True)
        st.info("กด Ctrl+P เพื่อพิมพ์ครับ")

    # แสดงประวัติแยกตาม asset_code
    if f_code:
        st.divider()
        st.subheader(f"📋 ประวัติการซ่อม: {f_code}")
        asset_hist = dfs[dfs["asset_code"].astype(str) == f_code]
        if not asset_hist.empty:
            adf = _active_assets()
            arow = adf[adf["asset_code"].astype(str) == f_code]
            if not arow.empty:
                r = arow.iloc[0]
                st.info(f"🏢 **{r.get('asset_name','')}** | {r.get('brand','')} {r.get('model','')} | สาขา: {r.get('branch_name','')}")
            st.dataframe(asset_hist.sort_values("repair_start_date", ascending=False),
                         use_container_width=True)


def _render_repair_cost_report():
    st.subheader("📊 รายงานค่าใช้จ่ายซ่อมตามทรัพย์สิน")
    rep_df = read_sheet(SHEET_ASSET_REPAIRS)

    if rep_df.empty:
        st.info("ยังไม่มีข้อมูลการซ่อม")
        return

    rep_df["repair_cost"] = pd.to_numeric(rep_df["repair_cost"], errors="coerce").fillna(0)
    rep_df["repair_days"] = pd.to_numeric(rep_df["repair_days"], errors="coerce").fillna(0)

    summary = rep_df.groupby("asset_code").agg(
        asset_name    =("asset_name","first"),
        branch_name   =("branch_name","first"),
        จำนวนครั้ง   =("repair_id","count"),
        ค่าใช้จ่ายรวม =("repair_cost","sum"),
        วันล่าสุด     =("repair_start_date","max"),
        สถานะล่าสุด   =("repair_status","last"),
    ).reset_index()
    summary["ค่าใช้จ่ายเฉลี่ย/ครั้ง"] = (summary["ค่าใช้จ่ายรวม"] / summary["จำนวนครั้ง"]).round(2)
    summary = summary.sort_values("ค่าใช้จ่ายรวม", ascending=False)

    st.dataframe(summary, use_container_width=True)

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        summary.to_excel(w, index=False, sheet_name="repair_cost_report")
    st.download_button("⬇️ Export Excel", data=buf.getvalue(),
                       file_name=f"repair_cost_{datetime.date.today()}.xlsx",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
