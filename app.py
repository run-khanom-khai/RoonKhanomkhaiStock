"""
# Updated: 2026-07-05 15:37:15
app.py  –  ROON KHANOMKHAI Management System
ผู้พัฒนา: ดร.อภิวรรณ์ ดำแสงสวัสดิ์ | Copyright © 12/06/2026
"""
import io, base64, traceback, os
import streamlit as st
from config import APP_TITLE, APP_ICON, APP_LAYOUT, ALL_SHEETS

st.set_page_config(page_title=APP_TITLE, page_icon=APP_ICON, layout=APP_LAYOUT)

# ── Load Logo ────────────────────────────────────────────────
def _load_logo_b64():
    logo_path = os.path.join(os.path.dirname(__file__), "logo_roon.png")
    if os.path.exists(logo_path):
        with open(logo_path, "rb") as f:
            return base64.b64encode(f.read()).decode()
    return ""

LOGO_B64 = _load_logo_b64()

# ── Bootstrap ────────────────────────────────────────────────
try:
    from modules.excel_db import init_workbook, read_sheet, write_sheet
    from modules.master_data import seed_all
    from modules.auth import (
        render_login, render_manage_passwords,
        _init_auth_sheet, SHEET_AUTH
    )
    # ⚡ รันเตรียมข้อมูล (seed/init) แค่ครั้งเดียวต่อ session
    #    ป้องกันการอ่าน/เขียนฐานข้อมูลซ้ำทุกครั้งที่เปลี่ยนเมนู (ต้นเหตุความช้า)
    if not st.session_state.get("_bootstrapped"):
        init_workbook()
        seed_all()
        _init_auth_sheet()
        st.session_state["_bootstrapped"] = True
except Exception as e:
    err_msg = str(e).lower()
    if any(k in err_msg for k in ["zip file","not a zip","badzip","quota","429","rate"]):
        # quota หรือ Excel error — ข้ามไปเงียบๆ ระบบยังทำงานได้
        pass
    else:
        st.error(f"❌ ไม่สามารถเริ่มต้นระบบได้: {e}")
        st.stop()

# ── Session init ─────────────────────────────────────────────
# ── SESSION STATE INIT ──────────────────────────────────────
# ใช้ query params เป็น fallback กัน session หลุด
def _restore_session():
    """กู้คืน session จาก query params ถ้า session หลุด"""
    if st.session_state.get("logged_in"):
        return  # session ยังอยู่ ไม่ต้องทำอะไร

    # ลอง restore จาก query params
    try:
        params   = st.query_params
        dept_id  = params.get("d", "")
        dept_tok = params.get("t", "")
        if dept_id and dept_tok:
            import hashlib
            expected = hashlib.md5(
                f"{dept_id}:roon2026".encode()
            ).hexdigest()[:8]
            if dept_tok == expected:
                from modules.auth import get_dept_info, get_allowed_menus
                try:
                    info = get_dept_info(dept_id)
                    if info:
                        st.session_state["logged_in"]     = True
                        st.session_state["dept_id"]       = dept_id
                        st.session_state["dept_name"]     = info.get("name", dept_id)
                        st.session_state["allowed_menus"] = get_allowed_menus(dept_id)
                        return
                except Exception:
                    pass
    except Exception:
        pass

if "logged_in" not in st.session_state:
    st.session_state["logged_in"]     = False
    st.session_state["dept_id"]       = ""
    st.session_state["dept_name"]     = ""
    st.session_state["allowed_menus"] = []

_restore_session()

# ── LOGIN CHECK ──────────────────────────────────────────────
if not st.session_state["logged_in"]:
    render_login(LOGO_B64, app_title="🥚 ระบบการบริหารจัดการร้านรุนขนมไข่")
    st.stop()

# ── บันทึก session ใน query params ──────────────────────────
try:
    import hashlib
    dept_id = st.session_state.get("dept_id","")
    if dept_id:
        tok = hashlib.md5(f"{dept_id}:roon2026".encode()).hexdigest()[:8]
        st.query_params["d"] = dept_id
        st.query_params["t"] = tok
except Exception:
    pass

# ── Lazy import ──────────────────────────────────────────────
def _safe_import(module_path: str):
    try:
        import importlib
        return importlib.import_module(module_path)
    except Exception:
        return None

# ── Clear Sheets ─────────────────────────────────────────────
def _clear_sheets(sheet_names: list):
    import pandas as pd
    cleared = []
    for s in sheet_names:
        try:
            df = read_sheet(s)
            if not df.empty:
                write_sheet(s, pd.DataFrame(columns=df.columns))
                cleared.append(s)
        except Exception:
            pass
    return cleared

# ⚠️ ตารางที่ "ห้ามล้างเด็ดขาด" (ข้อมูลหลัก) — โดยเฉพาะ สาขา และ พนักงาน
#    รวมถึงข้อมูลตั้งต้นที่ต้องใช้ตอนเปิดร้าน (รหัสสาขา, สินค้า, ต้นทุน, บทบาท)
PROTECTED_SHEETS = {
    "branches",            # ❌ ห้ามลบสาขา
    "employees",           # ❌ ห้ามลบชื่อพนักงาน
    "branch_login",        # รหัสผ่านเข้าระบบของสาขา
    "users", "roles",      # ผู้ใช้/บทบาท
    "items", "products",   # แคตตาล็อกวัตถุดิบ/บรรจุภัณฑ์/สินค้าสำเร็จรูป
    "material_cost",       # ตารางต้นทุนวัตถุดิบ
    "late_deduction_rules",# กฎหักเงินมาสาย
    "branch_groups", "area_master", "item_categories", "sales_channels",
    "coupons",             # คูปองแม่ (HQ ออกให้)
}

# กลุ่มข้อมูล "ธุรกรรม" ที่ล้างได้ (ปรับตามโครงสร้างฐานข้อมูลใหม่ 2026)
_TXN_BRANCH = ["branch_sales", "branch_sales_coupons", "branch_sales_slips",
               "branch_sales_delivery", "branch_stock_daily"]
_TXN_AUDIT  = ["audit_stock_balance"]
_TXN_PUR    = ["purchase_orders", "purchase_order_items", "stock_in_to_branch",
               "stock_movements", "production_batches", "production_material_used",
               "material_daily"]
_TXN_PAY    = ["payroll_periods", "payroll_records"]
_TXN_FIN    = ["bank_transactions", "daily_sales_accounting", "branch_expenses",
               "marketing_daily_sales", "marketing_daily_sales_items", "sales_reconcile"]
_TXN_PETTY  = ["petty_cash_requests", "petty_cash_attachments",
               "petty_cash_transactions", "petty_cash_funds"]
# ตารางรายงานชุดเก่า (เผื่อยังมีข้อมูลค้าง)
_TXN_LEGACY = ["branch_daily_reports", "branch_front_sales_packaging",
               "branch_drink_sales_detail", "branch_material_balance",
               "branch_packaging_balance", "delivery_packaging_sales",
               "branch_other_stock_balance", "branch_special_remark",
               "branch_sales_recheck", "audit_sessions",
               "audit_packaging_balance", "audit_packaging_diff",
               "true_stock_balance", "daily_stock_usage", "daily_packaging_cost"]

CLEAR_GROUPS = {
    "🧹 ล้างข้อมูลทดสอบทั้งหมด (ก่อนเปิดใช้จริง)":
        (_TXN_BRANCH + _TXN_AUDIT + _TXN_PUR + _TXN_PAY + _TXN_FIN +
         _TXN_PETTY + _TXN_LEGACY),
    "🧾 ขาย / สต๊อกสาขา": _TXN_BRANCH,
    "🔎 ตรวจนับ (Audit)": _TXN_AUDIT,
    "🛒 จัดซื้อ / รับเข้า / ผลิต / Movement": _TXN_PUR,
    "💵 เงินเดือน (ไม่ลบชื่อพนักงาน)": _TXN_PAY,
    "💰 การเงิน / บัญชี / การตลาด": _TXN_FIN,
    "🪙 เงินสดย่อย": _TXN_PETTY,
}


def _safe_clear_list(sheets):
    """คัดตารางที่ห้ามลบออก (กันลบสาขา/พนักงาน/ข้อมูลหลักโดยพลาด)"""
    return [s for s in sheets if s not in PROTECTED_SHEETS]


def _render_clear_data():
    st.markdown("<h1 style='color:#B71C1C;font-size:1.8rem;font-weight:800;"
                "border-left:6px solid #B71C1C;padding-left:12px;'>🗑️ Clear Data</h1>",
                unsafe_allow_html=True)
    st.markdown("<div style='background:#FFEBEE;border:2px solid #EF9A9A;border-radius:8px;"
                "padding:14px;margin-bottom:16px;'><b style='color:#B71C1C;'>⚠️ คำเตือน</b><br>"
                "<span style='color:#C62828;'>การลบข้อมูลไม่สามารถกู้คืนได้ "
                "กรุณา Backup ก่อนทุกครั้ง</span></div>",
                unsafe_allow_html=True)
    st.info("🔒 ระบบจะ **ไม่ลบ** ข้อมูลหลักเด็ดขาด: **สาขา**, **ชื่อพนักงาน**, "
            "รหัสเข้าระบบสาขา, แคตตาล็อกวัตถุดิบ/สินค้า, ต้นทุน และคูปอง")

    group_name  = st.selectbox("กลุ่มข้อมูลที่จะล้าง", list(CLEAR_GROUPS.keys()))
    sheets_to_c = _safe_clear_list(CLEAR_GROUPS[group_name])
    with st.expander(f"📋 Sheet ที่จะถูกล้าง ({len(sheets_to_c)})"):
        for s in sheets_to_c:
            try: rows = len(read_sheet(s))
            except: rows = 0
            st.markdown(f"{'🔴' if rows>0 else '⚪'} `{s}` — **{rows} แถว**")
    confirm = st.text_input("พิมพ์ **ยืนยันลบ** เพื่อยืนยัน", placeholder="ยืนยันลบ")
    col1, col2 = st.columns(2)
    with col1:
        do_clear = st.button("🗑️ ลบข้อมูล", type="primary",
                              use_container_width=True, disabled=(confirm != "ยืนยันลบ"))
    with col2:
        st.button("❌ ยกเลิก", use_container_width=True)
    if do_clear and confirm == "ยืนยันลบ":
        with st.spinner("กำลังลบข้อมูล..."):
            cleared = _clear_sheets(sheets_to_c)
        st.success(f"✅ ล้างข้อมูลสำเร็จ {len(cleared)} Sheet "
                   f"(ไม่แตะ สาขา/พนักงาน/ข้อมูลหลัก)")
        try: seed_all()
        except: pass
        st.rerun()

def _render_export():
    import pandas as pd
    st.markdown("<h1 style='color:#7B1FA2;font-size:1.8rem;font-weight:800;"
                "border-left:6px solid #7B1FA2;padding-left:12px;'>📤 Export Data</h1>",
                unsafe_allow_html=True)
    sheet_sel = st.selectbox("เลือก Sheet", ALL_SHEETS)
    try: df = read_sheet(sheet_sel)
    except Exception as e:
        st.error(f"ไม่สามารถอ่าน Sheet ได้: {e}"); return
    if df.empty:
        st.warning(f"Sheet '{sheet_sel}' ยังไม่มีข้อมูล")
    else:
        st.success(f"✅ {len(df)} แถว | {len(df.columns)} คอลัมน์")
        st.dataframe(df, use_container_width=True)
    buf = io.BytesIO()
    try:
        with pd.ExcelWriter(buf, engine="openpyxl") as w:
            df.to_excel(w, index=False, sheet_name=sheet_sel[:31])
        st.download_button(f"⬇️ ดาวน์โหลด {sheet_sel}.xlsx",
                           data=buf.getvalue(), file_name=f"{sheet_sel}.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                           type="primary")
    except Exception as e:
        st.error(f"ไม่สามารถสร้างไฟล์ Excel ได้: {e}")

# ── SIDEBAR ──────────────────────────────────────────────────
ALLOWED = st.session_state.get("allowed_menus", [])
IS_ADMIN = "clear_data" in ALLOWED

with st.sidebar:
    # Logo
    if LOGO_B64:
        st.markdown(
            f"<div style='text-align:center;padding:8px 0;'>"
            f"<img src='data:image/png;base64,{LOGO_B64}' style='height:70px;'></div>",
            unsafe_allow_html=True)
    else:
        st.markdown("<div style='text-align:center;font-size:2.5rem;'>🥚</div>",
                    unsafe_allow_html=True)

    st.markdown(
        f"<div style='text-align:center;padding:0 0 8px;'>"
        f"<b style='font-size:1rem;color:#FF6B35;'>ROON KHANOMKHAI</b><br>"
        f"<small style='color:#888;'>{st.session_state['dept_name']}</small></div>",
        unsafe_allow_html=True)
    st.divider()

    # App ฝ่ายบริหาร = "ดูอย่างเดียว" ทุกฝ่าย (เพิ่ม/แก้ไข/ลบ ย้ายไปแอปของแต่ละฝ่ายแล้ว)
    # + เครื่องมือผู้ดูแล: Dashboard / Export / Clear Data / จัดการรหัสผ่าน
    VIEW_MENU = {
        "📈 Dashboard":                    "dashboard",
        "🏪 ข้อมูลหลัก (ดู)":              "view_master",
        "👥 งานบุคคล HR (ดู)":             "view_hr",
        "🏭 ฝ่ายผลิต (ดู)":                "view_production",
        "🛒 จัดซื้อ / สต๊อก (ดู)":         "view_purchase",
        "🧺 วัตถุดิบรายวัน / ต้นทุน (ดู)":  "view_material",
        "📊 ข้อมูลสาขา ขาย/สต๊อก (ดู)":    "view_branch_ops",
        "💵 รายได้ & ตรวจยอด (ดู)":        "view_sales_pos",
        "📈 กำไร-ขาดทุนสาขา (ดู)":         "view_pnl",
        "🔎 ตรวจสอบ Audit (ดู)":           "view_audit",
        "💰 การเงินและบัญชี (ดู)":         "view_finance",
        "📢 Marketing & Reconcile (ดู)":   "view_marketing",
        "💵 เงินสดย่อย (ดู)":              "view_petty",
        "🎟️ รายงานคูปอง (ดู)":            "coupon_reports",
    }
    visible_menu = dict(VIEW_MENU)
    visible_menu["📤 Export Data"] = "export"     # ดู/ดาวน์โหลดได้ทุกคน (อ่านอย่างเดียว)
    # เครื่องมือผู้ดูแล: แสดงเฉพาะผู้ดูแลระบบ (admin) เท่านั้น
    if IS_ADMIN or st.session_state.get("dept_id") == "admin":
        visible_menu["🗑️ Clear Data"]     = "clear_data"
        visible_menu["🔑 จัดการรหัสผ่าน"] = "manage_pw"

    selected_label = st.radio("เมนู", list(visible_menu.keys()),
                               label_visibility="collapsed")
    selected = visible_menu[selected_label]
    st.divider()

    with st.expander("🔧 System Info"):
        st.markdown(f"**Sheets:** {len(ALL_SHEETS)}")
        # โหลดตัวเลขเมื่อกดเท่านั้น (กันการอ่าน DB ทุกครั้งที่เปลี่ยนเมนู = ช้า)
        if st.button("🔄 โหลดสถิติข้อมูล", key="sysinfo_load"):
            try:
                st.markdown(f"**Reports:** {len(read_sheet('branch_daily_reports'))}  \n"
                            f"**Audits:** {len(read_sheet('audit_sessions'))}")
            except Exception:
                st.error("ไม่สามารถอ่านข้อมูลได้")

    if st.button("🚪 ออกจากระบบ", use_container_width=True):
        for k in ["logged_in","dept_id","dept_name","allowed_menus"]:
            st.session_state[k] = False if k=="logged_in" else []
        st.rerun()

    st.markdown(
        "<div style='text-align:center;padding:8px 0 0;'>"
        "<small style='color:#aaa;'>ออกแบบโดย<br>"
        "<b>ดร.อภิวรรณ์ ดำแสงสวัสดิ์</b><br>"
        "Copyright © 12/06/2026</small></div>",
        unsafe_allow_html=True)

# ── Router ────────────────────────────────────────────────────
def _run(module_path, func="render"):
    mod = _safe_import(module_path)
    if not mod: st.error(f"❌ โหลด module ไม่ได้: {module_path}"); return
    fn = getattr(mod, func, None)
    if not fn: st.error(f"❌ ไม่พบฟังก์ชัน '{func}'"); return
    try:
        fn()
    except Exception as e:
        st.error(f"❌ เกิดข้อผิดพลาด: {e}")
        with st.expander("🔍 รายละเอียด Error"):
            st.code(traceback.format_exc())

def _run_view(section_key):
    mod = _safe_import("modules.exec_views")
    if not mod:
        st.error("❌ โหลด module ไม่ได้: modules.exec_views"); return
    try:
        mod.render(section_key)
    except Exception as e:
        st.error(f"❌ เกิดข้อผิดพลาด: {e}")
        with st.expander("🔍 รายละเอียด Error"):
            st.code(traceback.format_exc())

if selected == "dashboard":        _run("modules.dashboard")
elif selected == "coupon_reports": _run("modules.coupon_reports")
elif selected.startswith("view_"): _run_view(selected)
elif selected == "export":         _render_export()
elif selected == "clear_data":     _render_clear_data()
elif selected == "manage_pw":      render_manage_passwords()

# ── Footer ───────────────────────────────────────────────────
st.markdown(
    "<hr style='margin-top:40px;border:1px solid #eee;'>"
    "<p style='text-align:center;color:#bbb;font-size:0.75rem;'>"
    "ROON KHANOMKHAI Management System | "
    "ออกแบบและพัฒนาโดย <b>ดร.อภิวรรณ์ ดำแสงสวัสดิ์</b> | "
    "Copyright © 12/06/2026</p>",
    unsafe_allow_html=True)
