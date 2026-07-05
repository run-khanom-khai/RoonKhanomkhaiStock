"""
# Updated: 2026-07-05 14:19:04
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
    init_workbook()
    seed_all()
    _init_auth_sheet()
except Exception as e:
    st.error(f"❌ ไม่สามารถเริ่มต้นระบบได้: {e}")
    st.stop()

# ── Session init ─────────────────────────────────────────────
if "logged_in" not in st.session_state:
    st.session_state["logged_in"]     = False
    st.session_state["dept_id"]       = ""
    st.session_state["dept_name"]     = ""
    st.session_state["allowed_menus"] = []

# ── LOGIN CHECK ──────────────────────────────────────────────
if not st.session_state["logged_in"]:
    render_login(LOGO_B64)
    st.stop()

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

CLEAR_GROUPS = {
    "🗂️ ข้อมูลธุรกรรมทั้งหมด (แนะนำ)": [
        "branch_daily_reports","branch_front_sales_packaging","branch_drink_sales_detail",
        "branch_material_balance","branch_packaging_balance","delivery_packaging_sales",
        "branch_other_stock_balance","branch_special_remark","branch_sales_recheck",
        "audit_sessions","audit_packaging_balance","audit_packaging_diff",
        "true_stock_balance","daily_stock_usage","daily_packaging_cost",
        "purchase_orders","purchase_order_items","stock_in_to_branch","stock_movements",
        "production_batches","production_material_used",
        "employees","payroll_periods","payroll_records","late_deduction_rules",
        "bank_accounts","bank_transactions","daily_sales_accounting","branch_expenses",
        "marketing_daily_sales","marketing_daily_sales_items","sales_reconcile",
    ],
    "📊 Branch Daily Report": ["branch_daily_reports","branch_front_sales_packaging",
        "branch_drink_sales_detail","branch_material_balance","branch_packaging_balance",
        "delivery_packaging_sales","branch_other_stock_balance","branch_special_remark","branch_sales_recheck"],
    "🔎 Audit & Stock": ["audit_sessions","audit_packaging_balance","audit_packaging_diff",
        "true_stock_balance","daily_stock_usage","daily_packaging_cost","stock_movements"],
    "🛒 Purchase & Production": ["purchase_orders","purchase_order_items","stock_in_to_branch",
        "stock_movements","production_batches","production_material_used"],
    "👥 HR & เงินเดือน": ["employees","payroll_periods","payroll_records","late_deduction_rules"],
    "💰 Finance & Accounting": ["bank_accounts","bank_transactions","daily_sales_accounting",
        "branch_expenses","marketing_daily_sales","marketing_daily_sales_items","sales_reconcile"],
    "🏪 Master Data (สาขา, Items, Products)": ["branches","items","products"],
}

def _render_clear_data():
    st.markdown("<h1 style='color:#B71C1C;font-size:1.8rem;font-weight:800;"
                "border-left:6px solid #B71C1C;padding-left:12px;'>🗑️ Clear Data</h1>",
                unsafe_allow_html=True)
    st.markdown("<div style='background:#FFEBEE;border:2px solid #EF9A9A;border-radius:8px;"
                "padding:14px;margin-bottom:16px;'><b style='color:#B71C1C;'>⚠️ คำเตือน</b><br>"
                "<span style='color:#C62828;'>การลบข้อมูลไม่สามารถกู้คืนได้ "
                "กรุณา Backup ไฟล์ roon_database.xlsx ก่อน</span></div>",
                unsafe_allow_html=True)
    group_name  = st.selectbox("กลุ่มข้อมูล", list(CLEAR_GROUPS.keys()))
    sheets_to_c = CLEAR_GROUPS[group_name]
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
        st.success(f"✅ ล้างข้อมูลสำเร็จ {len(cleared)} Sheet")
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

    ALL_MENU = {
        "📋 Master Data":         "master_data",
        "👥 HR":                  "hr",
        "🏭 Production":          "production",
        "🛒 Purchase / Stock":    "purchase_stock",
        "📊 Branch Daily Report": "branch_report",
        "🔎 Audit":               "audit",
        "💰 Finance":             "finance",
        "📒 Accounting":          "accounting",
        "💵 เงินสดย่อย":          "petty_cash",
        "📈 Dashboard":           "dashboard",
        "📤 Export Data":         "export",
        "🗑️ Clear Data":          "clear_data",
        "🔑 จัดการรหัสผ่าน":     "manage_pw",
    }

    # กรองเมนูตามสิทธิ์
    visible_menu = {}
    for label, key in ALL_MENU.items():
        if key in ALLOWED or key == "manage_pw":
            visible_menu[label] = key

    if not visible_menu:
        visible_menu = ALL_MENU  # fallback

    selected_label = st.radio("เมนู", list(visible_menu.keys()),
                               label_visibility="collapsed")
    selected = visible_menu[selected_label]
    st.divider()

    with st.expander("🔧 System Info"):
        try:
            st.markdown(f"**Sheets:** {len(ALL_SHEETS)}  \n"
                        f"**Reports:** {len(read_sheet('branch_daily_reports'))}  \n"
                        f"**Audits:** {len(read_sheet('audit_sessions'))}")
        except: st.error("ไม่สามารถอ่านข้อมูลได้")

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

if selected == "master_data":      _run("modules.master_data","render_master_data")
elif selected == "hr":             _run("modules.hr")
elif selected == "production":     _run("modules.production")
elif selected == "purchase_stock": _run("modules.purchase")
elif selected == "branch_report":  _run("modules.branch_report")
elif selected == "audit":          _run("modules.audit")
elif selected == "finance":        _run("modules.finance")
elif selected == "accounting":     _run("modules.accounting")
elif selected == "petty_cash":     _run("modules.petty_cash")
elif selected == "dashboard":      _run("modules.dashboard")
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
