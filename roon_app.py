"""
roon_app.py  –  ROON KHANOMKHAI: แอปรวม "ทางเข้าเดียว" (All-in-One)
ผู้พัฒนา: ดร.วรรณ (ดร.อภิวรรณ์ ดำแสงสวัสดิ์) | Copyright © 2026

รวมทุกแผนกไว้ในแอปเดียว (โฮสต์ก้อนเดียว ง่ายต่อการส่งมอบ):
  - ล็อกอิน 2 แบบในหน้าเดียว: (ก) พนักงานสำนักงาน/แผนก (รหัสแผนก)  (ข) พนักงานสาขา (รหัสสาขา)
  - ล็อกอินแล้วเห็นเฉพาะเมนูของแผนกตัวเอง
  - เรียกใช้โมดูลเดิมทั้งหมด (ไม่แก้โมดูล) — แอปเดิม 7 ตัวยังใช้ได้ปกติเป็น fallback
"""
import os
import base64
import traceback
import importlib
import streamlit as st

st.set_page_config(page_title="รุนขนมไข่ – ระบบบริหารร้าน (All-in-One)",
                   page_icon="🥚", layout="wide")


def _load_logo_b64() -> str:
    p = os.path.join(os.path.dirname(__file__), "logo_roon.png")
    if os.path.exists(p):
        try:
            with open(p, "rb") as f:
                return base64.b64encode(f.read()).decode()
        except Exception:
            return ""
    return ""


LOGO_B64 = _load_logo_b64()

# ── Bootstrap: เตรียมระบบล็อกอินทั้ง 2 แบบ ──
try:
    from modules.auth import (render_login, render_manage_passwords,
                              _init_auth_sheet)
    _init_auth_sheet()
except Exception as e:
    if not any(k in str(e).lower() for k in ["zip", "quota", "429", "rate", "timeout"]):
        st.error(f"❌ ไม่สามารถเริ่มต้นระบบได้: {e}")
        st.stop()

try:
    from modules.branch_auth import render_branch_login, init_branch_login
    init_branch_login()
except Exception:
    render_branch_login = None

# ── Session ──
_DEFAULT_SESSION = {
    "logged_in": False, "dept_id": "", "dept_name": "", "allowed_menus": [],
    "locked_branch_id": "", "locked_branch_name": "", "user_branch": "",
}
for _k, _v in _DEFAULT_SESSION.items():
    st.session_state.setdefault(_k, _v)


def _reset_session():
    for k, v in _DEFAULT_SESSION.items():
        st.session_state[k] = v


def _run(module_path, func_name="render", *args):
    try:
        mod = importlib.import_module(module_path)
    except Exception:
        st.error(f"❌ โหลด module ไม่ได้: {module_path}")
        with st.expander("🔍 รายละเอียด"):
            st.code(traceback.format_exc())
        return
    fn = getattr(mod, func_name, None)
    if fn is None:
        st.error(f"❌ ไม่พบฟังก์ชัน '{func_name}' ใน {module_path}")
        return
    try:
        fn(*args)
    except Exception as e:
        st.error(f"❌ เกิดข้อผิดพลาด: {e}")
        with st.expander("🔍 รายละเอียด Error"):
            st.code(traceback.format_exc())


def _run_view(section_key):
    _run("modules.exec_views", "render", section_key)


# ══════════════════════════════════════════════════════════════
# LOGIN (2 แบบในหน้าเดียว)
# ══════════════════════════════════════════════════════════════
if not st.session_state["logged_in"]:
    if LOGO_B64:
        st.markdown(f"<div style='text-align:center;padding:16px 0 4px;'>"
                    f"<img src='data:image/png;base64,{LOGO_B64}' style='height:90px;'></div>",
                    unsafe_allow_html=True)
    st.markdown("<h1 style='text-align:center;color:#FF6B35;font-weight:900;margin:4px 0;'>"
                "🥚 ระบบบริหารร้านรุนขนมไข่</h1>"
                "<p style='text-align:center;color:#888;'>เลือกประเภทผู้ใช้เพื่อเข้าสู่ระบบ</p>",
                unsafe_allow_html=True)
    t_office, t_branch = st.tabs(["🏢 พนักงานสำนักงาน / แผนก", "🏪 พนักงานสาขา"])
    with t_office:
        st.caption("สำหรับพนักงานสำนักงาน/แผนก — ถ้าเป็นพนักงานสาขา ให้กดแท็บ '🏪 พนักงานสาขา'")
        # ตัด 'branch' ออก — พนักงานสาขาต้องเลือกสาขาก่อนใส่รหัส จึงต้องใช้แท็บสาขา
        render_login(LOGO_B64, app_title="🏢 เข้าสู่ระบบ (สำนักงาน/แผนก)",
                     subtitle="เลือกแผนกของคุณ แล้วกรอกรหัสผ่าน",
                     exclude_depts=["branch"])
    with t_branch:
        st.caption("สำหรับพนักงานสาขา — เลือก 'รหัสสาขา' ของตัวเอง แล้วกรอกรหัสผ่าน 6 หลักของสาขานั้น")
        if render_branch_login is not None:
            render_branch_login(LOGO_B64)
        else:
            st.error("ยังไม่พร้อมใช้งานระบบล็อกอินสาขา")
    st.stop()


# ══════════════════════════════════════════════════════════════
# เมนูของแต่ละแผนก  (label -> (module, func))
# ══════════════════════════════════════════════════════════════
BRANCH_MENU = {
    "🧾 บันทึกรายการขาย": ("modules.record_sales", "render"),
    "📦 บันทึกสต๊อก":      ("modules.record_stock", "render"),
    "💵 บันทึกเงินสดย่อย": ("modules.petty_cash",   "render"),
}
ACCOUNTING_MENU = {
    "📋 เพิ่ม สาขา/สินค้า": ("modules.master_data", "render_master_data_accounting"),
    "👥 HR (งานบุคคล)":     ("modules.hr",          "render"),
    "💰 การเงินและบัญชี":   ("modules.finance",     "render_accounting"),
    "💵 เงินสดย่อย":        ("modules.petty_cash",  "render"),
}
PURCHASE_MENU   = {"🛒 ฝ่ายจัดซื้อ / สต๊อก": ("modules.purchase",  "render")}
PRODUCTION_MENU = {"🏭 ฝ่ายผลิต":            ("modules.production", "render")}
AUDIT_MENU      = {"🔎 ตรวจสอบบรรจุภัณฑ์":   ("modules.audit_stock", "render")}
SALEAUDIT_MENU  = {"🔍 Sale Audit":          ("modules.sale_audit", "render")}
HR_MENU         = {"👥 HR (งานบุคคล)":        ("modules.hr",        "render")}
PETTY_MENU      = {"💵 เงินสดย่อย":           ("modules.petty_cash", "render")}

# เมนูผู้บริหาร (ดูรวม) — ใช้โมดูลเดิม dashboard / exec_views / coupon_reports
EXEC_MENU = {
    "📈 Dashboard":                 ("dashboard",),
    "🏪 ข้อมูลหลัก (ดู)":           ("view", "view_master"),
    "👥 งานบุคคล HR (ดู)":          ("view", "view_hr"),
    "🏭 ฝ่ายผลิต (ดู)":             ("view", "view_production"),
    "🛒 จัดซื้อ/สต๊อก (ดู)":        ("view", "view_purchase"),
    "🧺 วัตถุดิบ/ต้นทุน (ดู)":       ("view", "view_material"),
    "📊 ข้อมูลสาขา ขาย/สต๊อก (ดู)": ("view", "view_branch_ops"),
    "💵 รายได้ & ตรวจยอด (ดู)":     ("view", "view_sales_pos"),
    "📈 กำไร-ขาดทุนสาขา (ดู)":      ("view", "view_pnl"),
    "🔎 ตรวจสอบ Audit (ดู)":        ("view", "view_audit"),
    "💰 การเงินและบัญชี (ดู)":      ("view", "view_finance"),
    "📢 Marketing (ดู)":            ("view", "view_marketing"),
    "💵 เงินสดย่อย (ดู)":           ("view", "view_petty"),
    "🎟️ รายงานคูปอง":              ("coupon",),
}

# แผนก -> (ชื่อระบบ, สีธีม, เมนู)
DEPT_ROUTES = {
    "branch":     ("ระบบสาขา",        "#FF6B35", BRANCH_MENU),
    "accounting": ("ระบบหลังบ้าน/บัญชี", "#00695C", ACCOUNTING_MENU),
    "finance":    ("การเงินและบัญชี",  "#00695C", ACCOUNTING_MENU),
    "purchase":   ("ฝ่ายจัดซื้อ",      "#7B1FA2", PURCHASE_MENU),
    "production": ("ฝ่ายผลิต",         "#5D4037", PRODUCTION_MENU),
    "audit":      ("ฝ่ายตรวจสอบ",      "#0D47A1", AUDIT_MENU),
    "sale_audit": ("Sale Audit",       "#6A1B9A", SALEAUDIT_MENU),
    "hr":         ("งานบุคคล (HR)",    "#00838F", HR_MENU),
    "petty_cash": ("เงินสดย่อย",       "#455A64", PETTY_MENU),
}

dept = st.session_state.get("dept_id", "")

# ══════════════════════════════════════════════════════════════
# ADMIN — เลือกได้ทุกระบบ  /  DASHBOARD — เมนูผู้บริหาร
# ══════════════════════════════════════════════════════════════
ADMIN_SYSTEMS = {
    "📈 ผู้บริหาร (ดูรวม + รายงาน)": ("exec", None),
    "📒 หลังบ้าน / บัญชี":          ("menu", ACCOUNTING_MENU),
    "🛒 ฝ่ายจัดซื้อ / สต๊อก":       ("menu", PURCHASE_MENU),
    "🏭 ฝ่ายผลิต":                  ("menu", PRODUCTION_MENU),
    "🔎 ตรวจสอบบรรจุภัณฑ์":         ("menu", AUDIT_MENU),
    "🔍 Sale Audit":                ("menu", SALEAUDIT_MENU),
    "👥 งานบุคคล (HR)":             ("menu", HR_MENU),
    "🔑 จัดการรหัสผ่าน":            ("managepw", None),
}


def _render_exec_menu():
    """เมนูผู้บริหาร (ใช้กับ dept=dashboard และ admin)"""
    label = st.sidebar.radio("เมนูผู้บริหาร", list(EXEC_MENU.keys()),
                             label_visibility="collapsed", key="exec_menu")
    spec = EXEC_MENU[label]
    if spec[0] == "dashboard":
        _run("modules.dashboard", "render")
    elif spec[0] == "coupon":
        _run("modules.coupon_reports", "render")
    elif spec[0] == "view":
        _run_view(spec[1])


def _render_menu(menu_dict):
    label = st.sidebar.radio("เมนู", list(menu_dict.keys()),
                             label_visibility="collapsed", key="dept_menu")
    mod, func = menu_dict[label]
    _run(mod, func)


# ── Sidebar header (โลโก้ + ชื่อผู้ใช้ + ออกจากระบบ) ──
def _sidebar_header(title, color):
    with st.sidebar:
        if LOGO_B64:
            st.markdown(f"<div style='text-align:center;padding:6px 0;'>"
                        f"<img src='data:image/png;base64,{LOGO_B64}' style='height:64px;'></div>",
                        unsafe_allow_html=True)
        who = (st.session_state.get("locked_branch_name")
               or st.session_state.get("dept_name", ""))
        st.markdown(
            f"<div style='text-align:center;background:{color}22;border:2px solid {color};"
            f"border-radius:8px;padding:8px;margin-bottom:8px;'>"
            f"<small style='color:#888;'>{title}</small><br>"
            f"<b style='color:{color};font-size:1.02rem;'>{who}</b></div>",
            unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════
# ROUTER หลัก
# ══════════════════════════════════════════════════════════════
if dept == "admin":
    _sidebar_header("ผู้ดูแลระบบ (Admin)", "#B71C1C")
    with st.sidebar:
        sys_label = st.selectbox("เลือกระบบที่จะเข้า", list(ADMIN_SYSTEMS.keys()),
                                 key="admin_sys")
        st.divider()
        if st.button("🚪 ออกจากระบบ", use_container_width=True):
            _reset_session(); st.rerun()
    kind, payload = ADMIN_SYSTEMS[sys_label]
    st.title("🥚 ระบบบริหารร้านรุนขนมไข่")
    if kind == "exec":
        _render_exec_menu()
    elif kind == "menu":
        _render_menu(payload)
    elif kind == "managepw":
        render_manage_passwords()

elif dept == "dashboard":
    _sidebar_header("ผู้บริหาร", "#0D47A1")
    with st.sidebar:
        st.divider()
        if st.button("🚪 ออกจากระบบ", use_container_width=True):
            _reset_session(); st.rerun()
    st.title("🥚 ระบบบริหารร้านรุนขนมไข่ — ผู้บริหาร")
    _render_exec_menu()

elif dept == "branch":
    if not st.session_state.get("locked_branch_id"):
        st.error("⛔ บัญชีสาขายังไม่ได้ล็อกสาขา — กรุณาออกแล้วเข้าใหม่ผ่านแท็บ 'พนักงานสาขา'")
        if st.button("🚪 ออกจากระบบ"):
            _reset_session(); st.rerun()
        st.stop()
    _sidebar_header("ระบบสาขา", "#FF6B35")
    with st.sidebar:
        st.divider()
        if st.button("🚪 ออกจากระบบ", use_container_width=True):
            _reset_session(); st.rerun()
    st.title("🥚 ระบบบันทึกข้อมูลสาขา")
    _render_menu(BRANCH_MENU)

elif dept in DEPT_ROUTES:
    title, color, menu = DEPT_ROUTES[dept]
    _sidebar_header(title, color)
    with st.sidebar:
        st.divider()
        if st.button("🚪 ออกจากระบบ", use_container_width=True):
            _reset_session(); st.rerun()
    st.title(f"🥚 รุนขนมไข่ — {title}")
    _render_menu(menu)

else:
    _sidebar_header("ผู้ใช้", "#607D8B")
    with st.sidebar:
        if st.button("🚪 ออกจากระบบ", use_container_width=True):
            _reset_session(); st.rerun()
    st.warning(f"บัญชีนี้ (แผนก: {dept or '-'}) ยังไม่ได้กำหนดเมนูในระบบรวม "
               "กรุณาติดต่อผู้ดูแลระบบ")

st.markdown(
    "<hr style='margin-top:36px;border:1px solid #eee;'>"
    "<p style='text-align:center;color:#bbb;font-size:0.75rem;'>"
    "ROON KHANOMKHAI Management System (All-in-One) | "
    "ออกแบบและพัฒนาโดย <b>ดร.วรรณ</b> | Copyright © 2026</p>",
    unsafe_allow_html=True)
