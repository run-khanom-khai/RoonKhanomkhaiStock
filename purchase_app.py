"""
purchase_app.py  –  ROON KHANOMKHAI Management System (เวอร์ชันฝ่ายจัดซื้อ)
ผู้พัฒนา: ดร.วรรณ (ดร.อภิวรรณ์ ดำแสงสวัสดิ์) | Copyright © 2026

หน้าที่ของไฟล์นี้:
  - เป็นแอปแยกสำหรับ "ฝ่ายจัดซื้อ (Purchase)" (คนละ URL กับแอปอื่น)
  - Login ด้วยแผนก (dept) — อนุญาตเฉพาะ ฝ่ายจัดซื้อ และผู้ดูแลระบบ (admin)
  - เมนู: บันทึกใบสั่งซื้อ / เบิกของเข้าสาขา / รายงานการเบิก / สต๊อกคงเหลือ
  - ใช้ database และ modules ร่วมกับ app.py ทั้งหมด (ไม่แตะโค้ดเดิม)
"""
import os
import base64
import traceback
import importlib
import streamlit as st

st.set_page_config(
    page_title="รุนขนมไข่ – ฝ่ายจัดซื้อ (Purchase)",
    page_icon="🛒",
    layout="wide",
)

# สีประจำแอป (ม่วง) — ต่างจากแอปอื่น กันเปิดผิดแอป
THEME_BG     = "#F3E5F5"
THEME_BORDER = "#7B1FA2"
THEME_TEXT   = "#4A148C"


# ══════════════════════════════════════════════════════════════
# LOGO
# ══════════════════════════════════════════════════════════════
def _load_logo_b64() -> str:
    logo_path = os.path.join(os.path.dirname(__file__), "logo_roon.png")
    if os.path.exists(logo_path):
        try:
            with open(logo_path, "rb") as f:
                return base64.b64encode(f.read()).decode()
        except Exception:
            return ""
    return ""


LOGO_B64 = _load_logo_b64()


# ══════════════════════════════════════════════════════════════
# BOOTSTRAP
# ══════════════════════════════════════════════════════════════
try:
    from modules.auth import render_login, _init_auth_sheet
    _init_auth_sheet()
except Exception as e:
    _msg = str(e).lower()
    if any(k in _msg for k in ["zip", "quota", "429", "rate", "timeout"]):
        pass  # ปัญหาชั่วคราวของ database — ระบบยังทำงานต่อได้
    else:
        st.error(f"❌ ไม่สามารถเริ่มต้นระบบได้: {e}")
        st.stop()


# ══════════════════════════════════════════════════════════════
# SESSION INIT
# ══════════════════════════════════════════════════════════════
_DEFAULT_SESSION = {
    "logged_in":     False,
    "dept_id":       "",
    "dept_name":     "",
    "allowed_menus": [],
}

for _k, _v in _DEFAULT_SESSION.items():
    if _k not in st.session_state:
        st.session_state[_k] = _v


def _reset_session():
    for k, v in _DEFAULT_SESSION.items():
        st.session_state[k] = v
    for k in ["locked_branch_id", "locked_branch_name", "user_branch"]:
        if k in st.session_state:
            st.session_state[k] = ""


# ══════════════════════════════════════════════════════════════
# LOGIN
# ══════════════════════════════════════════════════════════════
if not st.session_state["logged_in"]:
    render_login(LOGO_B64, app_title="🛒 ระบบฝ่ายจัดซื้อ (Purchase)",
                 subtitle="ฝ่ายจัดซื้อ (Purchase) — กรุณาเข้าสู่ระบบ")
    st.stop()

# แอปนี้อนุญาตเฉพาะฝ่ายจัดซื้อและผู้ดูแลระบบเท่านั้น
if st.session_state.get("dept_id") not in {"purchase", "admin"}:
    st.error("⛔ บัญชีนี้ไม่มีสิทธิ์ใช้งานระบบฝ่ายจัดซื้อ")
    st.caption("กรุณาเข้าสู่ระบบด้วยบัญชีฝ่ายจัดซื้อหรือ Admin")
    if st.button("🚪 ออกจากระบบ", use_container_width=True, key="purch_access_logout"):
        _reset_session()
        st.rerun()
    st.stop()


# ══════════════════════════════════════════════════════════════
# ROUTER
# ══════════════════════════════════════════════════════════════
def _run(module_path: str, func_name: str = "render"):
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
        fn()
    except Exception as e:
        st.error(f"❌ เกิดข้อผิดพลาด: {e}")
        with st.expander("🔍 รายละเอียด Error"):
            st.code(traceback.format_exc())


# ── เมนูของฝ่ายจัดซื้อ ──
PURCHASE_MENU = {
    "🛒 จัดซื้อ / เบิกของ / สต๊อก": ("modules.purchase", "render"),
}


# ══════════════════════════════════════════════════════════════
# SIDEBAR
# ══════════════════════════════════════════════════════════════
with st.sidebar:
    if LOGO_B64:
        st.markdown(
            f"<div style='text-align:center;padding:8px 0;'>"
            f"<img src='data:image/png;base64,{LOGO_B64}' style='height:70px;'></div>",
            unsafe_allow_html=True,
        )
    else:
        st.markdown("<div style='text-align:center;font-size:2.5rem;'>🛒</div>",
                    unsafe_allow_html=True)

    st.markdown(
        f"<div style='text-align:center;background:{THEME_BG};"
        f"border:2px solid {THEME_BORDER};border-radius:8px;"
        f"padding:10px;margin-bottom:10px;'>"
        f"<small style='color:#888;'>ระบบฝ่ายจัดซื้อ (Purchase)</small><br>"
        f"<b style='font-size:1.05rem;color:{THEME_TEXT};'>"
        f"{st.session_state.get('dept_name','')}</b></div>",
        unsafe_allow_html=True,
    )

    selected_label = st.radio("เมนู", list(PURCHASE_MENU.keys()),
                              label_visibility="collapsed")
    st.divider()

    if st.button("🚪 ออกจากระบบ", use_container_width=True):
        _reset_session()
        st.rerun()

    st.markdown(
        "<div style='text-align:center;padding:8px 0 0;'>"
        "<small style='color:#aaa;'>ออกแบบโดย<br>"
        "<b>ดร.วรรณ</b><br>"
        "Copyright © 2026</small></div>",
        unsafe_allow_html=True,
    )


# ══════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════
_module_path, _func_name = PURCHASE_MENU[selected_label]
_run(_module_path, _func_name)

st.markdown(
    "<hr style='margin-top:40px;border:1px solid #eee;'>"
    "<p style='text-align:center;color:#bbb;font-size:0.75rem;'>"
    "ROON KHANOMKHAI – ระบบฝ่ายจัดซื้อ (Purchase) | "
    "ออกแบบและพัฒนาโดย <b>ดร.วรรณ</b> | "
    "Copyright © 2026</p>",
    unsafe_allow_html=True,
)
