"""
sale_audit_app.py  –  ROON KHANOMKHAI Management System (แอป Sale Audit)
ผู้พัฒนา: ดร.วรรณ (ดร.อภิวรรณ์ ดำแสงสวัสดิ์) | Copyright © 2026

แอปแยกสำหรับ "Sale Audit" (ตรวจสอบยอดขาย) — คนละ URL กับแอปอื่น
Login ด้วยแผนก (dept) — อนุญาต: ฝ่ายตรวจสอบ / ฝ่ายการเงิน / ฝ่ายบัญชี / admin
"""
import os
import base64
import traceback
import streamlit as st

st.set_page_config(page_title="รุนขนมไข่ – Sale Audit", page_icon="🔍", layout="wide")

THEME_BG, THEME_BORDER, THEME_TEXT = "#EDE7F6", "#6A1B9A", "#4A148C"


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

try:
    from modules.auth import render_login, _init_auth_sheet
    _init_auth_sheet()
except Exception as e:
    _msg = str(e).lower()
    if not any(k in _msg for k in ["zip", "quota", "429", "rate", "timeout"]):
        st.error(f"❌ ไม่สามารถเริ่มต้นระบบได้: {e}")
        st.stop()

_DEFAULT_SESSION = {"logged_in": False, "dept_id": "", "dept_name": "", "allowed_menus": []}
for _k, _v in _DEFAULT_SESSION.items():
    if _k not in st.session_state:
        st.session_state[_k] = _v


def _reset_session():
    for k, v in _DEFAULT_SESSION.items():
        st.session_state[k] = v


if not st.session_state["logged_in"]:
    render_login(LOGO_B64, app_title="🔍 Sale Audit",
                 subtitle="Sale Audit (ตรวจสอบยอดขาย) — กรุณาเข้าสู่ระบบ")
    st.stop()

if st.session_state.get("dept_id") not in {"audit", "finance", "accounting", "admin"}:
    st.error("⛔ บัญชีนี้ไม่มีสิทธิ์ใช้งาน Sale Audit")
    st.caption("กรุณาเข้าสู่ระบบด้วยบัญชีฝ่ายตรวจสอบ / การเงิน / บัญชี / Admin")
    if st.button("🚪 ออกจากระบบ", use_container_width=True, key="sa_logout0"):
        _reset_session()
        st.rerun()
    st.stop()


with st.sidebar:
    if LOGO_B64:
        st.markdown(f"<div style='text-align:center;padding:8px 0;'>"
                    f"<img src='data:image/png;base64,{LOGO_B64}' style='height:70px;'></div>",
                    unsafe_allow_html=True)
    st.markdown(
        f"<div style='text-align:center;background:{THEME_BG};border:2px solid {THEME_BORDER};"
        f"border-radius:8px;padding:10px;margin-bottom:10px;'>"
        f"<small style='color:#888;'>Sale Audit</small><br>"
        f"<b style='font-size:1.05rem;color:{THEME_TEXT};'>{st.session_state.get('dept_name','')}</b></div>",
        unsafe_allow_html=True)
    st.divider()
    if st.button("🚪 ออกจากระบบ", use_container_width=True):
        _reset_session()
        st.rerun()
    st.markdown("<div style='text-align:center;color:#aaa;font-size:.8rem;margin-top:20px;'>"
                "ออกแบบโดย<br><b>ดร.วรรณ</b><br>Copyright © 2026</div>", unsafe_allow_html=True)

try:
    from modules import sale_audit
    sale_audit.render()
except Exception as e:
    st.error(f"❌ เกิดข้อผิดพลาด: {e}")
    with st.expander("🔍 รายละเอียด Error"):
        st.code(traceback.format_exc())
