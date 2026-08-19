"""
branch_app.py  –  ระบบบันทึกข้อมูลสาขา (เวอร์ชันสาขา)
ผู้พัฒนา: ดร.วรรณ (ดร.อภิวรรณ์ ดำแสงสวัสดิ์) | Copyright © 2026

หน้าที่ของไฟล์นี้ (รอบที่ 1):
  - แอปแยกสำหรับพนักงานสาขา
  - Login ด้วย "รหัสสาขา" (USER) + รหัสผ่าน 6 หลัก — ไม่โชว์ชื่อสาขา
  - login สำเร็จ = ล็อกสาขานั้นทันที (ไม่ต้องเลือกสาขาซ้ำ)
  - 3 เมนู: บันทึกรายการขาย / บันทึกสต๊อก / บันทึกเงินสดย่อย
"""
import os
import base64
import traceback
import importlib
import streamlit as st

st.set_page_config(
    page_title="รุนขนมไข่ – ระบบบันทึกข้อมูลสาขา",
    page_icon="📝",
    layout="wide",
)


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
    from modules.branch_auth import render_branch_login, init_branch_login
    init_branch_login()
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
    "logged_in":          False,
    "dept_id":            "",
    "dept_name":          "",
    "allowed_menus":      [],
    "locked_branch_id":   "",
    "locked_branch_name": "",
    "user_branch":        "",
}

for _k, _v in _DEFAULT_SESSION.items():
    if _k not in st.session_state:
        st.session_state[_k] = _v


def _reset_session():
    for k, v in _DEFAULT_SESSION.items():
        st.session_state[k] = v


# ══════════════════════════════════════════════════════════════
# ด่านที่ 1 : LOGIN ด้วยรหัสสาขา (login สำเร็จ = ล็อกสาขาอัตโนมัติ)
# ══════════════════════════════════════════════════════════════
if not st.session_state["logged_in"]:
    render_branch_login(LOGO_B64)
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
    except Exception:
        st.error("❌ เกิดข้อผิดพลาด")
        with st.expander("🔍 รายละเอียด Error"):
            st.code(traceback.format_exc())


# ── เมนูของสาขา : 3 เมนูตามที่ ดร.วรรณ กำหนด ─────────────────
# รูปแบบ  "ชื่อที่แสดง" : (module, ฟังก์ชัน)
BRANCH_MENU = {
    "🧾 บันทึกรายการขาย":   ("modules.record_sales",  "render"),
    "📦 บันทึกสต๊อก":        ("modules.record_stock",  "render"),
    "💵 บันทึกเงินสดย่อย":   ("modules.petty_cash",    "render"),
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
        st.markdown("<div style='text-align:center;font-size:2.5rem;'>🥚</div>",
                    unsafe_allow_html=True)

    # แสดงชื่อสาขาตัวใหญ่ กันสับสนว่าเข้าผิดสาขา
    st.markdown(
        f"<div style='text-align:center;background:#FFF3E0;border:2px solid #FF6B35;"
        f"border-radius:8px;padding:10px;margin-bottom:10px;'>"
        f"<small style='color:#888;'>กำลังใช้งานในนาม</small><br>"
        f"<b style='font-size:1.1rem;color:#E65100;'>"
        f"{st.session_state.get('locked_branch_name','')}</b><br>"
        f"<small style='color:#aaa;'>รหัส {st.session_state.get('locked_branch_id','')}</small>"
        f"</div>",
        unsafe_allow_html=True,
    )

    selected_label = st.radio("เมนู", list(BRANCH_MENU.keys()),
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
_module_path, _func_name = BRANCH_MENU[selected_label]
_run(_module_path, _func_name)

st.markdown(
    "<hr style='margin-top:40px;border:1px solid #eee;'>"
    "<p style='text-align:center;color:#bbb;font-size:0.75rem;'>"
    "ROON KHANOMKHAI – ระบบบันทึกข้อมูลสาขา | "
    "ออกแบบและพัฒนาโดย <b>ดร.วรรณ</b> | "
    "Copyright © 2026</p>",
    unsafe_allow_html=True,
)
