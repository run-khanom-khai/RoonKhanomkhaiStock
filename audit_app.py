"""
audit_app.py  –  ROON KHANOMKHAI Management System (เวอร์ชันฝ่ายตรวจสอบ)
ผู้พัฒนา: ดร.อภิวรรณ์ ดำแสงสวัสดิ์ | Copyright © 12/06/2026

หน้าที่ของไฟล์นี้:
  - เป็นแอปแยกสำหรับฝ่าย Audit (คนละ URL กับแอปผู้บริหารและแอปสาขา)
  - เลือกสาขาได้อิสระทุกสาขา (ต่างจากแอปสาขาที่ล็อกไว้สาขาเดียว)
  - ไม่มีเมนูเงินสดย่อย ตามหลักการควบคุมภายใน
    (ผู้ตรวจสอบไม่ควรมีอำนาจอนุมัติจ่ายเงิน)
  - ใช้ database และ modules ร่วมกับ app.py ทั้งหมด (ไม่แตะโค้ดเดิม)
"""
import os
import io
import base64
import traceback
import importlib
import streamlit as st

st.set_page_config(
    page_title="รุนขนมไข่ – ตรวจสอบ",
    page_icon="🔍",
    layout="wide",
)

# สีประจำแอป (น้ำเงิน) — ต่างจากแอปสาขา (ส้ม) เพื่อกันเปิดผิดแอป
THEME_BG     = "#E3F2FD"
THEME_BORDER = "#1565C0"
THEME_TEXT   = "#0D47A1"


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
# หมายเหตุ: ไม่เรียก seed_all() เพราะแอปผู้บริหารทำหน้าที่นั้นอยู่แล้ว
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
# ไม่ใช้ query params เพื่อความปลอดภัย (กันคนเดา URL เข้าระบบ)
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
    # เผื่อกรณีเคยเปิดแอปสาขาในเบราว์เซอร์เดียวกัน
    for k in ["locked_branch_id", "locked_branch_name", "user_branch"]:
        if k in st.session_state:
            st.session_state[k] = ""


# ══════════════════════════════════════════════════════════════
# LOGIN
# ══════════════════════════════════════════════════════════════
if not st.session_state["logged_in"]:
    render_login(LOGO_B64, app_title="🔎 ระบบบันทึกการตรวจสอบบรรจุภัณฑ์",
                 subtitle="ฝ่ายตรวจสอบ (Audit) — กรุณาเข้าสู่ระบบ")
    st.stop()

# แอปนี้อนุญาตเฉพาะฝ่ายตรวจสอบและผู้ดูแลระบบเท่านั้น
if st.session_state.get("dept_id") not in {"audit", "admin"}:
    st.error("⛔ บัญชีนี้ไม่มีสิทธิ์ใช้งานระบบตรวจสอบ")
    st.caption("กรุณาเข้าสู่ระบบด้วยบัญชีฝ่ายตรวจสอบหรือ Admin")
    if st.button("🚪 ออกจากระบบ", width="stretch", key="audit_access_logout"):
        _reset_session()
        st.rerun()
    st.stop()


# ══════════════════════════════════════════════════════════════
# EXPORT DATA (ในตัว — ฝ่ายตรวจสอบต้องดึงข้อมูลไปวิเคราะห์ต่อ)
# ══════════════════════════════════════════════════════════════
def _render_export():
    import pandas as pd
    from modules.excel_db import read_sheet
    from config import ALL_SHEETS

    st.markdown(
        f"<h1 style='color:{THEME_BORDER};font-size:1.8rem;font-weight:800;"
        f"border-left:6px solid {THEME_BORDER};padding-left:12px;'>"
        f"📤 Export Data</h1>",
        unsafe_allow_html=True,
    )

    sheet_sel = st.selectbox("เลือกตารางข้อมูล", ALL_SHEETS)

    try:
        df = read_sheet(sheet_sel)
    except Exception as e:
        st.error(f"ไม่สามารถอ่านข้อมูลได้: {e}")
        return

    if df is None or df.empty:
        st.warning(f"ตาราง '{sheet_sel}' ยังไม่มีข้อมูล")
        return

    st.success(f"✅ {len(df)} แถว | {len(df.columns)} คอลัมน์")
    st.dataframe(df, width="stretch")

    buf = io.BytesIO()
    try:
        with pd.ExcelWriter(buf, engine="openpyxl") as w:
            df.to_excel(w, index=False, sheet_name=sheet_sel[:31])
        st.download_button(
            f"⬇️ ดาวน์โหลด {sheet_sel}.xlsx",
            data=buf.getvalue(),
            file_name=f"{sheet_sel}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            type="primary",
        )
    except Exception as e:
        st.error(f"ไม่สามารถสร้างไฟล์ Excel ได้: {e}")


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


# ── เมนูของฝ่ายตรวจสอบ : แก้ตรงนี้ที่เดียวถ้าต้องการเพิ่ม/ลด ──
# รูปแบบ  "ชื่อที่แสดง" : (module หรือ None, ฟังก์ชัน, รหัสสิทธิ์)
# หมายเหตุ: เหลือเมนูเดียว "ตรวจนับสต๊อกบรรจุภัณฑ์" ตามที่ ดร.วรรณ กำหนด
#          เมนูที่ตัดออก (ตรวจสอบสาขา / รายงานสาขา / Dashboard / Export)
#          ย้ายไปอยู่ในเมนูหลักของสำนักงาน (app.py) ซึ่งมีอยู่แล้ว
AUDIT_MENU = {
    "🔎 ตรวจนับสต๊อกบรรจุภัณฑ์":       ("modules.audit_stock",   "render", "audit"),
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
        st.markdown("<div style='text-align:center;font-size:2.5rem;'>🔍</div>",
                    unsafe_allow_html=True)

    st.markdown(
        f"<div style='text-align:center;background:{THEME_BG};"
        f"border:2px solid {THEME_BORDER};border-radius:8px;"
        f"padding:10px;margin-bottom:10px;'>"
        f"<small style='color:#888;'>ระบบตรวจสอบ</small><br>"
        f"<b style='font-size:1.05rem;color:{THEME_TEXT};'>"
        f"{st.session_state.get('dept_name','')}</b></div>",
        unsafe_allow_html=True,
    )

    # ผ่านด่านสิทธิ์ด้านบนแล้ว จึงแสดงเมนูสำหรับงานตรวจสอบทั้งหมด
    visible_menu = {
        label: (mod, fn)
        for label, (mod, fn, _perm) in AUDIT_MENU.items()
    }

    if not visible_menu:
        st.error("⛔ บัญชีนี้ไม่มีสิทธิ์ใช้งานระบบตรวจสอบ")
        st.caption("กรุณา login ด้วยแผนก 'ฝ่ายตรวจสอบ'")
        if st.button("🚪 ออกจากระบบ", width="stretch"):
            _reset_session()
            st.rerun()
        st.stop()

    selected_label = st.radio("เมนู", list(visible_menu.keys()),
                              label_visibility="collapsed")
    st.divider()

    if st.button("🚪 ออกจากระบบ", width="stretch"):
        _reset_session()
        st.rerun()

    st.markdown(
        "<div style='text-align:center;padding:8px 0 0;'>"
        "<small style='color:#aaa;'>ออกแบบโดย<br>"
        "<b>ดร.อภิวรรณ์ ดำแสงสวัสดิ์</b><br>"
        "Copyright © 12/06/2026</small></div>",
        unsafe_allow_html=True,
    )


# ══════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════
_module_path, _func_name = visible_menu[selected_label]

if _module_path is None:
    _render_export()
else:
    _run(_module_path, _func_name)

st.markdown(
    "<hr style='margin-top:40px;border:1px solid #eee;'>"
    "<p style='text-align:center;color:#bbb;font-size:0.75rem;'>"
    "ROON KHANOMKHAI – ระบบตรวจสอบ | "
    "ออกแบบและพัฒนาโดย <b>ดร.อภิวรรณ์ ดำแสงสวัสดิ์</b> | "
    "Copyright © 12/06/2026</p>",
    unsafe_allow_html=True,
)
