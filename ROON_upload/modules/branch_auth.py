"""
branch_auth.py  –  ระบบ Login สำหรับ "ระบบบันทึกข้อมูลสาขา"
ผู้พัฒนา: ดร.วรรณ (ดร.อภิวรรณ์ ดำแสงสวัสดิ์)

หลักการ (รอบที่ 1):
  - USER = รหัสสาขา (เช่น BR002)  ← ไม่แสดงชื่อสาขา เพื่อกันคนภายนอกเห็นชื่อสาขา
  - รหัสผ่าน = ตัวเลข 6 หลัก ที่ generate ไว้ให้แต่ละสาขา (เก็บแบบ hash)
  - login สำเร็จ = ล็อกสาขานั้นทันที ไม่ต้องเลือกสาขาซ้ำ
  - เก็บรหัสผ่านในตาราง branch_login (branch_id, pw_hash, is_active)
"""
import hashlib
import pandas as pd
import streamlit as st

from config import SHEET_BRANCH_LOGIN, SHEET_BRANCHES
from modules.excel_db import read_sheet, write_sheet, init_workbook


# ══════════════════════════════════════════════════════════════
# รหัสผ่านเริ่มต้นของแต่ละสาขา (ตัวเลข 6 หลัก — generate แบบสุ่ม)
# ⚠️ นี่คือรหัส "ตัวจริง" ที่แจกให้สาขา — เก็บเป็นความลับ
#    ระบบจะ seed ลงตาราง branch_login แบบ hash ในการรันครั้งแรก
# ══════════════════════════════════════════════════════════════
BRANCH_LOGIN_SEED = {
    "BR000": "880375",
    "BR004": "353763",
    "BR005": "113454",
    "BR006": "427500",
    "BR007": "381212",
    "BR008": "177263",
    "BR011": "153966",
    "BR012": "536192",
    "BR013": "121308",
    "BR014": "813044",
    "BR015": "293668",
    "BR017": "689139",
    "BR018": "909361",
    "BR019": "669972",
    "BR021": "760833",
    "BR022": "174736",
    "BR024": "183929",
    "BR025": "531838",
    "BR026": "999978",
    "BR027": "805122",
    "BR028": "657417",
}


def _hash(pw: str) -> str:
    return hashlib.sha256(str(pw).encode()).hexdigest()


# ══════════════════════════════════════════════════════════════
# INIT / SEED
# ══════════════════════════════════════════════════════════════
def init_branch_login():
    """สร้างตาราง branch_login และ seed รหัสผ่านครั้งแรก (ถ้ายังว่าง)"""
    if st.session_state.get("_init_branch_login"):
        return
    try:
        init_workbook()
    except Exception:
        pass

    try:
        df = read_sheet(SHEET_BRANCH_LOGIN)
    except Exception:
        df = pd.DataFrame()

    # ถ้ายังไม่มีข้อมูล → seed จาก BRANCH_LOGIN_SEED
    if df is None or df.empty or "branch_id" not in df.columns:
        rows = [
            {"branch_id": bid, "pw_hash": _hash(pw), "is_active": "TRUE"}
            for bid, pw in BRANCH_LOGIN_SEED.items()
        ]
        try:
            write_sheet(SHEET_BRANCH_LOGIN, pd.DataFrame(rows))
        except Exception:
            # quota / network — ปล่อยผ่าน แล้วใช้ fallback ตอน login
            pass
    st.session_state["_init_branch_login"] = True


def _get_login_df() -> pd.DataFrame:
    """ดึงตาราง branch_login — ถ้ายังไม่พร้อม ใช้ค่า seed แทน"""
    try:
        init_branch_login()
        df = read_sheet(SHEET_BRANCH_LOGIN)
        if df is not None and not df.empty and "branch_id" in df.columns:
            return df
    except Exception:
        pass
    # Fallback: สร้างจาก seed
    rows = [
        {"branch_id": bid, "pw_hash": _hash(pw), "is_active": "TRUE"}
        for bid, pw in BRANCH_LOGIN_SEED.items()
    ]
    return pd.DataFrame(rows)


def check_branch_login(branch_id: str, password: str) -> bool:
    df = _get_login_df()
    if df.empty:
        return False
    row = df[df["branch_id"].astype(str).str.strip() == str(branch_id).strip()]
    if row.empty:
        return False
    if str(row.iloc[0].get("is_active", "TRUE")).upper() == "FALSE":
        return False
    return str(row.iloc[0]["pw_hash"]) == _hash(password)


def _branch_name_map() -> dict:
    """ชื่อสาขา (ใช้ภายในหลัง login แล้วเท่านั้น — ไม่โชว์ตอนเลือก user)"""
    try:
        bdf = read_sheet(SHEET_BRANCHES)
        if bdf is not None and not bdf.empty and "branch_id" in bdf.columns:
            return dict(zip(
                bdf["branch_id"].astype(str).str.strip(),
                bdf["branch_name"].astype(str).str.strip(),
            ))
    except Exception:
        pass
    return {}


def set_branch_password(branch_id: str, new_pw: str) -> bool:
    """ตั้ง/เปลี่ยนรหัสผ่านของสาขา (ใช้ตอนพนักงานลาออก ฯลฯ)
    - ถ้ามีสาขานี้อยู่แล้ว → อัปเดต pw_hash
    - ถ้ายังไม่มี → เพิ่มแถวใหม่
    คืน True ถ้าสำเร็จ
    """
    from modules.excel_db import append_row, update_row
    branch_id = str(branch_id).strip()
    new_pw = str(new_pw).strip()
    if not branch_id or not new_pw:
        return False
    try:
        init_branch_login()
    except Exception:
        pass
    try:
        df = read_sheet(SHEET_BRANCH_LOGIN)
    except Exception:
        df = pd.DataFrame()
    exists = (not df.empty and "branch_id" in df.columns and
              (df["branch_id"].astype(str).str.strip() == branch_id).any())
    try:
        if exists:
            update_row(SHEET_BRANCH_LOGIN, "branch_id", branch_id,
                       {"pw_hash": _hash(new_pw), "is_active": "TRUE"})
        else:
            append_row(SHEET_BRANCH_LOGIN, {
                "branch_id": branch_id, "pw_hash": _hash(new_pw),
                "is_active": "TRUE"})
        return True
    except Exception:
        return False


def branch_login_options() -> dict:
    """คืน {branch_id: branch_name} ของสาขาที่มีในระบบ login (ไว้ให้ผู้ดูแลเลือก)"""
    ids = _active_branch_ids()
    names = _branch_name_map()
    return {b: names.get(b, "") for b in ids}


def _active_branch_ids() -> list:
    df = _get_login_df()
    if df.empty:
        return []
    active = df[df["is_active"].astype(str).str.upper() != "FALSE"] \
        if "is_active" in df.columns else df
    ids = sorted(active["branch_id"].astype(str).str.strip().unique().tolist())
    return ids


# ══════════════════════════════════════════════════════════════
# LOGIN PAGE
# ══════════════════════════════════════════════════════════════
def render_branch_login(logo_b64: str = ""):
    """หน้า Login ของระบบสาขา — USER = รหัสสาขา (ไม่โชว์ชื่อสาขา)"""
    if logo_b64:
        st.markdown(
            f"<div style='text-align:center;padding:20px 0 6px;'>"
            f"<img src='data:image/png;base64,{logo_b64}' style='height:96px;'></div>",
            unsafe_allow_html=True,
        )

    # ── ชื่อ app ตัวใหญ่ ──────────────────────────────────────
    st.markdown(
        "<h1 style='text-align:center;color:#FF6B35;font-size:2.4rem;"
        "font-weight:900;margin:6px 0 0;letter-spacing:0.5px;'>"
        "📝 ระบบบันทึกข้อมูลสาขา</h1>"
        "<p style='text-align:center;color:#888;margin-top:2px;'>"
        "รุนขนมไข่ไส้เนย สงขลา — กรุณาเข้าสู่ระบบ</p>",
        unsafe_allow_html=True,
    )

    st.divider()
    c1, c2, c3 = st.columns([1, 2, 1])
    with c2:
        st.markdown("### 🔐 เข้าสู่ระบบ")

        branch_ids = _active_branch_ids()
        if not branch_ids:
            st.error("❌ ยังไม่มีรหัสสาขาในระบบ กรุณาแจ้งสำนักงานใหญ่")
            st.stop()

        # ใช้ st.form เพื่อให้กด Enter ในช่องรหัสผ่านแล้วเข้าสู่ระบบได้ทันที (ครั้งเดียว)
        with st.form("branch_login_form", clear_on_submit=False):
            # ⚠️ แสดงเฉพาะ "รหัสสาขา" เท่านั้น — ไม่โชว์ชื่อสาขา
            branch_id = st.selectbox("รหัสสาขา (USER)", branch_ids)
            password  = st.text_input(
                "รหัสผ่าน 6 หลัก", type="password",
                max_chars=6, placeholder="● ● ● ● ● ●",
            )
            login_btn = st.form_submit_button("🔓 เข้าสู่ระบบ", type="primary",
                                              use_container_width=True)

        if login_btn:
            if check_branch_login(branch_id, password):
                name_map = _branch_name_map()
                st.session_state["logged_in"]          = True
                st.session_state["dept_id"]            = "branch"
                st.session_state["dept_name"]          = "📊 พนักงานสาขา"
                st.session_state["allowed_menus"]      = []  # ไม่จำกัดเมนูภายในระบบสาขา
                st.session_state["locked_branch_id"]   = branch_id
                st.session_state["locked_branch_name"] = name_map.get(branch_id, branch_id)
                st.session_state["user_branch"]        = name_map.get(branch_id, branch_id)
                st.success("✅ เข้าสู่ระบบสำเร็จ")
                st.rerun()
            else:
                st.error("❌ รหัสสาขาหรือรหัสผ่านไม่ถูกต้อง")

    st.divider()
    st.markdown(
        "<p style='text-align:center;color:#aaa;font-size:0.8rem;'>"
        "ออกแบบและพัฒนาโดย <b>ดร.วรรณ</b> (ดร.อภิวรรณ์ ดำแสงสวัสดิ์)<br>"
        "Copyright © 2026 ROON KHANOMKHAI — รุนขนมไข่ไส้เนย สงขลา</p>",
        unsafe_allow_html=True,
    )
