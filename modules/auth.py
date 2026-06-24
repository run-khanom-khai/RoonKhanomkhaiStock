"""
auth.py  –  ระบบ Login รหัสผ่านแยกแผนก
"""
import hashlib
import streamlit as st
import pandas as pd
from modules.excel_db import read_sheet, write_sheet, init_workbook

SHEET_AUTH = "department_passwords"

# ── Default departments & passwords ──────────────────────────
DEFAULT_DEPTS = {
    "admin":        {"name": "🔐 Admin",              "password": "admin1234",    "menus": "all"},
    "master_data":  {"name": "📋 Master Data",        "password": "master1234",   "menus": "master_data"},
    "hr":           {"name": "👥 HR",                  "password": "hr1234",       "menus": "hr"},
    "production":   {"name": "🏭 ฝ่ายผลิต",           "password": "prod1234",     "menus": "production"},
    "purchase":     {"name": "🛒 ฝ่ายจัดซื้อ",        "password": "purch1234",    "menus": "purchase_stock"},
    "branch":       {"name": "📊 พนักงานสาขา",        "password": "branch1234",   "menus": "branch_report"},
    "audit":        {"name": "🔎 ฝ่ายตรวจสอบ",       "password": "audit1234",    "menus": "audit"},
    "finance":      {"name": "💰 ฝ่ายการเงิน",        "password": "fin1234",      "menus": "finance"},
    "accounting":   {"name": "📒 ฝ่ายบัญชี",          "password": "acc1234",      "menus": "accounting"},
    "dashboard":    {"name": "📈 Dashboard/ผู้บริหาร","password": "dash1234",     "menus": "dashboard"},
}

DEPT_MENU_ACCESS = {
    "all":          ["master_data","hr","production","purchase_stock","branch_report",
                     "audit","finance","accounting","dashboard","export","clear_data"],
    "master_data":  ["master_data","export"],
    "hr":           ["hr","export"],
    "production":   ["production","purchase_stock","export"],
    "purchase_stock":["purchase_stock","export"],
    "branch_report":["branch_report","export"],
    "audit":        ["audit","export"],
    "finance":      ["finance","accounting","export"],
    "accounting":   ["accounting","finance","export"],
    "dashboard":    ["dashboard","export"],
}


def _hash(pw: str) -> str:
    return hashlib.sha256(pw.encode()).hexdigest()


def _init_auth_sheet():
    init_workbook()
    df = read_sheet(SHEET_AUTH)
    if df.empty or "dept_id" not in df.columns:
        rows = []
        for dept_id, info in DEFAULT_DEPTS.items():
            rows.append({
                "dept_id":   dept_id,
                "dept_name": info["name"],
                "pw_hash":   _hash(info["password"]),
                "menus":     info["menus"],
                "is_active": "TRUE",
            })
        write_sheet(SHEET_AUTH, pd.DataFrame(rows))


def _get_dept_df():
    _init_auth_sheet()
    return read_sheet(SHEET_AUTH)


def check_login(dept_id: str, password: str) -> bool:
    df = _get_dept_df()
    if df.empty:
        return False
    row = df[df["dept_id"].astype(str) == dept_id]
    if row.empty:
        return False
    stored_hash = row.iloc[0]["pw_hash"]
    return stored_hash == _hash(password)


def get_allowed_menus(dept_id: str) -> list:
    df = _get_dept_df()
    if df.empty:
        return []
    row = df[df["dept_id"].astype(str) == dept_id]
    if row.empty:
        return []
    menus_key = row.iloc[0].get("menus", "")
    return DEPT_MENU_ACCESS.get(menus_key, [])


# ══════════════════════════════════════════════════════════════
# LOGIN PAGE
# ══════════════════════════════════════════════════════════════
def render_login(logo_b64: str = ""):
    """แสดงหน้า Login — return True ถ้า login สำเร็จ"""
    # Header
    if logo_b64:
        st.markdown(
            f"<div style='text-align:center;padding:20px 0 10px;'>"
            f"<img src='data:image/png;base64,{logo_b64}' style='height:100px;'></div>",
            unsafe_allow_html=True,
        )
    st.markdown(
        "<h1 style='text-align:center;color:#FF6B35;font-size:2rem;'>"
        "ROON KHANOMKHAI</h1>"
        "<p style='text-align:center;color:#888;'>ระบบบริหารจัดการร้าน — กรุณาเข้าสู่ระบบ</p>",
        unsafe_allow_html=True,
    )

    df = _get_dept_df()
    dept_opts = {}
    if not df.empty:
        active = df[df["is_active"].astype(str) == "TRUE"]
        dept_opts = dict(zip(active["dept_id"], active["dept_name"]))

    st.divider()
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.markdown("### 🔐 เข้าสู่ระบบ")
        dept_id  = st.selectbox("เลือกแผนก", list(dept_opts.keys()),
                                 format_func=lambda k: dept_opts[k])
        password = st.text_input("รหัสผ่าน", type="password", placeholder="กรอกรหัสผ่าน")
        login_btn = st.button("🔓 เข้าสู่ระบบ", type="primary", use_container_width=True)

        if login_btn:
            if check_login(dept_id, password):
                st.session_state["logged_in"]   = True
                st.session_state["dept_id"]     = dept_id
                st.session_state["dept_name"]   = dept_opts[dept_id]
                st.session_state["allowed_menus"] = get_allowed_menus(dept_id)
                st.success(f"✅ เข้าสู่ระบบสำเร็จ — ยินดีต้อนรับ {dept_opts[dept_id]}")
                st.rerun()
            else:
                st.error("❌ รหัสผ่านไม่ถูกต้อง")

    st.divider()
    st.markdown(
        "<p style='text-align:center;color:#aaa;font-size:0.8rem;'>"
        "ออกแบบและพัฒนาโดย <b>ดร.อภิวรรณ์ ดำแสงสวัสดิ์</b> | "
        "Copyright © 12/06/2026 ROON KHANOMKHAI</p>",
        unsafe_allow_html=True,
    )


# ══════════════════════════════════════════════════════════════
# MANAGE PASSWORDS PAGE
# ══════════════════════════════════════════════════════════════
def render_manage_passwords():
    st.markdown(
        "<h1 style='color:#7B1FA2;font-size:1.6rem;font-weight:800;"
        "border-left:6px solid #7B1FA2;padding-left:12px;'>🔑 จัดการรหัสผ่านแผนก</h1>",
        unsafe_allow_html=True,
    )

    df = _get_dept_df()
    if df.empty:
        st.error("ไม่สามารถโหลดข้อมูลได้")
        return

    st.info("💡 รหัสผ่านถูกเก็บแบบเข้ารหัส (SHA-256) ไม่สามารถดูรหัสเดิมได้ — เปลี่ยนได้เสมอ")
    st.dataframe(df[["dept_id","dept_name","menus","is_active"]], use_container_width=True)
    st.divider()

    st.markdown("#### 🔄 เปลี่ยนรหัสผ่านแผนก")
    dept_opts = dict(zip(df["dept_id"], df["dept_name"]))
    with st.form("form_change_pw"):
        c1, c2, c3 = st.columns(3)
        with c1:
            sel_dept = st.selectbox("เลือกแผนก", list(dept_opts.keys()),
                                     format_func=lambda k: dept_opts[k])
        with c2:
            new_pw   = st.text_input("รหัสผ่านใหม่ *", type="password")
        with c3:
            confirm_pw = st.text_input("ยืนยันรหัสผ่านใหม่ *", type="password")
        saved = st.form_submit_button("💾 บันทึกรหัสผ่าน", type="primary")

    if saved:
        if not new_pw:
            st.error("กรุณากรอกรหัสผ่านใหม่")
        elif new_pw != confirm_pw:
            st.error("รหัสผ่านไม่ตรงกัน")
        elif len(new_pw) < 6:
            st.error("รหัสผ่านต้องมีอย่างน้อย 6 ตัวอักษร")
        else:
            from modules.excel_db import update_row
            update_row(SHEET_AUTH, "dept_id", sel_dept, {"pw_hash": _hash(new_pw)})
            st.success(f"✅ เปลี่ยนรหัสผ่าน {dept_opts[sel_dept]} สำเร็จ")
