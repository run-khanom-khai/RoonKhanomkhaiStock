"""
petty_cash.py  –  ระบบเงินสดย่อย (Petty Cash)
Step 3: บันทึกเบิกเงินสดย่อย + แนบใบเสร็จ

Role:
  branch_staff  → สาขาตัวเอง เท่านั้น
  finance_hq    → ทุกสาขา + อนุมัติโอน
  admin         → ทุกอย่าง
"""
import io
import datetime
import streamlit as st
import pandas as pd

from config import (
    SHEET_BRANCHES, SHEET_EMPLOYEES,
    SHEET_PETTY_CASH_FUNDS,
    SHEET_PETTY_CASH_TRANSACTIONS,
    SHEET_PETTY_CASH_REQUESTS,
    SHEET_PETTY_CASH_ATTACHMENTS,
    PETTY_CASH_REQUEST_STATUSES,
    PETTY_CASH_REQUEST_STATUS_TH,
    PETTY_CASH_EXPENSE_TYPES,
    PETTY_CASH_FILE_TYPES,
)
from modules.excel_db import (
    read_sheet, write_sheet, append_row, update_row, init_workbook,
)
from utils.id_generator import next_id
try:
    from utils.gdrive_upload import (
        upload_file_to_drive, validate_uploaded_file,
        get_file_url, get_thumbnail_url, delete_file_from_drive,
    )
    GDRIVE_AVAILABLE = True
except Exception:
    GDRIVE_AVAILABLE = False

# ── Schemas ─────────────────────────────────────────────────
PETTY_CASH_SCHEMAS = {
    SHEET_PETTY_CASH_FUNDS: [
        "fund_id","branch_id","branch_name","staff_name",
        "staff_position","phone","bank_name","bank_account_no",
        "bank_account_name","fund_limit","current_balance",
        "is_active","created_at","updated_at",
    ],
    SHEET_PETTY_CASH_TRANSACTIONS: [
        "txn_id","txn_date","fund_id","branch_id","branch_name",
        "staff_name","expense_type","description",
        "amount","receipt_no","receipt_date",
        "status","transfer_date","transfer_slip",
        "approved_by","approved_at",
        "remark","created_at","updated_at",
    ],
    SHEET_PETTY_CASH_REQUESTS: [
        "request_id","request_no",
        "employee_id","employee_name","email","phone",
        "branch_id","branch_code","branch_name",
        "bank_name","bank_account_no","bank_account_name","promptpay_no",
        "request_date","expense_type","expense_detail",
        "total_amount","receipt_files","id_card_file",
        "approver_department","note",
        "status","created_by","updated_by",
        "created_at","updated_at","deleted_at",
    ],
    SHEET_PETTY_CASH_ATTACHMENTS: [
        "attachment_id","request_id","file_type",
        "file_name","drive_file_id","drive_url",
        "file_data_b64","storage_type",
        "mime_type","file_size_kb",
        "uploaded_by","uploaded_at",
    ],
}

EDITABLE_STATUSES = {"draft","submitted","waiting_transfer","waiting_original_document"}
LOCKED_STATUSES   = {"paid"}


def _init_petty_cash_sheets():
    init_workbook()
    for sheet_name, columns in PETTY_CASH_SCHEMAS.items():
        df = read_sheet(sheet_name)
        if df.empty or list(df.columns) != columns:
            write_sheet(sheet_name, pd.DataFrame(columns=columns))


def _now() -> str:
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _gen_request_no() -> str:
    today = datetime.date.today()
    df = read_sheet(SHEET_PETTY_CASH_REQUESTS)
    count = 0
    if not df.empty and "request_date" in df.columns:
        count = len(df[df["request_date"].astype(str).str.startswith(str(today))])
    return f"PC-{today.strftime('%Y%m%d')}-{count+1:03d}"


def _branches_dict() -> dict:
    df = read_sheet(SHEET_BRANCHES)
    if df.empty or "branch_name" not in df.columns:
        return {}
    bid = df.get("branch_id", pd.Series(range(len(df))).astype(str))
    return dict(zip(bid, df["branch_name"]))


def _get_petty_role() -> str:
    dept = st.session_state.get("dept_id","")
    if dept == "admin":    return "admin"
    if dept == "finance":  return "finance_hq"
    return "branch_staff"


def _get_user_branch() -> str:
    return st.session_state.get("user_branch","")


def _status_th(status: str) -> str:
    return PETTY_CASH_REQUEST_STATUS_TH.get(status, status)


# Note: ไฟล์แนบเก็บใน Google Drive (ไม่ใช้ base64 แล้ว)
def _get_file_size_kb(uploaded_file) -> float:
    """คืนขนาดไฟล์ใน KB"""
    try:
        uploaded_file.seek(0, 2)
        size = uploaded_file.tell()
        uploaded_file.seek(0)
        return round(size / 1024, 1)
    except Exception:
        return 0.0


# ══════════════════════════════════════════════════════════════
# MAIN RENDER
# ══════════════════════════════════════════════════════════════
def render():
    _init_petty_cash_sheets()
    petty_role = _get_petty_role()

    st.markdown(
        "<h2 style='color:#1565C0;font-size:1.4rem;font-weight:800;"
        "border-left:5px solid #1565C0;padding-left:10px;'>"
        "💵 ระบบเงินสดย่อย (Petty Cash)</h2>",
        unsafe_allow_html=True,
    )

    role_label = {
        "admin":        ("🔐 Admin",          "#7B1FA2"),
        "finance_hq":   ("💰 ฝ่ายการเงิน HQ", "#1565C0"),
        "branch_staff": ("🏪 พนักงานสาขา",    "#2E7D32"),
    }
    rl, rc = role_label.get(petty_role, ("👤", "#555"))
    st.markdown(
        f"<span style='background:{rc};color:white;padding:3px 12px;"
        f"border-radius:12px;font-size:0.85rem;font-weight:600;'>{rl}</span>"
        f"&nbsp;<small style='color:#888;'>{st.session_state.get('dept_name','')}</small>",
        unsafe_allow_html=True,
    )
    st.divider()

    tab1, tab2, tab3, tab4 = st.tabs([
        "📝 บันทึกเบิกเงินสดย่อย",
        "⏳ รายการรอโอน",
        "📋 ประวัติการโอน",
        "📊 รายงานเงินสดย่อย",
    ])
    with tab1: _render_request_form(petty_role)
    with tab2: _render_pending(petty_role)
    with tab3: _render_history(petty_role)
    with tab4: _render_report(petty_role)


# ══════════════════════════════════════════════════════════════
# TAB 1 : ข้อมูลพนักงานสาขา (เดิม — ไม่แก้ logic)
# ══════════════════════════════════════════════════════════════
def _render_staff_info(role: str):
    st.subheader("👤 ข้อมูลพนักงานสาขา (ผู้รับเงินสดย่อย)")

    br_df    = read_sheet(SHEET_BRANCHES)
    branches = _branches_dict()
    funds_df = read_sheet(SHEET_PETTY_CASH_FUNDS)

    # ── แสดงรายการที่ลงทะเบียนแล้ว ──────────────────────────
    if not funds_df.empty:
        active = funds_df[
            funds_df.get("is_active", pd.Series(["TRUE"]*len(funds_df)))
            .astype(str).str.upper() == "TRUE"
        ]
        if role == "branch_staff":
            user_branch = _get_user_branch()
            if user_branch:
                active = active[active["branch_name"].astype(str) == user_branch]
        if not active.empty:
            st.dataframe(active[[c for c in [
                "fund_id","branch_name","staff_name",
                "phone","bank_name","bank_account_no","fund_limit",
            ] if c in active.columns]], use_container_width=True)
            st.caption(f"ลงทะเบียนแล้ว {len(active)} คน")
    else:
        st.info("ยังไม่มีพนักงานลงทะเบียนรับเงินสดย่อย")

    st.divider()

    if role in ["admin","finance_hq"]:
        action = st.radio("การดำเนินการ",
                          ["➕ เพิ่มพนักงาน (ดึงจาก HR)","✏️ แก้ไขวงเงิน"],
                          horizontal=True, key="staff_action")
        if action == "➕ เพิ่มพนักงาน (ดึงจาก HR)":
            _form_add_staff_from_hr(branches, funds_df)
        else:
            _form_edit_staff_limit(funds_df)
    else:
        st.info("💡 ติดต่อฝ่ายการเงิน HQ เพื่อเพิ่มหรือแก้ไขข้อมูลพนักงานครับ")


def _form_add_staff_from_hr(branches: dict, funds_df: pd.DataFrame):
    """เพิ่มพนักงานโดยดึงข้อมูลจาก HR — รอรับแค่ วงเงินสดย่อย"""
    st.markdown("#### ➕ เพิ่มพนักงานผู้รับเงินสดย่อย (ดึงจาก HR)")
    st.caption("เลือกสาขา → เลือกพนักงาน → ระบบดึงข้อมูลจาก HR อัตโนมัติ")

    # ── ① เลือกสาขา ──────────────────────────────────────────
    br_keys = list(branches.keys()) if branches else []
    if not br_keys:
        st.warning("ยังไม่มีสาขาในระบบ"); return

    sel_branch_id = st.selectbox(
        "① เลือกสาขา *",
        br_keys,
        format_func=lambda k: f"{k} – {branches.get(k,k)}",
        key="add_staff_branch",
    )
    sel_branch_name = branches.get(sel_branch_id, sel_branch_id)

    # ── ② เลือกพนักงานจาก HR ─────────────────────────────────
    emp_df = read_sheet(SHEET_EMPLOYEES)
    if emp_df.empty:
        st.warning("⚠️ ยังไม่มีข้อมูลพนักงานใน HR กรุณาเพิ่มที่เมนู HR → เพิ่มพนักงาน ก่อนครับ")
        return

    # กรองพนักงานสาขานั้น
    br_emps = emp_df[
        emp_df["branch_id"].astype(str).str.strip() == str(sel_branch_id).strip()
    ].copy()

    if br_emps.empty:
        st.markdown(
            f"<div style='background:#FFF3E0;border:2px solid #FF8F00;"
            f"border-radius:8px;padding:12px;color:#E65100;'>"
            f"⚠️ ไม่พบพนักงานของสาขา <b>{sel_branch_id} – {sel_branch_name}</b> ใน HR<br>"
            f"กรุณาเพิ่มพนักงานที่เมนู <b>HR → เพิ่มพนักงาน</b> ก่อนครับ</div>",
            unsafe_allow_html=True,
        )
        return

    # กรองคนที่ลงทะเบียนแล้ว
    registered_ids = []
    if not funds_df.empty:
        registered_ids = funds_df["staff_name"].astype(str).tolist()

    br_emps["full_name"] = br_emps["first_name"].astype(str) + " " + br_emps["last_name"].astype(str)
    emp_opts = br_emps["employee_id"].tolist()

    sel_emp_id = st.selectbox(
        "② เลือกพนักงาน *",
        emp_opts,
        format_func=lambda x: (
            f"{br_emps[br_emps['employee_id']==x]['full_name'].values[0]} "
            f"({'✅ ลงทะเบียนแล้ว' if br_emps[br_emps['employee_id']==x]['full_name'].values[0] in registered_ids else ''})"
        ),
        key="add_staff_emp",
    )
    emp_row  = br_emps[br_emps["employee_id"] == sel_emp_id].iloc[0]
    emp_name = emp_row["full_name"]

    # ── ③ แสดงข้อมูลจาก HR (read-only) ──────────────────────
    st.markdown(
        f"<div style='background:#E3F2FD;border:1.5px solid #1565C0;"
        f"border-radius:8px;padding:12px;margin:8px 0;color:#000000;'>"
        f"<b style='color:#1565C0;'>ข้อมูลจาก HR (ดึงอัตโนมัติ)</b>"
        f"<table style='width:100%;margin-top:6px;'>"
        f"<tr><td style='color:#000;font-weight:600;width:130px;'>👤 ชื่อ</td>"
        f"<td style='color:#000;'><b>{emp_name}</b></td></tr>"
        f"<tr><td style='color:#000;font-weight:600;'>📞 เบอร์โทร</td>"
        f"<td style='color:#000;'>{emp_row.get('phone','-') or '-'}</td></tr>"
        f"<tr><td style='color:#000;font-weight:600;'>📧 e-mail</td>"
        f"<td style='color:#000;'>{emp_row.get('email','-') or '-'}</td></tr>"
        f"<tr><td style='color:#000;font-weight:600;'>🏦 ธนาคาร</td>"
        f"<td style='color:#000;'>{emp_row.get('bank_name','-') or '-'} "
        f"สาขา {emp_row.get('bank_branch','-') or '-'}</td></tr>"
        f"<tr><td style='color:#000;font-weight:600;'>💳 เลขบัญชี</td>"
        f"<td style='color:#000;'><code style='color:#000;'>{emp_row.get('bank_account_no','-') or '-'}</code></td></tr>"
        f"<tr><td style='color:#000;font-weight:600;'>👤 ชื่อบัญชี</td>"
        f"<td style='color:#000;'>{emp_row.get('bank_account_name','-') or '-'}</td></tr>"
        f"</table></div>",
        unsafe_allow_html=True,
    )

    # ── ④ รอรับแค่วงเงินสดย่อย ──────────────────────────────
    st.divider()
    st.markdown("**④ กำหนดวงเงินสดย่อย**")

    # ตรวจว่าลงทะเบียนแล้วหรือยัง
    if not funds_df.empty:
        dup = funds_df[
            (funds_df["staff_name"].astype(str).str.strip() == emp_name.strip()) &
            (funds_df["branch_id"].astype(str).str.strip() == str(sel_branch_id).strip()) &
            (funds_df.get("is_active", pd.Series(["TRUE"]*len(funds_df)))
             .astype(str).str.upper() == "TRUE")
        ]
        if not dup.empty:
            st.warning(
                f"⚠️ **{emp_name}** ได้ลงทะเบียนแล้ว (ID: {dup.iloc[0]['fund_id']}) "
                f"— ไม่ต้องเพิ่มซ้ำครับ"
            )
            return

    fund_limit = st.number_input(
        "วงเงินสดย่อย (บาท) *",
        min_value=0.0, step=500.0,
        help="กำหนดวงเงินสูงสุดที่พนักงานคนนี้เบิกได้ต่อครั้ง"
    )

    if st.button("💾 บันทึกพนักงาน", type="primary", use_container_width=True,
                  key="btn_save_staff"):
        if fund_limit <= 0:
            st.error("❌ กรุณากำหนดวงเงินสดย่อย (ต้องมากกว่า 0 บาท)")
            return

        df_funds = read_sheet(SHEET_PETTY_CASH_FUNDS)
        fid = next_id(df_funds, "fund_id", "PCF")
        now = _now()
        append_row(SHEET_PETTY_CASH_FUNDS, {
            "fund_id":          fid,
            "branch_id":        sel_branch_id,
            "branch_name":      sel_branch_name,
            "staff_name":       emp_name,
            "staff_position":   emp_row.get("position",""),
            "phone":            emp_row.get("phone",""),
            "bank_name":        emp_row.get("bank_name",""),
            "bank_account_no":  emp_row.get("bank_account_no",""),
            "bank_account_name":emp_row.get("bank_account_name",""),
            "fund_limit":       fund_limit,
            "current_balance":  0.0,
            "is_active":        "TRUE",
            "created_at":       now,
            "updated_at":       now,
        })
        st.success(
            f"✅ ลงทะเบียน **{emp_name}** สาขา {sel_branch_name} "
            f"วงเงิน ฿{fund_limit:,.2f} สำเร็จ! (ID: {fid})"
        )
        st.rerun()


def _form_edit_staff_limit(funds_df: pd.DataFrame):
    """แก้ไขวงเงินสดย่อยเท่านั้น"""
    st.markdown("#### ✏️ แก้ไขวงเงินสดย่อย")
    if funds_df.empty:
        st.info("ยังไม่มีพนักงานลงทะเบียน"); return

    opts = funds_df["fund_id"].tolist()
    sel  = st.selectbox(
        "เลือกพนักงาน", opts,
        format_func=lambda x: (
            f"{funds_df[funds_df['fund_id']==x]['staff_name'].values[0]} "
            f"— {funds_df[funds_df['fund_id']==x]['branch_name'].values[0]}"
        ), key="edit_fund_sel"
    )
    row = funds_df[funds_df["fund_id"] == sel].iloc[0]
    try:    cur_limit = float(row.get("fund_limit", 0))
    except: cur_limit = 0.0

    st.info(
        f"👤 {row.get('staff_name','')} | "
        f"🏪 {row.get('branch_name','')} | "
        f"💰 วงเงินปัจจุบัน: ฿{cur_limit:,.2f}"
    )
    new_limit = st.number_input(
        "วงเงินใหม่ (บาท)", min_value=0.0,
        step=500.0, value=cur_limit, key="new_limit"
    )
    if st.button("💾 บันทึกวงเงินใหม่", type="primary", key="btn_save_limit"):
        if new_limit <= 0:
            st.error("วงเงินต้องมากกว่า 0 บาท"); return
        update_row(SHEET_PETTY_CASH_FUNDS, "fund_id", sel, {
            "fund_limit": new_limit,
            "updated_at": _now(),
        })
        st.success(f"✅ แก้ไขวงเงินเป็น ฿{new_limit:,.2f} สำเร็จ")
        st.rerun()


def _form_edit_staff(df: pd.DataFrame, branches: dict):
    if df.empty:
        st.info("ยังไม่มีพนักงานให้แก้ไข"); return
    opts = df["fund_id"].tolist()
    sel  = st.selectbox("เลือกพนักงาน", opts,
        format_func=lambda x: (
            f"{x} — "
            f"{df[df['fund_id']==x]['staff_name'].values[0]} "
            f"({df[df['fund_id']==x]['branch_name'].values[0]})"
        ))
    row = df[df["fund_id"]==sel].iloc[0]
    with st.form("form_edit_petty_staff"):
        c1,c2 = st.columns(2)
        with c1:
            staff_name    = st.text_input("ชื่อพนักงาน", value=row.get("staff_name",""))
            staff_pos     = st.text_input("ตำแหน่ง",    value=row.get("staff_position",""))
            phone         = st.text_input("เบอร์โทร",   value=row.get("phone",""))
        with c2:
            bank_name     = st.text_input("ชื่อธนาคาร",   value=row.get("bank_name",""))
            bank_acc_no   = st.text_input("เลขที่บัญชี", value=row.get("bank_account_no",""))
            bank_acc_name = st.text_input("ชื่อบัญชี",   value=row.get("bank_account_name",""))
            try:   fl = float(row.get("fund_limit",0))
            except:fl = 0.0
            fund_limit = st.number_input("วงเงินสดย่อย", min_value=0.0, step=500.0, value=fl)
            act_opts = ["TRUE","FALSE"]
            act_idx  = act_opts.index(row.get("is_active","TRUE")) if row.get("is_active") in act_opts else 0
            is_active = st.selectbox("สถานะ", act_opts, index=act_idx)
        save = st.form_submit_button("💾 บันทึก", type="primary")
    if save:
        update_row(SHEET_PETTY_CASH_FUNDS, "fund_id", sel, {
            "staff_name":staff_name,"staff_position":staff_pos,"phone":phone,
            "bank_name":bank_name,"bank_account_no":bank_acc_no,
            "bank_account_name":bank_acc_name,
            "fund_limit":fund_limit,"is_active":is_active,"updated_at":_now(),
        })
        st.success("✅ แก้ไขสำเร็จ"); st.rerun()


# ══════════════════════════════════════════════════════════════
# TAB 2 : บันทึกเบิกเงินสดย่อย
# ══════════════════════════════════════════════════════════════
def _render_request_form(role: str):
    st.subheader("📝 บันทึกเบิกเงินสดย่อย")

    # sub-tab: สร้างใหม่ / ดูรายการของฉัน
    sub1, sub2 = st.tabs(["➕ สร้างรายการเบิกใหม่","📋 รายการของฉัน"])
    with sub1:
        _form_new_request(role)
    with sub2:
        _my_requests(role)


def _form_new_request(role: str):
    """Form บันทึกเบิกเงินสดย่อย — ดึงพนักงานจาก HR"""

    # ── โหลดข้อมูลสาขา ───────────────────────────────────────
    br_df = read_sheet(SHEET_BRANCHES)
    if br_df.empty:
        st.warning("⚠️ ยังไม่มีข้อมูลสาขา — กรุณาเพิ่มสาขาใน Master Data ก่อนครับ")
        return

    branches_dict = {}
    if not br_df.empty and "branch_id" in br_df.columns:
        branches_dict = dict(zip(br_df["branch_id"], br_df["branch_name"]))

    # ── ① เลือกสาขาก่อน ──────────────────────────────────────
    st.markdown("#### ① เลือกสาขา")

    # branch_staff เห็นเฉพาะสาขาตัวเอง
    if role == "branch_staff":
        user_branch_name = _get_user_branch()
        # หา branch_id จาก branch_name
        matched = br_df[br_df["branch_name"].astype(str) == str(user_branch_name)]
        if matched.empty:
            st.error(f"❌ ไม่พบสาขา '{user_branch_name}' ในระบบ")
            return
        branch_opts = matched["branch_id"].tolist()
    else:
        branch_opts = br_df["branch_id"].tolist()

    sel_branch_id = st.selectbox(
        "สาขา *",
        branch_opts,
        format_func=lambda k: f"{k} – {branches_dict.get(k, k)}",
        key="req_branch_sel",
    )
    sel_branch_name = branches_dict.get(sel_branch_id, sel_branch_id)

    # ── ② เลือกพนักงานจาก HR ─────────────────────────────────
    st.markdown("#### ② เลือกพนักงาน")

    emp_df = read_sheet(SHEET_EMPLOYEES)
    branch_emps = pd.DataFrame()
    if not emp_df.empty:
        # กรองพนักงานด้วย branch_id หรือ branch_name
        # HR เก็บ branch_id, petty_cash_funds เก็บ branch_name
        mask_id   = emp_df["branch_id"].astype(str).str.strip() == str(sel_branch_id).strip()
        mask_name = emp_df["branch_id"].astype(str).str.strip() == str(sel_branch_name).strip()

        # กรณีพิเศษ: ถ้า HR เก็บชื่อสาขาใน branch_id field
        mask_name2 = pd.Series([False] * len(emp_df))
        if "branch_name" in emp_df.columns:
            mask_name2 = emp_df["branch_name"].astype(str).str.strip() == str(sel_branch_name).strip()

        mask = mask_id | mask_name | mask_name2

        # กรอง status — ไม่แสดงเฉพาะคนที่ลาออกแล้ว
        if "status" in emp_df.columns:
            status_mask = ~emp_df["status"].astype(str).str.lower().isin(
                ["resigned","on_leave","ลาออก","ลาพัก"]
            )
            mask = mask & status_mask

        branch_emps = emp_df[mask].copy()

    if branch_emps.empty:
        st.markdown(
            "<div style='background:#FFF3E0;border:2px solid #FF8F00;"
            "border-radius:8px;padding:14px;color:#E65100;'>"
            "⚠️ <b>ไม่พบข้อมูลพนักงานของสาขานี้</b><br>"
            "กรุณาเพิ่มข้อมูลที่เมนู <b>HR &gt; เพิ่มพนักงาน</b> "
            "ก่อนทำรายการเงินสดย่อย<br>"
            f"<small style='color:#888;'>รหัสสาขา: <b>{sel_branch_id}</b> | "
            f"ชื่อสาขา: <b>{sel_branch_name}</b></small>"
            "</div>",
            unsafe_allow_html=True,
        )
        return

    emp_opts = branch_emps["employee_id"].tolist()
    sel_emp_id = st.selectbox(
        "ชื่อพนักงาน *",
        emp_opts,
        format_func=lambda x: (
            f"{branch_emps[branch_emps['employee_id']==x]['first_name'].values[0]} "
            f"{branch_emps[branch_emps['employee_id']==x]['last_name'].values[0]}"
        ),
        key="req_emp_sel",
    )
    emp_row = branch_emps[branch_emps["employee_id"] == sel_emp_id].iloc[0]
    emp_name = f"{emp_row.get('first_name','')} {emp_row.get('last_name','')}".strip()

    # ── แสดง Card ข้อมูลพนักงาน (read-only) ─────────────────
    emp_email      = emp_row.get('email','').strip() or '-'
    emp_phone      = emp_row.get('phone','').strip() or '-'
    emp_bank       = emp_row.get('bank_name','').strip() or '-'
    emp_bank_br    = emp_row.get('bank_branch','').strip() or '-'
    emp_acc_no     = emp_row.get('bank_account_no','').strip() or '-'
    emp_acc_name   = emp_row.get('bank_account_name','').strip() or '-'
    emp_promptpay  = emp_row.get('promptpay_no','').strip()
    promptpay_line = (
        f"<tr><td style='color:#000000;padding:4px 8px;font-weight:600;'>📲 PromptPay</td>"
        f"<td style='color:#000000;padding:4px 8px;'><code style='color:#000000;'>{emp_promptpay}</code></td></tr>"
    ) if emp_promptpay else ""
    st.markdown(
        f"<div style='background:#E3F2FD;border:1.5px solid #1565C0;"
        f"border-radius:8px;padding:14px;margin:8px 0;color:#000000;'>"
        f"<b style='color:#1565C0;font-size:1rem;'>ข้อมูลพนักงาน (ดึงจาก HR อัตโนมัติ)</b>"
        f"<table style='width:100%;margin-top:8px;border-collapse:collapse;'>"
        f"<tr><td style='color:#000000;padding:4px 8px;font-weight:600;width:140px;'>👤 ชื่อพนักงาน</td>"
        f"<td style='color:#000000;padding:4px 8px;'><b>{emp_name}</b></td></tr>"
        f"<tr><td style='color:#000000;padding:4px 8px;font-weight:600;'>📧 e-mail</td>"
        f"<td style='color:#000000;padding:4px 8px;'>{emp_email}</td></tr>"
        f"<tr><td style='color:#000000;padding:4px 8px;font-weight:600;'>📞 เบอร์โทร</td>"
        f"<td style='color:#000000;padding:4px 8px;'>{emp_phone}</td></tr>"
        f"<tr><td style='color:#000000;padding:4px 8px;font-weight:600;'>🏪 รหัสสาขา</td>"
        f"<td style='color:#000000;padding:4px 8px;'>{sel_branch_id} – {sel_branch_name}</td></tr>"
        f"<tr><td style='color:#000000;padding:4px 8px;font-weight:600;'>🏦 ธนาคาร</td>"
        f"<td style='color:#000000;padding:4px 8px;'>{emp_bank} สาขา {emp_bank_br}</td></tr>"
        f"<tr><td style='color:#000000;padding:4px 8px;font-weight:600;'>💳 เลขบัญชี</td>"
        f"<td style='color:#000000;padding:4px 8px;'>"
        f"<code style='color:#000000;background:#f0f4ff;padding:2px 6px;border-radius:4px;'>{emp_acc_no}</code></td></tr>"
        f"<tr><td style='color:#000000;padding:4px 8px;font-weight:600;'>👤 ชื่อบัญชี</td>"
        f"<td style='color:#000000;padding:4px 8px;'><b>{emp_acc_name}</b></td></tr>"
        f"{promptpay_line}"
        f"</table></div>",
        unsafe_allow_html=True,
    )

    st.divider()
    st.markdown("#### ② รายละเอียดการเบิก")

    # ── ข้อมูลการเบิก ────────────────────────────────────────
    c1, c2 = st.columns(2)
    with c1:
        request_date  = st.date_input("วันที่เบิก *", value=datetime.date.today(), key="req_date")
        expense_type  = st.selectbox("ประเภทการเบิก *", PETTY_CASH_EXPENSE_TYPES, key="req_etype")
    with c2:
        approver_dept = st.text_input("แผนกผู้อนุมัติ", key="req_approver")
        note          = st.text_input("หมายเหตุ", key="req_note")

    expense_detail = st.text_area("รายละเอียดค่าใช้จ่าย *", height=100, key="req_detail",
                                   placeholder="อธิบายรายละเอียดค่าใช้จ่าย เช่น ค่าน้ำมันรถ, ค่าซื้อวัสดุ...")

    # ── คำเตือนพิเศษตามประเภท ────────────────────────────────
    if expense_type == "เบิกซื้อของภายในร้าน":
        st.warning("⚠️ กรุณาขอใบกำกับภาษีทุกครั้ง และเก็บเอกสารจริงส่งสำนักงานใหญ่")

    if expense_type == "ค่าเดินทาง":
        st.info("📎 กรุณาแนบหลักฐานการเดินทาง เช่น ใบเสร็จน้ำมัน, ตั๋วรถ, ใบจองที่พัก")

    st.divider()
    st.markdown("#### ③ แนบใบเสร็จ / หลักฐาน")

    # ── Counter แสดงจำนวน ────────────────────────────────────
    MAX_RECEIPTS = 20
    st.markdown(
        f"<div style='background:#E8F5E9;border:1.5px solid #2E7D32;"
        f"border-radius:6px;padding:8px 14px;margin-bottom:8px;'>"
        f"📎 แนบใบเสร็จได้สูงสุด <b>{MAX_RECEIPTS} ไฟล์</b> ต่อ 1 รายการ | "
        f"รองรับ JPG, PNG, PDF | ขนาดสูงสุด <b>10 MB</b> ต่อไฟล์</div>",
        unsafe_allow_html=True,
    )

    receipt_files = st.file_uploader(
        "แนบใบเสร็จ / หลักฐาน *",
        type=["jpg","jpeg","png","pdf"],
        accept_multiple_files=True,
        key="req_receipts",
        help=f"สูงสุด {MAX_RECEIPTS} ไฟล์ | JPG, PNG, PDF | ขนาดสูงสุด 10 MB/ไฟล์",
    )

    # ── Counter + Validate ───────────────────────────────────
    if receipt_files:
        num_files = len(receipt_files)
        bar_pct   = min(num_files / MAX_RECEIPTS, 1.0)
        bar_color = "#2E7D32" if num_files <= MAX_RECEIPTS * 0.7 else                     "#FF8F00" if num_files < MAX_RECEIPTS else "#E53935"

        # Progress bar
        st.markdown(
            f"<div style='margin:6px 0;'>"
            f"<div style='display:flex;justify-content:space-between;margin-bottom:2px;'>"
            f"<span style='color:{bar_color};font-weight:bold;'>แนบแล้ว {num_files} / {MAX_RECEIPTS} ไฟล์</span>"
            f"{'<span style=color:#E53935;font-weight:bold;>⚠️ เกินจำนวนสูงสุด!</span>' if num_files > MAX_RECEIPTS else ''}"
            f"</div>"
            f"<div style='background:#eee;border-radius:4px;height:8px;'>"
            f"<div style='background:{bar_color};width:{bar_pct*100:.0f}%;height:8px;border-radius:4px;'></div>"
            f"</div></div>",
            unsafe_allow_html=True,
        )

        if num_files > MAX_RECEIPTS:
            st.error(f"❌ แนบไฟล์เกินกำหนด — ระบบรับได้สูงสุด {MAX_RECEIPTS} ไฟล์ กรุณาลบบางไฟล์ออก")

        # ── Preview ไฟล์ทีละไฟล์ ─────────────────────────────
        st.markdown(f"**รายการไฟล์ที่แนบ ({num_files} ไฟล์):**")
        valid_files = []
        for i, f in enumerate(receipt_files[:MAX_RECEIPTS]):  # แสดงแค่ MAX_RECEIPTS
            with st.container():
                col_num, col_prev, col_info = st.columns([0.5, 3, 4])
                with col_num:
                    st.markdown(
                        f"<div style='background:#1565C0;color:white;border-radius:50%;"
                        f"width:28px;height:28px;display:flex;align-items:center;"
                        f"justify-content:center;font-weight:bold;margin-top:8px;'>"
                        f"{i+1}</div>",
                        unsafe_allow_html=True,
                    )
                with col_prev:
                    # validate ขนาด
                    file_ok = True
                    if GDRIVE_AVAILABLE:
                        ok, err = validate_uploaded_file(f, max_mb=10.0)
                        if not ok:
                            st.error(f"❌ {err}")
                            file_ok = False
                    if file_ok:
                        if f.type in ["image/jpeg","image/png","image/jpg"]:
                            st.image(f, use_container_width=True)
                        elif f.type == "application/pdf":
                            # PDF preview + download button
                            pdf_bytes = f.read()
                            f.seek(0)
                            st.markdown(f"📄 **{f.name}**")
                            st.download_button(
                                label="👁️ เปิดดู PDF",
                                data=pdf_bytes,
                                file_name=f.name,
                                mime="application/pdf",
                                key=f"pdf_view_{i}_{f.name}",
                            )
                        else:
                            st.markdown(f"📎 {f.name}")
                with col_info:
                    kb = _get_file_size_kb(f)
                    ext = f.name.rsplit(".",1)[-1].upper() if "." in f.name else "?"
                    st.markdown(
                        f"<small><b>ชื่อไฟล์:</b> {f.name}<br>"
                        f"<b>ประเภท:</b> {ext} | <b>ขนาด:</b> {kb:.1f} KB</small>",
                        unsafe_allow_html=True,
                    )
                    if file_ok:
                        valid_files.append(f)
            st.divider()

        # เก็บ valid_files ไว้ใช้ตอน save
        receipt_files = valid_files if valid_files else receipt_files

    # แนบหลักฐานการเดินทาง (optional)
    if expense_type == "ค่าเดินทาง":
        st.markdown("**หลักฐานการเดินทาง (เพิ่มเติม)**")
        travel_proof = st.file_uploader(
            "แนบหลักฐานการเดินทาง",
            type=["jpg","jpeg","png","pdf"],
            accept_multiple_files=False,
            key="req_travel",
            help="ใบเสร็จน้ำมัน, ตั๋วรถ, ใบจองที่พัก",
        )
    else:
        travel_proof = None

    # ── ⑤ ยอดเงินรวม (พนักงานรวมยอดเอง) ─────────────────────
    st.divider()
    st.markdown(
        "<h4 style='color:#1565C0;'>⑤ ยอดเงินรวมทั้งหมด (บาท)</h4>",
        unsafe_allow_html=True,
    )
    st.markdown(
        "<div style='background:#FFF8E1;border:1.5px solid #FF8F00;"
        "border-radius:6px;padding:10px 14px;margin-bottom:10px;'>"
        "💡 <b>กรุณารวมยอดจากใบเสร็จทุกใบให้ถูกต้องก่อนกดบันทึก</b><br>"
        "<small style='color:#555;'>• ระบบไม่ได้อ่านยอดจากใบเสร็จอัตโนมัติ</small><br>"
        "<small style='color:#555;'>• พนักงานต้องรวมยอดเองจากใบเสร็จทุกใบ</small><br>"
        "<small style='color:#E53935;'>• ยอดเงินต้องมากกว่า 0 บาท และห้ามติดลบ</small>"
        "</div>",
        unsafe_allow_html=True,
    )

    col_amt, col_info = st.columns([2, 1])
    with col_amt:
        total_amount = st.number_input(
            "ยอดเงินรวมทั้งหมด (บาท) *",
            min_value=0.0,
            step=10.0,
            format="%.2f",
            key="req_total",
            help="รวมยอดจากใบเสร็จทุกใบ",
        )
    with col_info:
        st.write("")
        st.write("")
        if total_amount > 0:
            st.markdown(
                f"<div style='background:#E8F5E9;border:2px solid #2E7D32;"
                f"border-radius:8px;padding:12px;text-align:center;'>"
                f"<div style='color:#2E7D32;font-size:0.85rem;'>💰 ยอดที่จะเบิก</div>"
                f"<div style='color:#1B5E20;font-size:1.4rem;font-weight:800;'>"
                f"฿{total_amount:,.2f}</div><div style='color:#888;font-size:0.75rem;'>บาท</div>"
                f"</div>",
                unsafe_allow_html=True,
            )
        elif total_amount == 0:
            st.markdown(
                "<div style='background:#FFEBEE;border:2px solid #E53935;"
                "border-radius:8px;padding:12px;text-align:center;'>"
                "<span style='color:#E53935;font-weight:bold;'>⚠️ ยอดต้องมากกว่า 0</span>"
                "</div>",
                unsafe_allow_html=True,
            )

    st.divider()

    # ── ปุ่มบันทึก ────────────────────────────────────────────
    col_save, col_draft = st.columns(2)
    with col_save:
        btn_submit = st.button("📨 บันทึกและส่งสำนักงานใหญ่", type="primary",
                               use_container_width=True, key="btn_submit")
    with col_draft:
        btn_draft  = st.button("💾 บันทึกแบบร่าง (Draft)",
                               use_container_width=True, key="btn_draft")

    # ── Validation & Save ─────────────────────────────────────
    for (clicked, target_status) in [(btn_submit,"waiting_transfer"),(btn_draft,"draft")]:
        if not clicked:
            continue

        # Validation
        errors = []
        if not expense_type:                   errors.append("กรุณาเลือกประเภทการเบิก")
        if not expense_detail.strip():         errors.append("กรุณากรอกรายละเอียดค่าใช้จ่าย")
        if total_amount is None or total_amount == 0:
            errors.append("กรุณากรอกยอดเงินรวม — ยอดต้องมากกว่า 0 บาท")
        elif total_amount < 0:
            errors.append("ยอดเงินรวมต้องไม่ติดลบ — กรุณากรอกยอดที่ถูกต้อง")
        if target_status == "waiting_transfer" and not receipt_files:
            errors.append("กรุณาแนบใบเสร็จ / หลักฐานอย่างน้อย 1 ไฟล์")
        # ตรวจจำนวนไฟล์สูงสุด
        total_attach = len(receipt_files or []) + (1 if travel_proof else 0)
        if total_attach > MAX_RECEIPTS:
            errors.append(f"แนบไฟล์เกินกำหนด ({total_attach}/{MAX_RECEIPTS}) — กรุณาลดจำนวนไฟล์")

        # branch_staff ห้ามสร้างรายการให้สาขาอื่น
        if role == "branch_staff":
            user_branch = _get_user_branch()
            emp_branch  = sel_branch_name
            if user_branch and emp_branch != user_branch:
                errors.append("คุณไม่มีสิทธิ์บันทึกรายการให้สาขาอื่น")

        if errors:
            for e in errors: st.error(e)
            break

        # ── ตรวจซ้ำ: พนักงาน + วันที่ + รายละเอียด เดียวกัน ──────
        req_check = read_sheet(SHEET_PETTY_CASH_REQUESTS)
        if not req_check.empty:
            today_str = str(request_date)
            dup_mask = (
                (req_check["employee_id"].astype(str) == str(sel_emp_id)) &
                (req_check["request_date"].astype(str) == today_str) &
                (req_check["expense_detail"].astype(str).str.strip() == expense_detail.strip()) &
                (~req_check["status"].astype(str).isin(["cancelled","rejected"]))
            )
            if "deleted_at" in req_check.columns:
                dup_mask = dup_mask & (
                    req_check["deleted_at"].astype(str).str.strip() == ""
                )
            if dup_mask.any():
                dup_row = req_check[dup_mask].iloc[0]
                dup_no = dup_row.get("request_no","")
                st.warning(
                    f"⚠️ รายการนี้บันทึกซ้ำ! "
                    f"พนักงาน '{emp_name}' วันที่ {today_str} "
                    f"รายละเอียด '{expense_detail.strip()}' "
                    f"มีอยู่แล้ว (เลขที่: {dup_no}) "
                    f"ถ้าต้องการบันทึกรายการใหม่ กรุณาเปลี่ยนรายละเอียดให้แตกต่างกันครับ"
                )
                break

        # ── บันทึก petty_cash_requests (snapshot ณ วันที่ทำรายการ) ──
        req_df     = read_sheet(SHEET_PETTY_CASH_REQUESTS)
        request_id = next_id(req_df, "request_id", "PCR")
        request_no = _gen_request_no()
        now        = _now()
        created_by = st.session_state.get("dept_name","")

        # รวมชื่อไฟล์
        file_names = ",".join([f.name for f in receipt_files]) if receipt_files else ""

        append_row(SHEET_PETTY_CASH_REQUESTS, {
            "request_id":        request_id,
            "request_no":        request_no,
            "employee_id":       sel_emp_id,
            "employee_name":     emp_name,
            "email":             emp_row.get("email",""),
            "phone":             emp_row.get("phone",""),
            "branch_id":         sel_branch_id,
            "branch_code":       sel_branch_id,
            "branch_name":       sel_branch_name,
            "bank_name":         emp_row.get("bank_name",""),
            "bank_account_no":   emp_row.get("bank_account_no",""),
            "bank_account_name": emp_row.get("bank_account_name",""),
            "promptpay_no":      emp_row.get("promptpay_no",""),
            "request_date":      str(request_date),
            "expense_type":      expense_type,
            "expense_detail":    expense_detail.strip(),
            "total_amount":      total_amount,
            "receipt_files":     file_names,
            "id_card_file":      "",
            "approver_department": approver_dept,
            "note":              note,
            "status":            target_status,
            "created_by":        created_by,
            "updated_by":        created_by,
            "created_at":        now,
            "updated_at":        now,
            "deleted_at":        "",
        })

        # ── บันทึก petty_cash_attachments → Google Drive ────────
        all_files = list(receipt_files) if receipt_files else []
        if travel_proof:
            all_files.append(travel_proof)

        upload_errors   = []
        upload_success  = []
        ts_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

        for idx, uploaded_f in enumerate(all_files):
            ftype    = "travel_proof" if uploaded_f == travel_proof else "receipt"
            ext      = uploaded_f.name.rsplit(".",1)[-1] if "." in uploaded_f.name else "bin"
            new_name = f"{request_id}_{ftype}_{idx+1:02d}_{ts_str}.{ext}"

            drive_file_id = ""
            drive_url     = ""
            file_data_b64 = ""
            storage_type  = "failed"
            mime_type     = ""
            file_size_kb  = _get_file_size_kb(uploaded_f)

            # ── อัปโหลด (Drive หรือ base64 fallback) ────────
            try:
                uploaded_f.name = new_name
                result = upload_file_to_drive(
                    uploaded_f,
                    request_id=request_id,
                )
                drive_file_id = result.get("file_id","")
                drive_url     = result.get("url","")
                mime_type     = result.get("mime_type","")
                file_data_b64 = result.get("b64","")
                storage_type  = result.get("storage","gdrive")
                upload_success.append(new_name)
            except Exception as upload_err:
                upload_errors.append(f"ไฟล์ {idx+1}: {upload_err}")
                drive_file_id = "UPLOAD_FAILED"
                storage_type  = "failed"

            # ── บันทึก metadata ใน Google Sheets ────────────
            att_df = read_sheet(SHEET_PETTY_CASH_ATTACHMENTS)
            att_id = next_id(att_df, "attachment_id", "ATT")
            append_row(SHEET_PETTY_CASH_ATTACHMENTS, {
                "attachment_id": att_id,
                "request_id":    request_id,
                "file_type":     ftype,
                "file_name":     new_name,
                "drive_file_id": drive_file_id,
                "drive_url":     drive_url,
                "file_data_b64": file_data_b64,
                "storage_type":  storage_type,
                "mime_type":     mime_type,
                "file_size_kb":  file_size_kb,
                "uploaded_by":   created_by,
                "uploaded_at":   now,
            })

        if upload_errors:
            st.warning(
                f"⚠️ บางไฟล์อัปโหลดไม่สำเร็จ {len(upload_errors)} ไฟล์: "
                + " | ".join(upload_errors)
            )
        if upload_success:
            st.caption(f"✅ บันทึกไฟล์สำเร็จ {len(upload_success)} ไฟล์")

        # ── แสดงผลสำเร็จ ─────────────────────────────────────
        status_label = _status_th(target_status)
        if target_status == "waiting_transfer":
            st.success(
                f"✅ ส่งรายการเบิกสำเร็จ! "
                f"เลขที่: **{request_no}** (ID: {request_id}) | "
                f"ยอด: ฿{total_amount:,.2f} | "
                f"สถานะ: {status_label} | "
                f"แนบ {len(all_files)} ไฟล์"
            )
            st.balloons()
        else:
            st.info(f"💾 บันทึกแบบร่างสำเร็จ เลขที่: **{request_no}** | สถานะ: {status_label}")

        st.rerun()
        break


def _my_requests(role: str):
    """แสดงรายการเบิกของฉัน"""
    st.subheader("📋 รายการเบิกของฉัน")

    req_df = read_sheet(SHEET_PETTY_CASH_REQUESTS)
    if req_df.empty:
        st.info("ยังไม่มีรายการ")
        return

    # ซ่อนรายการที่ถูกลบ
    if "deleted_at" in req_df.columns:
        req_df = req_df[req_df["deleted_at"].astype(str).str.strip() == ""]

    # กรอง branch_staff
    if role == "branch_staff":
        user_branch = _get_user_branch()
        if user_branch:
            req_df = req_df[req_df["branch_name"].astype(str) == user_branch]

    if req_df.empty:
        st.info("ไม่พบรายการของคุณ")
        return

    # แสดงตาราง
    show_cols = ["request_no","request_date","employee_name","branch_name",
                 "expense_detail","total_amount","status","receipt_files"]
    display   = req_df[[c for c in show_cols if c in req_df.columns]].copy()
    if "status" in display.columns:
        display["status"] = display["status"].map(
            lambda s: PETTY_CASH_REQUEST_STATUS_TH.get(s, s)
        )
    st.dataframe(display.sort_values("request_date", ascending=False)
                 if "request_date" in display.columns else display,
                 use_container_width=True)

    st.divider()

    # ── แก้ไข / ลบ ───────────────────────────────────────────
    st.markdown("#### ✏️ แก้ไข / ลบรายการ")
    editable = req_df[req_df["status"].astype(str).isin(EDITABLE_STATUSES)]
    if editable.empty:
        st.info("ไม่มีรายการที่แก้ไขได้ (รายการที่ paid แล้วไม่สามารถแก้ไขได้)")
        return

    sel_req = st.selectbox("เลือกรายการที่ต้องการแก้ไข",
                            editable["request_id"].tolist(),
                            format_func=lambda x: (
                                f"{editable[editable['request_id']==x]['request_no'].values[0]} — "
                                f"{editable[editable['request_id']==x]['expense_type'].values[0]} — "
                                f"฿{float(editable[editable['request_id']==x]['total_amount'].values[0]):,.2f}"
                            ))
    row = editable[editable["request_id"] == sel_req].iloc[0]

    # ตรวจสิทธิ์ branch_staff
    if role == "branch_staff":
        user_branch = _get_user_branch()
        if user_branch and str(row.get("branch_name","")) != user_branch:
            st.error("❌ คุณไม่มีสิทธิ์แก้ไขรายการของสาขาอื่น")
            return

    # ตรวจ status paid — lock (ยกเว้น admin)
    if str(row.get("status","")) == "paid":
        if role != "admin":
            st.markdown(
                "<div style='background:#FFEBEE;border:2px solid #E53935;"
                "border-radius:8px;padding:12px;'>"
                "🔒 <b style='color:#B71C1C;'>รายการนี้ paid แล้ว — ไม่สามารถแก้ไขหรือลบไฟล์แนบได้</b><br>"
                "<small style='color:#888;'>ติดต่อ Admin เพื่อแก้ไขครับ</small></div>",
                unsafe_allow_html=True,
            )
            return
        else:
            st.warning("⚠️ [Admin Mode] รายการนี้ paid แล้ว — แก้ไขได้เนื่องจากสิทธิ์ Admin")

    col_edit, col_del = st.columns(2)
    with col_edit:
        with st.expander("✏️ แก้ไขรายละเอียด"):
            with st.form(f"form_edit_req_{sel_req}"):
                try:   td = float(row.get("total_amount",0))
                except: td = 0.0
                new_detail = st.text_area("รายละเอียด",
                                           value=row.get("expense_detail",""), height=80)
                # lock total_amount ถ้า paid (ยกเว้น admin)
                is_paid = str(row.get("status","")) == "paid"
                if is_paid and role != "admin":
                    st.markdown(
                        f"<div style='background:#F5F5F5;border:1px solid #ccc;"
                        f"border-radius:6px;padding:10px;'>"
                        f"💰 ยอดเงินรวม: <b>฿{td:,.2f}</b> บาท<br>"
                        f"<small style='color:#E53935;'>🔒 ไม่สามารถแก้ไขได้ (status = paid)</small>"
                        f"</div>",
                        unsafe_allow_html=True,
                    )
                    new_amount = td  # ใช้ค่าเดิม
                else:
                    new_amount = st.number_input("ยอดเงินรวม (บาท)",
                                                  min_value=0.0, step=10.0, value=td,
                                                  help="กรุณารวมยอดจากใบเสร็จทุกใบให้ถูกต้อง")
                new_note   = st.text_input("หมายเหตุ", value=row.get("note",""))
                save_edit  = st.form_submit_button("💾 บันทึก", type="primary")
            if save_edit:
                if new_amount <= 0: st.error("ยอดเงินต้องมากกว่า 0 บาท"); return
                update_row(SHEET_PETTY_CASH_REQUESTS, "request_id", sel_req, {
                    "expense_detail": new_detail,
                    "total_amount":   new_amount,
                    "note":           new_note,
                    "updated_by":     st.session_state.get("dept_name",""),
                    "updated_at":     _now(),
                })
                st.success("✅ แก้ไขสำเร็จ"); st.rerun()

    with col_del:
        with st.expander("🗑️ ลบรายการ"):
            st.warning("⚠️ การลบจะเป็น Soft Delete — ข้อมูลยังคงอยู่ในระบบ")
            confirm_del = st.text_input("พิมพ์ 'ยืนยันลบ' เพื่อยืนยัน", key=f"del_confirm_{sel_req}")
            if st.button("🗑️ ลบรายการ", key=f"btn_del_{sel_req}"):
                if confirm_del != "ยืนยันลบ":
                    st.error("กรุณาพิมพ์ 'ยืนยันลบ' ให้ถูกต้อง")
                else:
                    update_row(SHEET_PETTY_CASH_REQUESTS, "request_id", sel_req, {
                        "status":     "cancelled",
                        "deleted_at": _now(),
                        "updated_by": st.session_state.get("dept_name",""),
                        "updated_at": _now(),
                    })
                    st.success("🗑️ ลบรายการสำเร็จ (Soft Delete)"); st.rerun()


# ══════════════════════════════════════════════════════════════
# TAB 3 : รายการรอโอน
# ══════════════════════════════════════════════════════════════
def _render_pending(role: str):
    st.subheader("⏳ รายการรอโอน")

    # ดึงจาก petty_cash_requests ก่อน (ใหม่)
    req_df = read_sheet(SHEET_PETTY_CASH_REQUESTS)
    txn_df = read_sheet(SHEET_PETTY_CASH_TRANSACTIONS)  # เก่า

    # รวม requests ที่ waiting_transfer
    pending_new = pd.DataFrame()
    if not req_df.empty:
        mask = req_df["status"].astype(str) == "waiting_transfer"
        if "deleted_at" in req_df.columns:
            mask = mask & (req_df["deleted_at"].astype(str).str.strip() == "")
        pending_new = req_df[mask].copy()

    # รวม transactions เก่า
    pending_old = pd.DataFrame()
    if not txn_df.empty:
        pending_old = txn_df[txn_df["status"].astype(str) == "รอโอน"].copy()

    if role == "branch_staff":
        user_branch = _get_user_branch()
        if user_branch and not pending_new.empty:
            pending_new = pending_new[pending_new["branch_name"].astype(str) == user_branch]

    # KPI
    try:
        total_new = pd.to_numeric(pending_new["total_amount"], errors="coerce").fillna(0).sum() if not pending_new.empty else 0
        total_old = pd.to_numeric(pending_old.get("amount", pd.Series()), errors="coerce").fillna(0).sum() if not pending_old.empty else 0
    except: total_new = total_old = 0

    c1, c2, c3 = st.columns(3)
    c1.metric("รายการรอโอน", len(pending_new) + len(pending_old))
    c2.metric("ยอดรอโอน (ระบบใหม่)", f"฿{total_new:,.2f}")
    c3.metric("ยอดรอโอน (ระบบเก่า)", f"฿{total_old:,.2f}")

    if not pending_new.empty:
        st.markdown("#### 📋 รายการใหม่ (Petty Cash Requests)")
        # Format total_amount ก่อนแสดง
        show = pending_new[[c for c in [
            "request_no","request_date","employee_name","branch_name",
            "expense_type","expense_detail","total_amount","receipt_files","status"
        ] if c in pending_new.columns]].copy()
        if "total_amount" in show.columns:
            show["total_amount"] = pd.to_numeric(
                show["total_amount"], errors="coerce"
            ).fillna(0).apply(lambda x: f"฿{x:,.2f}")
            show = show.rename(columns={"total_amount": "ยอดเงินรวม (฿)"})
        st.dataframe(show, use_container_width=True)

        if role in ["admin","finance_hq"]:
            _approve_transfer(pending_new)

    if not pending_old.empty:
        st.markdown("#### 📋 รายการเก่า (Petty Cash Transactions)")
        st.dataframe(pending_old[[c for c in [
            "txn_id","txn_date","branch_name","staff_name",
            "expense_type","description","amount","status"
        ] if c in pending_old.columns]], use_container_width=True)

    if pending_new.empty and pending_old.empty:
        st.success("✅ ไม่มีรายการรอโอน")


def _approve_transfer(pending_df: pd.DataFrame):
    st.divider()
    st.markdown("#### ✅ อนุมัติโอนเงิน (Finance HQ)")
    opts = pending_df["request_id"].tolist()
    sel  = st.selectbox("เลือกรายการ", opts,
                         format_func=lambda x: (
                             f"{pending_df[pending_df['request_id']==x]['request_no'].values[0]} — "
                             f"{pending_df[pending_df['request_id']==x]['employee_name'].values[0]} — "
                             f"฿{float(pending_df[pending_df['request_id']==x]['total_amount'].values[0]):,.2f}"
                         ))
    row = pending_df[pending_df["request_id"]==sel].iloc[0]

    # แสดงข้อมูลบัญชีปลายทาง
    st.info(
        f"💳 โอนไปยัง: **{row.get('bank_name','')}** "
        f"เลขบัญชี: `{row.get('bank_account_no','')}` "
        f"ชื่อบัญชี: {row.get('bank_account_name','')} | "
        f"ยอด: ฿{float(row.get('total_amount',0)):,.2f}"
    )

    # แสดงไฟล์แนบ
    att_df = read_sheet(SHEET_PETTY_CASH_ATTACHMENTS)
    if not att_df.empty:
        my_att = att_df[att_df["request_id"].astype(str) == str(sel)]
        if not my_att.empty:
            st.markdown("**ไฟล์แนบ:**")
            cols = st.columns(min(len(my_att), 3))
            for i, (_, a) in enumerate(my_att.iterrows()):
                fname      = a.get("file_name","")
                drive_url  = a.get("drive_url","")
                drive_id   = a.get("drive_file_id","")
                file_size  = a.get("file_size_kb","")
                with cols[i % 3]:
                    b64_data   = a.get("file_data_b64","")
                    storage    = a.get("storage_type","")

                    if drive_id and drive_url and not drive_id.startswith("b64_") and drive_id != "UPLOAD_FAILED":
                        # เก็บใน Google Drive
                        if fname.lower().endswith((".jpg",".jpeg",".png")):
                            thumb_url = get_thumbnail_url(drive_id) if GDRIVE_AVAILABLE else ""
                            if thumb_url:
                                st.image(thumb_url, caption=fname, use_container_width=True)
                            else:
                                st.markdown(f"🖼️ [{fname}]({drive_url})")
                        else:
                            st.markdown(f"📄 [{fname}]({drive_url})")
                        st.markdown(f"[🔗 เปิดไฟล์]({drive_url})")
                        if file_size:
                            st.caption(f"ขนาด: {file_size} KB")

                    elif b64_data and storage == "base64":
                        # เก็บแบบ base64 ใน Sheets
                        import base64 as b64mod
                        try:
                            raw = b64mod.b64decode(b64_data)
                            if fname.lower().endswith((".jpg",".jpeg",".png")):
                                st.image(raw, caption=fname, use_container_width=True)
                            st.download_button(
                                f"⬇️ ดาวน์โหลด",
                                data=raw,
                                file_name=fname,
                                key=f"dl_{a.get('attachment_id','')}",
                            )
                            if file_size:
                                st.caption(f"ขนาด: {file_size} KB")
                        except Exception:
                            st.error(f"❌ ไม่สามารถแสดงไฟล์ {fname}")
                    else:
                        st.error(f"❌ {fname} — อัปโหลดไม่สำเร็จ")

    with st.form(f"form_approve_{sel}"):
        c1, c2 = st.columns(2)
        with c1:
            transfer_date = st.date_input("วันที่โอน", value=datetime.date.today())
            slip_no       = st.text_input("หมายเลขสลิป / อ้างอิง *")
        with c2:
            approved_by   = st.text_input("ผู้อนุมัติ *")
            remark        = st.text_input("หมายเหตุ")
        approve = st.form_submit_button("✅ ยืนยันการโอน", type="primary")
    if approve:
        if not slip_no.strip():    st.error("กรุณากรอกหมายเลขสลิป"); return
        if not approved_by.strip():st.error("กรุณากรอกชื่อผู้อนุมัติ"); return
        update_row(SHEET_PETTY_CASH_REQUESTS, "request_id", sel, {
            "status":     "paid",
            "note":       remark,
            "updated_by": approved_by,
            "updated_at": _now(),
        })
        st.success(f"✅ ยืนยันโอน {row.get('request_no','')} สำเร็จ | สลิป: {slip_no}")
        st.rerun()


# ══════════════════════════════════════════════════════════════
# TAB 4 : ประวัติการโอน
# ══════════════════════════════════════════════════════════════
def _render_history(role: str):
    st.subheader("📋 ประวัติการโอนเงินสดย่อย")

    req_df = read_sheet(SHEET_PETTY_CASH_REQUESTS)
    if req_df.empty:
        st.info("ยังไม่มีประวัติ"); return

    paid = req_df[req_df["status"].astype(str) == "paid"].copy()
    if "deleted_at" in paid.columns:
        paid = paid[paid["deleted_at"].astype(str).str.strip() == ""]

    if role == "branch_staff":
        user_branch = _get_user_branch()
        if user_branch:
            paid = paid[paid["branch_name"].astype(str) == user_branch]

    c1, c2 = st.columns(2)
    with c1:
        f_from = st.date_input("จากวันที่", value=None, key="hist_from")
    with c2:
        f_to   = st.date_input("ถึงวันที่",  value=None, key="hist_to")

    if f_from: paid = paid[paid["request_date"].astype(str) >= str(f_from)]
    if f_to:   paid = paid[paid["request_date"].astype(str) <= str(f_to)]

    if paid.empty:
        st.info("ไม่พบรายการในช่วงที่เลือก"); return

    try:    total = pd.to_numeric(paid["total_amount"],errors="coerce").fillna(0).sum()
    except: total = 0
    c1,c2 = st.columns(2)
    c1.metric("รายการทั้งหมด", len(paid))
    c2.metric("ยอดรวม", f"฿{total:,.2f}")

    paid_sort = paid.sort_values("request_date", ascending=False) if "request_date" in paid.columns else paid
    st.dataframe(paid_sort[[c for c in [
        "request_no","request_date","employee_name","branch_name",
        "expense_detail","total_amount","status","receipt_files"
    ] if c in paid_sort.columns]], use_container_width=True)

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        paid_sort.to_excel(w, index=False, sheet_name="petty_cash_paid")
    st.download_button("⬇️ Export Excel", data=buf.getvalue(),
                       file_name=f"petty_cash_history_{datetime.date.today()}.xlsx",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


# ══════════════════════════════════════════════════════════════
# TAB 5 : รายงาน
# ══════════════════════════════════════════════════════════════
def _render_report(role: str):
    st.subheader("📊 รายงานเงินสดย่อย")

    req_df = read_sheet(SHEET_PETTY_CASH_REQUESTS)
    if req_df.empty:
        st.info("ยังไม่มีข้อมูล"); return

    if "deleted_at" in req_df.columns:
        req_df = req_df[req_df["deleted_at"].astype(str).str.strip() == ""]

    req_df["total_amount"] = pd.to_numeric(req_df["total_amount"], errors="coerce").fillna(0)

    if role == "branch_staff":
        user_branch = _get_user_branch()
        if user_branch:
            req_df = req_df[req_df["branch_name"].astype(str) == user_branch]

    total_all  = req_df["total_amount"].sum()
    total_wait = req_df[req_df["status"]=="waiting_transfer"]["total_amount"].sum()
    total_paid = req_df[req_df["status"]=="paid"]["total_amount"].sum()

    c1,c2,c3,c4 = st.columns(4)
    c1.metric("รายการทั้งหมด", len(req_df))
    c2.metric("ยอดรวม",        f"฿{total_all:,.2f}")
    c3.metric("รอโอน",         f"฿{total_wait:,.2f}")
    c4.metric("โอนแล้ว",       f"฿{total_paid:,.2f}")

    st.divider()
    st.markdown("#### สรุปแยกตามสาขา")
    br_sum = req_df.groupby("branch_name").agg(
        จำนวน=("request_id","count"),
        ยอดรวม=("total_amount","sum"),
    ).reset_index()
    st.dataframe(br_sum, use_container_width=True)

    st.markdown("#### สรุปแยกตามรายละเอียดค่าใช้จ่าย")
    tp_sum = req_df.groupby("expense_detail").agg(
        จำนวน=("request_id","count"),
        ยอดรวม=("total_amount","sum"),
    ).reset_index().sort_values("ยอดรวม", ascending=False)
    tp_sum = tp_sum.rename(columns={"expense_detail": "รายละเอียดค่าใช้จ่าย"})
    st.dataframe(tp_sum, use_container_width=True)

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        req_df.to_excel(w,  index=False, sheet_name="all")
        br_sum.to_excel(w,  index=False, sheet_name="by_branch")
        tp_sum.to_excel(w,  index=False, sheet_name="by_type")
    st.download_button("⬇️ Export รายงานสรุป Excel",
                       data=buf.getvalue(),
                       file_name=f"petty_cash_report_{datetime.date.today()}.xlsx",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                       type="primary")
