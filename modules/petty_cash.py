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
    SHEET_BRANCHES,
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

    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "👤 ข้อมูลพนักงานสาขา",
        "📝 บันทึกเบิกเงินสดย่อย",
        "⏳ รายการรอโอน",
        "📋 ประวัติการโอน",
        "📊 รายงานเงินสดย่อย",
    ])
    with tab1: _render_staff_info(petty_role)
    with tab2: _render_request_form(petty_role)
    with tab3: _render_pending(petty_role)
    with tab4: _render_history(petty_role)
    with tab5: _render_report(petty_role)


# ══════════════════════════════════════════════════════════════
# TAB 1 : ข้อมูลพนักงานสาขา (เดิม — ไม่แก้ logic)
# ══════════════════════════════════════════════════════════════
def _render_staff_info(role: str):
    st.subheader("👤 ข้อมูลพนักงานสาขา (ผู้รับเงินสดย่อย)")

    df = read_sheet(SHEET_PETTY_CASH_FUNDS)
    branches = _branches_dict()

    if role == "branch_staff":
        user_branch = _get_user_branch()
        if user_branch and not df.empty:
            df = df[df["branch_name"].astype(str) == user_branch]

    if not df.empty:
        active = df[df.get("is_active", pd.Series(["TRUE"]*len(df))).astype(str).str.upper() == "TRUE"]
        st.dataframe(active[[c for c in [
            "fund_id","branch_name","staff_name","staff_position",
            "phone","bank_name","bank_account_no","fund_limit","current_balance",
        ] if c in active.columns]], use_container_width=True)
        st.caption(f"พบ {len(active)} รายการ")
    else:
        st.info("ยังไม่มีข้อมูลพนักงานสาขา")

    st.divider()

    if role in ["admin","finance_hq"]:
        action = st.radio("การดำเนินการ",
                          ["➕ เพิ่มพนักงาน","✏️ แก้ไขพนักงาน"],
                          horizontal=True, key="staff_action")
        if action == "➕ เพิ่มพนักงาน":
            _form_add_staff(branches)
        else:
            _form_edit_staff(df, branches)
    else:
        st.info("💡 ติดต่อฝ่ายการเงิน HQ เพื่อเพิ่มหรือแก้ไขข้อมูลพนักงานครับ")


def _form_add_staff(branches: dict):
    with st.form("form_add_petty_staff"):
        st.markdown("#### ➕ เพิ่มพนักงานผู้รับเงินสดย่อย")
        c1, c2 = st.columns(2)
        with c1:
            br_list     = list(branches.values()) if branches else ["ยังไม่มีสาขา"]
            branch_name = st.selectbox("สาขา *", br_list)
            staff_name  = st.text_input("ชื่อพนักงาน *")
            staff_pos   = st.text_input("ตำแหน่ง")
            phone       = st.text_input("เบอร์โทร")
        with c2:
            bank_name     = st.text_input("ชื่อธนาคาร *")
            bank_acc_no   = st.text_input("เลขที่บัญชี *")
            bank_acc_name = st.text_input("ชื่อบัญชี *")
            fund_limit    = st.number_input("วงเงินสดย่อย (บาท) *", min_value=0.0, step=500.0)
        saved = st.form_submit_button("💾 บันทึก", type="primary")
    if saved:
        errs = []
        if not staff_name.strip():   errs.append("กรุณากรอกชื่อพนักงาน")
        if not bank_name.strip():    errs.append("กรุณากรอกชื่อธนาคาร")
        if not bank_acc_no.strip():  errs.append("กรุณากรอกเลขที่บัญชี")
        if not bank_acc_name.strip():errs.append("กรุณากรอกชื่อบัญชี")
        if errs:
            for e in errs: st.error(e)
            return
        df  = read_sheet(SHEET_PETTY_CASH_FUNDS)
        fid = next_id(df, "fund_id", "PCF")
        now = _now()
        append_row(SHEET_PETTY_CASH_FUNDS, {
            "fund_id": fid, "branch_id":"","branch_name":branch_name,
            "staff_name":staff_name.strip(),"staff_position":staff_pos,
            "phone":phone,"bank_name":bank_name.strip(),
            "bank_account_no":bank_acc_no.strip(),
            "bank_account_name":bank_acc_name.strip(),
            "fund_limit":fund_limit,"current_balance":0.0,
            "is_active":"TRUE","created_at":now,"updated_at":now,
        })
        st.success(f"✅ เพิ่มพนักงาน '{staff_name}' สำเร็จ (ID: {fid})")
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
# TAB 2 : บันทึกเบิกเงินสดย่อย (Step 3 — ใหม่ทั้งหมด)
# ══════════════════════════════════════════════════════════════
def _render_request_form(role: str):
    st.subheader("📝 บันทึกเบิกเงินสดย่อย")

    # ── โหลด fund (พนักงาน) ──────────────────────────────────
    fund_df = read_sheet(SHEET_PETTY_CASH_FUNDS)
    if fund_df.empty:
        st.warning("⚠️ ยังไม่มีข้อมูลพนักงานสาขา — เพิ่มที่แท็บ 'ข้อมูลพนักงานสาขา' ก่อนครับ")
        return

    # กรอง branch_staff เห็นเฉพาะสาขาตัวเอง
    if role == "branch_staff":
        user_branch = _get_user_branch()
        if user_branch:
            fund_df = fund_df[fund_df["branch_name"].astype(str) == user_branch]

    active_funds = fund_df[
        fund_df.get("is_active", pd.Series(["TRUE"]*len(fund_df)))
        .astype(str).str.upper() == "TRUE"
    ]
    if active_funds.empty:
        st.info("ไม่พบข้อมูลพนักงานสาขา (active) ของสาขาคุณ"); return

    # sub-tab: สร้างใหม่ / ดูรายการของฉัน
    sub1, sub2 = st.tabs(["➕ สร้างรายการเบิกใหม่","📋 รายการของฉัน"])
    with sub1:
        _form_new_request(active_funds, role)
    with sub2:
        _my_requests(role)


def _form_new_request(active_funds: pd.DataFrame, role: str):
    """Form บันทึกเบิกเงินสดย่อย"""

    st.markdown("#### ① เลือกพนักงาน")

    # ── Dropdown เลือกพนักงาน ────────────────────────────────
    fund_opts = active_funds["fund_id"].tolist()
    sel_fund  = st.selectbox(
        "ชื่อพนักงาน *",
        fund_opts,
        format_func=lambda x: (
            f"{active_funds[active_funds['fund_id']==x]['staff_name'].values[0]} "
            f"— {active_funds[active_funds['fund_id']==x]['branch_name'].values[0]}"
        ),
        key="req_fund_sel",
    )

    # ── ดึงข้อมูลพนักงานอัตโนมัติ ────────────────────────────
    emp_row = active_funds[active_funds["fund_id"] == sel_fund].iloc[0]

    # แสดงข้อมูลพนักงาน (read-only card)
    with st.container():
        st.markdown(
            f"""<div style='background:#E3F2FD;border:1.5px solid #1565C0;
            border-radius:8px;padding:14px;margin:8px 0;'>
            <b style='color:#1565C0;'>ข้อมูลพนักงาน (ดึงอัตโนมัติ)</b><br>
            👤 <b>{emp_row.get('staff_name','')}</b> &nbsp;|&nbsp;
            🏪 {emp_row.get('branch_name','')} &nbsp;|&nbsp;
            📞 {emp_row.get('phone','')}<br>
            🏦 {emp_row.get('bank_name','')} &nbsp;
            เลขบัญชี: <code>{emp_row.get('bank_account_no','')}</code> &nbsp;
            ชื่อบัญชี: {emp_row.get('bank_account_name','')}<br>
            💰 วงเงิน: ฿{float(emp_row.get('fund_limit',0)):,.2f}
            </div>""",
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
    st.markdown("#### ③ ยอดเงินรวม")
    st.caption("💡 กรุณารวมยอดจากใบเสร็จเองก่อนกรอกในช่องนี้")
    total_amount = st.number_input(
        "ยอดเงินรวมทั้งหมด (บาท) *",
        min_value=0.0, step=10.0, format="%.2f",
        key="req_total",
    )
    if total_amount > 0:
        st.success(f"💰 ยอดที่จะเบิก: **฿{total_amount:,.2f}**")

    st.divider()
    st.markdown("#### ④ แนบใบเสร็จ / หลักฐาน")
    st.caption("รองรับ JPG, JPEG, PNG, PDF | แนบได้หลายไฟล์ | ต้องแนบอย่างน้อย 1 ไฟล์")

    receipt_files = st.file_uploader(
        "แนบใบเสร็จ / หลักฐาน *",
        type=["jpg","jpeg","png","pdf"],
        accept_multiple_files=True,
        key="req_receipts",
        help="รองรับ JPG, PNG, PDF | ขนาดสูงสุด 10 MB ต่อไฟล์",
    )

    # Preview + Validate ไฟล์ที่แนบ
    if receipt_files:
        st.markdown(f"**แนบแล้ว {len(receipt_files)} ไฟล์:**")
        cols = st.columns(min(len(receipt_files), 3))
        for i, f in enumerate(receipt_files):
            with cols[i % 3]:
                # validate
                if GDRIVE_AVAILABLE:
                    ok, err = validate_uploaded_file(f, max_mb=10.0)
                    if not ok:
                        st.error(f"❌ {f.name}: {err}")
                        continue
                # preview
                if f.type in ["image/jpeg","image/png","image/jpg"]:
                    st.image(f, caption=f.name, use_container_width=True)
                elif f.type == "application/pdf":
                    st.markdown(f"📄 **{f.name}**")
                else:
                    st.write(f"📎 {f.name}")
                # แสดงขนาดไฟล์
                kb = _get_file_size_kb(f)
                st.caption(f"ขนาด: {kb:.1f} KB")

    # แนบหลักฐานการเดินทาง (optional)
    if expense_type == "ค่าเดินทาง":
        travel_proof = st.file_uploader(
            "แนบหลักฐานการเดินทาง (เพิ่มเติม)",
            type=["jpg","jpeg","png","pdf"],
            accept_multiple_files=False,
            key="req_travel",
            help="ใบเสร็จน้ำมัน, ตั๋วรถ, ใบจองที่พัก",
        )
    else:
        travel_proof = None

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
        if total_amount <= 0:                  errors.append("ยอดเงินต้องมากกว่า 0 บาท")
        if target_status == "waiting_transfer" and not receipt_files:
            errors.append("กรุณาแนบใบเสร็จ / หลักฐานอย่างน้อย 1 ไฟล์")

        # branch_staff ห้ามสร้างรายการให้สาขาอื่น
        if role == "branch_staff":
            user_branch = _get_user_branch()
            emp_branch  = str(emp_row.get("branch_name",""))
            if user_branch and emp_branch != user_branch:
                errors.append("คุณไม่มีสิทธิ์บันทึกรายการให้สาขาอื่น")

        if errors:
            for e in errors: st.error(e)
            break

        # ── บันทึก petty_cash_requests ───────────────────────
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
            "employee_id":       sel_fund,
            "employee_name":     emp_row.get("staff_name",""),
            "email":             "",
            "phone":             emp_row.get("phone",""),
            "branch_id":         emp_row.get("branch_id",""),
            "branch_code":       emp_row.get("branch_id",""),
            "branch_name":       emp_row.get("branch_name",""),
            "bank_name":         emp_row.get("bank_name",""),
            "bank_account_no":   emp_row.get("bank_account_no",""),
            "bank_account_name": emp_row.get("bank_account_name",""),
            "promptpay_no":      "",
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

        upload_errors = []
        for uploaded_f in all_files:
            ftype = "travel_proof" if uploaded_f == travel_proof else "receipt"

            drive_file_id = ""
            drive_url     = ""
            mime_type     = ""
            file_size_kb  = _get_file_size_kb(uploaded_f)

            # ── อัปโหลดขึ้น Google Drive ─────────────────────
            if GDRIVE_AVAILABLE:
                try:
                    result = upload_file_to_drive(
                        uploaded_f,
                        request_id=request_id,
                    )
                    drive_file_id = result["file_id"]
                    drive_url     = result["url"]
                    mime_type     = result["mime_type"]
                except Exception as upload_err:
                    upload_errors.append(f"{uploaded_f.name}: {upload_err}")
                    drive_file_id = "UPLOAD_FAILED"
                    drive_url     = ""
            else:
                upload_errors.append(f"Google Drive ไม่พร้อมใช้งาน")

            # ── บันทึก metadata ใน Google Sheets ────────────
            att_df = read_sheet(SHEET_PETTY_CASH_ATTACHMENTS)
            att_id = next_id(att_df, "attachment_id", "ATT")
            append_row(SHEET_PETTY_CASH_ATTACHMENTS, {
                "attachment_id": att_id,
                "request_id":    request_id,
                "file_type":     ftype,
                "file_name":     uploaded_f.name,
                "drive_file_id": drive_file_id,
                "drive_url":     drive_url,
                "mime_type":     mime_type,
                "file_size_kb":  file_size_kb,
                "uploaded_by":   created_by,
                "uploaded_at":   now,
            })

        if upload_errors:
            st.warning(
                f"⚠️ อัปโหลดสำเร็จบางส่วน — มีข้อผิดพลาด {len(upload_errors)} ไฟล์: "
                + ", ".join(upload_errors)
            )

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
                 "expense_type","total_amount","status","receipt_files"]
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

    # ตรวจ status paid — lock
    if str(row.get("status","")) == "paid":
        st.warning("⚠️ รายการนี้ paid แล้ว ไม่สามารถแก้ไขได้")
        return

    col_edit, col_del = st.columns(2)
    with col_edit:
        with st.expander("✏️ แก้ไขรายละเอียด"):
            with st.form(f"form_edit_req_{sel_req}"):
                try:   td = float(row.get("total_amount",0))
                except: td = 0.0
                new_detail = st.text_area("รายละเอียด",
                                           value=row.get("expense_detail",""), height=80)
                new_amount = st.number_input("ยอดเงินรวม", min_value=0.0,
                                              step=10.0, value=td)
                new_note   = st.text_input("หมายเหตุ", value=row.get("note",""))
                save_edit  = st.form_submit_button("💾 บันทึก", type="primary")
            if save_edit:
                if new_amount <= 0: st.error("ยอดเงินต้องมากกว่า 0"); return
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
        show = pending_new[[c for c in [
            "request_no","request_date","employee_name","branch_name",
            "expense_type","expense_detail","total_amount","receipt_files","status"
        ] if c in pending_new.columns]]
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
                    if drive_id and drive_id != "UPLOAD_FAILED":
                        # รูปภาพ → แสดง thumbnail จาก Drive
                        if fname.lower().endswith((".jpg",".jpeg",".png")):
                            thumb_url = get_thumbnail_url(drive_id) if GDRIVE_AVAILABLE else ""
                            if thumb_url:
                                st.image(thumb_url, caption=fname, use_container_width=True)
                            else:
                                st.markdown(f"🖼️ [{fname}]({drive_url})")
                        # PDF → ลิงก์เปิดใน Drive
                        else:
                            st.markdown(
                                f"📄 [{fname}]({drive_url})",
                                help="คลิกเพื่อเปิดไฟล์ใน Google Drive"
                            )
                        if file_size:
                            st.caption(f"ขนาด: {file_size} KB")
                        st.markdown(f"[🔗 เปิดไฟล์]({drive_url})")
                    elif drive_id == "UPLOAD_FAILED":
                        st.error(f"❌ {fname} — อัปโหลดไม่สำเร็จ")
                    else:
                        st.markdown(f"📎 {fname}")

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
        "expense_type","total_amount","status","receipt_files"
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

    st.markdown("#### สรุปแยกตามประเภทค่าใช้จ่าย")
    tp_sum = req_df.groupby("expense_type").agg(
        จำนวน=("request_id","count"),
        ยอดรวม=("total_amount","sum"),
    ).reset_index().sort_values("ยอดรวม", ascending=False)
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
