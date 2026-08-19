"""
hr.py  –  ระบบ HR และเงินเดือนพนักงาน (รอบที่ 6)
"""
import io, re, datetime
import streamlit as st
import pandas as pd

from config import (
    SHEET_BRANCHES, SHEET_EMPLOYEES, SHEET_PAYROLL_PERIODS,
    SHEET_PAYROLL_RECORDS, SHEET_LATE_DEDUCTION_RULES,
    EMPLOYEE_STATUSES, POSITIONS,
)
from modules.excel_db import read_sheet, write_sheet, append_row, update_row, delete_row, init_workbook
from utils.id_generator import next_id

# สัญชาติ (ไทย = คนไทย, ที่เหลือ = ต่างด้าว)
NATIONALITIES = ["ไทย", "พม่า", "ลาว", "เวียดนาม", "มาเลเซีย", "จีน", "อื่นๆ"]

HR_SCHEMAS = {
    SHEET_EMPLOYEES: [
        "employee_id","first_name","last_name","nickname","age","birthdate","education",
        "position","salary","branch_id","start_date","resign_date","resign_reason","status",
        "nationality","national_id","passport_no","mou_no",
        "email","phone",
        "bank_name","bank_branch","bank_account_no","bank_account_name","promptpay_no",
    ],
    SHEET_PAYROLL_PERIODS: [
        "payroll_period_id","month","year","period_no",
        "start_date","end_date","pay_date",
    ],
    SHEET_PAYROLL_RECORDS: [
        "payroll_id","payroll_period_id","employee_id",
        "normal_days","normal_rate","double_shift_days","double_shift_rate",
        "holiday_days","holiday_rate","wage_total",
        "diligence_allowance","marketing_share","position_allowance","other_income",
        "leave_days","leave_deduction","late_minutes","late_deduction",
        "other_deduction","gross_income","social_security","mou_deduction","net_income",
        "base_salary","income1","income2","income3","total_income",
    ],
    SHEET_LATE_DEDUCTION_RULES: [
        "rule_id","daily_wage","working_hours","hourly_wage",
        "minute_wage","late_minutes","deduction_amount",
    ],
}


def _init_hr_sheets():
    """Lazy init — สร้าง headers เฉพาะ Sheet ที่ว่างจริงๆ"""
    init_workbook()
    try:
        from gsheets_db import init_sheet_headers
        for sheet_name, columns in HR_SCHEMAS.items():
            init_sheet_headers(sheet_name, columns)
    except ImportError:
        # Local mode
        for sheet_name, columns in HR_SCHEMAS.items():
            df = read_sheet(sheet_name)
            if df.empty or list(df.columns) != columns:
                write_sheet(sheet_name, pd.DataFrame(columns=columns))


def _branches_dict():
    """รายชื่อสาขา — ใช้ branch_auth (20 สาขา) เป็นหลัก + เสริมจากตาราง branches"""
    result = {}
    try:
        from modules.branch_auth import BRANCH_NAMES, BRANCH_LOGIN_SEED
        for b in BRANCH_LOGIN_SEED.keys():
            result[b] = BRANCH_NAMES.get(b, b)
    except Exception:
        pass
    try:
        df = read_sheet(SHEET_BRANCHES)
        if df is not None and not df.empty and "branch_id" in df.columns:
            for _, r in df.iterrows():
                bid = str(r.get("branch_id", "")).strip()
                if bid and bid not in result:
                    result[bid] = str(r.get("branch_name", "")).strip() or bid
    except Exception:
        pass
    return result


def _hr_num(v):
    try:
        return float(str(v).replace(",", "").strip() or 0)
    except Exception:
        return 0.0


# ══════════════════════════════════════════════════════════════════════
def render():
    _init_hr_sheets()
    st.title("👥 HR — ระบบพนักงานและเงินเดือน")

    tab1, tab2, tab3, tab4 = st.tabs([
        "👤 จัดการพนักงาน",
        "📅 รอบจ่ายเงินเดือน",
        "💵 บันทึกเงินเดือนพนักงานสาขา",
        "📤 Export รายงาน",
    ])
    with tab1: _render_employees()
    with tab2: _render_payroll_periods()
    with tab3: _render_payroll_calc()
    with tab4: _render_export()


# ══════════════════════════════════════════════════════════════════════
# TAB 1 : จัดการพนักงาน
# ══════════════════════════════════════════════════════════════════════
def _render_employees():
    st.subheader("👤 จัดการพนักงาน")
    df = read_sheet(SHEET_EMPLOYEES)
    branches = _branches_dict()

    search = st.text_input("🔍 ค้นหาจากชื่อ / รหัส / ตำแหน่ง")
    df_show = df.copy()
    if search and not df.empty:
        mask = df_show.apply(lambda r: search.lower() in " ".join(r.values).lower(), axis=1)
        df_show = df_show[mask]

    if not df_show.empty:
        # เรียงตามรหัสสาขา (branch_id) แล้วตามด้วยชื่อ
        sort_cols = [c for c in ["branch_id", "first_name"] if c in df_show.columns]
        if sort_cols:
            df_show = df_show.sort_values(sort_cols, ignore_index=True)
        st.dataframe(df_show, use_container_width=True)
    else:
        st.info("ยังไม่มีข้อมูลพนักงาน")

    st.divider()
    action = st.radio("การดำเนินการ", ["➕ เพิ่มพนักงาน", "✏️ แก้ไข / ลบพนักงาน"],
                      horizontal=True, key="hr_emp_action")
    if action == "➕ เพิ่มพนักงาน":
        _form_add_employee(branches)
    else:
        _form_edit_employee(df, branches)


def _form_add_employee(branches):
    # ── auto-fill helper ─────────────────────────────────────
    if "add_fname" not in st.session_state:
        st.session_state["add_fname"] = ""
    if "add_lname" not in st.session_state:
        st.session_state["add_lname"] = ""

    st.markdown("#### ➕ เพิ่มพนักงานใหม่")
    st.caption("* = จำเป็นต้องกรอก")
    # ── สัญชาติ (อยู่นอกฟอร์ม เพื่อให้สลับช่องบัตรได้ทันที) ──
    nationality = st.selectbox(
        "🌏 สัญชาติ *", NATIONALITIES, key="add_nationality",
        help="ถ้าเป็นคนไทย จะกรอกเลขบัตรประชาชน / ถ้าเป็นต่างด้าว จะกรอก PASSPORT หรือ MOU")
    is_thai = (nationality == "ไทย")

    with st.form("form_add_emp"):
        # ── ส่วนที่ 1: ข้อมูลส่วนตัว ─────────────────────────
        st.markdown("**👤 ข้อมูลส่วนตัว**")
        c1, c2, c3 = st.columns(3)
        with c1:
            first_name = st.text_input("ชื่อ *", key="add_fn")
            last_name  = st.text_input("นามสกุล *", key="add_ln")
            nickname   = st.text_input("ชื่อเล่น", key="add_nick")
            birthdate  = st.date_input(
                "วันเกิด *",
                min_value=datetime.date(1980, 1, 1),
                max_value=datetime.date.today(),
                value=datetime.date(1990, 1, 1),
                help="ระบบจะคำนวณอายุจากวันเกิดอัตโนมัติ"
            )
            # คำนวณอายุอัตโนมัติจากวันเกิด
            today = datetime.date.today()
            age = today.year - birthdate.year - (
                (today.month, today.day) < (birthdate.month, birthdate.day)
            )
            st.info(f"🎂 อายุ: **{age} ปี** (คำนวณจากวันเกิดอัตโนมัติ)")
        with c2:
            email = st.text_input("e-mail", placeholder="example@email.com")
            phone = st.text_input("เบอร์โทรศัพท์ *",
                                   placeholder="0812345678",
                                   help="กรอกตัวเลขเท่านั้น ไม่ต้องใส่เครื่องหมาย")
            education = st.text_input("การศึกษา")
        with c3:
            position   = st.selectbox("ตำแหน่ง", POSITIONS, key="add_position")
            salary     = st.number_input("เงินเดือน / ค่าแรงรายวัน (บาท)",
                                          min_value=0.0, step=50.0)
            branch_opts = list(branches.keys()) if branches else []
            branch_id   = st.selectbox(
                "สาขา * (รหัส – ชื่อสาขา)",
                [""] + branch_opts,
                format_func=lambda k: f"{k} – {branches.get(k,'')}" if k else "– กรุณาเลือกสาขา –",
                help="เลือกสาขาให้ตรงกับสาขาในระบบเงินสดย่อย",
                key="add_branch_id"
            )
            if branch_id:
                st.caption(f"✅ สาขาที่เลือก: {branch_id} – {branches.get(branch_id,'')}")
            start_date = st.date_input("วันเริ่มงาน")
            status     = st.selectbox("สถานะ", EMPLOYEE_STATUSES, key="add_status")

        st.divider()

        # ── ส่วนบัตร / สัญชาติ ────────────────────────────────
        st.markdown(f"**🪪 ข้อมูลบัตร (สัญชาติ: {nationality})**")
        national_id = passport_no = mou_no = ""
        if is_thai:
            national_id = st.text_input("เลขที่บัตรประชาชน (13 หลัก)", key="add_natid")
        else:
            k1, k2 = st.columns(2)
            with k1:
                passport_no = st.text_input("เลขที่ PASSPORT", key="add_passport")
            with k2:
                mou_no = st.text_input("เลขที่บัตร MOU", key="add_mou")
            st.caption("กรอกอย่างน้อย 1 ช่อง (PASSPORT หรือ MOU)")

        st.divider()

        # ── ส่วนที่ 2: ข้อมูลธนาคาร ──────────────────────────
        st.markdown("**🏦 ข้อมูลธนาคาร**")
        st.caption("ต้องมีอย่างน้อย เลขที่บัญชี หรือ PromptPay")
        b1, b2, b3 = st.columns(3)
        with b1:
            bank_name   = st.text_input("ชื่อธนาคาร")
            bank_branch = st.text_input("สาขาธนาคาร")
        with b2:
            bank_account_no   = st.text_input("เลขที่บัญชี")
            bank_account_name = st.text_input(
                "ชื่อบัญชีธนาคาร *",
                placeholder="ใส่ชื่อ-นามสกุล เจ้าของบัญชี",
                help="โดยทั่วไปตรงกับชื่อพนักงาน"
            )
        with b3:
            promptpay_no = st.text_input(
                "หมายเลข PromptPay (ถ้ามี)",
                placeholder="เบอร์โทร หรือ เลขบัตรประชาชน"
            )

        saved = st.form_submit_button("💾 บันทึกพนักงาน", type="primary",
                                       use_container_width=True)

    if saved:
        # ── Validation ────────────────────────────────────────
        errors = []
        fn = first_name.strip()
        ln = last_name.strip()

        if not fn or not ln:
            errors.append("กรุณากรอกชื่อและนามสกุล")

        # ตรวจพนักงานซ้ำ (ชื่อ+นามสกุล+สาขาเดียวกัน)
        df_check = read_sheet(SHEET_EMPLOYEES)
        if not df_check.empty and not errors:
            dup = df_check[
                (df_check["first_name"].astype(str).str.strip() == fn.strip()) &
                (df_check["last_name"].astype(str).str.strip() == ln.strip()) &
                (df_check["branch_id"].astype(str).str.strip() == str(branch_id).strip()) &
                (df_check["status"].astype(str) != "resigned")
            ]
            if not dup.empty:
                errors.append(
                    f"มีข้อมูลพนักงาน '{fn} {ln}' ของสาขานี้แล้วครับ "
                    f"(ID: {dup.iloc[0]['employee_id']}) — ไม่ต้องเพิ่มซ้ำครับ"
                )
        if not branch_id:
            errors.append("กรุณาเลือกสาขา")
        if not phone.strip():
            errors.append("กรุณากรอกเบอร์โทรศัพท์")
        elif not re.match(r"^[0-9+\-\s]{8,15}$", phone.strip()):
            errors.append("เบอร์โทรศัพท์ต้องเป็นตัวเลข 8-15 หลัก")
        if email.strip() and not re.match(r"^[\w\.\-]+@[\w\.\-]+\.\w{2,}$", email.strip()):
            errors.append("รูปแบบ e-mail ไม่ถูกต้อง เช่น example@email.com")
        # birthdate ถูกจำกัดด้วย min_value/max_value แล้ว
        # ตรวจอายุต้องไม่น้อยกว่า 15 ปี
        if age < 15:
            errors.append("อายุพนักงานต้องไม่น้อยกว่า 15 ปี")
        if not bank_account_no.strip() and not promptpay_no.strip():
            errors.append("ต้องมีอย่างน้อย เลขที่บัญชี หรือ หมายเลข PromptPay")
        if bank_account_no.strip() and not bank_account_name.strip():
            errors.append("กรุณากรอกชื่อบัญชีธนาคาร (bank_account_name)")

        if errors:
            for e in errors:
                st.error(f"❌ {e}")
            return

        # ── auto-fill bank_account_name ถ้าว่าง ──────────────
        final_acc_name = bank_account_name.strip() or f"{fn} {ln}"

        df     = read_sheet(SHEET_EMPLOYEES)
        emp_id = next_id(df, "employee_id", "EMP")
        append_row(SHEET_EMPLOYEES, {
            "employee_id":      emp_id,
            "first_name":       fn,
            "last_name":        ln,
            "nickname":         nickname.strip(),
            "age":              age,
            "birthdate":        str(birthdate),
            "education":        education.strip(),
            "position":         position,
            "salary":           salary,
            "branch_id":        branch_id,
            "start_date":       str(start_date),
            "resign_date":      "",
            "status":           status,
            "nationality":      nationality,
            "national_id":      national_id.strip(),
            "passport_no":      passport_no.strip(),
            "mou_no":           mou_no.strip(),
            "email":            email.strip(),
            "phone":            phone.strip(),
            "bank_name":        bank_name.strip(),
            "bank_branch":      bank_branch.strip(),
            "bank_account_no":  bank_account_no.strip(),
            "bank_account_name": final_acc_name,
            "promptpay_no":     promptpay_no.strip(),
        })
        st.success(
            f"✅ เพิ่มพนักงาน **{fn} {ln}** สำเร็จ (ID: {emp_id}) | "
            f"สาขา: {branches.get(branch_id, branch_id)}"
        )
        st.rerun()


def _form_edit_employee(df, branches):
    if df.empty:
        st.info("ยังไม่มีพนักงาน")
        return
    emp_opts = df["employee_id"].tolist()
    sel = st.selectbox("เลือกพนักงาน", emp_opts,
                       format_func=lambda x: f"{x} – {df[df['employee_id']==x]['first_name'].values[0]} {df[df['employee_id']==x]['last_name'].values[0]}")
    row = df[df["employee_id"] == sel].iloc[0]

    # ── สัญชาติ (นอกฟอร์ม เพื่อสลับช่องบัตรได้ทันที) ──
    _cur_nat = row.get("nationality", "ไทย")
    _nat_idx = NATIONALITIES.index(_cur_nat) if _cur_nat in NATIONALITIES else 0
    nationality = st.selectbox("🌏 สัญชาติ", NATIONALITIES, index=_nat_idx,
                               key=f"edit_nat_{sel}")
    is_thai = (nationality == "ไทย")

    with st.form("form_edit_emp"):
        c1, c2, c3 = st.columns(3)
        with c1:
            first_name = st.text_input("ชื่อ", value=row.get("first_name",""))
            last_name  = st.text_input("นามสกุล", value=row.get("last_name",""))
            nickname   = st.text_input("ชื่อเล่น", value=row.get("nickname",""))
            try:
                bd_val = datetime.date.fromisoformat(str(row.get("birthdate","1990-01-01")))
            except:
                bd_val = datetime.date(1990, 1, 1)
            birthdate = st.date_input(
                "วันเกิด *", value=bd_val,
                min_value=datetime.date(1980, 1, 1),
                max_value=datetime.date.today(),
                help="ระบบจะคำนวณอายุจากวันเกิดอัตโนมัติ"
            )
            # คำนวณอายุอัตโนมัติจากวันเกิด
            today_e = datetime.date.today()
            age = today_e.year - birthdate.year - (
                (today_e.month, today_e.day) < (birthdate.month, birthdate.day)
            )
            st.info(f"🎂 อายุ: **{age} ปี** (คำนวณจากวันเกิดอัตโนมัติ)")
            email = st.text_input("Email", value=row.get("email",""))
            phone = st.text_input("เบอร์โทรศัพท์", value=row.get("phone",""))
        with c2:
            education = st.text_input("การศึกษา", value=row.get("education",""))
            pos_idx   = POSITIONS.index(row.get("position",POSITIONS[0])) if row.get("position") in POSITIONS else 0
            position  = st.selectbox("ตำแหน่ง", POSITIONS, index=pos_idx, key="edit_position")
            try: sal_v = float(row.get("salary",0))
            except: sal_v = 0.0
            salary = st.number_input("เงินเดือน", min_value=0.0, step=50.0, value=sal_v)
            st.markdown("**ข้อมูลธนาคาร**")
            bank_name   = st.text_input("ชื่อธนาคาร",   value=row.get("bank_name",""))
            bank_branch = st.text_input("สาขาธนาคาร", value=row.get("bank_branch",""))
        with c3:
            bank_account_no   = st.text_input("เลขที่บัญชี",     value=row.get("bank_account_no",""))
            bank_account_name = st.text_input("ชื่อบัญชีธนาคาร", value=row.get("bank_account_name",""))
            promptpay_no      = st.text_input("PromptPay",        value=row.get("promptpay_no",""))
            branch_opts = list(branches.keys()) if branches else []
            all_br = [""] + branch_opts
            cur_br = row.get("branch_id","")
            br_idx = all_br.index(cur_br) if cur_br in all_br else 0
            branch_id  = st.selectbox("สาขา", all_br, index=br_idx,
                                       format_func=lambda k: f"{k} – {branches.get(k,'')}" if k else "– ไม่ระบุ –",
                                       key="edit_branch_id")
            st_opts = EMPLOYEE_STATUSES
            st_idx  = st_opts.index(row.get("status","active")) if row.get("status") in st_opts else 0
            status  = st.selectbox("สถานะ", st_opts, index=st_idx, key="edit_status")
            resign_date = st.text_input("วันลาออก (ว่าง = ยังทำงาน)", value=row.get("resign_date",""))
            resign_reason = st.text_input("เหตุผลที่ลาออก", value=row.get("resign_reason",""),
                                          help="กรอกเมื่อพนักงานลาออก")

        # ── ข้อมูลบัตร / สัญชาติ ──
        st.markdown(f"**🪪 ข้อมูลบัตร (สัญชาติ: {nationality})**")
        national_id = passport_no = mou_no = ""
        if is_thai:
            national_id = st.text_input("เลขที่บัตรประชาชน (13 หลัก)",
                                        value=row.get("national_id",""))
        else:
            k1, k2 = st.columns(2)
            with k1:
                passport_no = st.text_input("เลขที่ PASSPORT", value=row.get("passport_no",""))
            with k2:
                mou_no = st.text_input("เลขที่บัตร MOU", value=row.get("mou_no",""))

        cs, cd = st.columns(2)
        with cs: save = st.form_submit_button("💾 บันทึก", type="primary")
        with cd: delete = st.form_submit_button("🗑️ ลบพนักงาน")

    if save:
        update_row(SHEET_EMPLOYEES, "employee_id", sel, {
            "first_name": first_name, "last_name": last_name,
            "nickname": nickname, "age": age,
            "birthdate": str(birthdate), "education": education,
            "position": position, "salary": salary,
            "branch_id": branch_id, "status": status, "resign_date": resign_date,
            "resign_reason": resign_reason,
            "nationality": nationality, "national_id": national_id,
            "passport_no": passport_no, "mou_no": mou_no,
            "email": email, "phone": phone,
            "bank_name": bank_name, "bank_branch": bank_branch,
            "bank_account_no": bank_account_no,
            "bank_account_name": bank_account_name,
            "promptpay_no": promptpay_no,
        })
        st.success("✅ แก้ไขสำเร็จ"); st.rerun()
    if delete:
        delete_row(SHEET_EMPLOYEES, "employee_id", sel)
        st.warning(f"🗑️ ลบ {sel} แล้ว"); st.rerun()


# ══════════════════════════════════════════════════════════════════════
# TAB 2 : รอบจ่ายเงินเดือน
# ══════════════════════════════════════════════════════════════════════
def _render_payroll_periods():
    st.subheader("📅 รอบจ่ายเงินเดือน (เดือนละ 2 รอบ)")

    df = read_sheet(SHEET_PAYROLL_PERIODS)
    if not df.empty:
        st.dataframe(df.sort_values(["year","month","period_no"], ascending=False)
                     if all(c in df.columns for c in ["year","month","period_no"]) else df,
                     use_container_width=True)

    st.divider()
    with st.form("form_payroll_period"):
        st.markdown("#### เพิ่มรอบจ่ายเงินเดือน")
        c1, c2 = st.columns(2)
        with c1:
            month     = st.selectbox("เดือน", list(range(1,13)),
                                     format_func=lambda m: ["","ม.ค.","ก.พ.","มี.ค.","เม.ย.","พ.ค.","มิ.ย.",
                                                              "ก.ค.","ส.ค.","ก.ย.","ต.ค.","พ.ย.","ธ.ค."][m])
            year      = st.number_input("ปี (พ.ศ.)", min_value=2560, max_value=2580,
                                         value=datetime.date.today().year + 543, step=1)
            period_no = st.selectbox("รอบที่", [1, 2],
                                     format_func=lambda p: f"รอบ {p} ({'1-15' if p==1 else '16-สิ้นเดือน'})")
        with c2:
            start_date = st.date_input("วันเริ่มรอบ")
            end_date   = st.date_input("วันสิ้นสุดรอบ")
            pay_date   = st.date_input("วันจ่ายเงิน")
        saved = st.form_submit_button("💾 บันทึกรอบ", type="primary")

    if saved:
        pf_df = read_sheet(SHEET_PAYROLL_PERIODS)
        pp_id = next_id(pf_df, "payroll_period_id", "PP")
        append_row(SHEET_PAYROLL_PERIODS, {
            "payroll_period_id": pp_id, "month": month, "year": int(year),
            "period_no": period_no, "start_date": str(start_date),
            "end_date": str(end_date), "pay_date": str(pay_date),
        })
        st.success(f"✅ เพิ่มรอบ {pp_id} สำเร็จ")
        st.rerun()


# ══════════════════════════════════════════════════════════════════════
# TAB 3 : บันทึกเงินเดือนพนักงานสาขา
# ══════════════════════════════════════════════════════════════════════
def _pp_label(pp_df, k):
    r = pp_df[pp_df["payroll_period_id"].astype(str) == str(k)]
    if r.empty:
        return str(k)
    r = r.iloc[0]
    return (f"เดือน {r.get('month','')}/{r.get('year','')} "
            f"รอบ {r.get('period_no','')} (จ่าย {r.get('pay_date','')})")


def _render_payroll_calc():
    st.subheader("💵 บันทึกเงินเดือนพนักงานสาขา")
    st.caption("เลือกสาขา + รอบจ่าย → เลือกพนักงาน → กรอกเงินเดือนและรายได้ ระบบรวมยอดให้ก่อนบันทึก")

    emp_df = read_sheet(SHEET_EMPLOYEES)
    pp_df  = read_sheet(SHEET_PAYROLL_PERIODS)
    branches = _branches_dict()
    if emp_df.empty:
        st.warning("ยังไม่มีพนักงาน — เพิ่มที่แท็บ 'จัดการพนักงาน' ก่อน")
        return
    if pp_df.empty:
        st.warning("ยังไม่มีรอบจ่ายเงินเดือน — สร้างที่แท็บ 'รอบจ่ายเงินเดือน' ก่อน (ให้มีทั้ง 2 รอบ)")
        return

    # ── ① สาขา + รอบจ่าย (แสดงทั้ง 2 รอบให้เลือก) ──
    c1, c2 = st.columns(2)
    with c1:
        br_ids = sorted(emp_df["branch_id"].astype(str).str.strip().unique().tolist()) \
            if "branch_id" in emp_df.columns else list(branches.keys())
        sel_branch = st.selectbox("🏪 สาขา", br_ids,
                                  format_func=lambda k: f"{k} – {branches.get(k, '')}",
                                  key="sal_branch")
    with c2:
        pp_ids = pp_df["payroll_period_id"].astype(str).tolist()
        sel_pp = st.selectbox("📅 รอบจ่าย", pp_ids,
                              format_func=lambda k: _pp_label(pp_df, k), key="sal_pp")

    # ── ② พนักงานของสาขานั้น ──
    be = emp_df[emp_df["branch_id"].astype(str).str.strip() == str(sel_branch)]
    if "status" in be.columns:
        be = be[be["status"].astype(str).str.lower() != "resigned"]
    if be.empty:
        st.warning("ไม่มีพนักงานของสาขานี้")
        return
    emp_opts = be["employee_id"].astype(str).tolist()
    sel_emp = st.selectbox(
        "👤 ชื่อพนักงาน", emp_opts,
        format_func=lambda k: f"{be[be['employee_id'].astype(str)==k]['first_name'].values[0]} "
                              f"{be[be['employee_id'].astype(str)==k]['last_name'].values[0]}",
        key="sal_emp")
    emp_row = be[be["employee_id"].astype(str) == sel_emp].iloc[0]
    base_default = _hr_num(emp_row.get("salary", 0))

    # ── ③ กรอกเงินเดือน + รายได้ 1/2/3 ──
    with st.form("form_salary"):
        st.markdown("#### กรอกเงินเดือนและรายได้")
        c1, c2 = st.columns(2)
        with c1:
            base_salary = st.number_input("เงินเดือน (บาท)", min_value=0.0, step=100.0,
                                          value=base_default)
            income1 = st.number_input("รายได้ 1 (บาท)", min_value=0.0, step=50.0)
        with c2:
            income2 = st.number_input("รายได้ 2 (บาท)", min_value=0.0, step=50.0)
            income3 = st.number_input("รายได้ 3 (บาท)", min_value=0.0, step=50.0)
        total_income = base_salary + income1 + income2 + income3
        st.markdown(
            f"<div style='background:#E8F5E9;border:2px solid #2E7D32;border-radius:8px;"
            f"padding:12px;text-align:center;'>"
            f"<span style='color:#1B5E20;'>รายได้ทั้งหมด (คำนวณอัตโนมัติ)</span><br>"
            f"<b style='color:#1B5E20;font-size:1.8rem;'>฿{total_income:,.2f}</b></div>",
            unsafe_allow_html=True)
        submitted = st.form_submit_button("💾 ยืนยันบันทึกเงินเดือน", type="primary")

    if submitted:
        pr_df = read_sheet(SHEET_PAYROLL_RECORDS)
        pr_id = next_id(pr_df, "payroll_id", "PAY")
        append_row(SHEET_PAYROLL_RECORDS, {
            "payroll_id": pr_id, "payroll_period_id": sel_pp, "employee_id": sel_emp,
            "base_salary": base_salary, "income1": income1, "income2": income2,
            "income3": income3, "total_income": round(total_income, 2),
            "wage_total": base_salary, "gross_income": round(total_income, 2),
            "net_income": round(total_income, 2),
        })
        st.success(f"✅ บันทึกเงินเดือนสำเร็จ ({pr_id}) | รายได้ทั้งหมด ฿{total_income:,.2f}")
        st.rerun()

    # ── รายงานเงินเดือนทั้งหมด (เลือกรอบ) ──
    st.divider()
    st.markdown("#### 📋 รายงานเงินเดือน (เลือกรอบ)")
    rep_pp = st.selectbox("เลือกรอบเพื่อดูรายงาน", ["ทั้งหมด"] + pp_ids,
                          format_func=lambda k: k if k == "ทั้งหมด" else _pp_label(pp_df, k),
                          key="sal_report_pp")
    pr = read_sheet(SHEET_PAYROLL_RECORDS)
    if pr is None or pr.empty:
        st.info("ยังไม่มีข้อมูลเงินเดือน")
        return
    if rep_pp != "ทั้งหมด":
        pr = pr[pr["payroll_period_id"].astype(str) == rep_pp]
    emap = {}
    for _, e in emp_df.iterrows():
        emap[str(e["employee_id"])] = (str(e.get("first_name", "")) + " " +
                                       str(e.get("last_name", ""))).strip()
    bmap = {}
    for _, e in emp_df.iterrows():
        bmap[str(e["employee_id"])] = str(e.get("branch_id", ""))
    disp = pd.DataFrame({
        "พนักงาน": pr["employee_id"].astype(str).map(emap).fillna(pr["employee_id"]),
        "สาขา": pr["employee_id"].astype(str).map(bmap).map(lambda b: f"{b} – {branches.get(b, '')}"),
        "เงินเดือน": pr.get("base_salary", "").map(lambda x: f"{_hr_num(x):,.2f}"),
        "รายได้ทั้งหมด": pr.get("total_income", pr.get("net_income", "")).map(lambda x: f"{_hr_num(x):,.2f}"),
    })
    st.dataframe(disp, use_container_width=True, hide_index=True)
    st.metric("รวมจ่ายทั้งหมด", f"฿{pr.get('total_income', pr.get('net_income', 0)).map(_hr_num).sum():,.2f}")


# ══════════════════════════════════════════════════════════════════════
# TAB 4 : Export รายงาน
# ══════════════════════════════════════════════════════════════════════
def _render_export():
    st.subheader("📤 Export รายงานเงินเดือน")

    pr_df  = read_sheet(SHEET_PAYROLL_RECORDS)
    emp_df = read_sheet(SHEET_EMPLOYEES)
    pp_df  = read_sheet(SHEET_PAYROLL_PERIODS)

    if pr_df.empty:
        st.info("ยังไม่มีข้อมูลรายได้")
        return

    # เลือกรอบ
    if not pp_df.empty:
        pp_opts = ["ทั้งหมด"] + pp_df["payroll_period_id"].tolist()
        sel_pp  = st.selectbox("เลือกรอบจ่าย", pp_opts)
        if sel_pp != "ทั้งหมด":
            pr_df = pr_df[pr_df["payroll_period_id"].astype(str) == sel_pp]

    # merge ชื่อพนักงาน
    if not emp_df.empty:
        emp_df["full_name"] = emp_df["first_name"] + " " + emp_df["last_name"]
        pr_df = pr_df.merge(emp_df[["employee_id","full_name","position","branch_id"]],
                            on="employee_id", how="left")
        pr_df = pr_df.sort_values("full_name")

    st.dataframe(pr_df, use_container_width=True)

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        pr_df.to_excel(w, index=False, sheet_name="payroll_report")
    st.download_button("⬇️ ดาวน์โหลด Excel",
                       data=buf.getvalue(),
                       file_name="payroll_report.xlsx",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
