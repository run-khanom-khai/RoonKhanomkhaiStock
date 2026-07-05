"""
hr.py  –  ระบบ HR และเงินเดือนพนักงาน (รอบที่ 6)
"""
import io, datetime
import streamlit as st
import pandas as pd

from config import (
    SHEET_BRANCHES, SHEET_EMPLOYEES, SHEET_PAYROLL_PERIODS,
    SHEET_PAYROLL_RECORDS, SHEET_LATE_DEDUCTION_RULES,
    EMPLOYEE_STATUSES, POSITIONS,
)
from modules.excel_db import read_sheet, write_sheet, append_row, update_row, delete_row, init_workbook
from utils.id_generator import next_id

HR_SCHEMAS = {
    SHEET_EMPLOYEES: [
        "employee_id","first_name","last_name","age","birthdate","education",
        "position","salary","branch_id","start_date","resign_date","status",
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
    ],
    SHEET_LATE_DEDUCTION_RULES: [
        "rule_id","daily_wage","working_hours","hourly_wage",
        "minute_wage","late_minutes","deduction_amount",
    ],
}


def _init_hr_sheets():
    init_workbook()
    for sheet_name, columns in HR_SCHEMAS.items():
        df = read_sheet(sheet_name)
        if df.empty or list(df.columns) != columns:
            write_sheet(sheet_name, pd.DataFrame(columns=columns))


def _branches_dict():
    df = read_sheet(SHEET_BRANCHES)
    if df.empty:
        return {}
    return dict(zip(df["branch_id"], df["branch_name"]))


# ══════════════════════════════════════════════════════════════════════
def render():
    _init_hr_sheets()
    st.title("👥 HR — ระบบพนักงานและเงินเดือน")

    tab1, tab2, tab3, tab4 = st.tabs([
        "👤 จัดการพนักงาน",
        "📅 รอบจ่ายเงินเดือน",
        "💵 คำนวณรายได้",
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
        # เรียงชื่อ ก-ฮ
        df_show = df_show.sort_values("first_name", ignore_index=True)
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

    with st.form("form_add_emp"):
        st.markdown("#### ➕ เพิ่มพนักงานใหม่")
        st.caption("* = จำเป็นต้องกรอก")

        # ── ส่วนที่ 1: ข้อมูลส่วนตัว ─────────────────────────
        st.markdown("**👤 ข้อมูลส่วนตัว**")
        c1, c2, c3 = st.columns(3)
        with c1:
            first_name = st.text_input("ชื่อ *", key="add_fn")
            last_name  = st.text_input("นามสกุล *", key="add_ln")
            age        = st.number_input("อายุ", min_value=15, max_value=70, step=1)
            birthdate  = st.date_input(
                "วันเกิด (date_of_birth)",
                min_value=datetime.date(1980, 1, 1),
                max_value=datetime.date.today(),
                value=datetime.date(1990, 1, 1),
                help="เลือกปีเกิดได้ตั้งแต่ปี 1980 ถึงวันปัจจุบัน"
            )
        with c2:
            email = st.text_input("e-mail", placeholder="example@email.com")
            phone = st.text_input("เบอร์โทรศัพท์ *",
                                   placeholder="0812345678",
                                   help="กรอกตัวเลขเท่านั้น ไม่ต้องใส่เครื่องหมาย")
            education = st.text_input("การศึกษา")
        with c3:
            position   = st.selectbox("ตำแหน่ง", POSITIONS)
            salary     = st.number_input("เงินเดือน / ค่าแรงรายวัน (บาท)",
                                          min_value=0.0, step=50.0)
            branch_opts = list(branches.keys()) if branches else []
            branch_id   = st.selectbox(
                "สาขา *", [""] + branch_opts,
                format_func=lambda k: f"{k} – {branches.get(k,'')}" if k else "– กรุณาเลือกสาขา –"
            )
            start_date = st.date_input("วันเริ่มงาน")
            status     = st.selectbox("สถานะ", EMPLOYEE_STATUSES)

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
        if not branch_id:
            errors.append("กรุณาเลือกสาขา")
        if not phone.strip():
            errors.append("กรุณากรอกเบอร์โทรศัพท์")
        elif not re.match(r"^[0-9+\-\s]{8,15}$", phone.strip()):
            errors.append("เบอร์โทรศัพท์ต้องเป็นตัวเลข 8-15 หลัก")
        if email.strip() and not re.match(r"^[\w\.\-]+@[\w\.\-]+\.\w{2,}$", email.strip()):
            errors.append("รูปแบบ e-mail ไม่ถูกต้อง เช่น example@email.com")
        if birthdate < datetime.date(1980, 1, 1):
            errors.append("วันเกิดต้องไม่ก่อนปี 1980")
        if birthdate > datetime.date.today():
            errors.append("วันเกิดต้องไม่เกินวันที่ปัจจุบัน")
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
            "age":              age,
            "birthdate":        str(birthdate),
            "education":        education.strip(),
            "position":         position,
            "salary":           salary,
            "branch_id":        branch_id,
            "start_date":       str(start_date),
            "resign_date":      "",
            "status":           status,
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

    with st.form("form_edit_emp"):
        c1, c2, c3 = st.columns(3)
        with c1:
            first_name = st.text_input("ชื่อ", value=row.get("first_name",""))
            last_name  = st.text_input("นามสกุล", value=row.get("last_name",""))
            try: age_v = int(float(row.get("age",18)))
            except: age_v = 18
            age = st.number_input("อายุ", min_value=15, max_value=70, step=1, value=age_v)
            try:
                bd_val = datetime.date.fromisoformat(str(row.get("birthdate","1990-01-01")))
            except:
                bd_val = datetime.date(1990, 1, 1)
            birthdate = st.date_input(
                "วันเกิด (date_of_birth)", value=bd_val,
                min_value=datetime.date(1980, 1, 1),
                max_value=datetime.date.today(),
                help="เลือกปีเกิดได้ตั้งแต่ปี 1980 ถึงวันปัจจุบัน"
            )
            email = st.text_input("Email", value=row.get("email",""))
            phone = st.text_input("เบอร์โทรศัพท์", value=row.get("phone",""))
        with c2:
            education = st.text_input("การศึกษา", value=row.get("education",""))
            pos_idx   = POSITIONS.index(row.get("position",POSITIONS[0])) if row.get("position") in POSITIONS else 0
            position  = st.selectbox("ตำแหน่ง", POSITIONS, index=pos_idx)
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
                                       format_func=lambda k: f"{k} – {branches.get(k,'')}" if k else "– ไม่ระบุ –")
            st_opts = EMPLOYEE_STATUSES
            st_idx  = st_opts.index(row.get("status","active")) if row.get("status") in st_opts else 0
            status  = st.selectbox("สถานะ", st_opts, index=st_idx)
            resign_date = st.text_input("วันลาออก (ว่าง = ยังทำงาน)", value=row.get("resign_date",""))

        cs, cd = st.columns(2)
        with cs: save = st.form_submit_button("💾 บันทึก", type="primary")
        with cd: delete = st.form_submit_button("🗑️ ลบพนักงาน")

    if save:
        update_row(SHEET_EMPLOYEES, "employee_id", sel, {
            "first_name": first_name, "last_name": last_name, "age": age,
            "birthdate": str(birthdate), "education": education,
            "position": position, "salary": salary,
            "branch_id": branch_id, "status": status, "resign_date": resign_date,
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
# TAB 3 : คำนวณรายได้
# ══════════════════════════════════════════════════════════════════════
def _render_payroll_calc():
    st.subheader("💵 คำนวณรายได้พนักงาน")

    emp_df = read_sheet(SHEET_EMPLOYEES)
    pp_df  = read_sheet(SHEET_PAYROLL_PERIODS)

    if emp_df.empty:
        st.warning("ยังไม่มีพนักงาน")
        return
    if pp_df.empty:
        st.warning("ยังไม่มีรอบจ่ายเงิน — กรุณาสร้างรอบก่อน")
        return

    c1, c2 = st.columns(2)
    with c1:
        pp_ids = pp_df["payroll_period_id"].tolist()
        sel_pp = st.selectbox("เลือกรอบจ่าย",pp_ids,
                              format_func=lambda k: f"{k} – เดือน {pp_df[pp_df['payroll_period_id']==k]['month'].values[0]}/{pp_df[pp_df['payroll_period_id']==k]['year'].values[0]} รอบ{pp_df[pp_df['payroll_period_id']==k]['period_no'].values[0]}")
    with c2:
        active_emp = emp_df[emp_df["status"].astype(str)=="active"] if "status" in emp_df.columns else emp_df
        if active_emp.empty:
            st.warning("ไม่มีพนักงาน active")
            return
        emp_opts = active_emp["employee_id"].tolist()
        sel_emp  = st.selectbox("เลือกพนักงาน", emp_opts,
                                format_func=lambda k: f"{k} – {active_emp[active_emp['employee_id']==k]['first_name'].values[0]} {active_emp[active_emp['employee_id']==k]['last_name'].values[0]}")

    emp_row = emp_df[emp_df["employee_id"]==sel_emp].iloc[0]
    try: daily_rate = float(emp_row.get("salary", 0))
    except: daily_rate = 0.0

    st.info(f"พนักงาน: {emp_row.get('first_name','')} {emp_row.get('last_name','')} | ตำแหน่ง: {emp_row.get('position','')} | ค่าแรง: ฿{daily_rate:,.2f}/วัน")

    with st.form("form_payroll_calc"):
        st.markdown("#### ① วันทำงาน")
        c1, c2, c3 = st.columns(3)
        with c1:
            normal_days       = st.number_input("วันทำงานปกติ",    min_value=0, step=1)
            normal_rate       = st.number_input("อัตราต่อวัน (ปกติ)",  min_value=0.0, step=50.0, value=daily_rate)
        with c2:
            double_shift_days = st.number_input("วันทำงาน 2 กะ",   min_value=0, step=1)
            double_shift_rate = st.number_input("อัตรา (2 กะ)",     min_value=0.0, step=50.0)
        with c3:
            holiday_days      = st.number_input("วันหยุดที่ทำงาน", min_value=0, step=1)
            holiday_rate      = st.number_input("อัตรา (วันหยุด)", min_value=0.0, step=50.0)

        st.markdown("#### ② เบี้ยต่าง ๆ")
        c1, c2, c3, c4 = st.columns(4)
        with c1: diligence_allowance = st.number_input("เบี้ยขยัน",     min_value=0.0, step=50.0)
        with c2: marketing_share     = st.number_input("ส่วนแบ่งการตลาด", min_value=0.0, step=50.0)
        with c3: position_allowance  = st.number_input("ค่าตำแหน่ง",    min_value=0.0, step=50.0)
        with c4: other_income        = st.number_input("รายได้อื่น ๆ",   min_value=0.0, step=50.0)

        st.markdown("#### ③ หัก")
        c1, c2, c3 = st.columns(3)
        with c1:
            leave_days      = st.number_input("วันลา",             min_value=0, step=1)
        with c2:
            late_minutes    = st.number_input("นาทีมาสาย",         min_value=0, step=1)
        with c3:
            other_deduction = st.number_input("หักอื่น ๆ",         min_value=0.0, step=50.0)
        mou_deduction = st.number_input("MOU / เงินกู้ยืม (หัก)", min_value=0.0, step=50.0)

        # คำนวณ
        wage_total      = (normal_days*normal_rate + double_shift_days*double_shift_rate + holiday_days*holiday_rate)
        leave_deduction = normal_rate * leave_days
        minute_wage     = (normal_rate / 8 / 60) if normal_rate > 0 else 0
        late_deduction  = minute_wage * late_minutes
        gross_income    = (wage_total + diligence_allowance + marketing_share +
                           position_allowance + other_income -
                           leave_deduction - late_deduction - other_deduction)
        social_security = round((gross_income * 0.03) / 2, 2)
        net_income      = gross_income - social_security - mou_deduction

        st.markdown("#### ④ สรุปรายได้")
        c1,c2,c3,c4 = st.columns(4)
        c1.metric("ค่าแรงรวม",    f"฿{wage_total:,.2f}")
        c2.metric("รายได้รวม",    f"฿{gross_income:,.2f}")
        c3.metric("ประกันสังคม",  f"฿{social_security:,.2f}")
        c4.metric("💰 รายได้สุทธิ", f"฿{net_income:,.2f}")

        submitted = st.form_submit_button("💾 บันทึกรายได้", type="primary")

    if submitted:
        pr_df  = read_sheet(SHEET_PAYROLL_RECORDS)
        pr_id  = next_id(pr_df, "payroll_id", "PAY")
        append_row(SHEET_PAYROLL_RECORDS, {
            "payroll_id": pr_id, "payroll_period_id": sel_pp,
            "employee_id": sel_emp,
            "normal_days": normal_days, "normal_rate": normal_rate,
            "double_shift_days": double_shift_days, "double_shift_rate": double_shift_rate,
            "holiday_days": holiday_days, "holiday_rate": holiday_rate,
            "wage_total": round(wage_total,2),
            "diligence_allowance": diligence_allowance,
            "marketing_share": marketing_share,
            "position_allowance": position_allowance,
            "other_income": other_income,
            "leave_days": leave_days, "leave_deduction": round(leave_deduction,2),
            "late_minutes": late_minutes, "late_deduction": round(late_deduction,2),
            "other_deduction": other_deduction,
            "gross_income": round(gross_income,2),
            "social_security": social_security,
            "mou_deduction": mou_deduction,
            "net_income": round(net_income,2),
        })
        st.success(f"✅ บันทึกรายได้ {pr_id} สำเร็จ | รายได้สุทธิ ฿{net_income:,.2f}")


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
