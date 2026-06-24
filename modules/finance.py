"""
finance.py  –  ระบบการเงินและบัญชี (รอบที่ 7)
"""
import io, datetime
import streamlit as st
import pandas as pd

from config import (
    SHEET_BRANCHES, SHEET_BANK_ACCOUNTS, SHEET_BANK_TRANSACTIONS,
    SHEET_DAILY_SALES_ACCOUNTING, SHEET_BRANCH_EXPENSES,
)
from modules.excel_db import read_sheet, write_sheet, append_row, update_row, init_workbook
from utils.id_generator import next_id

FIN_SCHEMAS = {
    SHEET_BANK_ACCOUNTS: [
        "bank_account_id","bank_name","bank_branch","account_no",
        "account_name","current_balance","is_active",
    ],
    SHEET_BANK_TRANSACTIONS: [
        "transaction_id","transaction_date","bank_account_id",
        "deposit_amount","deposit_detail",
        "withdraw_amount","withdraw_detail",
        "balance_after","remark",
    ],
    SHEET_DAILY_SALES_ACCOUNTING: [
        "accounting_sales_id","sales_date","branch_id",
        "total_sales","created_by","created_at",
    ],
    SHEET_BRANCH_EXPENSES: [
        "expense_id","expense_date","month","year","branch_id",
        "hr_cost","marketing_cost","water_cost","electricity_cost","rent_cost",
        "accounting_cost","transport_cost","mall_gp_cost","lineman_gp_cost",
        "grab_gp_cost","operating_cost","cogs_cost","other_cost","total_expense",
    ],
}

EXPENSE_COST_COLS = [
    ("hr_cost",          "👥 HR / เงินเดือน"),
    ("marketing_cost",   "📢 การตลาด"),
    ("water_cost",       "💧 ค่าน้ำ"),
    ("electricity_cost", "⚡ ค่าไฟ"),
    ("rent_cost",        "🏠 ค่าเช่า"),
    ("accounting_cost",  "📒 ค่าบัญชี"),
    ("transport_cost",   "🚚 ค่าขนส่ง"),
    ("mall_gp_cost",     "🏬 GP Mall"),
    ("lineman_gp_cost",  "🛵 GP Line Man"),
    ("grab_gp_cost",     "🟢 GP Grab"),
    ("operating_cost",   "🔧 Operating"),
    ("cogs_cost",        "📦 COGS"),
    ("other_cost",       "➕ อื่น ๆ"),
]


def _init_fin_sheets():
    init_workbook()
    for sheet_name, columns in FIN_SCHEMAS.items():
        df = read_sheet(sheet_name)
        if df.empty or list(df.columns) != columns:
            write_sheet(sheet_name, pd.DataFrame(columns=columns))


def _branches_dict():
    df = read_sheet(SHEET_BRANCHES)
    return dict(zip(df["branch_id"], df["branch_name"])) if not df.empty else {}


def _get_account_balance(account_id: str) -> float:
    df = read_sheet(SHEET_BANK_ACCOUNTS)
    if df.empty:
        return 0.0
    row = df[df["bank_account_id"].astype(str) == str(account_id)]
    if row.empty:
        return 0.0
    try:
        return float(row.iloc[0]["current_balance"])
    except Exception:
        return 0.0


def _update_account_balance(account_id: str, new_balance: float):
    update_row(SHEET_BANK_ACCOUNTS, "bank_account_id", account_id,
               {"current_balance": round(new_balance, 2)})


# ══════════════════════════════════════════════════════════════════════
def render():
    _init_fin_sheets()
    st.title("💰 Finance & Accounting — การเงินและบัญชี")

    tab1, tab2, tab3, tab4 = st.tabs([
        "🏦 บัญชีธนาคาร",
        "💸 เงินเข้า / เงินออก",
        "📊 ยอดขายฝ่ายบัญชี",
        "📋 ค่าใช้จ่ายสาขา",
    ])
    with tab1: _render_bank_accounts()
    with tab2: _render_transactions()
    with tab3: _render_daily_sales_accounting()
    with tab4: _render_branch_expenses()


# ══════════════════════════════════════════════════════════════════════
# TAB 1 : บัญชีธนาคาร
# ══════════════════════════════════════════════════════════════════════
def _render_bank_accounts():
    st.subheader("🏦 บัญชีธนาคาร")
    df = read_sheet(SHEET_BANK_ACCOUNTS)

    if not df.empty:
        # summary cards
        try:
            df["current_balance"] = pd.to_numeric(df["current_balance"], errors="coerce").fillna(0)
            cols = st.columns(min(len(df), 4))
            for i, (_, r) in enumerate(df.iterrows()):
                cols[i % 4].metric(
                    f"🏦 {r['bank_name']} – {r['account_no'][-4:]}",
                    f"฿{r['current_balance']:,.2f}"
                )
        except Exception:
            pass
        st.dataframe(df, use_container_width=True)

    st.divider()
    with st.form("form_add_bank"):
        st.markdown("#### เพิ่มบัญชีธนาคาร")
        c1, c2 = st.columns(2)
        with c1:
            bank_name    = st.text_input("ชื่อธนาคาร *")
            bank_branch  = st.text_input("สาขาธนาคาร")
            account_no   = st.text_input("เลขที่บัญชี *")
        with c2:
            account_name    = st.text_input("ชื่อบัญชี *")
            current_balance = st.number_input("ยอดเงินเริ่มต้น (บาท)", min_value=0.0, step=100.0)
            is_active       = st.selectbox("สถานะ", ["TRUE","FALSE"])
        saved = st.form_submit_button("💾 บันทึก", type="primary")
    if saved:
        if not bank_name.strip() or not account_no.strip() or not account_name.strip():
            st.error("กรุณากรอกข้อมูลที่จำเป็น")
            return
        df2 = read_sheet(SHEET_BANK_ACCOUNTS)
        ba_id = next_id(df2, "bank_account_id", "BA")
        append_row(SHEET_BANK_ACCOUNTS, {
            "bank_account_id": ba_id, "bank_name": bank_name.strip(),
            "bank_branch": bank_branch, "account_no": account_no.strip(),
            "account_name": account_name.strip(),
            "current_balance": current_balance, "is_active": is_active,
        })
        st.success(f"✅ เพิ่มบัญชี {ba_id} สำเร็จ")
        st.rerun()


# ══════════════════════════════════════════════════════════════════════
# TAB 2 : เงินเข้า / เงินออก
# ══════════════════════════════════════════════════════════════════════
def _render_transactions():
    st.subheader("💸 บันทึกเงินเข้า / เงินออก")
    ba_df = read_sheet(SHEET_BANK_ACCOUNTS)
    if ba_df.empty:
        st.warning("ยังไม่มีบัญชีธนาคาร — เพิ่มบัญชีก่อน")
        return

    active_ba = ba_df[ba_df["is_active"].astype(str)=="TRUE"] if "is_active" in ba_df.columns else ba_df
    ba_opts = dict(zip(active_ba["bank_account_id"],
                       active_ba.apply(lambda r: f"{r['bank_name']} {r['account_no'][-4:]}", axis=1)))

    with st.form("form_transaction"):
        c1, c2 = st.columns(2)
        with c1:
            txn_date    = st.date_input("📅 วันที่", value=datetime.date.today())
            account_id  = st.selectbox("🏦 บัญชี",
                                        list(ba_opts.keys()),
                                        format_func=lambda k: ba_opts[k])
        with c2:
            deposit_amount  = st.number_input("💚 เงินเข้า (บาท)",  min_value=0.0, step=1.0, format="%.2f")
            deposit_detail  = st.text_input("รายละเอียดเงินเข้า")
            withdraw_amount = st.number_input("🔴 เงินออก (บาท)",  min_value=0.0, step=1.0, format="%.2f")
            withdraw_detail = st.text_input("รายละเอียดเงินออก")
        remark = st.text_input("หมายเหตุ")

        prev_balance  = _get_account_balance(account_id)
        balance_after = prev_balance + deposit_amount - withdraw_amount
        st.metric("ยอดหลังรายการ", f"฿{balance_after:,.2f}",
                  delta=f"{deposit_amount - withdraw_amount:+.2f}")

        saved = st.form_submit_button("💾 บันทึกรายการ", type="primary")

    if saved:
        txn_df = read_sheet(SHEET_BANK_TRANSACTIONS)
        txn_id = next_id(txn_df, "transaction_id", "TXN")
        append_row(SHEET_BANK_TRANSACTIONS, {
            "transaction_id":   txn_id,
            "transaction_date": str(txn_date),
            "bank_account_id":  account_id,
            "deposit_amount":   deposit_amount,
            "deposit_detail":   deposit_detail,
            "withdraw_amount":  withdraw_amount,
            "withdraw_detail":  withdraw_detail,
            "balance_after":    round(balance_after, 2),
            "remark":           remark,
        })
        _update_account_balance(account_id, balance_after)
        st.success(f"✅ บันทึก {txn_id} สำเร็จ | ยอดคงเหลือ ฿{balance_after:,.2f}")
        st.rerun()

    # ประวัติ
    st.subheader("📋 ประวัติรายการ")
    txn_df = read_sheet(SHEET_BANK_TRANSACTIONS)
    if not txn_df.empty:
        st.dataframe(txn_df.sort_values("transaction_date", ascending=False)
                     if "transaction_date" in txn_df.columns else txn_df,
                     use_container_width=True)

        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as w:
            txn_df.to_excel(w, index=False, sheet_name="bank_transactions")
        st.download_button("⬇️ Export Excel",
                           data=buf.getvalue(), file_name="bank_transactions.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


# ══════════════════════════════════════════════════════════════════════
# TAB 3 : ยอดขายฝ่ายบัญชี
# ══════════════════════════════════════════════════════════════════════
def _render_daily_sales_accounting():
    st.subheader("📊 บันทึกยอดขายฝ่ายบัญชี")
    branches = _branches_dict()

    with st.form("form_acc_sales"):
        c1, c2, c3 = st.columns(3)
        with c1:
            sales_date = st.date_input("📅 วันที่", value=datetime.date.today())
        with c2:
            branch_id = st.selectbox("🏪 สาขา",
                                      list(branches.keys()) if branches else [""],
                                      format_func=lambda k: f"{k} – {branches.get(k,'')}" if k else "– ไม่มีสาขา –")
        with c3:
            total_sales = st.number_input("💰 ยอดขายรวม (บาท)", min_value=0.0, step=1.0, format="%.2f")
        created_by = st.text_input("👤 บันทึกโดย")
        saved = st.form_submit_button("💾 บันทึก", type="primary")

    if saved:
        ds_df = read_sheet(SHEET_DAILY_SALES_ACCOUNTING)
        ds_id = next_id(ds_df, "accounting_sales_id", "ACS")
        append_row(SHEET_DAILY_SALES_ACCOUNTING, {
            "accounting_sales_id": ds_id,
            "sales_date": str(sales_date),
            "branch_id": branch_id,
            "total_sales": total_sales,
            "created_by": created_by,
            "created_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        })
        st.success(f"✅ บันทึก {ds_id} ยอดขาย ฿{total_sales:,.2f}")
        st.rerun()

    ds_df = read_sheet(SHEET_DAILY_SALES_ACCOUNTING)
    if not ds_df.empty:
        st.dataframe(ds_df, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════
# TAB 4 : ค่าใช้จ่ายสาขา
# ══════════════════════════════════════════════════════════════════════
def _render_branch_expenses():
    st.subheader("📋 บันทึกค่าใช้จ่ายสาขา")
    branches = _branches_dict()

    with st.form("form_expense"):
        c1, c2 = st.columns(2)
        with c1:
            expense_date = st.date_input("📅 วันที่", value=datetime.date.today())
            branch_id    = st.selectbox("🏪 สาขา",
                                         list(branches.keys()) if branches else [""],
                                         format_func=lambda k: f"{k} – {branches.get(k,'')}" if k else "–")
        with c2:
            month = st.selectbox("เดือน", list(range(1,13)),
                                  format_func=lambda m: ["","ม.ค.","ก.พ.","มี.ค.","เม.ย.","พ.ค.","มิ.ย.",
                                                           "ก.ค.","ส.ค.","ก.ย.","ต.ค.","พ.ย.","ธ.ค."][m])
            year  = st.number_input("ปี", min_value=2560, max_value=2580,
                                     value=datetime.date.today().year + 543, step=1)

        st.markdown("#### รายการค่าใช้จ่าย")
        cost_vals = {}
        cols_per_row = 4
        cost_list = list(EXPENSE_COST_COLS)
        rows = [cost_list[i:i+cols_per_row] for i in range(0, len(cost_list), cols_per_row)]
        for row_items in rows:
            cols = st.columns(cols_per_row)
            for idx, (col_key, col_label) in enumerate(row_items):
                with cols[idx]:
                    cost_vals[col_key] = st.number_input(col_label, min_value=0.0,
                                                          step=100.0, format="%.2f",
                                                          key=f"exp_{col_key}")

        total_expense = sum(cost_vals.values())
        st.metric("💰 ค่าใช้จ่ายรวม", f"฿{total_expense:,.2f}")
        saved = st.form_submit_button("💾 บันทึก", type="primary")

    if saved:
        exp_df = read_sheet(SHEET_BRANCH_EXPENSES)
        exp_id = next_id(exp_df, "expense_id", "EXP")
        row_data = {
            "expense_id": exp_id, "expense_date": str(expense_date),
            "month": month, "year": int(year), "branch_id": branch_id,
        }
        row_data.update(cost_vals)
        row_data["total_expense"] = round(total_expense, 2)
        append_row(SHEET_BRANCH_EXPENSES, row_data)
        st.success(f"✅ บันทึก {exp_id} ค่าใช้จ่ายรวม ฿{total_expense:,.2f}")
        st.rerun()

    exp_df = read_sheet(SHEET_BRANCH_EXPENSES)
    if not exp_df.empty:
        st.dataframe(exp_df, use_container_width=True)
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as w:
            exp_df.to_excel(w, index=False, sheet_name="branch_expenses")
        st.download_button("⬇️ Export Excel",
                           data=buf.getvalue(), file_name="branch_expenses.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
