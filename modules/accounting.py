"""
accounting.py  –  ระบบการตลาดและ Recheck ยอดขาย (รอบที่ 8)
"""
import datetime
import streamlit as st
import pandas as pd

from config import (
    SHEET_BRANCHES, SHEET_PRODUCTS, SHEET_SALES_CHANNELS,
    SHEET_BRANCH_DAILY_REPORTS,
    SHEET_DAILY_SALES_ACCOUNTING,
    SHEET_MARKETING_DAILY_SALES, SHEET_MARKETING_DAILY_SALES_ITEMS,
    SHEET_SALES_RECONCILE,
)
from modules.excel_db import read_sheet, write_sheet, append_row, init_workbook
from utils.id_generator import next_id

MKT_SCHEMAS = {
    SHEET_MARKETING_DAILY_SALES: [
        "marketing_sales_id","sales_date","branch_id","channel_id",
        "created_by","total_sales","remark",
    ],
    SHEET_MARKETING_DAILY_SALES_ITEMS: [
        "marketing_sales_item_id","marketing_sales_id","product_id",
        "qty_sold","unit_price","total_amount",
    ],
    SHEET_SALES_RECONCILE: [
        "reconcile_id","sales_date","branch_id",
        "branch_report_id","accounting_sales_id","marketing_sales_id",
        "branch_total_sales","accounting_total_sales","marketing_total_sales",
        "diff_branch_accounting","diff_branch_marketing","diff_accounting_marketing",
        "status","remark",
    ],
}


def _init_mkt_sheets():
    init_workbook()
    for sheet_name, columns in MKT_SCHEMAS.items():
        df = read_sheet(sheet_name)
        if df.empty or list(df.columns) != columns:
            write_sheet(sheet_name, pd.DataFrame(columns=columns))


def _branches_dict():
    df = read_sheet(SHEET_BRANCHES)
    return dict(zip(df["branch_id"], df["branch_name"])) if not df.empty else {}


def _channels_dict():
    df = read_sheet(SHEET_SALES_CHANNELS)
    return dict(zip(df["channel_id"], df["channel_name"])) if not df.empty else {}


def _products_dict():
    df = read_sheet(SHEET_PRODUCTS)
    return dict(zip(df["product_id"], df["product_name"])) if not df.empty else {}


# ══════════════════════════════════════════════════════════════════════
def render():
    _init_mkt_sheets()
    st.title("📢 Marketing & Sales Reconcile")
    st.caption("บันทึกยอดขายฝ่ายการตลาด • Reconcile เทียบยอด 3 ฝ่าย")

    tab1, tab2 = st.tabs([
        "📝 บันทึกยอดขายการตลาด",
        "🔍 Reconcile เทียบยอด 3 ฝ่าย",
    ])
    with tab1: _render_marketing_sales()
    with tab2: _render_reconcile()


# ══════════════════════════════════════════════════════════════════════
# TAB 1 : บันทึกยอดขายการตลาด
# ══════════════════════════════════════════════════════════════════════
def _render_marketing_sales():
    st.subheader("📝 บันทึกยอดขายฝ่ายการตลาด")
    branches  = _branches_dict()
    channels  = _channels_dict()
    products  = _products_dict()

    with st.form("form_mkt_sales"):
        st.markdown("#### ข้อมูลหลัก")
        c1, c2, c3 = st.columns(3)
        with c1:
            sales_date = st.date_input("📅 วันที่", value=datetime.date.today())
        with c2:
            branch_id = st.selectbox("🏪 สาขา",
                                      list(branches.keys()) if branches else [""],
                                      format_func=lambda k: f"{k} – {branches.get(k,'')}" if k else "–")
        with c3:
            channel_id = st.selectbox("📡 ช่องทางขาย",
                                       list(channels.keys()) if channels else [""],
                                       format_func=lambda k: f"{k} – {channels.get(k,'')}" if k else "–")
        c1, c2 = st.columns(2)
        with c1: created_by = st.text_input("👤 บันทึกโดย")
        with c2: remark     = st.text_input("📝 หมายเหตุ")

        st.markdown("#### รายการสินค้าที่ขาย")
        num_items = st.number_input("จำนวนรายการ", min_value=1, max_value=20, value=1, step=1)
        item_rows = []
        if products:
            prod_keys = list(products.keys())
            for i in range(int(num_items)):
                c1, c2, c3 = st.columns([3,1,2])
                with c1:
                    sel = st.selectbox(f"สินค้า #{i+1}", prod_keys,
                                       format_func=lambda k: f"{k} – {products[k]}",
                                       key=f"mkt_prod_{i}")
                with c2:
                    qty = st.number_input(f"จำนวน #{i+1}", min_value=0, step=1, key=f"mkt_qty_{i}")
                with c3:
                    price = st.number_input(f"ราคา/ชิ้น #{i+1}", min_value=0.0, step=1.0,
                                             format="%.2f", key=f"mkt_price_{i}")
                item_rows.append((sel, qty, price))
        else:
            st.info("ยังไม่มีสินค้าในระบบ — กรุณาเพิ่มที่ Master Data ก่อน")

        total_sales = sum(q * p for _, q, p in item_rows if q > 0)
        st.metric("💰 ยอดขายรวม", f"฿{total_sales:,.2f}")
        submitted = st.form_submit_button("💾 บันทึก", type="primary")

    if submitted:
        _save_marketing_sales(str(sales_date), branch_id, channel_id,
                               created_by, remark, total_sales, item_rows)


def _save_marketing_sales(sales_date, branch_id, channel_id,
                           created_by, remark, total_sales, item_rows):
    ms_df = read_sheet(SHEET_MARKETING_DAILY_SALES)
    ms_id = next_id(ms_df, "marketing_sales_id", "MKT")
    append_row(SHEET_MARKETING_DAILY_SALES, {
        "marketing_sales_id": ms_id, "sales_date": sales_date,
        "branch_id": branch_id, "channel_id": channel_id,
        "created_by": created_by, "total_sales": total_sales, "remark": remark,
    })
    saved = 0
    for product_id, qty, unit_price in item_rows:
        if qty <= 0:
            continue
        total_amount = qty * unit_price
        mi_df = read_sheet(SHEET_MARKETING_DAILY_SALES_ITEMS)
        mi_id = next_id(mi_df, "marketing_sales_item_id", "MKTI")
        append_row(SHEET_MARKETING_DAILY_SALES_ITEMS, {
            "marketing_sales_item_id": mi_id, "marketing_sales_id": ms_id,
            "product_id": product_id, "qty_sold": qty,
            "unit_price": unit_price, "total_amount": total_amount,
        })
        saved += 1
    st.success(f"✅ บันทึก {ms_id} สำเร็จ | {saved} รายการ | ฿{total_sales:,.2f}")


# ══════════════════════════════════════════════════════════════════════
# TAB 2 : Reconcile เทียบยอด 3 ฝ่าย
# ══════════════════════════════════════════════════════════════════════
def _render_reconcile():
    st.subheader("🔍 Reconcile เทียบยอดขาย 3 ฝ่าย")
    st.caption("สาขา  •  บัญชี  •  การตลาด")

    branches = _branches_dict()
    c1, c2 = st.columns(2)
    with c1:
        recon_date = st.date_input("📅 วันที่ Reconcile", value=datetime.date.today())
    with c2:
        branch_id  = st.selectbox("🏪 สาขา",
                                   list(branches.keys()) if branches else [""],
                                   format_func=lambda k: f"{k} – {branches.get(k,'')}" if k else "–",
                                   key="recon_branch")

    # ── ดึงข้อมูลจาก 3 แหล่ง ──────────────────────────────────────────
    date_str = str(recon_date)

    # 1. สาขา
    branch_rpt_df = read_sheet(SHEET_BRANCH_DAILY_REPORTS)
    branch_total    = 0.0
    branch_rpt_id   = ""
    if not branch_rpt_df.empty:
        mask = ((branch_rpt_df["report_date"].astype(str) == date_str) &
                (branch_rpt_df["branch_id"].astype(str) == str(branch_id)))
        if mask.any():
            row = branch_rpt_df[mask].iloc[0]
            branch_rpt_id = row["branch_report_id"]
            try: branch_total = float(row["total_received"])
            except: pass

    # 2. บัญชี
    acc_df = read_sheet(SHEET_DAILY_SALES_ACCOUNTING)
    acc_total    = 0.0
    acc_sales_id = ""
    if not acc_df.empty:
        mask2 = ((acc_df["sales_date"].astype(str) == date_str) &
                 (acc_df["branch_id"].astype(str) == str(branch_id)))
        if mask2.any():
            row2 = acc_df[mask2].iloc[0]
            acc_sales_id = row2["accounting_sales_id"]
            try: acc_total = float(row2["total_sales"])
            except: pass

    # 3. การตลาด
    mkt_df = read_sheet(SHEET_MARKETING_DAILY_SALES)
    mkt_total    = 0.0
    mkt_sales_id = ""
    if not mkt_df.empty:
        mask3 = ((mkt_df["sales_date"].astype(str) == date_str) &
                 (mkt_df["branch_id"].astype(str) == str(branch_id)))
        if mask3.any():
            row3 = mkt_df[mask3].iloc[0]
            mkt_sales_id = row3["marketing_sales_id"]
            try: mkt_total = float(row3["total_sales"])
            except: pass

    # ── คำนวณ Diff ─────────────────────────────────────────────────────
    diff_ba  = branch_total - acc_total
    diff_bm  = branch_total - mkt_total
    diff_am  = acc_total    - mkt_total

    all_ok = (diff_ba == 0 and diff_bm == 0 and diff_am == 0)
    status = "OK" if all_ok else "DIFF"

    # ── Summary Cards ───────────────────────────────────────────────────
    col1, col2, col3 = st.columns(3)
    col1.metric("🏪 สาขา",    f"฿{branch_total:,.2f}", help=branch_rpt_id)
    col2.metric("📒 บัญชี",   f"฿{acc_total:,.2f}",    help=acc_sales_id)
    col3.metric("📢 การตลาด", f"฿{mkt_total:,.2f}",    help=mkt_sales_id)

    # ── ตาราง DIFF ──────────────────────────────────────────────────────
    st.subheader("📊 ตารางเปรียบเทียบ DIFF")
    _show_diff_table([
        ("สาขา vs บัญชี",   branch_total, acc_total,    diff_ba),
        ("สาขา vs การตลาด", branch_total, mkt_total,   diff_bm),
        ("บัญชี vs การตลาด", acc_total,   mkt_total,   diff_am),
    ])

    # ── banner DIFF ──────────────────────────────────────────────────────
    if all_ok:
        st.success("✅ ยอดตรงทั้ง 3 ฝ่าย!")
    else:
        st.markdown(
            f"""<div style="background:#FF0000;color:white;padding:16px;border-radius:8px;
                font-size:22px;font-weight:bold;text-align:center;">
            ⚠️ DIFF — ยอดขายไม่ตรงกัน! กรุณาตรวจสอบ</div>""",
            unsafe_allow_html=True
        )

    remark_recon = st.text_input("📝 หมายเหตุ Reconcile")
    if st.button("💾 บันทึก Reconcile", type="primary"):
        rc_df = read_sheet(SHEET_SALES_RECONCILE)
        rc_id = next_id(rc_df, "reconcile_id", "RC")
        append_row(SHEET_SALES_RECONCILE, {
            "reconcile_id": rc_id, "sales_date": date_str, "branch_id": branch_id,
            "branch_report_id": branch_rpt_id, "accounting_sales_id": acc_sales_id,
            "marketing_sales_id": mkt_sales_id,
            "branch_total_sales": branch_total, "accounting_total_sales": acc_total,
            "marketing_total_sales": mkt_total,
            "diff_branch_accounting": diff_ba, "diff_branch_marketing": diff_bm,
            "diff_accounting_marketing": diff_am,
            "status": status, "remark": remark_recon,
        })
        st.success(f"✅ บันทึก Reconcile {rc_id} สำเร็จ")
        st.rerun()

    # ── ประวัติ Reconcile ──────────────────────────────────────────────
    st.subheader("📋 ประวัติ Reconcile")
    rc_df = read_sheet(SHEET_SALES_RECONCILE)
    if not rc_df.empty:
        _show_reconcile_history(rc_df)
    else:
        st.info("ยังไม่มีประวัติ Reconcile")


def _show_diff_table(rows):
    header = "<tr>" + "".join(
        f"<th style='padding:8px;background:#1e1e1e;color:white;'>{h}</th>"
        for h in ["เปรียบเทียบ", "ฝ่าย A (บาท)", "ฝ่าย B (บาท)", "DIFF", "สถานะ"]
    ) + "</tr>"
    body = ""
    for label, a, b, diff in rows:
        is_diff = diff != 0
        row_bg  = "#fff3cd" if is_diff else "#d4edda"
        diff_td = (f"<td style='padding:8px;background:#FF0000;color:white;"
                   f"font-size:16px;font-weight:bold;text-align:center;'>DIFF {diff:+,.2f}</td>"
                   if is_diff else
                   f"<td style='padding:8px;color:green;text-align:center;'>0</td>")
        status_td = (
            "<td style='padding:8px;background:#FF0000;color:white;font-weight:bold;text-align:center;'>❌ DIFF</td>"
            if is_diff else
            "<td style='padding:8px;color:green;text-align:center;'>✅ ตรง</td>"
        )
        cells  = f"<td style='padding:8px;background:{row_bg};font-weight:bold;'>{label}</td>"
        cells += f"<td style='padding:8px;background:{row_bg};text-align:right;'>฿{a:,.2f}</td>"
        cells += f"<td style='padding:8px;background:{row_bg};text-align:right;'>฿{b:,.2f}</td>"
        cells += diff_td + status_td
        body   += f"<tr>{cells}</tr>"
    st.markdown(
        f"<table style='border-collapse:collapse;width:100%;font-size:14px;'>"
        f"<thead>{header}</thead><tbody>{body}</tbody></table>",
        unsafe_allow_html=True
    )


def _show_reconcile_history(df: pd.DataFrame):
    header = "<tr>" + "".join(
        f"<th style='padding:6px;background:#1e1e1e;color:white;font-size:12px;'>{h}</th>"
        for h in ["ID","วันที่","สาขา","สาขา(฿)","บัญชี(฿)","การตลาด(฿)","Diff B-Acc","Diff B-Mkt","Diff Acc-Mkt","สถานะ"]
    ) + "</tr>"
    body = ""
    for _, r in df.sort_values("sales_date", ascending=False).iterrows():
        is_diff = str(r.get("status","")) == "DIFF"
        row_bg  = "#fff3cd" if is_diff else "#d4edda"
        status_td = (
            "<td style='padding:6px;background:#FF0000;color:white;font-weight:bold;text-align:center;font-size:12px;'>⚠️ DIFF</td>"
            if is_diff else
            "<td style='padding:6px;color:green;text-align:center;font-size:12px;'>✅ OK</td>"
        )
        def fmt(v):
            try: return f"฿{float(v):,.2f}"
            except: return str(v)
        cells = "".join(
            f"<td style='padding:6px;background:{row_bg};font-size:12px;'>{r.get(c,'')}</td>"
            for c in ["reconcile_id","sales_date","branch_id"]
        )
        cells += "".join(
            f"<td style='padding:6px;background:{row_bg};text-align:right;font-size:12px;'>{fmt(r.get(c,0))}</td>"
            for c in ["branch_total_sales","accounting_total_sales","marketing_total_sales",
                      "diff_branch_accounting","diff_branch_marketing","diff_accounting_marketing"]
        )
        cells += status_td
        body   += f"<tr>{cells}</tr>"
    st.markdown(
        f"<table style='border-collapse:collapse;width:100%;font-size:13px;'>"
        f"<thead>{header}</thead><tbody>{body}</tbody></table>",
        unsafe_allow_html=True
    )
