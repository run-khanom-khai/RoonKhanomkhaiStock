"""
purchase.py  –  ระบบ Purchase / Stock (รอบที่ 4)
บันทึกการซื้อเข้า, เบิกของเข้าสาขา, ดู Stock คงเหลือ
"""
import datetime
import streamlit as st
import pandas as pd

from config import (
    SHEET_BRANCHES, SHEET_ITEMS,
    SHEET_PURCHASE_ORDERS, SHEET_PURCHASE_ORDER_ITEMS,
    SHEET_STOCK_IN_TO_BRANCH, SHEET_STOCK_MOVEMENTS,
    PURCHASE_CATEGORIES,
)
from modules.excel_db import read_sheet, write_sheet, append_row, init_workbook
from utils.id_generator import next_id

# ══════════════════════════════════════════════════════════════════════
# SCHEMAS & INIT
# ══════════════════════════════════════════════════════════════════════
PURCHASE_SCHEMAS = {
    SHEET_PURCHASE_ORDERS: [
        "purchase_id", "purchase_date", "supplier_name", "invoice_no",
        "purchase_category", "total_amount", "vat_amount", "grand_total",
        "created_by", "remark",
    ],
    SHEET_PURCHASE_ORDER_ITEMS: [
        "purchase_item_id", "purchase_id", "item_id",
        "qty", "unit_price_inc_vat", "total_value",
    ],
    SHEET_STOCK_IN_TO_BRANCH: [
        "stock_in_id", "stock_in_date", "branch_id", "item_id",
        "qty_in", "unit", "unit_cost", "total_cost", "recorded_by", "remark",
    ],
    SHEET_STOCK_MOVEMENTS: [
        "stock_movement_id", "movement_date", "item_id", "branch_id",
        "movement_type", "qty_in", "qty_out", "unit_cost", "total_value",
        "reference_type", "reference_id", "remark",
    ],
}


def _init_purchase_sheets():
    init_workbook()
    for sheet_name, columns in PURCHASE_SCHEMAS.items():
        df = read_sheet(sheet_name)
        if df.empty or list(df.columns) != columns:
            write_sheet(sheet_name, pd.DataFrame(columns=columns))


def _get_items_dict():
    df = read_sheet(SHEET_ITEMS)
    if df.empty:
        return {}
    return dict(zip(df["item_id"], df["item_name"]))


def _get_branches_dict():
    df = read_sheet(SHEET_BRANCHES)
    if df.empty:
        return {}
    return dict(zip(df["branch_id"], df["branch_name"]))


def append_movement(movement_date, item_id, branch_id, movement_type,
                    qty_in, qty_out, unit_cost, total_value,
                    reference_type, reference_id, remark=""):
    """Public helper ที่ production.py ก็เรียกใช้ได้"""
    mv_df = read_sheet(SHEET_STOCK_MOVEMENTS)
    mv_id = next_id(mv_df, "stock_movement_id", "MV")
    append_row(SHEET_STOCK_MOVEMENTS, {
        "stock_movement_id": mv_id,
        "movement_date":     str(movement_date),
        "item_id":           item_id,
        "branch_id":         branch_id,
        "movement_type":     movement_type,
        "qty_in":            qty_in,
        "qty_out":           qty_out,
        "unit_cost":         unit_cost,
        "total_value":       total_value,
        "reference_type":    reference_type,
        "reference_id":      reference_id,
        "remark":            remark,
    })
    return mv_id


# ══════════════════════════════════════════════════════════════════════
# MAIN RENDER
# ══════════════════════════════════════════════════════════════════════
def render():
    _init_purchase_sheets()
    st.title("🛒 Purchase / Stock")
    st.caption("บันทึกการจัดซื้อ • เบิกของเข้าสาขา • ดู Stock คงเหลือ")

    tab1, tab2, tab3, tab4 = st.tabs([
        "📦 บันทึกการซื้อเข้า",
        "🚛 เบิกของเข้าสาขา",
        "📊 Stock คงเหลือ",
        "📋 ประวัติ Movement",
    ])
    with tab1:
        _render_purchase_form()
    with tab2:
        _render_stock_in_form()
    with tab3:
        _render_stock_balance()
    with tab4:
        _render_movement_history()


# ══════════════════════════════════════════════════════════════════════
# TAB 1 : บันทึกการซื้อเข้า
# ══════════════════════════════════════════════════════════════════════
def _render_purchase_form():
    st.subheader("📦 บันทึกใบสั่งซื้อ (Purchase Order)")
    items_dict = _get_items_dict()
    if not items_dict:
        st.warning("⚠️ ยังไม่มี Item ในระบบ — กรุณาเพิ่มที่ Master Data ก่อน")

    with st.form("form_purchase"):
        col1, col2 = st.columns(2)
        with col1:
            purchase_date     = st.date_input("📅 วันที่ซื้อ", value=datetime.date.today())
            supplier_name     = st.text_input("🏢 ชื่อผู้ขาย / Supplier *")
            invoice_no        = st.text_input("🧾 เลขที่ Invoice")
        with col2:
            purchase_category = st.selectbox("📂 ประเภทการซื้อ", PURCHASE_CATEGORIES)
            created_by        = st.text_input("👤 บันทึกโดย *")
            remark            = st.text_input("📝 หมายเหตุ")

        st.markdown("#### รายการสินค้าที่ซื้อ")
        num_items = st.number_input("จำนวนรายการ", min_value=1, max_value=10,
                                     value=1, step=1, key="po_num_items")
        item_rows = []
        if items_dict:
            item_keys = list(items_dict.keys())
            for i in range(int(num_items)):
                c1, c2, c3 = st.columns([3, 1, 2])
                with c1:
                    sel = st.selectbox(f"สินค้า #{i+1}", item_keys,
                                       format_func=lambda k: f"{k} – {items_dict[k]}",
                                       key=f"po_item_{i}")
                with c2:
                    qty = st.number_input(f"จำนวน #{i+1}", min_value=0.0,
                                          step=1.0, format="%.2f", key=f"po_qty_{i}")
                with c3:
                    price = st.number_input(f"ราคา/หน่วย #{i+1}", min_value=0.0,
                                             step=0.01, format="%.4f", key=f"po_price_{i}")
                item_rows.append((sel, qty, price))

        total_amount = sum(q * p for _, q, p in item_rows)
        vat_amount   = st.number_input("VAT (บาท) — รวม VAT ในราคาแล้วใส่ 0",
                                        min_value=0.0, step=0.01, value=0.0, key="po_vat")
        grand_total  = total_amount + vat_amount

        col1, col2, col3 = st.columns(3)
        col1.metric("ยอดก่อน VAT", f"฿{total_amount:,.2f}")
        col2.metric("VAT",          f"฿{vat_amount:,.2f}")
        col3.metric("ยอดรวม",       f"฿{grand_total:,.2f}")

        submitted = st.form_submit_button("💾 บันทึกใบสั่งซื้อ", type="primary")

    if submitted:
        if not supplier_name.strip():
            st.error("กรุณากรอกชื่อผู้ขาย")
            return
        if not created_by.strip():
            st.error("กรุณากรอกชื่อผู้บันทึก")
            return
        _save_purchase(str(purchase_date), supplier_name.strip(), invoice_no,
                       purchase_category, total_amount, vat_amount, grand_total,
                       created_by.strip(), remark, item_rows)


def _save_purchase(purchase_date, supplier_name, invoice_no, purchase_category,
                   total_amount, vat_amount, grand_total, created_by, remark, item_rows):
    po_df = read_sheet(SHEET_PURCHASE_ORDERS)
    po_id = next_id(po_df, "purchase_id", "PO")
    append_row(SHEET_PURCHASE_ORDERS, {
        "purchase_id": po_id, "purchase_date": purchase_date,
        "supplier_name": supplier_name, "invoice_no": invoice_no,
        "purchase_category": purchase_category, "total_amount": total_amount,
        "vat_amount": vat_amount, "grand_total": grand_total,
        "created_by": created_by, "remark": remark,
    })
    saved = 0
    for item_id, qty, unit_price in item_rows:
        if qty <= 0:
            continue
        total_value = qty * unit_price
        pi_df = read_sheet(SHEET_PURCHASE_ORDER_ITEMS)
        pi_id = next_id(pi_df, "purchase_item_id", "POI")
        append_row(SHEET_PURCHASE_ORDER_ITEMS, {
            "purchase_item_id": pi_id, "purchase_id": po_id,
            "item_id": item_id, "qty": qty,
            "unit_price_inc_vat": unit_price, "total_value": total_value,
        })
        append_movement(purchase_date, item_id, "CENTRAL", "purchase_in",
                        qty, 0, unit_price, total_value,
                        "purchase_order", po_id)
        saved += 1
    st.success(f"✅ บันทึก PO สำเร็จ! ID: **{po_id}** | {saved} รายการ | ฿{grand_total:,.2f}")


# ══════════════════════════════════════════════════════════════════════
# TAB 2 : เบิกของเข้าสาขา
# ══════════════════════════════════════════════════════════════════════
def _render_stock_in_form():
    st.subheader("🚛 เบิกสินค้าเข้าสาขา")
    items_dict    = _get_items_dict()
    branches_dict = _get_branches_dict()
    if not items_dict:
        st.warning("⚠️ ยังไม่มี Item ในระบบ")
        return
    if not branches_dict:
        st.warning("⚠️ ยังไม่มีสาขาในระบบ")
        return

    with st.form("form_stock_in"):
        col1, col2 = st.columns(2)
        with col1:
            stock_in_date = st.date_input("📅 วันที่เบิก", value=datetime.date.today())
            branch_id = st.selectbox("🏪 สาขาปลายทาง *",
                                      list(branches_dict.keys()),
                                      format_func=lambda k: f"{k} – {branches_dict[k]}")
        with col2:
            recorded_by = st.text_input("👤 บันทึกโดย *")
            remark      = st.text_input("📝 หมายเหตุ")

        st.markdown("#### รายการที่เบิก")
        num_items = st.number_input("จำนวนรายการ", min_value=1, max_value=20,
                                     value=1, step=1, key="si_num")
        si_rows   = []
        item_keys = list(items_dict.keys())
        for i in range(int(num_items)):
            c1, c2, c3, c4 = st.columns([3, 1, 1, 2])
            with c1:
                sel = st.selectbox(f"สินค้า #{i+1}", item_keys,
                                   format_func=lambda k: f"{k} – {items_dict[k]}",
                                   key=f"si_item_{i}")
            with c2:
                qty = st.number_input(f"จำนวน #{i+1}", min_value=0.0,
                                       step=1.0, key=f"si_qty_{i}")
            with c3:
                unit = st.text_input(f"หน่วย #{i+1}", value="ชิ้น", key=f"si_unit_{i}")
            with c4:
                unit_cost = st.number_input(f"ต้นทุน/หน่วย #{i+1}", min_value=0.0,
                                             step=0.01, format="%.4f", key=f"si_cost_{i}")
            si_rows.append((sel, qty, unit, unit_cost))

        total_preview = sum(q * c for _, q, _, c in si_rows if q > 0)
        st.metric("มูลค่าเบิกรวม", f"฿{total_preview:,.2f}")
        submitted = st.form_submit_button("💾 บันทึกการเบิก", type="primary")

    if submitted:
        if not recorded_by.strip():
            st.error("กรุณากรอกชื่อผู้บันทึก")
            return
        saved = _save_stock_in(str(stock_in_date), branch_id,
                                recorded_by.strip(), remark, si_rows)
        st.success(f"✅ บันทึกการเบิก {saved} รายการ → สาขา {branch_id}")


def _save_stock_in(stock_in_date, branch_id, recorded_by, remark, si_rows):
    saved = 0
    for item_id, qty, unit, unit_cost in si_rows:
        if qty <= 0:
            continue
        total_cost = qty * unit_cost
        si_df = read_sheet(SHEET_STOCK_IN_TO_BRANCH)
        si_id = next_id(si_df, "stock_in_id", "SI")
        append_row(SHEET_STOCK_IN_TO_BRANCH, {
            "stock_in_id": si_id, "stock_in_date": stock_in_date,
            "branch_id": branch_id, "item_id": item_id,
            "qty_in": qty, "unit": unit, "unit_cost": unit_cost,
            "total_cost": total_cost, "recorded_by": recorded_by, "remark": remark,
        })
        append_movement(stock_in_date, item_id, branch_id, "transfer_in",
                        qty, 0, unit_cost, total_cost, "stock_in_to_branch", si_id)
        append_movement(stock_in_date, item_id, "CENTRAL", "transfer_out",
                        0, qty, unit_cost, total_cost, "stock_in_to_branch", si_id)
        saved += 1
    return saved


# ══════════════════════════════════════════════════════════════════════
# TAB 3 : Stock คงเหลือ
# ══════════════════════════════════════════════════════════════════════
def _render_stock_balance():
    st.subheader("📊 Stock คงเหลือแยกตามสาขา")
    mv_df = read_sheet(SHEET_STOCK_MOVEMENTS)
    if mv_df.empty:
        st.info("ยังไม่มีข้อมูล Stock Movement")
        return

    items_df   = read_sheet(SHEET_ITEMS)
    items_dict = dict(zip(items_df["item_id"], items_df["item_name"])) if not items_df.empty else {}
    min_stock_dict = {}
    if not items_df.empty and "min_stock" in items_df.columns:
        for _, r in items_df.iterrows():
            try:
                min_stock_dict[r["item_id"]] = float(r["min_stock"])
            except Exception:
                min_stock_dict[r["item_id"]] = 0

    all_branches = ["ทั้งหมด"] + sorted(mv_df["branch_id"].dropna().unique().tolist())
    sel_branch   = st.selectbox("🏪 เลือกสาขา", all_branches, key="stock_branch")

    df = mv_df.copy()
    if sel_branch != "ทั้งหมด":
        df = df[df["branch_id"].astype(str) == sel_branch]

    df["qty_in"]  = pd.to_numeric(df["qty_in"],  errors="coerce").fillna(0)
    df["qty_out"] = pd.to_numeric(df["qty_out"], errors="coerce").fillna(0)

    balance = df.groupby("item_id").agg(
        total_in=("qty_in", "sum"), total_out=("qty_out", "sum")
    ).reset_index()
    balance["คงเหลือ"]      = balance["total_in"] - balance["total_out"]
    balance["ชื่อ Item"]    = balance["item_id"].map(items_dict).fillna(balance["item_id"])
    balance["min_stock"]    = balance["item_id"].map(min_stock_dict).fillna(0)
    balance["ต่ำกว่าขั้นต่ำ"] = balance["คงเหลือ"] < balance["min_stock"]

    show_low = st.checkbox("🔴 แสดงเฉพาะรายการต่ำกว่า min_stock", value=False)
    df_show  = balance[balance["ต่ำกว่าขั้นต่ำ"]] if show_low else balance

    if df_show.empty:
        st.success("✅ ไม่มีรายการ")
        return

    html = _build_stock_table(df_show)
    st.markdown(html, unsafe_allow_html=True)

    low_count = int(balance["ต่ำกว่าขั้นต่ำ"].sum())
    if low_count > 0:
        st.markdown(
            f"<div style='background:#FF0000;color:white;padding:10px;border-radius:6px;"
            f"font-size:16px;font-weight:bold;margin-top:10px;'>"
            f"⚠️ มี {low_count} รายการที่ต่ำกว่า min_stock!</div>",
            unsafe_allow_html=True,
        )


def _build_stock_table(df: pd.DataFrame) -> str:
    header = "<tr>" + "".join(
        f"<th style='padding:8px;background:#1e1e1e;color:white;'>{h}</th>"
        for h in ["Item ID", "ชื่อ", "รับเข้า", "จ่ายออก", "คงเหลือ", "ขั้นต่ำ", "สถานะ"]
    ) + "</tr>"
    rows_html = ""
    for _, r in df.iterrows():
        low    = bool(r["ต่ำกว่าขั้นต่ำ"])
        row_bg = "#ffe0e0" if low else "#d4edda"
        qty_td = (f"<td style='padding:8px;background:#FF0000;color:white;"
                  f"font-weight:bold;text-align:center;'>{r['คงเหลือ']:.0f}</td>"
                  if low else
                  f"<td style='padding:8px;color:green;text-align:center;'>{r['คงเหลือ']:.0f}</td>")
        status_td = (
            "<td style='padding:8px;background:#FF0000;color:white;font-weight:bold;text-align:center;'>⚠️ ต่ำ</td>"
            if low else
            "<td style='padding:8px;color:green;text-align:center;'>✅ ปกติ</td>"
        )
        cells  = f"<td style='padding:8px;background:{row_bg};'>{r['item_id']}</td>"
        cells += f"<td style='padding:8px;background:{row_bg};'>{r['ชื่อ Item']}</td>"
        cells += "".join(
            f"<td style='padding:8px;background:{row_bg};text-align:right;'>{r[c]:.0f}</td>"
            for c in ["total_in", "total_out"]
        )
        cells += qty_td
        cells += f"<td style='padding:8px;background:{row_bg};text-align:right;'>{r['min_stock']:.0f}</td>"
        cells += status_td
        rows_html += f"<tr>{cells}</tr>"
    return (f"<table style='border-collapse:collapse;width:100%;font-size:13px;'>"
            f"<thead>{header}</thead><tbody>{rows_html}</tbody></table>")


# ══════════════════════════════════════════════════════════════════════
# TAB 4 : ประวัติ Movement
# ══════════════════════════════════════════════════════════════════════
def _render_movement_history():
    st.subheader("📋 ประวัติ Stock Movements")
    mv_df = read_sheet(SHEET_STOCK_MOVEMENTS)
    if mv_df.empty:
        st.info("ยังไม่มีข้อมูล")
        return

    items_dict = _get_items_dict()
    col1, col2 = st.columns(2)
    with col1:
        mv_types = ["ทั้งหมด"] + sorted(mv_df["movement_type"].dropna().unique().tolist())
        sel_type = st.selectbox("กรองตาม Type", mv_types)
    with col2:
        all_br = ["ทั้งหมด"] + sorted(mv_df["branch_id"].dropna().unique().tolist())
        sel_br = st.selectbox("กรองตามสาขา", all_br, key="mv_branch")

    df_show = mv_df.copy()
    if sel_type != "ทั้งหมด":
        df_show = df_show[df_show["movement_type"].astype(str) == sel_type]
    if sel_br != "ทั้งหมด":
        df_show = df_show[df_show["branch_id"].astype(str) == sel_br]

    df_show = df_show.copy()
    df_show["item_name"] = df_show["item_id"].map(items_dict).fillna(df_show["item_id"])
    st.dataframe(df_show, use_container_width=True)

    st.subheader("🧾 ประวัติใบสั่งซื้อ")
    po_df = read_sheet(SHEET_PURCHASE_ORDERS)
    if not po_df.empty:
        st.dataframe(po_df, use_container_width=True)
    else:
        st.info("ยังไม่มีใบสั่งซื้อ")
