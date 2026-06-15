"""
production.py  –  ระบบฝ่ายผลิต (รอบที่ 5)
บันทึก Batch การผลิตแป้งสำเร็จ + วัตถุดิบที่ใช้ + stock_movements
"""
import datetime
import streamlit as st
import pandas as pd

from config import (
    SHEET_ITEMS,
    SHEET_PRODUCTION_BATCHES,
    SHEET_PRODUCTION_MATERIAL_USED,
    SHEET_STOCK_MOVEMENTS,
)
from modules.excel_db import read_sheet, write_sheet, append_row, init_workbook
from utils.id_generator import next_id

# ──────────────────────────────────────────────────────────────────────
# item_id คงที่สำหรับแป้งสำเร็จรูป (ใช้ใน stock_movements)
# ──────────────────────────────────────────────────────────────────────
FINISHED_FLOUR_BIG_ID   = "FINISHED_FLOUR_BIG"
FINISHED_FLOUR_SMALL_ID = "FINISHED_FLOUR_SMALL"
INGREDIENT_MIX_BIG_ID   = "INGREDIENT_MIX_BIG"
INGREDIENT_MIX_SMALL_ID = "INGREDIENT_MIX_SMALL"

PRODUCTION_SCHEMAS = {
    SHEET_PRODUCTION_BATCHES: [
        "batch_id", "production_date",
        "finished_flour_big_bag", "finished_flour_small_bag",
        "ingredient_mix_big_bag", "ingredient_mix_small_bag",
        "produced_by", "remark",
    ],
    SHEET_PRODUCTION_MATERIAL_USED: [
        "production_used_id", "batch_id", "item_id",
        "qty_used", "unit", "unit_cost", "total_cost",
    ],
}


def _init_production_sheets():
    init_workbook()
    for sheet_name, columns in PRODUCTION_SCHEMAS.items():
        df = read_sheet(sheet_name)
        if df.empty or list(df.columns) != columns:
            write_sheet(sheet_name, pd.DataFrame(columns=columns))


def _get_items_dict():
    df = read_sheet(SHEET_ITEMS)
    if df.empty:
        return {}
    return dict(zip(df["item_id"], df["item_name"]))


def _append_movement(movement_date, item_id, movement_type,
                     qty_in, qty_out, unit_cost, total_value,
                     reference_type, reference_id, remark=""):
    from modules.purchase import append_movement as _mv
    return _mv(
        movement_date=movement_date, item_id=item_id,
        branch_id="CENTRAL", movement_type=movement_type,
        qty_in=qty_in, qty_out=qty_out,
        unit_cost=unit_cost, total_value=total_value,
        reference_type=reference_type, reference_id=reference_id,
        remark=remark,
    )


# ══════════════════════════════════════════════════════════════════════
# MAIN RENDER
# ══════════════════════════════════════════════════════════════════════
def render():
    _init_production_sheets()
    st.title("🏭 Production — ฝ่ายผลิต")
    st.caption("บันทึก Batch การผลิตแป้งสำเร็จ และวัตถุดิบที่ใช้")

    tab1, tab2 = st.tabs(["📝 บันทึก Batch การผลิต", "📋 ประวัติการผลิต"])
    with tab1:
        _render_production_form()
    with tab2:
        _render_production_history()


# ══════════════════════════════════════════════════════════════════════
# TAB 1 : บันทึก Batch
# ══════════════════════════════════════════════════════════════════════
def _render_production_form():
    st.subheader("📝 บันทึก Batch การผลิต")
    items_dict = _get_items_dict()

    with st.form("form_production"):
        # ── ข้อมูล Batch ──────────────────────────────────────────────
        st.markdown("#### ข้อมูล Batch")
        col1, col2 = st.columns(2)
        with col1:
            production_date = st.date_input("📅 วันที่ผลิต", value=datetime.date.today())
            produced_by     = st.text_input("👤 บันทึกโดย *")
        with col2:
            remark = st.text_input("📝 หมายเหตุ")

        # ── ผลผลิต ────────────────────────────────────────────────────
        st.markdown("#### ผลผลิต (Output)")
        col1, col2 = st.columns(2)
        with col1:
            finished_big   = st.number_input("🥣 แป้งสำเร็จรูป ถุงใหญ่ (ถุง)",     min_value=0, step=1)
            finished_small = st.number_input("🥣 แป้งสำเร็จรูป ถุงเล็ก (ถุง)",     min_value=0, step=1)
        with col2:
            mix_big        = st.number_input("🫙 ส่วนผสม ถุงใหญ่ (ถุง)",           min_value=0, step=1)
            mix_small      = st.number_input("🫙 ส่วนผสม ถุงเล็ก (ถุง)",           min_value=0, step=1)

        total_output = finished_big + finished_small + mix_big + mix_small
        st.metric("รวมผลผลิตทั้งหมด", f"{total_output} ถุง")

        # ── วัตถุดิบที่ใช้ ────────────────────────────────────────────
        st.markdown("#### วัตถุดิบที่ใช้ในการผลิต")
        st.caption("เพิ่มได้สูงสุด 15 รายการ")

        num_mat = st.number_input("จำนวนวัตถุดิบ", min_value=1, max_value=15,
                                   value=3, step=1, key="prod_num_mat")
        mat_rows = []

        if items_dict:
            item_keys = list(items_dict.keys())
            for i in range(int(num_mat)):
                c1, c2, c3, c4 = st.columns([3, 1, 1, 2])
                with c1:
                    sel = st.selectbox(
                        f"วัตถุดิบ #{i+1}", item_keys,
                        format_func=lambda k: f"{k} – {items_dict[k]}",
                        key=f"prod_item_{i}"
                    )
                with c2:
                    qty = st.number_input(f"จำนวน #{i+1}", min_value=0.0,
                                          step=0.5, key=f"prod_qty_{i}")
                with c3:
                    unit = st.text_input(f"หน่วย #{i+1}", value="กก.", key=f"prod_unit_{i}")
                with c4:
                    unit_cost = st.number_input(f"ต้นทุน/หน่วย #{i+1}",
                                                 min_value=0.0, step=0.01,
                                                 format="%.4f", key=f"prod_cost_{i}")
                mat_rows.append((sel, qty, unit, unit_cost))
        else:
            st.info("ยังไม่มี Item ในระบบ — กรุณาเพิ่มที่ Master Data ก่อน")
            mat_rows = []

        total_mat_cost = sum(q * c for _, q, _, c in mat_rows if q > 0)
        st.metric("ต้นทุนวัตถุดิบรวม", f"฿{total_mat_cost:,.2f}")

        submitted = st.form_submit_button("💾 บันทึก Batch การผลิต", type="primary")

    if submitted:
        if not produced_by.strip():
            st.error("กรุณากรอกชื่อผู้บันทึก")
            return
        if total_output == 0:
            st.error("กรุณากรอกจำนวนผลผลิตอย่างน้อย 1 รายการ")
            return
        _save_batch(
            production_date=str(production_date),
            finished_big=finished_big,
            finished_small=finished_small,
            mix_big=mix_big,
            mix_small=mix_small,
            produced_by=produced_by.strip(),
            remark=remark,
            mat_rows=mat_rows,
        )


def _save_batch(production_date, finished_big, finished_small,
                mix_big, mix_small, produced_by, remark, mat_rows):

    # ─ 1. production_batches ─────────────────────────────────────────
    batch_df = read_sheet(SHEET_PRODUCTION_BATCHES)
    batch_id = next_id(batch_df, "batch_id", "BATCH")
    append_row(SHEET_PRODUCTION_BATCHES, {
        "batch_id":                  batch_id,
        "production_date":           production_date,
        "finished_flour_big_bag":    finished_big,
        "finished_flour_small_bag":  finished_small,
        "ingredient_mix_big_bag":    mix_big,
        "ingredient_mix_small_bag":  mix_small,
        "produced_by":               produced_by,
        "remark":                    remark,
    })

    # ─ 2. วัตถุดิบที่ใช้ + movement = used ──────────────────────────
    for item_id, qty_used, unit, unit_cost in mat_rows:
        if qty_used <= 0:
            continue
        total_cost = qty_used * unit_cost
        pu_df = read_sheet(SHEET_PRODUCTION_MATERIAL_USED)
        pu_id = next_id(pu_df, "production_used_id", "PU")
        append_row(SHEET_PRODUCTION_MATERIAL_USED, {
            "production_used_id": pu_id,
            "batch_id":           batch_id,
            "item_id":            item_id,
            "qty_used":           qty_used,
            "unit":               unit,
            "unit_cost":          unit_cost,
            "total_cost":         total_cost,
        })
        # stock movement: used
        _append_movement(
            production_date, item_id, "used",
            0, qty_used, unit_cost, total_cost,
            "production_batch", batch_id,
            remark=f"ผลิต Batch {batch_id}",
        )

    # ─ 3. แป้งสำเร็จที่ผลิตได้ + movement = production_in ───────────
    output_items = [
        (FINISHED_FLOUR_BIG_ID,   finished_big,   "แป้งสำเร็จรูปถุงใหญ่"),
        (FINISHED_FLOUR_SMALL_ID, finished_small, "แป้งสำเร็จรูปถุงเล็ก"),
        (INGREDIENT_MIX_BIG_ID,   mix_big,        "ส่วนผสมถุงใหญ่"),
        (INGREDIENT_MIX_SMALL_ID, mix_small,      "ส่วนผสมถุงเล็ก"),
    ]
    for item_id, qty, label in output_items:
        if qty <= 0:
            continue
        _append_movement(
            production_date, item_id, "production_in",
            qty, 0, 0.0, 0.0,
            "production_batch", batch_id,
            remark=label,
        )

    st.success(
        f"✅ บันทึก Batch สำเร็จ! Batch ID: **{batch_id}** | "
        f"ผลผลิต: {finished_big+finished_small+mix_big+mix_small} ถุง"
    )
    st.balloons()


# ══════════════════════════════════════════════════════════════════════
# TAB 2 : ประวัติการผลิต
# ══════════════════════════════════════════════════════════════════════
def _render_production_history():
    st.subheader("📋 ประวัติ Batch การผลิต")

    batch_df = read_sheet(SHEET_PRODUCTION_BATCHES)
    if batch_df.empty:
        st.info("ยังไม่มีประวัติการผลิต")
        return

    st.dataframe(batch_df, use_container_width=True)

    # สรุปผลผลิตรวม
    st.subheader("📊 สรุปผลผลิตรวม")
    try:
        for col in ["finished_flour_big_bag", "finished_flour_small_bag",
                    "ingredient_mix_big_bag", "ingredient_mix_small_bag"]:
            batch_df[col] = pd.to_numeric(batch_df[col], errors="coerce").fillna(0)
        totals = batch_df[["finished_flour_big_bag", "finished_flour_small_bag",
                            "ingredient_mix_big_bag", "ingredient_mix_small_bag"]].sum()
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("แป้งฯ ถุงใหญ่",   f"{totals['finished_flour_big_bag']:.0f} ถุง")
        col2.metric("แป้งฯ ถุงเล็ก",   f"{totals['finished_flour_small_bag']:.0f} ถุง")
        col3.metric("ส่วนผสม ถุงใหญ่", f"{totals['ingredient_mix_big_bag']:.0f} ถุง")
        col4.metric("ส่วนผสม ถุงเล็ก", f"{totals['ingredient_mix_small_bag']:.0f} ถุง")
    except Exception:
        pass

    # วัตถุดิบที่ใช้
    st.subheader("🧪 รายการวัตถุดิบที่ใช้ทั้งหมด")
    mat_df = read_sheet(SHEET_PRODUCTION_MATERIAL_USED)
    if not mat_df.empty:
        items_dict = _get_items_dict()
        mat_df = mat_df.copy()
        mat_df["item_name"] = mat_df["item_id"].map(items_dict).fillna(mat_df["item_id"])
        st.dataframe(mat_df, use_container_width=True)
    else:
        st.info("ยังไม่มีข้อมูลวัตถุดิบที่ใช้")
