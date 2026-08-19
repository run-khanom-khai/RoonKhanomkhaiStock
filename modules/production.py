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
from modules.excel_db import (
    read_sheet, write_sheet, append_row, update_row, delete_row, init_workbook,
)
from utils.id_generator import next_id


def _pnum(v):
    try:
        return float(str(v).replace(",", "").strip() or 0)
    except Exception:
        return 0.0

# ──────────────────────────────────────────────────────────────────────
# item_id คงที่สำหรับแป้งสำเร็จรูป (ใช้ใน stock_movements)
# ──────────────────────────────────────────────────────────────────────
FINISHED_FLOUR_BIG_ID   = "FINISHED_FLOUR_BIG"
FINISHED_FLOUR_SMALL_ID = "FINISHED_FLOUR_SMALL"
INGREDIENT_MIX_BIG_ID   = "INGREDIENT_MIX_BIG"
INGREDIENT_MIX_SMALL_ID = "INGREDIENT_MIX_SMALL"

# วัตถุดิบที่ใช้ในการผลิต — 3 รายการคงที่ (คำนวณอัตโนมัติจากยอดผลผลิต)
# (item_id, ชื่อ, หน่วย) — ลำดับการแสดงผล: แป้ง → น้ำตาล → เกลือ
FIXED_MATERIALS = [
    ("RAW_FLOUR", "แป้ง",   "กก."),
    ("RAW_SUGAR", "น้ำตาล", "กก."),
    ("RAW_SALT",  "เกลือ",  "กก."),
]
FIXED_MATERIAL_NAMES = {mid: name for mid, name, _u in FIXED_MATERIALS}

# ──────────────────────────────────────────────────────────────────────
# สูตรคำนวณ "วัตถุดิบที่ใช้ไป" ต่อ 1 ถุงผลผลิต (หน่วย: กรัม)
#   - แป้งสำเร็จรูปถุงใหญ่  ใช้ แป้ง 703 กรัม
#   - แป้งสำเร็จรูปถุงเล็ก  ใช้ แป้ง 527 กรัม
#   - ส่วนผสมถุงใหญ่        ใช้ น้ำตาล 1343 กรัม + เกลือ 29.5 กรัม
#   - ส่วนผสมถุงเล็ก        ใช้ น้ำตาล 671.7 กรัม + เกลือ 14.7 กรัม
# ──────────────────────────────────────────────────────────────────────
FLOUR_G_PER_FINISHED_BIG   = 703.0
FLOUR_G_PER_FINISHED_SMALL = 527.0
SUGAR_G_PER_MIX_BIG        = 1343.0
SUGAR_G_PER_MIX_SMALL      = 671.7
SALT_G_PER_MIX_BIG         = 29.5
SALT_G_PER_MIX_SMALL       = 14.7


def calc_materials_used(finished_big, finished_small, mix_big, mix_small):
    """คำนวณวัตถุดิบที่ใช้ไปจากยอดผลผลิต — คืน dict{item_id: กก.}
    (แป้ง/น้ำตาล/เกลือ) โดยแปลงจากกรัมเป็นกิโลกรัม
    """
    fb, fs = float(finished_big or 0), float(finished_small or 0)
    mb, ms = float(mix_big or 0), float(mix_small or 0)
    flour_g = fb * FLOUR_G_PER_FINISHED_BIG + fs * FLOUR_G_PER_FINISHED_SMALL
    sugar_g = mb * SUGAR_G_PER_MIX_BIG      + ms * SUGAR_G_PER_MIX_SMALL
    salt_g  = mb * SALT_G_PER_MIX_BIG       + ms * SALT_G_PER_MIX_SMALL
    return {
        "RAW_FLOUR": round(flour_g / 1000.0, 3),
        "RAW_SUGAR": round(sugar_g / 1000.0, 3),
        "RAW_SALT":  round(salt_g / 1000.0, 3),
    }

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
def _find_batch(date_str):
    """หา Batch ล่าสุดของวันที่นั้น — คืน row (Series) หรือ None"""
    try:
        bdf = read_sheet(SHEET_PRODUCTION_BATCHES)
    except Exception:
        bdf = pd.DataFrame()
    if bdf is None or bdf.empty or "production_date" not in bdf.columns:
        return None
    m = bdf[bdf["production_date"].astype(str).str[:10] == str(date_str)[:10]]
    if m.empty:
        return None
    return m.iloc[-1]


def _render_production_form():
    st.subheader("📝 บันทึก / แก้ไข Batch การผลิต")
    st.caption("เลือกวันที่ผลิต — ถ้ามีข้อมูลของวันนั้นแล้ว ระบบจะดึงขึ้นมาให้แก้ไข")

    # ── เลือกวันที่ผลิต (ถ้ามีข้อมูลเดิมจะดึงมาแก้ไข) ──
    production_date = st.date_input("📅 วันที่ผลิต", value=datetime.date.today(),
                                    key="prod_date")
    dstr = str(production_date)
    ex = _find_batch(dstr)
    is_edit = ex is not None
    sfx = dstr[:10]
    if is_edit:
        st.info(f"✏️ พบข้อมูลการผลิตของวันที่ {dstr} (Batch {ex.get('batch_id','')}) — "
                f"แก้ไขแล้วกดบันทึกเพื่ออัปเดต")

    # ── ข้อมูล Batch ──
    st.markdown("#### ข้อมูล Batch")
    col1, col2 = st.columns(2)
    with col1:
        produced_by = st.text_input("👤 บันทึกโดย *",
                                    value=str(ex.get("produced_by", "")) if is_edit else "",
                                    key=f"prod_by_{sfx}")
    with col2:
        remark = st.text_input("📝 หมายเหตุ",
                               value=str(ex.get("remark", "")) if is_edit else "",
                               key=f"prod_remark_{sfx}")

    # ── ผลผลิต (Output) ──
    st.markdown("#### ผลผลิต (Output)")
    col1, col2 = st.columns(2)
    with col1:
        finished_big   = st.number_input("🥣 แป้งสำเร็จรูป ถุงใหญ่ (ถุง)", min_value=0, step=1,
                                         value=int(_pnum(ex.get("finished_flour_big_bag", 0))) if is_edit else 0,
                                         key=f"prod_fb_{sfx}")
        finished_small = st.number_input("🥣 แป้งสำเร็จรูป ถุงเล็ก (ถุง)", min_value=0, step=1,
                                         value=int(_pnum(ex.get("finished_flour_small_bag", 0))) if is_edit else 0,
                                         key=f"prod_fs_{sfx}")
    with col2:
        mix_big        = st.number_input("🫙 ส่วนผสม ถุงใหญ่ (ถุง)", min_value=0, step=1,
                                         value=int(_pnum(ex.get("ingredient_mix_big_bag", 0))) if is_edit else 0,
                                         key=f"prod_mb_{sfx}")
        mix_small      = st.number_input("🫙 ส่วนผสม ถุงเล็ก (ถุง)", min_value=0, step=1,
                                         value=int(_pnum(ex.get("ingredient_mix_small_bag", 0))) if is_edit else 0,
                                         key=f"prod_ms_{sfx}")

    total_output = finished_big + finished_small + mix_big + mix_small
    st.metric("รวมผลผลิตทั้งหมด", f"{total_output} ถุง")

    # ── วัตถุดิบที่ใช้: คำนวณเงียบ ๆ เพื่อบันทึก (ไม่แสดงผล — เป็นความลับทางการค้า) ──
    used = calc_materials_used(finished_big, finished_small, mix_big, mix_small)
    mat_rows = [(mid, used.get(mid, 0.0), unit, 0.0)
                for mid, _name, unit in FIXED_MATERIALS]

    st.divider()
    save_label = "💾 บันทึกการแก้ไข" if is_edit else "💾 บันทึก Batch การผลิต"
    submitted = st.button(save_label, type="primary",
                          use_container_width=True, key=f"prod_save_{sfx}")

    if submitted:
        if not produced_by.strip():
            st.error("กรุณากรอกชื่อผู้บันทึก")
            return
        if total_output == 0:
            st.error("กรุณากรอกจำนวนผลผลิตอย่างน้อย 1 รายการ")
            return
        _save_batch(
            production_date=dstr,
            finished_big=finished_big, finished_small=finished_small,
            mix_big=mix_big, mix_small=mix_small,
            produced_by=produced_by.strip(), remark=remark, mat_rows=mat_rows,
            batch_id=str(ex.get("batch_id")) if is_edit else None,
        )
        st.rerun()


def _save_batch(production_date, finished_big, finished_small,
                mix_big, mix_small, produced_by, remark, mat_rows, batch_id=None):

    # ─ 1. production_batches (แก้ไข = update, ใหม่ = insert) ─────────
    batch_df = read_sheet(SHEET_PRODUCTION_BATCHES)
    is_edit = bool(batch_id)
    batch_row = {
        "batch_id":                  batch_id or next_id(batch_df, "batch_id", "BATCH"),
        "production_date":           production_date,
        "finished_flour_big_bag":    finished_big,
        "finished_flour_small_bag":  finished_small,
        "ingredient_mix_big_bag":    mix_big,
        "ingredient_mix_small_bag":  mix_small,
        "produced_by":               produced_by,
        "remark":                    remark,
    }
    batch_id = batch_row["batch_id"]
    if is_edit:
        update_row(SHEET_PRODUCTION_BATCHES, "batch_id", batch_id, batch_row)
        # ลบ material_used + movement เดิมของ batch นี้ก่อน แล้วสร้างใหม่
        try:
            pu = read_sheet(SHEET_PRODUCTION_MATERIAL_USED)
            if pu is not None and not pu.empty and "batch_id" in pu.columns:
                for pid in pu[pu["batch_id"].astype(str) == str(batch_id)]["production_used_id"].astype(str):
                    delete_row(SHEET_PRODUCTION_MATERIAL_USED, "production_used_id", pid)
        except Exception:
            pass
        try:
            delete_row(SHEET_STOCK_MOVEMENTS, "reference_id", batch_id)
        except Exception:
            pass
    else:
        append_row(SHEET_PRODUCTION_BATCHES, batch_row)

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
    st.subheader("📋 ประวัติการผลิต")

    batch_df = read_sheet(SHEET_PRODUCTION_BATCHES)
    if batch_df is None or batch_df.empty:
        st.info("ยังไม่มีประวัติการผลิต")
        return

    OUT = [("finished_flour_big_bag", "แป้งสำเร็จรูป ถุงใหญ่"),
           ("finished_flour_small_bag", "แป้งสำเร็จรูป ถุงเล็ก"),
           ("ingredient_mix_big_bag", "ส่วนผสม ถุงใหญ่"),
           ("ingredient_mix_small_bag", "ส่วนผสม ถุงเล็ก")]

    # ── ดูยอดผลิตตามวันที่ ──
    st.markdown("#### 🔎 ดูยอดผลิตตามวันที่")
    sel_date = st.date_input("📅 วันที่ผลิต", value=datetime.date.today(),
                             key="prod_hist_date")
    m = batch_df[batch_df["production_date"].astype(str).str[:10] == str(sel_date)[:10]]
    if m.empty:
        st.caption(f"— ไม่มีการผลิตในวันที่ {sel_date} —")
    else:
        cols = st.columns(4)
        for c, (f, lab) in zip(cols, OUT):
            tot = sum(_pnum(x) for x in m[f].tolist()) if f in m.columns else 0
            c.metric(lab, f"{tot:,.0f} ถุง")
        st.metric("รวมผลผลิตทั้งหมดของวันนี้",
                  f"{sum(sum(_pnum(x) for x in m[f].tolist()) for f,_l in OUT if f in m.columns):,.0f} ถุง")

    st.divider()
    # ── ตารางประวัติ (เฉพาะผลผลิต ไม่แสดงวัตถุดิบ — เป็นความลับทางการค้า) ──
    st.markdown("#### 📊 ประวัติการผลิตทั้งหมด")
    show = pd.DataFrame({
        "Batch": batch_df.get("batch_id", ""),
        "วันที่ผลิต": batch_df.get("production_date", ""),
        "แป้งฯ ถุงใหญ่": batch_df.get("finished_flour_big_bag", "").map(lambda x: f"{_pnum(x):,.0f}"),
        "แป้งฯ ถุงเล็ก": batch_df.get("finished_flour_small_bag", "").map(lambda x: f"{_pnum(x):,.0f}"),
        "ส่วนผสม ถุงใหญ่": batch_df.get("ingredient_mix_big_bag", "").map(lambda x: f"{_pnum(x):,.0f}"),
        "ส่วนผสม ถุงเล็ก": batch_df.get("ingredient_mix_small_bag", "").map(lambda x: f"{_pnum(x):,.0f}"),
        "ผู้บันทึก": batch_df.get("produced_by", ""),
    })
    st.dataframe(show, use_container_width=True, hide_index=True)
