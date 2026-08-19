"""
purchase.py  –  ระบบฝ่ายจัดซื้อ (Purchase)
ผู้พัฒนา: ดร.วรรณ (ดร.อภิวรรณ์ ดำแสงสวัสดิ์)

เมนู (ปรับปรุง 14/8/2026):
  1) บันทึกชื่อวัตถุดิบและบรรจุภัณฑ์ (เพิ่ม/แก้ไข/ลบ) — รหัส run อัตโนมัติ + ยอดขั้นต่ำ (Min)
  2) บันทึกการจัดซื้อ (เพิ่ม/แก้ไข/ลบ)
  3) เบิกของเข้าสาขา (เพิ่ม/แก้ไข/ลบ)
  4) รายงานสต๊อกคงเหลือ — แสดง Min และเตือนสีแดงทั้งบรรทัดเมื่อถึงจุดขั้นต่ำ
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
from modules.excel_db import (
    read_sheet, write_sheet, append_row, update_row, delete_row, init_workbook,
)
from modules.pdf_util import make_table_pdf
from utils.id_generator import next_id

# ══════════════════════════════════════════════════════════════════════
# วัตถุดิบสำเร็จรูป (ผลิตเอง) — item_id ตรงกับที่ production.py เขียน movement
# เพื่อให้ยอดที่ผลิตได้ ไหลเข้าไปแสดงในสต๊อกคงเหลือถูกต้อง
# ══════════════════════════════════════════════════════════════════════
FINISHED_ITEMS_SEED = [
    ("FINISHED_FLOUR_BIG",   "แป้งสำเร็จรูปใหญ่"),
    ("FINISHED_FLOUR_SMALL", "แป้งสำเร็จรูปเล็ก"),
    ("INGREDIENT_MIX_BIG",   "ส่วนผสมถุงใหญ่"),
    ("INGREDIENT_MIX_SMALL", "ส่วนผสมถุงเล็ก"),
]

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

ITEM_SCHEMA = ["item_id", "item_name", "item_category_id", "unit",
               "standard_cost", "selling_cost", "min_stock", "is_active",
               "purchase_category"]


def _init_purchase_sheets():
    init_workbook()
    for sheet_name, columns in PURCHASE_SCHEMAS.items():
        df = read_sheet(sheet_name)
        if df.empty or list(df.columns) != columns:
            try:
                write_sheet(sheet_name, pd.DataFrame(columns=columns))
            except Exception:
                pass


def _num(v):
    try:
        return float(str(v).replace(",", "").strip() or 0)
    except Exception:
        return 0.0


def _get_items_dict():
    df = read_sheet(SHEET_ITEMS)
    if df is None or df.empty or "item_id" not in df.columns:
        return {}
    return dict(zip(df["item_id"].astype(str), df["item_name"].astype(str)))


def _get_branches_dict():
    df = read_sheet(SHEET_BRANCHES)
    if df is None or df.empty or "branch_id" not in df.columns:
        return {}
    return dict(zip(df["branch_id"].astype(str), df["branch_name"].astype(str)))


def append_movement(movement_date, item_id, branch_id, movement_type,
                    qty_in, qty_out, unit_cost, total_value,
                    reference_type, reference_id, remark=""):
    """Public helper ที่ production.py ก็เรียกใช้ได้"""
    mv_df = read_sheet(SHEET_STOCK_MOVEMENTS)
    mv_id = next_id(mv_df, "stock_movement_id", "MV")
    append_row(SHEET_STOCK_MOVEMENTS, {
        "stock_movement_id": mv_id, "movement_date": str(movement_date),
        "item_id": item_id, "branch_id": branch_id, "movement_type": movement_type,
        "qty_in": qty_in, "qty_out": qty_out, "unit_cost": unit_cost,
        "total_value": total_value, "reference_type": reference_type,
        "reference_id": reference_id, "remark": remark,
    })
    return mv_id


# ── เขียนข้อมูล item (รองรับกรณีตาราง items ยังไม่มีคอลัมน์ purchase_category) ──
def _write_item(item_id, fields, is_new):
    try:
        if is_new:
            append_row(SHEET_ITEMS, {"item_id": item_id, **fields})
        else:
            update_row(SHEET_ITEMS, "item_id", item_id, fields)
        return True, ""
    except Exception:
        f2 = {k: v for k, v in fields.items() if k != "purchase_category"}
        try:
            if is_new:
                append_row(SHEET_ITEMS, {"item_id": item_id, **f2})
            else:
                update_row(SHEET_ITEMS, "item_id", item_id, f2)
            return True, "no_cat"
        except Exception as e2:
            return False, str(e2)


def _seed_finished_items():
    """เพิ่มวัตถุดิบสำเร็จรูป 4 รายการ (ถ้ายังไม่มี) เพื่อให้เลือกเบิก/แสดงคงเหลือได้"""
    if st.session_state.get("_seeded_fin_items"):
        return
    try:
        df = read_sheet(SHEET_ITEMS)
    except Exception:
        df = pd.DataFrame()
    existing = (set(df["item_id"].astype(str)) if
                (df is not None and not df.empty and "item_id" in df.columns) else set())
    for iid, name in FINISHED_ITEMS_SEED:
        if iid not in existing:
            _write_item(iid, {
                "item_name": name, "item_category_id": "", "unit": "ถุง",
                "standard_cost": "0", "selling_cost": "0", "min_stock": "0",
                "is_active": "TRUE", "purchase_category": "วัตถุดิบ",
            }, is_new=True)
    st.session_state["_seeded_fin_items"] = True


# ══════════════════════════════════════════════════════════════════════
# MAIN RENDER
# ══════════════════════════════════════════════════════════════════════
def render():
    _init_purchase_sheets()
    _seed_finished_items()
    st.title("🛒 ฝ่ายจัดซื้อ (Purchase)")
    st.caption("จัดการชื่อวัตถุดิบ/บรรจุภัณฑ์ • บันทึกจัดซื้อ • เบิกเข้าสาขา • รายงานสต๊อกคงเหลือ")

    tabs = st.tabs([
        "🧾 ชื่อวัตถุดิบ/บรรจุภัณฑ์",   # บันทึก (เพิ่ม/แก้ไข/ลบ)
        "📦 บันทึกการจัดซื้อ",          # บันทึก
        "🚛 เบิกของเข้าสาขา",          # บันทึก
        "📅 ดูรายการสั่งซื้อ",          # แสดงผล (ตามวันที่)
        "🚚 ดูการเบิกเข้าสาขา (PDF)",   # แสดงผล (ตามวันที่) + PDF
        "📊 รายงานสต๊อกคงเหลือ",       # แสดงผล (เตือน min สีแดง)
        "🔴 วัตถุดิบถึงจุดสั่งซื้อ (PDF)",  # แสดงผล (เฉพาะที่ถึง min) + PDF
    ])
    with tabs[0]:
        _render_items_master()
    with tabs[1]:
        _render_purchase_form()
    with tabs[2]:
        _render_stock_in_form()
    with tabs[3]:
        _render_purchase_view()
    with tabs[4]:
        _render_stock_in_report()
    with tabs[5]:
        _render_stock_balance()
    with tabs[6]:
        _render_low_stock()


# ══════════════════════════════════════════════════════════════════════
# TAB 1 : บันทึกชื่อวัตถุดิบและบรรจุภัณฑ์ (เพิ่ม/แก้ไข/ลบ)
# ══════════════════════════════════════════════════════════════════════
def _render_items_master():
    st.subheader("🧾 บันทึกชื่อวัตถุดิบและบรรจุภัณฑ์")
    st.caption("รหัสระบบสร้างให้อัตโนมัติ • กำหนดยอดขั้นต่ำ (Min) ไว้เตือนเวลาสั่งซื้อ")

    # ── เพิ่มรายการใหม่ ──
    with st.form("form_add_item", clear_on_submit=True):
        st.markdown("#### ➕ เพิ่มรายการใหม่")
        c1, c2, c3 = st.columns([2, 3, 1.3])
        with c1:
            cat = st.selectbox("ประเภทการซื้อ *", PURCHASE_CATEGORIES, key="im_cat")
        with c2:
            name = st.text_input("ชื่อวัตถุดิบ / บรรจุภัณฑ์ *", key="im_name")
        with c3:
            min_stock = st.number_input("ยอดขั้นต่ำ (Min)", min_value=0.0,
                                        step=1.0, key="im_min")
        c4, c5 = st.columns([1, 3])
        with c4:
            unit = st.text_input("หน่วย", value="", key="im_unit")
        add_btn = st.form_submit_button("💾 บันทึกรายการ", type="primary")

    if add_btn:
        if not name.strip():
            st.error("กรุณากรอกชื่อวัตถุดิบ/บรรจุภัณฑ์")
        else:
            df = read_sheet(SHEET_ITEMS)
            new_id = next_id(df, "item_id", "ITM")
            ok, note = _write_item(new_id, {
                "item_name": name.strip(), "item_category_id": "",
                "unit": unit.strip(), "standard_cost": "0", "selling_cost": "0",
                "min_stock": str(int(min_stock)), "is_active": "TRUE",
                "purchase_category": cat,
            }, is_new=True)
            if ok:
                st.success(f"✅ เพิ่มรายการสำเร็จ! รหัส: {new_id} | {name.strip()}"
                           + (" (หมายเหตุ: ตาราง items ยังไม่มีคอลัมน์ purchase_category "
                              "— กรุณารัน roon_new_tables.sql เพื่อเก็บประเภทการซื้อ)"
                              if note == "no_cat" else ""))
                st.rerun()
            else:
                st.error(f"บันทึกไม่สำเร็จ: {note}")

    st.divider()
    st.markdown("#### 📋 รายการทั้งหมด (แก้ไข/ลบ)")
    df = read_sheet(SHEET_ITEMS)
    if df is None or df.empty:
        st.info("ยังไม่มีรายการ")
        return

    # ตัวกรอง
    cats = ["ทั้งหมด"] + PURCHASE_CATEGORIES
    fcat = st.selectbox("กรองตามประเภทการซื้อ", cats, key="im_filter_cat")
    show = df.copy()
    if fcat != "ทั้งหมด" and "purchase_category" in show.columns:
        show = show[show["purchase_category"].astype(str) == fcat]

    st.caption(f"พบ {len(show)} รายการ")
    for _, row in show.iterrows():
        iid = str(row["item_id"])
        nm  = str(row.get("item_name", ""))
        with st.expander(f"🔖 {iid} | {nm}"):
            if st.session_state.get(f"im_edit_{iid}"):
                _render_item_edit(row)
            else:
                pc = str(row.get("purchase_category", "")) or "-"
                mn = _num(row.get("min_stock", 0))
                un = str(row.get("unit", "")) or "-"
                st.write(f"**ประเภทการซื้อ:** {pc}  |  **หน่วย:** {un}  |  "
                         f"**ยอดขั้นต่ำ (Min):** {mn:,.0f}")
                b1, b2 = st.columns(2)
                if b1.button("✏️ แก้ไข", key=f"im_editbtn_{iid}",
                             use_container_width=True):
                    st.session_state[f"im_edit_{iid}"] = True
                    st.rerun()
                if st.session_state.get(f"im_del_{iid}"):
                    st.warning("⚠️ ยืนยันลบรายการนี้?")
                    d1, d2 = st.columns(2)
                    if d1.button("✅ ยืนยันลบ", key=f"im_delyes_{iid}",
                                 type="primary", use_container_width=True):
                        try:
                            delete_row(SHEET_ITEMS, "item_id", iid)
                            st.success("ลบแล้ว")
                        except Exception as e:
                            st.error(f"ลบไม่สำเร็จ: {e}")
                        st.session_state.pop(f"im_del_{iid}", None)
                        st.rerun()
                    if d2.button("ยกเลิก", key=f"im_delno_{iid}",
                                 use_container_width=True):
                        st.session_state.pop(f"im_del_{iid}", None)
                        st.rerun()
                else:
                    if b2.button("🗑️ ลบ", key=f"im_delbtn_{iid}",
                                 use_container_width=True):
                        st.session_state[f"im_del_{iid}"] = True
                        st.rerun()


def _render_item_edit(row):
    iid = str(row["item_id"])
    st.markdown(f"##### ✏️ แก้ไขรายการ {iid}")
    cats = PURCHASE_CATEGORIES
    cur_cat = str(row.get("purchase_category", ""))
    idx = cats.index(cur_cat) if cur_cat in cats else 0
    c1, c2, c3 = st.columns([2, 3, 1.3])
    with c1:
        cat = st.selectbox("ประเภทการซื้อ", cats, index=idx, key=f"im_e_cat_{iid}")
    with c2:
        name = st.text_input("ชื่อ", value=str(row.get("item_name", "")),
                             key=f"im_e_name_{iid}")
    with c3:
        min_stock = st.number_input("ยอดขั้นต่ำ (Min)", min_value=0.0, step=1.0,
                                    value=_num(row.get("min_stock", 0)),
                                    key=f"im_e_min_{iid}")
    unit = st.text_input("หน่วย", value=str(row.get("unit", "")), key=f"im_e_unit_{iid}")

    b1, b2 = st.columns(2)
    if b1.button("💾 บันทึกการแก้ไข", type="primary", use_container_width=True,
                 key=f"im_e_save_{iid}"):
        if not name.strip():
            st.error("กรุณากรอกชื่อ")
        else:
            ok, note = _write_item(iid, {
                "item_name": name.strip(), "unit": unit.strip(),
                "min_stock": str(int(min_stock)), "purchase_category": cat,
            }, is_new=False)
            if ok:
                st.session_state.pop(f"im_edit_{iid}", None)
                st.success("✅ แก้ไขสำเร็จ")
                st.rerun()
            else:
                st.error(f"แก้ไขไม่สำเร็จ: {note}")
    if b2.button("ยกเลิก", use_container_width=True, key=f"im_e_cancel_{iid}"):
        st.session_state.pop(f"im_edit_{iid}", None)
        st.rerun()


# ══════════════════════════════════════════════════════════════════════
# TAB 2 : บันทึกการจัดซื้อ (เพิ่ม/แก้ไข/ลบ)
# ══════════════════════════════════════════════════════════════════════
def _render_purchase_form():
    st.subheader("📦 บันทึกใบสั่งซื้อ (Purchase Order)")
    items_dict = _get_items_dict()
    if not items_dict:
        st.warning("⚠️ ยังไม่มีรายการวัตถุดิบ/บรรจุภัณฑ์ — เพิ่มที่เมนู 'ชื่อวัตถุดิบ/บรรจุภัณฑ์' ก่อน")

    with st.form("form_purchase"):
        col1, col2 = st.columns(2)
        with col1:
            purchase_date = st.date_input("📅 วันที่ซื้อ", value=datetime.date.today())
            supplier_name = st.text_input("🏢 ชื่อผู้ขาย / Supplier *")
            invoice_no    = st.text_input("🧾 เลขที่ Invoice")
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

        line_total = sum(q * p for _, q, p in item_rows)
        st.caption(f"ยอดรวมรายการสินค้า (อ้างอิงจากจำนวน×ราคา): ฿{line_total:,.2f}")
        grand_total = st.number_input(
            "💰 ยอดเงินซื้อ (รวม VAT) (บาท)", min_value=0.0, step=0.01,
            value=float(line_total), key="po_grand_total",
            help="ยอดเงินที่จ่ายจริงตามใบเสร็จ (รวมภาษีมูลค่าเพิ่มแล้ว)")

        submitted = st.form_submit_button("💾 บันทึกใบสั่งซื้อ", type="primary")

    if submitted:
        if not supplier_name.strip():
            st.error("กรุณากรอกชื่อผู้ขาย")
            return
        if not created_by.strip():
            st.error("กรุณากรอกชื่อผู้บันทึก")
            return
        _save_purchase(str(purchase_date), supplier_name.strip(), invoice_no,
                       purchase_category, line_total, "", grand_total,
                       created_by.strip(), remark, item_rows)
        st.rerun()

    st.divider()
    _render_purchase_manage(items_dict)


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
                        qty, 0, unit_price, total_value, "purchase_order", po_id)
        saved += 1
    st.success(f"✅ บันทึก PO สำเร็จ! ID: {po_id} | {saved} รายการ | ฿{grand_total:,.2f}")


def _render_purchase_manage(items_dict):
    st.markdown("#### 📋 ใบสั่งซื้อล่าสุด (แก้ไข/ลบ)")
    po_df = read_sheet(SHEET_PURCHASE_ORDERS)
    if po_df is None or po_df.empty:
        st.info("ยังไม่มีใบสั่งซื้อ")
        return
    po_df = po_df.sort_values("purchase_id", ascending=False).head(20)
    poi_df = read_sheet(SHEET_PURCHASE_ORDER_ITEMS)

    for _, row in po_df.iterrows():
        pid = str(row["purchase_id"])
        title = (f"📦 {pid} | {row.get('purchase_date','')} | "
                 f"{row.get('supplier_name','')} | ฿{_num(row.get('grand_total',0)):,.2f}")
        with st.expander(title):
            # รายการสินค้าในใบนี้
            if poi_df is not None and not poi_df.empty and "purchase_id" in poi_df.columns:
                lines = poi_df[poi_df["purchase_id"].astype(str) == pid]
                if not lines.empty:
                    disp = pd.DataFrame({
                        "สินค้า": lines["item_id"].map(items_dict).fillna(lines["item_id"]),
                        "จำนวน": lines["qty"].map(_num),
                        "ราคา/หน่วย": lines["unit_price_inc_vat"].map(lambda x: f"{_num(x):,.2f}"),
                        "มูลค่า": lines["total_value"].map(lambda x: f"{_num(x):,.2f}"),
                    })
                    st.dataframe(disp, use_container_width=True, hide_index=True)

            if st.session_state.get(f"po_edit_{pid}"):
                _render_purchase_edit(row)
            else:
                b1, b2 = st.columns(2)
                if b1.button("✏️ แก้ไขหัวบิล", key=f"po_editbtn_{pid}",
                             use_container_width=True):
                    st.session_state[f"po_edit_{pid}"] = True
                    st.rerun()
                if st.session_state.get(f"po_del_{pid}"):
                    st.warning("⚠️ ยืนยันลบใบสั่งซื้อนี้ (รวมรายการและ movement)?")
                    d1, d2 = st.columns(2)
                    if d1.button("✅ ยืนยันลบ", key=f"po_delyes_{pid}",
                                 type="primary", use_container_width=True):
                        _delete_purchase(pid)
                        st.session_state.pop(f"po_del_{pid}", None)
                        st.success("ลบแล้ว")
                        st.rerun()
                    if d2.button("ยกเลิก", key=f"po_delno_{pid}",
                                 use_container_width=True):
                        st.session_state.pop(f"po_del_{pid}", None)
                        st.rerun()
                else:
                    if b2.button("🗑️ ลบทั้งใบ", key=f"po_delbtn_{pid}",
                                 use_container_width=True):
                        st.session_state[f"po_del_{pid}"] = True
                        st.rerun()


def _render_purchase_edit(row):
    pid = str(row["purchase_id"])
    st.markdown("##### ✏️ แก้ไขหัวใบสั่งซื้อ")
    c1, c2 = st.columns(2)
    with c1:
        supplier = st.text_input("ผู้ขาย", value=str(row.get("supplier_name", "")),
                                 key=f"po_e_sup_{pid}")
        invoice  = st.text_input("เลขที่ Invoice", value=str(row.get("invoice_no", "")),
                                 key=f"po_e_inv_{pid}")
    with c2:
        cats = PURCHASE_CATEGORIES
        cur  = str(row.get("purchase_category", ""))
        cidx = cats.index(cur) if cur in cats else 0
        cat  = st.selectbox("ประเภทการซื้อ", cats, index=cidx, key=f"po_e_cat_{pid}")
        grand = st.number_input("ยอดเงินซื้อ (รวม VAT)", min_value=0.0, step=0.01,
                                value=_num(row.get("grand_total", 0)), key=f"po_e_gt_{pid}")
    remark = st.text_input("หมายเหตุ", value=str(row.get("remark", "")),
                           key=f"po_e_rm_{pid}")
    b1, b2 = st.columns(2)
    if b1.button("💾 บันทึก", type="primary", use_container_width=True,
                 key=f"po_e_save_{pid}"):
        update_row(SHEET_PURCHASE_ORDERS, "purchase_id", pid, {
            "supplier_name": supplier.strip(), "invoice_no": invoice.strip(),
            "purchase_category": cat, "grand_total": grand, "remark": remark,
        })
        st.session_state.pop(f"po_edit_{pid}", None)
        st.success("✅ แก้ไขสำเร็จ")
        st.rerun()
    if b2.button("ยกเลิก", use_container_width=True, key=f"po_e_cancel_{pid}"):
        st.session_state.pop(f"po_edit_{pid}", None)
        st.rerun()


def _delete_purchase(pid):
    try:
        delete_row(SHEET_PURCHASE_ORDER_ITEMS, "purchase_id", pid)
    except Exception:
        pass
    try:
        delete_row(SHEET_STOCK_MOVEMENTS, "reference_id", pid)
    except Exception:
        pass
    try:
        delete_row(SHEET_PURCHASE_ORDERS, "purchase_id", pid)
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════════════
# TAB 3 : เบิกของเข้าสาขา (เพิ่ม/แก้ไข/ลบ)
# ══════════════════════════════════════════════════════════════════════
def _render_stock_in_form():
    st.subheader("🚛 เบิกสินค้าเข้าสาขา")
    items_dict    = _get_items_dict()
    branches_dict = _get_branches_dict()
    if not items_dict:
        st.warning("⚠️ ยังไม่มีรายการวัตถุดิบ/บรรจุภัณฑ์")
        return
    if not branches_dict:
        st.warning("⚠️ ยังไม่มีสาขาในระบบ")
        return

    with st.form("form_stock_in"):
        col1, col2 = st.columns(2)
        with col1:
            stock_in_date = st.date_input("📅 วันที่เบิก", value=datetime.date.today())
            branch_id = st.selectbox("🏪 สาขาปลายทาง *", list(branches_dict.keys()),
                                     format_func=lambda k: f"{k} – {branches_dict[k]}")
        with col2:
            recorded_by = st.text_input("👤 บันทึกโดย *")
            remark      = st.text_input("📝 หมายเหตุ")

        st.markdown("#### รายการที่เบิก (วัตถุดิบ / บรรจุภัณฑ์ / แป้งสำเร็จรูป-ส่วนผสม / อื่นๆ)")
        num_items = st.number_input("จำนวนรายการ", min_value=1, max_value=20,
                                     value=1, step=1, key="si_num")
        si_rows   = []
        item_keys = list(items_dict.keys())
        for i in range(int(num_items)):
            c1, c2, c3 = st.columns([3, 1, 2])
            with c1:
                sel = st.selectbox(f"ชื่อวัตถุดิบ #{i+1}", item_keys,
                                   format_func=lambda k: f"{k} – {items_dict[k]}",
                                   key=f"si_item_{i}")
            with c2:
                qty = st.number_input(f"จำนวน #{i+1}", min_value=0.0,
                                       step=1.0, key=f"si_qty_{i}")
            with c3:
                unit_cost = st.number_input(f"ต้นทุน/หน่วย #{i+1}", min_value=0.0,
                                             step=0.01, format="%.4f", key=f"si_cost_{i}")
            si_rows.append((sel, qty, "", unit_cost))

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
        st.rerun()

    st.divider()
    _render_stock_in_manage(items_dict, branches_dict)


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
        _rebuild_stockin_movements(si_id, stock_in_date, item_id, branch_id,
                                   qty, unit_cost)
        saved += 1
    return saved


def _rebuild_stockin_movements(si_id, date, item_id, branch_id, qty, unit_cost):
    """ลบ movement เดิมของ si นี้ แล้วสร้างใหม่ (transfer_in ที่สาขา + transfer_out ที่ CENTRAL)"""
    try:
        delete_row(SHEET_STOCK_MOVEMENTS, "reference_id", si_id)
    except Exception:
        pass
    total_cost = qty * unit_cost
    append_movement(date, item_id, branch_id, "transfer_in",
                    qty, 0, unit_cost, total_cost, "stock_in_to_branch", si_id)
    append_movement(date, item_id, "CENTRAL", "transfer_out",
                    0, qty, unit_cost, total_cost, "stock_in_to_branch", si_id)


def _render_stock_in_manage(items_dict, branches_dict):
    st.markdown("#### 📋 รายการเบิกล่าสุด (แก้ไข/ลบ)")
    si_df = read_sheet(SHEET_STOCK_IN_TO_BRANCH)
    if si_df is None or si_df.empty:
        st.info("ยังไม่มีรายการเบิก")
        return
    si_df = si_df.sort_values("stock_in_id", ascending=False).head(30)
    for _, row in si_df.iterrows():
        sid = str(row["stock_in_id"])
        nm  = items_dict.get(str(row.get("item_id", "")), str(row.get("item_id", "")))
        br  = str(row.get("branch_id", ""))
        title = (f"🚛 {sid} | {row.get('stock_in_date','')} | สาขา {br} | "
                 f"{nm} × {_num(row.get('qty_in',0)):,.0f}")
        with st.expander(title):
            if st.session_state.get(f"si_edit_{sid}"):
                _render_stock_in_edit(row, items_dict)
            else:
                st.write(f"**สินค้า:** {nm}  |  **จำนวน:** {_num(row.get('qty_in',0)):,.2f}  |  "
                         f"**ต้นทุน/หน่วย:** {_num(row.get('unit_cost',0)):,.2f}  |  "
                         f"**มูลค่า:** {_num(row.get('total_cost',0)):,.2f}")
                b1, b2 = st.columns(2)
                if b1.button("✏️ แก้ไข", key=f"si_editbtn_{sid}",
                             use_container_width=True):
                    st.session_state[f"si_edit_{sid}"] = True
                    st.rerun()
                if st.session_state.get(f"si_del_{sid}"):
                    st.warning("⚠️ ยืนยันลบรายการเบิกนี้?")
                    d1, d2 = st.columns(2)
                    if d1.button("✅ ยืนยันลบ", key=f"si_delyes_{sid}",
                                 type="primary", use_container_width=True):
                        try:
                            delete_row(SHEET_STOCK_MOVEMENTS, "reference_id", sid)
                        except Exception:
                            pass
                        try:
                            delete_row(SHEET_STOCK_IN_TO_BRANCH, "stock_in_id", sid)
                        except Exception:
                            pass
                        st.session_state.pop(f"si_del_{sid}", None)
                        st.success("ลบแล้ว")
                        st.rerun()
                    if d2.button("ยกเลิก", key=f"si_delno_{sid}",
                                 use_container_width=True):
                        st.session_state.pop(f"si_del_{sid}", None)
                        st.rerun()
                else:
                    if b2.button("🗑️ ลบ", key=f"si_delbtn_{sid}",
                                 use_container_width=True):
                        st.session_state[f"si_del_{sid}"] = True
                        st.rerun()


def _render_stock_in_edit(row, items_dict):
    sid = str(row["stock_in_id"])
    st.markdown(f"##### ✏️ แก้ไขรายการเบิก {sid}")
    item_id  = str(row.get("item_id", ""))
    branch_id = str(row.get("branch_id", ""))
    date     = str(row.get("stock_in_date", ""))[:10]
    st.caption(f"สินค้า: {items_dict.get(item_id, item_id)} | สาขา: {branch_id} | วันที่: {date}")
    c1, c2 = st.columns(2)
    with c1:
        qty = st.number_input("จำนวน", min_value=0.0, step=1.0,
                              value=_num(row.get("qty_in", 0)), key=f"si_e_qty_{sid}")
    with c2:
        cost = st.number_input("ต้นทุน/หน่วย", min_value=0.0, step=0.01, format="%.4f",
                               value=_num(row.get("unit_cost", 0)), key=f"si_e_cost_{sid}")
    b1, b2 = st.columns(2)
    if b1.button("💾 บันทึกการแก้ไข", type="primary", use_container_width=True,
                 key=f"si_e_save_{sid}"):
        update_row(SHEET_STOCK_IN_TO_BRANCH, "stock_in_id", sid, {
            "qty_in": qty, "unit_cost": cost, "total_cost": qty * cost,
        })
        _rebuild_stockin_movements(sid, date, item_id, branch_id, qty, cost)
        st.session_state.pop(f"si_edit_{sid}", None)
        st.success("✅ แก้ไขสำเร็จ")
        st.rerun()
    if b2.button("ยกเลิก", use_container_width=True, key=f"si_e_cancel_{sid}"):
        st.session_state.pop(f"si_edit_{sid}", None)
        st.rerun()


# ══════════════════════════════════════════════════════════════════════
# TAB 4 : รายงานสต๊อกคงเหลือ (แสดง Min + เตือนสีแดงทั้งบรรทัด)
# ══════════════════════════════════════════════════════════════════════
def _render_stock_balance():
    st.subheader("📊 รายงานสต๊อกคงเหลือ")

    items_df = read_sheet(SHEET_ITEMS)
    if items_df is None or items_df.empty:
        st.info("ยังไม่มีรายการวัตถุดิบ/บรรจุภัณฑ์")
        return
    items_dict = dict(zip(items_df["item_id"].astype(str),
                          items_df["item_name"].astype(str)))
    min_dict = {}
    cat_dict = {}
    for _, r in items_df.iterrows():
        iid = str(r["item_id"])
        min_dict[iid] = _num(r.get("min_stock", 0))
        cat_dict[iid] = str(r.get("purchase_category", "")) if "purchase_category" in items_df.columns else ""
    # เฉพาะที่ยัง active
    active_ids = [str(r["item_id"]) for _, r in items_df.iterrows()
                 if str(r.get("is_active", "TRUE")).upper() != "FALSE"]

    mv_df = read_sheet(SHEET_STOCK_MOVEMENTS)
    branches_dict = _get_branches_dict()

    c1, c2 = st.columns(2)
    with c1:
        br_list = ["ทั้งหมด"]
        if mv_df is not None and not mv_df.empty and "branch_id" in mv_df.columns:
            br_list += sorted(mv_df["branch_id"].dropna().astype(str).str.strip().unique().tolist())
        sel_branch = st.selectbox(
            "🏪 เลือกสาขา", br_list,
            format_func=lambda k: k if k == "ทั้งหมด" else f"{k} – {branches_dict.get(k, '')}",
            key="stock_branch")
    with c2:
        sel_date = st.date_input("📅 วันที่ (สำหรับดูการเบิกส่งให้สาขา)",
                                 value=None, key="stock_date")

    # ── การเบิกส่งให้สาขา (ตามสาขา + วันที่) + PDF ──
    if sel_branch != "ทั้งหมด":
        si_df = read_sheet(SHEET_STOCK_IN_TO_BRANCH)
        if si_df is not None and not si_df.empty and "branch_id" in si_df.columns:
            d2 = si_df[si_df["branch_id"].astype(str).str.strip() == sel_branch].copy()
            if sel_date:
                d2 = d2[d2["stock_in_date"].astype(str).str[:10] == str(sel_date)]
            st.markdown("#### 🚛 การเบิกส่งให้สาขา"
                        + (f" (วันที่ {sel_date})" if sel_date else " (ทุกวัน)"))
            if d2.empty:
                st.caption("— ไม่มีรายการเบิกตามเงื่อนไข —")
            else:
                d2["ชื่อวัตถุดิบ"] = d2["item_id"].map(items_dict).fillna(d2["item_id"])
                d2["_qty"]  = d2["qty_in"].map(_num)
                d2["_cost"] = d2["unit_cost"].map(_num)
                d2["_val"]  = d2.apply(
                    lambda r: _num(r.get("total_cost")) or r["_qty"] * r["_cost"], axis=1)
                st.dataframe(pd.DataFrame({
                    "วันที่": d2["stock_in_date"].astype(str).str[:10],
                    "ชื่อวัตถุดิบ": d2["ชื่อวัตถุดิบ"],
                    "จำนวน": d2["_qty"],
                    "ต้นทุน/หน่วย (บาท)": d2["_cost"].map(lambda x: f"{x:,.2f}"),
                    "มูลค่า (บาท)": d2["_val"].map(lambda x: f"{x:,.2f}"),
                }), use_container_width=True, hide_index=True)
                tot = float(d2["_val"].sum())
                st.caption(f"รวมมูลค่าที่เบิก: ฿{tot:,.2f}")
                try:
                    pdf_bytes = make_table_pdf(
                        "รายงานการเบิกเข้าสาขา",
                        [f"สาขา: {sel_branch} – {branches_dict.get(sel_branch, '')}",
                         f"วันที่เบิก: {sel_date if sel_date else 'ทุกวัน'}"],
                        ["ชื่อวัตถุดิบ", "จำนวน", "ต้นทุน/หน่วย (บาท)", "มูลค่า (บาท)"],
                        [[r["ชื่อวัตถุดิบ"], f"{r['_qty']:,.2f}",
                          f"{r['_cost']:,.2f}", f"{r['_val']:,.2f}"]
                         for _, r in d2.iterrows()],
                        summary_lines=[f"รวมมูลค่าที่เบิกทั้งหมด: {tot:,.2f} บาท"],
                        col_widths=[4, 1.5, 2, 2], col_align=["L", "R", "R", "R"])
                    st.download_button(
                        "⬇️ ดาวน์โหลด PDF การเบิก (แนบให้สาขา)", data=pdf_bytes,
                        file_name=f"stock_in_{sel_branch}_{sel_date or 'all'}.pdf",
                        mime="application/pdf", use_container_width=True)
                except Exception as e:
                    st.warning(f"สร้าง PDF ไม่สำเร็จ: {e}")

    st.divider()
    st.markdown("#### 📦 ยอดคงเหลือ ทุกวัตถุดิบและบรรจุภัณฑ์")

    # คำนวณคงเหลือจาก movements
    sums = {}
    if mv_df is not None and not mv_df.empty and "item_id" in mv_df.columns:
        d = mv_df.copy()
        if sel_branch != "ทั้งหมด":
            d = d[d["branch_id"].astype(str).str.strip() == sel_branch]
        d["qty_in"]  = pd.to_numeric(d["qty_in"], errors="coerce").fillna(0)
        d["qty_out"] = pd.to_numeric(d["qty_out"], errors="coerce").fillna(0)
        g = d.groupby(d["item_id"].astype(str)).agg(
            total_in=("qty_in", "sum"), total_out=("qty_out", "sum"))
        sums = {idx: (r["total_in"], r["total_out"]) for idx, r in g.iterrows()}

    only_low = st.checkbox("🔴 แสดงเฉพาะรายการที่ถึง/ต่ำกว่าขั้นต่ำ", value=False,
                           key="stk_only_low")

    rows = []
    for iid in active_ids:
        tin, tout = sums.get(iid, (0, 0))
        bal = tin - tout
        mn  = min_dict.get(iid, 0)
        low = bal <= mn and mn > 0
        if only_low and not low:
            continue
        rows.append({
            "item_id": iid, "name": items_dict.get(iid, iid),
            "cat": cat_dict.get(iid, ""), "in": tin, "out": tout,
            "bal": bal, "min": mn, "low": low,
        })

    if not rows:
        st.success("✅ ไม่มีรายการ")
        return

    st.markdown(_build_stock_table(rows), unsafe_allow_html=True)

    low_count = sum(1 for r in rows if r["low"])
    if low_count > 0:
        st.markdown(
            f"<div style='background:#D32F2F;color:white;padding:10px;border-radius:6px;"
            f"font-size:16px;font-weight:bold;margin-top:10px;'>"
            f"⚠️ มี {low_count} รายการที่ถึงจุดสั่งซื้อ (ต่ำกว่า/เท่ากับขั้นต่ำ)!</div>",
            unsafe_allow_html=True)

    # ── ดาวน์โหลดยอดคงเหลือเป็น PDF ──
    _branch_label = ("ทุกสาขา" if sel_branch == "ทั้งหมด"
                     else f"{sel_branch} – {branches_dict.get(sel_branch, '')}")
    pdf_rows = [[r["item_id"], r["name"], f"{r['in']:.0f}", f"{r['out']:.0f}",
                 f"{r['bal']:.0f}", f"{r['min']:.0f}",
                 "ถึงขั้นต่ำ" if r["low"] else "ปกติ"] for r in rows]
    try:
        pdf_bytes = make_table_pdf(
            "รายงานสต๊อกคงเหลือ (วัตถุดิบและบรรจุภัณฑ์)",
            [f"สาขา: {_branch_label}", f"ณ วันที่พิมพ์: {datetime.date.today()}"],
            ["รหัส", "ชื่อรายการ", "รับเข้า", "จ่ายออก", "คงเหลือ", "ขั้นต่ำ", "สถานะ"],
            pdf_rows,
            summary_lines=[f"จำนวนรายการ: {len(rows)} | ถึงขั้นต่ำ: {low_count} รายการ"],
            col_widths=[1.6, 3, 1.2, 1.2, 1.2, 1.2, 1.6],
            col_align=["L", "L", "R", "R", "R", "R", "C"])
        st.download_button(
            "⬇️ ดาวน์โหลดยอดคงเหลือเป็น PDF", data=pdf_bytes,
            file_name=f"stock_balance_{sel_branch}.pdf", mime="application/pdf",
            type="primary", use_container_width=True)
    except Exception as e:
        st.warning(f"สร้าง PDF ไม่สำเร็จ: {e}")


def _build_stock_table(rows) -> str:
    heads = ["รหัส", "ชื่อรายการ", "รับเข้า", "จ่ายออก", "คงเหลือ", "ขั้นต่ำ (Min)", "สถานะ"]
    header = "<tr>" + "".join(
        f"<th style='padding:8px;background:#1e1e1e;color:white;'>{h}</th>"
        for h in heads) + "</tr>"
    rows_html = ""
    for r in rows:
        low = r["low"]
        if low:
            # เตือนสีแดงทั้งบรรทัด
            bg, tc, fw = "#D32F2F", "#ffffff", "bold"
        else:
            bg, tc, fw = "#E8F5E9", "#1B5E20", "normal"
        td = (f"padding:8px;background:{bg};color:{tc};font-weight:{fw};")
        status = "⚠️ ถึงขั้นต่ำ" if low else "✅ ปกติ"
        cells  = f"<td style='{td}'>{r['item_id']}</td>"
        cells += f"<td style='{td}'>{r['name']}</td>"
        cells += f"<td style='{td}text-align:right;'>{r['in']:.0f}</td>"
        cells += f"<td style='{td}text-align:right;'>{r['out']:.0f}</td>"
        cells += f"<td style='{td}text-align:right;'>{r['bal']:.0f}</td>"
        cells += f"<td style='{td}text-align:right;'>{r['min']:.0f}</td>"
        cells += f"<td style='{td}text-align:center;'>{status}</td>"
        rows_html += f"<tr>{cells}</tr>"
    return (f"<table style='border-collapse:collapse;width:100%;font-size:13px;'>"
            f"<thead>{header}</thead><tbody>{rows_html}</tbody></table>")


# ══════════════════════════════════════════════════════════════════════
# เมนูแสดงผล: ดูรายการสั่งซื้อ (ตามวันที่)
# ══════════════════════════════════════════════════════════════════════
def _render_stock_in_report():
    st.subheader("🚚 ดูการเบิกของเข้าสาขา (ตามวันที่) + PDF")
    st.caption("เลือกสาขา + วันที่ เพื่อดูรายการที่เบิกให้สาขา แล้วดาวน์โหลด/พิมพ์ PDF แนบให้สาขา")
    items_dict = _get_items_dict()
    branches_dict = _get_branches_dict()
    si_df = read_sheet(SHEET_STOCK_IN_TO_BRANCH)
    if si_df is None or si_df.empty:
        st.info("ยังไม่มีข้อมูลการเบิกเข้าสาขา")
        return
    c1, c2 = st.columns(2)
    with c1:
        br_ids = sorted(si_df["branch_id"].astype(str).str.strip().unique().tolist())
        sel_branch = st.selectbox(
            "🏪 เลือกสาขา", br_ids,
            format_func=lambda k: f"{k} – {branches_dict.get(k, '')}", key="sir_branch")
    with c2:
        sel_date = st.date_input("📅 วันที่เบิก (ว่าง = ทุกวัน)", value=None, key="sir_date")
    df = si_df[si_df["branch_id"].astype(str).str.strip() == str(sel_branch)].copy()
    if sel_date:
        df = df[df["stock_in_date"].astype(str).str[:10] == str(sel_date)]
    if df.empty:
        st.info("ไม่พบรายการเบิกตามเงื่อนไขที่เลือก")
        return
    df["ชื่อวัตถุดิบ"] = df["item_id"].map(items_dict).fillna(df["item_id"])
    df["_qty"] = df["qty_in"].map(_num)
    df["_cost"] = df["unit_cost"].map(_num)
    df["_val"] = df.apply(lambda r: _num(r.get("total_cost")) or r["_qty"] * r["_cost"], axis=1)
    st.dataframe(pd.DataFrame({
        "วันที่": df["stock_in_date"].astype(str).str[:10],
        "ชื่อวัตถุดิบ": df["ชื่อวัตถุดิบ"],
        "จำนวน": df["_qty"],
        "ต้นทุน/หน่วย (บาท)": df["_cost"].map(lambda x: f"{x:,.2f}"),
        "มูลค่า (บาท)": df["_val"].map(lambda x: f"{x:,.2f}"),
    }), use_container_width=True, hide_index=True)
    total_val = float(df["_val"].sum())
    st.metric("รวมมูลค่าที่เบิกทั้งหมด", f"฿{total_val:,.2f}")
    meta = [f"สาขา: {sel_branch} – {branches_dict.get(sel_branch, '')}",
            f"วันที่เบิก: {sel_date if sel_date else 'ทุกวัน'}"]
    pdf_rows = [[r["ชื่อวัตถุดิบ"], f"{r['_qty']:,.2f}", f"{r['_cost']:,.2f}", f"{r['_val']:,.2f}"]
                for _, r in df.iterrows()]
    try:
        pdf_bytes = make_table_pdf(
            "รายงานการเบิกเข้าสาขา", meta,
            ["ชื่อวัตถุดิบ", "จำนวน", "ต้นทุน/หน่วย (บาท)", "มูลค่า (บาท)"],
            pdf_rows, summary_lines=[f"รวมมูลค่าที่เบิกทั้งหมด: {total_val:,.2f} บาท"],
            col_widths=[4, 1.5, 2, 2], col_align=["L", "R", "R", "R"])
        st.download_button("⬇️ ดาวน์โหลด PDF (แนบให้สาขา)", data=pdf_bytes,
                           file_name=f"stock_in_{sel_branch}_{sel_date or 'all'}.pdf",
                           mime="application/pdf", type="primary", use_container_width=True)
    except Exception as e:
        st.warning(f"สร้าง PDF ไม่สำเร็จ: {e}")


def _render_purchase_view():
    st.subheader("📅 ดูรายการสั่งซื้อ (ตามวันที่)")
    items_dict = _get_items_dict()
    po_df = read_sheet(SHEET_PURCHASE_ORDERS)
    if po_df is None or po_df.empty:
        st.info("ยังไม่มีใบสั่งซื้อ")
        return
    sel_date = st.date_input("📅 วันที่สั่งซื้อ (ว่าง = ทั้งหมด)", value=None, key="pv_date")
    df = po_df.copy()
    if sel_date:
        df = df[df["purchase_date"].astype(str).str[:10] == str(sel_date)]
    if df.empty:
        st.info("ไม่พบใบสั่งซื้อตามวันที่เลือก")
        return
    tot = df["grand_total"].map(_num).sum()
    st.caption(f"พบ {len(df)} ใบ | รวมยอดซื้อ (รวม VAT) ฿{tot:,.2f}")
    poi = read_sheet(SHEET_PURCHASE_ORDER_ITEMS)
    for _, r in df.sort_values("purchase_id", ascending=False).iterrows():
        pid = str(r["purchase_id"])
        with st.expander(f"📦 {pid} | {r.get('purchase_date','')} | "
                         f"{r.get('supplier_name','')} | ฿{_num(r.get('grand_total',0)):,.2f}"):
            st.write(f"ประเภทการซื้อ: {r.get('purchase_category','')}  |  "
                     f"Invoice: {r.get('invoice_no','')}  |  บันทึกโดย: {r.get('created_by','')}")
            if poi is not None and not poi.empty and "purchase_id" in poi.columns:
                ln = poi[poi["purchase_id"].astype(str) == pid]
                if not ln.empty:
                    st.dataframe(pd.DataFrame({
                        "สินค้า": ln["item_id"].map(items_dict).fillna(ln["item_id"]),
                        "จำนวน": ln["qty"].map(_num),
                        "ราคา/หน่วย": ln["unit_price_inc_vat"].map(lambda x: f"{_num(x):,.2f}"),
                        "มูลค่า": ln["total_value"].map(lambda x: f"{_num(x):,.2f}"),
                    }), use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════
# เมนูแสดงผล: วัตถุดิบ/บรรจุภัณฑ์ที่ถึงจุดสั่งซื้อ (Minimum) + PDF
# ══════════════════════════════════════════════════════════════════════
def _balance_rows(sel_branch="ทั้งหมด", low_only=False):
    items_df = read_sheet(SHEET_ITEMS)
    if items_df is None or items_df.empty or "item_id" not in items_df.columns:
        return []
    items_dict = dict(zip(items_df["item_id"].astype(str), items_df["item_name"].astype(str)))
    min_dict = {}
    for _, r in items_df.iterrows():
        min_dict[str(r["item_id"])] = _num(r.get("min_stock", 0))
    active_ids = [str(r["item_id"]) for _, r in items_df.iterrows()
                 if str(r.get("is_active", "TRUE")).upper() != "FALSE"]
    mv_df = read_sheet(SHEET_STOCK_MOVEMENTS)
    sums = {}
    if mv_df is not None and not mv_df.empty and "item_id" in mv_df.columns:
        d = mv_df.copy()
        if sel_branch != "ทั้งหมด":
            d = d[d["branch_id"].astype(str).str.strip() == sel_branch]
        d["qty_in"] = pd.to_numeric(d["qty_in"], errors="coerce").fillna(0)
        d["qty_out"] = pd.to_numeric(d["qty_out"], errors="coerce").fillna(0)
        g = d.groupby(d["item_id"].astype(str)).agg(
            total_in=("qty_in", "sum"), total_out=("qty_out", "sum"))
        sums = {idx: (r["total_in"], r["total_out"]) for idx, r in g.iterrows()}
    rows = []
    for iid in active_ids:
        tin, tout = sums.get(iid, (0, 0))
        bal = tin - tout
        mn = min_dict.get(iid, 0)
        low = bal <= mn and mn > 0
        if low_only and not low:
            continue
        rows.append({"item_id": iid, "name": items_dict.get(iid, iid),
                     "cat": "", "in": tin, "out": tout, "bal": bal, "min": mn, "low": low})
    return rows


def _render_low_stock():
    st.subheader("🔴 วัตถุดิบ/บรรจุภัณฑ์ที่ถึงจุดสั่งซื้อ (Minimum Point)")
    st.caption("แสดงเฉพาะรายการที่ยอดคงเหลือ ≤ จุดขั้นต่ำ (Min) — เตือนสีแดง สำหรับเตรียมสั่งซื้อ")
    rows = _balance_rows("ทั้งหมด", low_only=True)
    if not rows:
        st.success("✅ ยังไม่มีรายการที่ถึงจุดสั่งซื้อ")
        return
    st.markdown(
        f"<div style='background:#D32F2F;color:white;padding:10px;border-radius:6px;"
        f"font-size:16px;font-weight:bold;margin-bottom:10px;'>"
        f"⚠️ มี {len(rows)} รายการที่ต้องสั่งซื้อ (ถึง/ต่ำกว่าขั้นต่ำ)</div>",
        unsafe_allow_html=True)
    st.markdown(_build_stock_table(rows), unsafe_allow_html=True)
    pdf_rows = [[r["item_id"], r["name"], f"{r['bal']:.0f}", f"{r['min']:.0f}",
                 f"{(r['min']-r['bal']):.0f}"] for r in rows]
    try:
        pdf_bytes = make_table_pdf(
            "รายการวัตถุดิบ/บรรจุภัณฑ์ที่ถึงจุดสั่งซื้อ (Minimum)",
            [f"ณ วันที่พิมพ์: {datetime.date.today()}"],
            ["รหัส", "ชื่อรายการ", "คงเหลือ", "ขั้นต่ำ (Min)", "ควรสั่งเพิ่ม(อย่างน้อย)"],
            pdf_rows,
            summary_lines=[f"รวม {len(rows)} รายการที่ต้องสั่งซื้อ"],
            col_widths=[1.6, 3.5, 1.4, 1.4, 2.2], col_align=["L", "L", "R", "R", "R"])
        st.download_button(
            "⬇️ ดาวน์โหลด PDF (รายการที่ต้องสั่งซื้อ)", data=pdf_bytes,
            file_name="low_stock_reorder.pdf", mime="application/pdf",
            type="primary", use_container_width=True)
    except Exception as e:
        st.warning(f"สร้าง PDF ไม่สำเร็จ: {e}")
