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
    SHEET_ASSETS, SHEET_ASSET_REPAIRS,
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
    SHEET_ASSETS: [
        "asset_id", "item_id", "item_name", "purchase_date", "brand",
        "spec", "seller", "seller_phone", "serial", "created_at",
    ],
    SHEET_ASSET_REPAIRS: [
        "repair_id", "item_id", "send_date", "symptom",
        "repair_shop", "repair_shop_phone", "repairer_name", "repairer_phone",
        "how_repaired", "return_date", "status", "created_at", "updated_at",
    ],
}

# คอลัมน์ข้อมูลทรัพย์สิน (นอกเหนือจาก id) — ใช้กรอกตอนเพิ่มรายการประเภททรัพย์สิน
ASSET_FIELDS = [
    ("purchase_date", "วันที่ซื้อ"), ("brand", "ยี่ห้อ"), ("spec", "Spec / รายละเอียด"),
    ("seller", "ผู้ขาย"), ("seller_phone", "เบอร์โทรผู้ขาย"), ("serial", "Serial No."),
]

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
    """รายชื่อสาขา — ใช้ branch_auth (20 สาขาต้นฉบับ) เป็นหลัก + เสริมจากตาราง branches
    (แก้ปัญหาตาราง branches ว่าง ทำให้ dropdown ไม่มีชื่อสาขา)"""
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


def _item_price_map():
    """ราคาต่อหน่วยของแต่ละวัตถุดิบ — ใช้ standard_cost จาก items ก่อน
    ถ้าไม่มี ใช้ราคาซื้อล่าสุดจากใบสั่งซื้อ (purchase_order_items)"""
    prices = {}
    try:
        idf = read_sheet(SHEET_ITEMS)
        if idf is not None and not idf.empty and "item_id" in idf.columns:
            for _, r in idf.iterrows():
                prices[str(r["item_id"])] = _num(r.get("standard_cost", 0))
    except Exception:
        pass
    # เติมจากราคาซื้อล่าสุด (ถ้า standard_cost = 0)
    try:
        poi = read_sheet(SHEET_PURCHASE_ORDER_ITEMS)
        if poi is not None and not poi.empty and "item_id" in poi.columns:
            for iid, grp in poi.groupby(poi["item_id"].astype(str)):
                if prices.get(iid, 0) <= 0:
                    prices[iid] = _num(grp.iloc[-1].get("unit_price_inc_vat", 0))
    except Exception:
        pass
    return prices


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
# ══════════════════════════════════════════════════════════════════════
# ทรัพย์สิน (ASSETS) — helper
# ══════════════════════════════════════════════════════════════════════
def _asset_items_dict():
    """รายการที่เป็น 'ทรัพย์สิน' (purchase_category = ทรัพย์สิน) → {item_id: ชื่อ}"""
    df = read_sheet(SHEET_ITEMS)
    if df is None or df.empty or "item_id" not in df.columns:
        return {}
    if "purchase_category" in df.columns:
        df = df[df["purchase_category"].astype(str).str.strip() == "ทรัพย์สิน"]
    return {str(r["item_id"]): str(r.get("item_name", "")) for _, r in df.iterrows()}


def _asset_info(item_id):
    """ข้อมูลทรัพย์สิน (วันที่ซื้อ/ยี่ห้อ/spec/ผู้ขาย/เบอร์/serial) — คืน dict (ว่างถ้าไม่มี)"""
    try:
        df = read_sheet(SHEET_ASSETS)
    except Exception:
        df = pd.DataFrame()
    if df is None or df.empty or "item_id" not in df.columns:
        return {}
    m = df[df["item_id"].astype(str) == str(item_id)]
    return m.iloc[-1].to_dict() if not m.empty else {}


def _save_asset(item_id, item_name, data):
    """บันทึก/อัปเดตข้อมูลทรัพย์สิน (1 item_id = 1 ทรัพย์สิน)"""
    import datetime as _dt
    df = read_sheet(SHEET_ASSETS)
    exists = (df is not None and not df.empty and "item_id" in df.columns
              and (df["item_id"].astype(str) == str(item_id)).any())
    row = {"item_id": str(item_id), "item_name": item_name, **data}
    if exists:
        update_row(SHEET_ASSETS, "item_id", str(item_id), row)
    else:
        adf = read_sheet(SHEET_ASSETS)
        row["asset_id"] = next_id(adf, "asset_id", "AST")
        row["created_at"] = str(_dt.date.today())
        append_row(SHEET_ASSETS, row)


def _asset_repairs(item_id):
    """ประวัติการซ่อมของทรัพย์สินนั้น (เรียงใหม่ล่าสุดก่อน)"""
    try:
        df = read_sheet(SHEET_ASSET_REPAIRS)
    except Exception:
        df = pd.DataFrame()
    if df is None or df.empty or "item_id" not in df.columns:
        return pd.DataFrame()
    m = df[df["item_id"].astype(str) == str(item_id)]
    if m.empty:
        return m
    return m.sort_values("send_date", ascending=False)


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
        "🔧 ซ่อมบำรุงทรัพย์สิน",        # ทรัพย์สิน + ประวัติการซ่อม
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
    with tabs[7]:
        _render_asset_maintenance()


# ══════════════════════════════════════════════════════════════════════
# TAB 1 : บันทึกชื่อวัตถุดิบและบรรจุภัณฑ์ (เพิ่ม/แก้ไข/ลบ)
# ══════════════════════════════════════════════════════════════════════
def _render_items_master():
    st.subheader("🧾 บันทึกชื่อวัตถุดิบและบรรจุภัณฑ์")
    st.caption("รหัสระบบสร้างให้อัตโนมัติ • กำหนดยอดขั้นต่ำ (Min) ไว้เตือนเวลาสั่งซื้อ")

    # ── เพิ่มรายการใหม่ (ใช้ widget สด เพื่อให้โชว์ช่องข้อมูลทรัพย์สินเมื่อเลือก 'ทรัพย์สิน') ──
    st.markdown("#### ➕ เพิ่มรายการใหม่")
    c1, c2, c3 = st.columns([2, 3, 1.3])
    with c1:
        cat = st.selectbox("ประเภทการซื้อ *", PURCHASE_CATEGORIES, key="im_cat")
    with c2:
        name = st.text_input("ชื่อวัตถุดิบ / บรรจุภัณฑ์ / ทรัพย์สิน *", key="im_name")
    with c3:
        min_stock = st.number_input("ยอดขั้นต่ำ (Min)", min_value=0.0,
                                    step=1.0, key="im_min")
    c4, _c5 = st.columns([1, 3])
    with c4:
        unit = st.text_input("หน่วย", value="", key="im_unit")

    is_asset = (str(cat).strip() == "ทรัพย์สิน")
    asset_data = {}
    if is_asset:
        st.markdown("##### 🏷️ ข้อมูลทรัพย์สิน (กรอกเพิ่มสำหรับประเภททรัพย์สิน)")
        ac1, ac2, ac3 = st.columns(3)
        with ac1:
            asset_data["purchase_date"] = str(st.date_input("วันที่ซื้อ",
                                             value=datetime.date.today(), key="im_a_date"))
            asset_data["seller"] = st.text_input("ผู้ขาย", key="im_a_seller").strip()
        with ac2:
            asset_data["brand"] = st.text_input("ยี่ห้อ", key="im_a_brand").strip()
            asset_data["seller_phone"] = st.text_input("เบอร์โทรผู้ขาย", key="im_a_sphone").strip()
        with ac3:
            asset_data["spec"] = st.text_input("Spec / รายละเอียด", key="im_a_spec").strip()
            asset_data["serial"] = st.text_input("Serial No.", key="im_a_serial").strip()

    if st.button("💾 บันทึกรายการ", type="primary", key="im_add_btn"):
        if not name.strip():
            st.error("กรุณากรอกชื่อวัตถุดิบ/บรรจุภัณฑ์/ทรัพย์สิน")
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
                if is_asset:
                    try:
                        _save_asset(new_id, name.strip(), asset_data)
                    except Exception as e:
                        st.warning(f"บันทึกชื่อสำเร็จ แต่เก็บข้อมูลทรัพย์สินไม่ได้: {e} "
                                   "(กรุณารัน roon_new_tables.sql)")
                st.success(f"✅ เพิ่มรายการสำเร็จ! รหัส: {new_id} | {name.strip()}"
                           + (" (หมายเหตุ: กรุณารัน roon_new_tables.sql เพื่อเก็บประเภทการซื้อ)"
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

    # ใช้ widget สด (ไม่ใช้ st.form) เพื่อให้ราคาต่อหน่วยดึงจากระบบทันทีที่เลือกสินค้า
    # และยอดเงินซื้อรวมอัปเดตถูกต้องแม้มีหลายรายการใน 1 ใบ
    col1, col2 = st.columns(2)
    with col1:
        purchase_date = st.date_input("📅 วันที่ซื้อ", value=datetime.date.today(), key="po_date")
        supplier_name = st.text_input("🏢 ชื่อผู้ขาย / Supplier *", key="po_supplier")
        invoice_no    = st.text_input("🧾 เลขที่ Invoice", key="po_invoice")
    with col2:
        purchase_category = st.selectbox("📂 ประเภทการซื้อ", PURCHASE_CATEGORIES, key="po_cat")
        created_by        = st.text_input("👤 บันทึกโดย *", key="po_by")
        remark            = st.text_input("📝 หมายเหตุ", key="po_remark")

    st.markdown("#### รายการสินค้าที่ซื้อ")
    st.caption("เลือกสินค้า — ระบบดึง 'ราคา/หน่วย' ที่บันทึกไว้มาให้ก่อน (แก้ไขได้) แล้วรวมยอดให้อัตโนมัติ")
    price_map = _item_price_map()
    num_items = st.number_input("จำนวนรายการ", min_value=1, max_value=20,
                                 value=1, step=1, key="po_num_items")
    item_rows = []
    if items_dict:
        item_keys = list(items_dict.keys())
        for i in range(int(num_items)):
            c1, c2, c3, c4 = st.columns([3, 1, 2, 1.4])
            with c1:
                sel = st.selectbox(f"สินค้า #{i+1}", item_keys,
                                   format_func=lambda k: f"{k} – {items_dict[k]}",
                                   key=f"po_item_{i}")
            # ดึงราคาต่อหน่วยจากระบบเมื่อเลือกสินค้า (เปลี่ยนสินค้า = รีเซ็ตราคา, แก้ไขต่อได้)
            sys_price = float(_num(price_map.get(str(sel), 0)))
            prev_key = f"po_item_prev_{i}"
            price_key = f"po_price_{i}"
            if st.session_state.get(prev_key) != sel:
                st.session_state[price_key] = sys_price
                st.session_state[prev_key] = sel
            with c2:
                qty = _num(st.number_input(f"จำนวน #{i+1}", min_value=0.0,
                                           step=1.0, format="%.2f", key=f"po_qty_{i}"))
            with c3:
                price = _num(st.number_input(f"ราคา/หน่วย #{i+1}", min_value=0.0,
                                             step=0.01, format="%.4f", key=price_key))
            with c4:
                st.markdown("รวม")
                st.markdown(f"**฿{qty*price:,.2f}**")
            item_rows.append((sel, qty, price))

    # ยอดเงินซื้อรวม — คำนวณสดจากทุกรายการ (จำนวน × ราคา)
    line_total = sum(q * p for _, q, p in item_rows)
    grand_total = line_total
    st.markdown(
        f"<div style='background:#EDE7F6;border:2px solid #7B1FA2;border-radius:8px;"
        f"padding:12px;text-align:center;'>"
        f"<span style='color:#4A148C;font-size:1.05rem;'>💰 ยอดเงินซื้อรวม ({len([r for r in item_rows if r[1]>0])} รายการ)</span><br>"
        f"<b style='color:#4A148C;font-size:1.8rem;'>฿{grand_total:,.2f}</b></div>",
        unsafe_allow_html=True)

    submitted = st.button("💾 บันทึกใบสั่งซื้อ", type="primary", key="po_save")

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

    col1, col2 = st.columns(2)
    with col1:
        stock_in_date = st.date_input("📅 วันที่เบิก", value=datetime.date.today(), key="si_date")
        branch_id = st.selectbox("🏪 สาขาปลายทาง *", list(branches_dict.keys()),
                                 format_func=lambda k: f"{k} – {branches_dict[k]}", key="si_branch")
    with col2:
        recorded_by = st.text_input("👤 บันทึกโดย *", key="si_by")
        remark      = st.text_input("📝 หมายเหตุ", key="si_remark")

    st.markdown("#### รายการที่เบิก (วัตถุดิบ / บรรจุภัณฑ์ / แป้งสำเร็จรูป-ส่วนผสม / อื่นๆ)")
    st.caption("เลือกชื่อวัตถุดิบ + จำนวน — ระบบดึง 'ต้นทุน/หน่วย' ที่บันทึกไว้มาให้ก่อน (แก้ไขได้)")
    price_map = _item_price_map()
    num_items = st.number_input("จำนวนรายการ", min_value=1, max_value=20,
                                 value=1, step=1, key="si_num")
    si_rows   = []
    item_keys = list(items_dict.keys())
    for i in range(int(num_items)):
        c1, c2, c3 = st.columns([3, 1, 1])
        with c1:
            sel = st.selectbox(f"ชื่อวัตถุดิบ #{i+1}", item_keys,
                               format_func=lambda k: f"{k} – {items_dict[k]}",
                               key=f"si_item_{i}")
        # ดึงต้นทุนจากระบบเมื่อ 'เลือกสินค้า' — ถ้าเปลี่ยนสินค้า รีเซ็ตเป็นราคาระบบ (แก้ไขต่อได้)
        sys_cost = float(_num(price_map.get(str(sel), 0)))
        prev_key = f"si_item_prev_{i}"
        cost_key = f"si_cost_{i}"
        if st.session_state.get(prev_key) != sel:
            st.session_state[cost_key] = sys_cost
            st.session_state[prev_key] = sel
        with c2:
            qty = st.number_input(f"จำนวน #{i+1}", min_value=0.0,
                                   step=1.0, key=f"si_qty_{i}")
        with c3:
            unit_cost = _num(st.number_input(f"ต้นทุน/หน่วย #{i+1}", min_value=0.0,
                                             step=1.0, format="%.4f", key=cost_key))
        si_rows.append((sel, qty, "", unit_cost))

    total_preview = sum(q * c for _, q, _, c in si_rows if q > 0)
    st.markdown(
        f"<div style='background:#FFF3E0;border:2px solid #FF6B35;border-radius:8px;"
        f"padding:14px;text-align:center;'>"
        f"<span style='color:#E65100;font-size:1.05rem;'>📦 มูลค่าเบิกรวม (ดึงต้นทุนจากระบบ • แก้ไขได้)</span><br>"
        f"<b style='color:#BF360C;font-size:2rem;'>฿{total_preview:,.2f}</b></div>",
        unsafe_allow_html=True)
    submitted = st.button("💾 บันทึกการเบิก", type="primary", key="si_save")

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
# ภาพรวมสต๊อก: ส่วนกลาง + แต่ละสาขา + รวมทั้งหมด
# ══════════════════════════════════════════════════════════════════════
def _render_stock_overview(mv_df, branches_dict, active_ids):
    if mv_df is None or mv_df.empty or "branch_id" not in mv_df.columns:
        st.caption("— ยังไม่มีข้อมูล movement —")
        return
    d = mv_df.copy()
    d["qty_in"] = pd.to_numeric(d["qty_in"], errors="coerce").fillna(0)
    d["qty_out"] = pd.to_numeric(d["qty_out"], errors="coerce").fillna(0)
    d["_bal"] = d["qty_in"] - d["qty_out"]
    rows = []
    grand_qty = 0.0
    grand_items = set()
    for bid, grp in d.groupby(d["branch_id"].astype(str).str.strip()):
        by_item = grp.groupby(grp["item_id"].astype(str))["_bal"].sum()
        n_items = int((by_item > 0).sum())
        tot = float(by_item[by_item > 0].sum())
        loc = "ส่วนกลาง (CENTRAL)" if bid == "CENTRAL" else f"{bid} – {branches_dict.get(bid, '')}"
        rows.append({"ที่จัดเก็บ": loc, "จำนวนชนิดที่มีของ": n_items,
                     "ยอดคงเหลือรวม (หน่วย)": f"{tot:,.0f}"})
        grand_qty += tot
        grand_items |= set(by_item[by_item > 0].index)
    if rows:
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
        k = st.columns(2)
        k[0].metric("รวมทุกที่ (สาขา + ส่วนกลาง)", f"{grand_qty:,.0f} หน่วย")
        k[1].metric("จำนวนชนิดทั้งหมด", f"{len(grand_items):,}")


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

    # ── ภาพรวมสต๊อก: ส่วนกลาง + แต่ละสาขา + รวมทั้งหมด ──
    with st.expander("📦 ภาพรวมสต๊อกคงเหลือ (ส่วนกลาง + ทุกสาขา + รวม)", expanded=False):
        _render_stock_overview(mv_df, branches_dict, active_ids)

    # รายชื่อสาขาให้เลือก = ทั้งหมด + ส่วนกลาง + ทุกสาขา (มีชื่อ) + ที่พบใน movement
    br_list = ["ทั้งหมด", "CENTRAL"] + sorted(branches_dict.keys())
    if mv_df is not None and not mv_df.empty and "branch_id" in mv_df.columns:
        for b in mv_df["branch_id"].dropna().astype(str).str.strip().unique().tolist():
            if b not in br_list:
                br_list.append(b)

    def _branch_label(k):
        if k == "ทั้งหมด":
            return "ทั้งหมด (ทุกที่)"
        if k == "CENTRAL":
            return "CENTRAL – ส่วนกลาง"
        return f"{k} – {branches_dict.get(k, '')}"

    c1, c2 = st.columns(2)
    with c1:
        sel_branch = st.selectbox("🏪 เลือกสาขา / ส่วนกลาง", br_list,
                                  format_func=_branch_label, key="stock_branch")
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

    # ── PDF: ใบส่งมอบและตรวจรับ (มีช่องติ๊ก 'ส่งของ' + 'รับแล้ว') ──
    # ประเภท (purchase_category) จากตาราง items
    items_df = read_sheet(SHEET_ITEMS)
    cat_map, unit_map = {}, {}
    if items_df is not None and not items_df.empty and "item_id" in items_df.columns:
        for _, ir in items_df.iterrows():
            iid = str(ir.get("item_id", ""))
            cat_map[iid] = str(ir.get("purchase_category", "") or "")
            unit_map[iid] = str(ir.get("unit", "") or "")
    note_rows = []
    for _, r in df.iterrows():
        iid = str(r.get("item_id", ""))
        unit = str(r.get("unit", "") or "").strip() or unit_map.get(iid, "")
        note_rows.append((iid, cat_map.get(iid, ""), r["ชื่อวัตถุดิบ"],
                          f"{r['_qty']:,.2f}", unit))
    doc_ids = df["stock_in_id"].astype(str).unique().tolist() if "stock_in_id" in df.columns else []
    doc_no = doc_ids[0] if len(doc_ids) == 1 else (f"{doc_ids[0]} +{len(doc_ids)-1} ใบ" if doc_ids else "-")
    send_date = str(sel_date) if sel_date else datetime.date.today().strftime("%d/%m/%Y")
    remark = df["remark"].dropna().astype(str).iloc[0] if "remark" in df.columns and df["remark"].notna().any() else "-"
    recorder = df["recorded_by"].dropna().astype(str).iloc[0] if "recorded_by" in df.columns and df["recorded_by"].notna().any() else "ฝ่ายจัดซื้อ"
    try:
        from modules.pdf_util import make_delivery_note_pdf
        pdf_bytes = make_delivery_note_pdf(
            doc_no, send_date, f"{sel_branch} – {branches_dict.get(sel_branch,'')}",
            recorder, remark, note_rows)
        st.download_button("⬇️ ดาวน์โหลด PDF ใบส่งมอบ (แนบให้สาขา)", data=pdf_bytes,
                           file_name=f"delivery_{sel_branch}_{sel_date or 'all'}.pdf",
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


# ══════════════════════════════════════════════════════════════════════
# TAB 8 : ซ่อมบำรุงทรัพย์สิน
# ══════════════════════════════════════════════════════════════════════
def _render_asset_maintenance():
    st.subheader("🔧 ซ่อมบำรุงทรัพย์สิน")
    assets = _asset_items_dict()
    if not assets:
        st.info("ยังไม่มีทรัพย์สินในระบบ — เพิ่มที่แท็บ 'ชื่อวัตถุดิบ/บรรจุภัณฑ์' "
                "แล้วเลือกประเภทการซื้อ = 'ทรัพย์สิน' (จะมีช่องกรอก วันที่ซื้อ/ยี่ห้อ/spec/ผู้ขาย/serial)")
        return

    item_id = st.selectbox("🏷️ เลือกทรัพย์สิน", list(assets.keys()),
                           format_func=lambda k: f"{k} – {assets[k]}", key="am_asset")
    info = _asset_info(item_id)

    st.markdown("##### 📇 ข้อมูลทรัพย์สิน")
    if info:
        st.markdown(
            f"- **ชื่อ:** {assets.get(item_id, '')}\n"
            f"- **วันที่ซื้อ:** {info.get('purchase_date') or '-'} · "
            f"**ยี่ห้อ:** {info.get('brand') or '-'} · "
            f"**Spec:** {info.get('spec') or '-'}\n"
            f"- **ผู้ขาย:** {info.get('seller') or '-'} · "
            f"**เบอร์โทรผู้ขาย:** {info.get('seller_phone') or '-'} · "
            f"**Serial:** {info.get('serial') or '-'}")
    else:
        st.caption("— ยังไม่มีรายละเอียดของทรัพย์สินนี้ —")
    with st.expander("✏️ เพิ่ม/แก้ไข รายละเอียดทรัพย์สินนี้"):
        a1, a2, a3 = st.columns(3)
        with a1:
            pd_ = st.text_input("วันที่ซื้อ", value=str(info.get("purchase_date", "")), key="am_i_date")
            seller = st.text_input("ผู้ขาย", value=str(info.get("seller", "")), key="am_i_seller")
        with a2:
            brand = st.text_input("ยี่ห้อ", value=str(info.get("brand", "")), key="am_i_brand")
            sphone = st.text_input("เบอร์โทรผู้ขาย", value=str(info.get("seller_phone", "")), key="am_i_sphone")
        with a3:
            spec = st.text_input("Spec / รายละเอียด", value=str(info.get("spec", "")), key="am_i_spec")
            serial = st.text_input("Serial No.", value=str(info.get("serial", "")), key="am_i_serial")
        if st.button("💾 บันทึกรายละเอียดทรัพย์สิน", key="am_i_save"):
            try:
                _save_asset(item_id, assets.get(item_id, ""), {
                    "purchase_date": pd_.strip(), "brand": brand.strip(), "spec": spec.strip(),
                    "seller": seller.strip(), "seller_phone": sphone.strip(), "serial": serial.strip(),
                })
                st.success("✅ บันทึกรายละเอียดทรัพย์สินแล้ว")
                st.rerun()
            except Exception as e:
                st.error(f"บันทึกไม่สำเร็จ: {e} (รัน roon_new_tables.sql)")

    st.divider()
    reps = _asset_repairs(item_id)
    st.markdown("##### 📋 ประวัติการซ่อม")
    if reps is None or reps.empty:
        st.caption("— ยังไม่เคยมีการส่งซ่อม —")
    else:
        show = pd.DataFrame({
            "วันที่ส่งซ่อม": reps["send_date"].astype(str),
            "อาการเสีย": reps.get("symptom", "").astype(str),
            "ร้านซ่อม": reps.get("repair_shop", "").astype(str),
            "ผู้ซ่อม": reps.get("repairer_name", "").astype(str),
            "วิธีซ่อม": reps.get("how_repaired", "").astype(str),
            "วันที่รับคืน": reps.get("return_date", "").astype(str),
            "สถานะ": reps.get("status", "").astype(str),
        })
        st.dataframe(show, use_container_width=True, hide_index=True)

    st.divider()
    mode = st.radio("การดำเนินการ",
                    ["➕ ส่งซ่อมใหม่", "✏️ อัปเดต/รับคืน รายการซ่อมเดิม", "🗑️ ลบรายการซ่อม"],
                    horizontal=True, key="am_mode")
    if mode.startswith("➕"):
        _asset_repair_new(item_id)
    elif mode.startswith("✏️"):
        _asset_repair_edit(item_id, reps, info)
    else:
        _asset_repair_delete(reps)


def _asset_repair_new(item_id):
    st.markdown("**➕ ส่งซ่อมใหม่** (ครั้งแรกของทรัพย์สินนี้ หรือรอบใหม่)")
    c1, c2 = st.columns(2)
    with c1:
        send_date = st.date_input("วันที่ส่งซ่อม", value=datetime.date.today(), key="am_n_send")
        symptom = st.text_area("อาการเสีย", key="am_n_sym", height=70)
        repair_shop = st.text_input("ร้านซ่อม", key="am_n_shop")
    with c2:
        repair_shop_phone = st.text_input("เบอร์โทรร้านซ่อม", key="am_n_shopph")
        repairer_name = st.text_input("ชื่อผู้ซ่อม", key="am_n_rname")
        repairer_phone = st.text_input("เบอร์โทรผู้ซ่อม", key="am_n_rphone")
    if st.button("💾 บันทึกส่งซ่อม", type="primary", key="am_n_save"):
        import datetime as _dt
        now = _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        rdf = read_sheet(SHEET_ASSET_REPAIRS)
        rid = next_id(rdf, "repair_id", "REP")
        try:
            append_row(SHEET_ASSET_REPAIRS, {
                "repair_id": rid, "item_id": str(item_id),
                "send_date": str(send_date), "symptom": symptom.strip(),
                "repair_shop": repair_shop.strip(), "repair_shop_phone": repair_shop_phone.strip(),
                "repairer_name": repairer_name.strip(), "repairer_phone": repairer_phone.strip(),
                "how_repaired": "", "return_date": "", "status": "ส่งซ่อม",
                "created_at": now, "updated_at": now,
            })
            st.success(f"✅ บันทึกส่งซ่อมสำเร็จ ({rid})")
            st.rerun()
        except Exception as e:
            st.error(f"บันทึกไม่สำเร็จ: {e} (รัน roon_new_tables.sql)")


def _asset_repair_edit(item_id, reps, info):
    if reps is None or reps.empty:
        st.info("ยังไม่มีรายการซ่อมให้แก้ไข — ใช้ '➕ ส่งซ่อมใหม่' ก่อน")
        return
    opts = reps["repair_id"].astype(str).tolist()
    labels = {str(r["repair_id"]): f"{r['repair_id']} · ส่งซ่อม {r.get('send_date','')} · {r.get('status','')}"
              for _, r in reps.iterrows()}
    rid = st.selectbox("เลือกรายการซ่อมที่จะอัปเดต/รับคืน", opts,
                       format_func=lambda k: labels.get(k, k), key="am_e_sel")
    row = reps[reps["repair_id"].astype(str) == str(rid)].iloc[-1]
    info = info or {}

    # แสดงข้อมูลก่อนแก้ไข (ผู้ขาย/ผู้ซ่อม/รายละเอียดการซ่อม)
    st.markdown(
        f"> **ผู้ขาย:** {info.get('seller') or '-'} · **เบอร์ผู้ขาย:** {info.get('seller_phone') or '-'}\n"
        f">\n> **ผู้ซ่อม:** {row.get('repairer_name') or '-'} · **เบอร์ผู้ซ่อม:** {row.get('repairer_phone') or '-'}\n"
        f">\n> **วันที่ส่งซ่อม:** {row.get('send_date') or '-'} · **อาการเสีย:** {row.get('symptom') or '-'}\n"
        f">\n> **ซ่อมอย่างไร:** {row.get('how_repaired') or '-'} · **วันที่รับคืน:** {row.get('return_date') or '-'}")

    c1, c2 = st.columns(2)
    with c1:
        symptom = st.text_area("อาการเสีย", value=str(row.get("symptom", "")), key=f"am_e_sym_{rid}", height=70)
        repair_shop = st.text_input("ร้านซ่อม", value=str(row.get("repair_shop", "")), key=f"am_e_shop_{rid}")
        repair_shop_phone = st.text_input("เบอร์โทรร้านซ่อม", value=str(row.get("repair_shop_phone", "")), key=f"am_e_shopph_{rid}")
    with c2:
        repairer_name = st.text_input("ชื่อผู้ซ่อม", value=str(row.get("repairer_name", "")), key=f"am_e_rname_{rid}")
        repairer_phone = st.text_input("เบอร์โทรผู้ซ่อม", value=str(row.get("repairer_phone", "")), key=f"am_e_rphone_{rid}")

    st.markdown("**✅ เมื่อซ่อมเสร็จ / รับคืน**")
    c3, c4 = st.columns(2)
    with c3:
        cur_ret = str(row.get("return_date", "")).strip()
        has_ret = st.checkbox("รับคืนแล้ว", value=bool(cur_ret), key=f"am_e_hasret_{rid}")
        return_date = ""
        if has_ret:
            try:
                dval = datetime.date.fromisoformat(cur_ret[:10]) if cur_ret else datetime.date.today()
            except Exception:
                dval = datetime.date.today()
            return_date = str(st.date_input("วันที่รับคืน", value=dval, key=f"am_e_ret_{rid}"))
    with c4:
        how_repaired = st.text_area("วิธีการซ่อม / ซ่อมอย่างไร", value=str(row.get("how_repaired", "")),
                                    key=f"am_e_how_{rid}", height=70)

    if st.button("💾 บันทึกการอัปเดต", type="primary", key=f"am_e_save_{rid}"):
        import datetime as _dt
        status = "รับคืนแล้ว" if (has_ret and return_date) else "ส่งซ่อม"
        try:
            update_row(SHEET_ASSET_REPAIRS, "repair_id", str(rid), {
                "symptom": symptom.strip(), "repair_shop": repair_shop.strip(),
                "repair_shop_phone": repair_shop_phone.strip(),
                "repairer_name": repairer_name.strip(), "repairer_phone": repairer_phone.strip(),
                "how_repaired": how_repaired.strip(), "return_date": return_date, "status": status,
                "updated_at": _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            })
            st.success("✅ อัปเดตรายการซ่อมสำเร็จ")
            st.rerun()
        except Exception as e:
            st.error(f"อัปเดตไม่สำเร็จ: {e}")


def _asset_repair_delete(reps):
    if reps is None or reps.empty:
        st.info("ยังไม่มีรายการซ่อมให้ลบ")
        return
    opts = reps["repair_id"].astype(str).tolist()
    labels = {str(r["repair_id"]): f"{r['repair_id']} · ส่งซ่อม {r.get('send_date','')}"
              for _, r in reps.iterrows()}
    rid = st.selectbox("เลือกรายการซ่อมที่จะลบ", opts,
                       format_func=lambda k: labels.get(k, k), key="am_d_sel")
    if st.button("🗑️ ลบรายการนี้", key="am_d_btn"):
        try:
            delete_row(SHEET_ASSET_REPAIRS, "repair_id", str(rid))
            st.success("ลบรายการซ่อมแล้ว")
            st.rerun()
        except Exception as e:
            st.error(f"ลบไม่สำเร็จ: {e}")
