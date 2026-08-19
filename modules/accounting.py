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
    SHEET_SALES_RECONCILE, SHEET_MARKETING_POS_RECONCILE,
    SHEET_BRANCH_SALES, SHEET_BRANCH_SALES_DELIVERY,
    SHEET_AUDIT_STOCK_BALANCE,
)
from modules.excel_db import (
    read_sheet, write_sheet, append_row, update_row, delete_row, init_workbook,
)
from utils.id_generator import next_id


def _num(v):
    try:
        return float(str(v).replace(",", "").strip() or 0)
    except Exception:
        return 0.0


def _branches_all():
    """รายชื่อสาขา — ใช้ branch_auth (20 สาขาต้นฉบับ) เป็นหลัก + เสริมจากตาราง branches"""
    result = {}
    try:
        from modules.branch_auth import BRANCH_NAMES, BRANCH_LOGIN_SEED
        for b in BRANCH_LOGIN_SEED.keys():
            result[b] = BRANCH_NAMES.get(b, b)
    except Exception:
        pass
    try:
        df = read_sheet(SHEET_BRANCHES)
        if not df.empty and "branch_id" in df.columns and "branch_name" in df.columns:
            for _, r in df.iterrows():
                bid = str(r["branch_id"]).strip()
                if bid and bid not in result:
                    result[bid] = str(r["branch_name"]).strip() or bid
    except Exception:
        pass
    return result


# บรรจุภัณฑ์ที่ก่อให้เกิดยอดขาย (ใช้ตรวจยอดจากบรรจุภัณฑ์ที่ใช้ไป) — (field, ชื่อ, ราคาเริ่มต้น)
PKG_REVENUE = [
    ("box_qty",           "ขนมไข่ กล่อง (20 ชิ้น)", 130.0),
    ("bag_qty",           "ขนมไข่ ถุง (10 ชิ้น)",   70.0),
    ("drip_box_qty",      "กล่องดริป",              0.0),
    ("water_cup_qty",     "แก้วน้ำ (เครื่องดื่ม)",  0.0),
    ("ice_cream_cup_qty", "แก้วไอศครีม",            0.0),
]

# ตรวจยอดจากบรรจุภัณฑ์ (รอบ 16/5/2569): แต่ละชนิดมีหลายราคา (จำนวน+ราคา หลายช่อง)
#   (field | None, ชื่อ, จำนวนช่องราคา)
PKG_CHECK = [
    ("box_qty",                "ขนมไข่ กล่อง (20 ชิ้น)", 5),
    ("bag_qty",                "ขนมไข่ ถุง (10 ชิ้น)",   5),
    ("drip_box_qty",           "กล่องดริป",              5),
    ("water_cup_qty",          "แก้วน้ำ",                5),
    ("ice_cream_cup_qty",      "แก้วไอศกรีม",            5),
    ("ice_cream_cone",         "โคนไอศกรีม",             5),   # ไม่มียอดใช้ไปในระบบ (กรอกเอง)
    ("yellow_premium_bag_qty", "ถุงหูหิ้วกระดาษพิมพ์ลาย", 2),   # 2 ราคา: 0 (แถมฟรี) + ราคา
]
# ชนิดที่มี "ยอดใช้ไป" บันทึกไว้ในระบบ (branch_sales_delivery)
FIELDS_WITH_USAGE = {"box_qty", "bag_qty", "drip_box_qty", "water_cup_qty",
                     "ice_cream_cup_qty", "yellow_premium_bag_qty"}

# แมปชนิดบรรจุภัณฑ์ (reconcile) → คอลัมน์ที่ฝ่ายตรวจสอบนับ (audit_stock_balance)
#   ใช้ยอดที่ฝ่ายตรวจสอบตรวจ ไม่ใช้ยอดที่สาขาแจ้ง (กันสาขาแจ้งเท็จ)
AUDIT_MAP = {
    "box_qty":                "plastic_box_qty",       # ขนมไข่กล่อง (กล่องพลาสติก/กล่องใส)
    "bag_qty":                "paper_bag_qty",          # ขนมไข่ถุง (ถุงกระดาษ)
    "yellow_premium_bag_qty": "printed_carry_bag_qty",  # ถุงหูหิ้วกระดาษพิมพ์ลาย
    "water_cup_qty":          "water_cup_qty",          # แก้วน้ำ
    "ice_cream_cup_qty":      "ice_cream_cup_qty",      # แก้วไอศกรีม
    # drip_box_qty, ice_cream_cone → ไม่มีในการตรวจนับ (กรอกเอง)
}


def _audit_used(branch_id, d_date):
    """บรรจุภัณฑ์ที่ 'ใช้ไปจริง' ของวัน D-1 = ยอดตรวจเช้าวัน D-1 − ยอดตรวจเช้าวัน D
    (ใช้ยอดจากฝ่ายตรวจสอบ audit_stock_balance) — คืน (used_dict, dateA(D-1), dateB(D), foundA, foundB)
    """
    import datetime as _dt
    used = {}
    try:
        dB = d_date if isinstance(d_date, _dt.date) else _dt.date.fromisoformat(str(d_date)[:10])
    except Exception:
        return used, "", "", False, False
    dA = dB - _dt.timedelta(days=1)
    try:
        df = read_sheet(SHEET_AUDIT_STOCK_BALANCE)
    except Exception:
        df = pd.DataFrame()
    if df is None or df.empty or "branch_id" not in df.columns:
        return used, str(dA), str(dB), False, False

    def _row(dt):
        m = df[(df["branch_id"].astype(str).str.strip() == str(branch_id)) &
               (df["audit_date"].astype(str).str[:10] == str(dt))]
        return m.iloc[-1] if not m.empty else None
    rA = _row(dA)   # เช้าวัน D-1
    rB = _row(dB)   # เช้าวัน D
    for af in set(AUDIT_MAP.values()):
        va = _num(rA.get(af, 0)) if rA is not None else 0
        vb = _num(rB.get(af, 0)) if rB is not None else 0
        used[af] = max(va - vb, 0)   # ยอดที่ลดลง = ใช้ไป
    return used, str(dA), str(dB), (rA is not None), (rB is not None)


def _pkg_usage_all(sale_id, fields):
    """ผลรวมบรรจุภัณฑ์ที่ใช้ไป (ทุกช่องทาง) ของบิลนั้น สำหรับ fields ที่ระบุ"""
    res = {f: 0.0 for f in fields}
    if not sale_id:
        return res
    try:
        d = read_sheet(SHEET_BRANCH_SALES_DELIVERY)
    except Exception:
        d = pd.DataFrame()
    if d is None or d.empty or "sale_id" not in d.columns:
        return res
    d = d[d["sale_id"].astype(str) == str(sale_id)]
    for f in fields:
        if f in d.columns:
            res[f] = sum(_num(x) for x in d[f].tolist())
    return res

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
    SHEET_MARKETING_POS_RECONCILE: [
        "reconcile_id","sales_date","branch_id","channel_id","channel_name",
        "pos_num_items","pos_total",
        "branch_cash","branch_transfer","branch_coupon","branch_total",
        "pkg_expected_total","diff_amount","diff_flag",
        "diff_reason","diff_solution","created_by","created_at","updated_at",
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


def _product_price_map():
    """product_id → ราคาขาย/ชิ้น (ดึงจากคอลัมน์ price หรือ selling_cost/unit_price)"""
    out = {}
    try:
        df = read_sheet(SHEET_PRODUCTS)
    except Exception:
        df = pd.DataFrame()
    if df is None or df.empty or "product_id" not in df.columns:
        return out
    price_col = None
    for c in ("price", "selling_cost", "unit_price", "sell_price", "standard_price"):
        if c in df.columns:
            price_col = c
            break
    for _, r in df.iterrows():
        pid = str(r.get("product_id", "")).strip()
        if pid:
            out[pid] = _num(r.get(price_col, 0)) if price_col else 0.0
    return out


# ══════════════════════════════════════════════════════════════════════
def render():
    _init_mkt_sheets()
    st.title("📢 Marketing & Sales Reconcile")
    st.caption("บันทึกยอดขายฝ่ายการตลาด • Reconcile เทียบยอด 3 ฝ่าย")

    tab1, tab2, tab3 = st.tabs([
        "📝 บันทึกยอดขายการตลาด",
        "🔍 ตรวจยอดขาย POS เทียบ สาขา",
        "🔁 Reconcile 3 ฝ่าย (เดิม)",
    ])
    with tab1: _render_marketing_sales()
    with tab2: _render_pos_reconcile()
    with tab3: _render_reconcile()


# ══════════════════════════════════════════════════════════════════════
# TAB 1 : บันทึกยอดขายการตลาด
# ══════════════════════════════════════════════════════════════════════
def _render_marketing_sales():
    st.subheader("📝 บันทึกยอดขายฝ่ายการตลาด")
    branches  = _branches_dict()
    channels  = _channels_dict()
    products  = _products_dict()

    st.markdown("#### ข้อมูลหลัก")
    c1, c2, c3 = st.columns(3)
    with c1:
        sales_date = st.date_input("📅 วันที่", value=datetime.date.today(), key="mkt_date")
    with c2:
        branch_id = st.selectbox("🏪 สาขา",
                                  list(branches.keys()) if branches else [""],
                                  format_func=lambda k: f"{k} – {branches.get(k,'')}" if k else "–",
                                  key="mkt_branch")
    with c3:
        channel_id = st.selectbox("📡 ช่องทางขาย",
                                   list(channels.keys()) if channels else [""],
                                   format_func=lambda k: f"{k} – {channels.get(k,'')}" if k else "–",
                                   key="mkt_channel")
    c1, c2 = st.columns(2)
    with c1: created_by = st.text_input("👤 บันทึกโดย", key="mkt_by")
    with c2: remark     = st.text_input("📝 หมายเหตุ", key="mkt_remark")

    st.markdown("#### รายการสินค้าที่ขาย")
    st.caption("เลือกชื่อสินค้า — ระบบดึง 'ราคา/ชิ้น' ที่บันทึกไว้มาให้ก่อน (แก้ไขได้)")
    price_map = _product_price_map()
    num_items = st.number_input("จำนวนรายการ", min_value=1, max_value=20, value=1, step=1, key="mkt_num")
    item_rows = []
    if products:
        prod_keys = list(products.keys())
        for i in range(int(num_items)):
            c1, c2, c3 = st.columns([3, 1, 2])
            with c1:
                sel = st.selectbox(f"สินค้า #{i+1}", prod_keys,
                                   format_func=lambda k: f"{k} – {products[k]}",
                                   key=f"mkt_prod_{i}")
            # ดึงราคาขายจากระบบเมื่อ 'เลือกสินค้า' — เปลี่ยนสินค้า รีเซ็ตเป็นราคาระบบ (แก้ไขต่อได้)
            sys_price = float(_num(price_map.get(str(sel), 0)))
            prev_key = f"mkt_prod_prev_{i}"
            price_key = f"mkt_price_{i}"
            if st.session_state.get(prev_key) != sel:
                st.session_state[price_key] = sys_price
                st.session_state[prev_key] = sel
            with c2:
                qty = st.number_input(f"จำนวน #{i+1}", min_value=0, step=1, key=f"mkt_qty_{i}")
            with c3:
                price = st.number_input(f"ราคา/ชิ้น #{i+1}", min_value=0.0, step=1.0,
                                         format="%.2f", key=price_key)
            item_rows.append((sel, qty, price))
    else:
        st.info("ยังไม่มีสินค้าในระบบ — กรุณาเพิ่มที่ Master Data ก่อน")

    total_sales = sum(q * p for _, q, p in item_rows if q > 0)
    st.metric("💰 ยอดขายรวม", f"฿{total_sales:,.2f}")
    submitted = st.button("💾 บันทึก", type="primary", key="mkt_save")

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
# TAB 2 (ใหม่) : ตรวจยอดขาย POS เทียบ สาขา (+ ตรวจจากบรรจุภัณฑ์)
# ══════════════════════════════════════════════════════════════════════
def _branch_sales_for(branch_id, date_str):
    """คืน (cash, transfer, coupon, total, sale_id) จาก branch_sales ของสาขา+วันที่"""
    try:
        df = read_sheet(SHEET_BRANCH_SALES)
    except Exception:
        df = pd.DataFrame()
    if df is None or df.empty or "branch_id" not in df.columns:
        return (0.0, 0.0, 0.0, 0.0, "")
    m = df[(df["branch_id"].astype(str).str.strip() == str(branch_id)) &
           (df["sale_date"].astype(str).str[:10] == str(date_str)[:10])]
    if m.empty:
        return (0.0, 0.0, 0.0, 0.0, "")
    r = m.iloc[-1]
    cash = _num(r.get("cash_amount", 0))
    transfer = _num(r.get("transfer_amount", 0))
    coupon = _num(r.get("coupon_amount", 0))
    return (cash, transfer, coupon, cash + transfer + coupon, str(r.get("sale_id", "")))


def _pkg_usage_for(sale_id):
    """ผลรวมบรรจุภัณฑ์ที่ใช้ไป (ทุกช่องทาง) ของบิลนั้น — dict{field: qty}"""
    res = {f: 0 for f, _l, _p in PKG_REVENUE}
    if not sale_id:
        return res
    try:
        d = read_sheet(SHEET_BRANCH_SALES_DELIVERY)
    except Exception:
        d = pd.DataFrame()
    if d is None or d.empty or "sale_id" not in d.columns:
        return res
    d = d[d["sale_id"].astype(str) == str(sale_id)]
    for f, _l, _p in PKG_REVENUE:
        if f in d.columns:
            res[f] = sum(_num(x) for x in d[f].tolist())
    return res


def _pkg_default_prices():
    """ราคาต่อชนิดเริ่มต้น — พยายามอ่านจากตาราง products (กล่อง=20 ชิ้น, ถุง=10 ชิ้น)"""
    prices = {f: p for f, _l, p in PKG_REVENUE}
    try:
        pdf = read_sheet(SHEET_PRODUCTS)
        if pdf is not None and not pdf.empty and "product_name" in pdf.columns:
            name = pdf["product_name"].astype(str)
            price = pdf.get("price", pdf.get("selling_cost", pd.Series([0] * len(pdf))))
            for i in range(len(pdf)):
                nm = name.iloc[i]
                pr = _num(price.iloc[i])
                if pr <= 0:
                    continue
                if "20" in nm or "กล่อง" in nm:
                    prices["box_qty"] = pr
                elif "10" in nm or ("ถุง" in nm and "หูหิ้ว" not in nm):
                    prices["bag_qty"] = pr
    except Exception:
        pass
    return prices


def _render_pos_reconcile():
    st.subheader("🔍 ตรวจยอดขาย: POS เทียบกับ สาขา")
    st.caption("พนักงานการตลาดกรอกยอดจาก POS แล้วระบบเทียบกับยอดที่สาขาบันทึก (เงินสด+โอน+คูปอง)")

    branches = _branches_all()
    channels = _channels_dict()
    c1, c2, c3 = st.columns(3)
    with c1:
        sales_date = st.date_input("📅 วันที่คำนวณเงิน (วันนี้ = D)",
                                   value=datetime.date.today(), key="pr_date")
    with c2:
        branch_id = st.selectbox(
            "🏪 สาขา", list(branches.keys()) if branches else [""],
            format_func=lambda k: f"{k} – {branches.get(k,'')}" if k else "–", key="pr_branch")
    with c3:
        ch_keys = list(channels.keys()) if channels else [""]
        channel_id = st.selectbox(
            "📡 ช่องทางการขาย", ch_keys,
            format_func=lambda k: f"{k} – {channels.get(k,'')}" if k else "–", key="pr_channel")
    channel_name = channels.get(channel_id, "")
    is_front = ("หน้าร้าน" in str(channel_name)) or (str(channel_id) in ("01", "CH001"))
    # ได้ข้อมูลยอดขายของ "เมื่อวาน" (D-1) ตอน ~22:00 มาบันทึกเช้าวันนี้ (D)
    # → ยอดที่นำมาเทียบทั้งหมด (POS / สาขา / บรรจุภัณฑ์) เป็นของวัน D-1
    target_date = sales_date - datetime.timedelta(days=1)
    date_str = str(target_date)   # วันที่ยอดขายจริงที่กำลังตรวจ = D-1
    st.info(f"🔎 กำลังตรวจยอดขายของวัน **D-1 = {target_date}**  "
            f"(บันทึก/คำนวณเช้าวันนี้ D = {sales_date})")

    # ── กันบันทึกซ้ำ (สาขา+วัน+ช่องทาง) ──
    rdf = read_sheet(SHEET_MARKETING_POS_RECONCILE)
    prev = None
    if rdf is not None and not rdf.empty and "branch_id" in rdf.columns:
        m = rdf[(rdf["branch_id"].astype(str) == str(branch_id)) &
                (rdf["sales_date"].astype(str) == date_str) &
                (rdf["channel_id"].astype(str) == str(channel_id))]
        if not m.empty:
            prev = m.iloc[-1]
    edit_mode = True
    if prev is not None:
        st.warning("⚠️ มีการบันทึกของสาขา/วันที่/ช่องทางนี้แล้ว")
        edit_mode = st.checkbox("✏️ ต้องการแก้ไขข้อมูลเดิม", value=False, key="pr_edit")
        if not edit_mode:
            st.info(f"POS: {_num(prev.get('pos_total',0)):,.2f} | "
                    f"สาขา: {_num(prev.get('branch_total',0)):,.2f} | "
                    f"ผล: {prev.get('diff_flag','')}")
            if str(prev.get("diff_reason", "")).strip():
                st.caption(f"เหตุผล DIFF: {prev.get('diff_reason')}")
            if str(prev.get("diff_solution", "")).strip():
                st.caption(f"การแก้ปัญหา: {prev.get('diff_solution')}")
            return

    # ── POS input (พิมพ์เอง) — เฉพาะหน้าร้าน ──
    st.markdown(f"#### ① ยอดจาก POS ของวัน {target_date} (พิมพ์กรอกเอง)")
    if not is_front:
        st.info("ℹ️ ยอด POS ใช้กับช่องทาง 'หน้าร้าน' เป็นหลัก — ช่องทางอื่นกรอกได้ถ้ามีข้อมูล")
    p1, p2 = st.columns(2)
    with p1:
        pos_num_items = st.number_input(
            "จำนวนรายการ (บิล) จาก POS", min_value=0, step=1,
            value=int(_num(prev.get("pos_num_items", 0))) if prev is not None else 0,
            key="pr_pos_items")
    with p2:
        pos_total = st.number_input(
            "ยอดขายรวมของวัน จาก POS (บาท)", min_value=0.0, step=1.0, format="%.2f",
            value=_num(prev.get("pos_total", 0)) if prev is not None else 0.0,
            key="pr_pos_total")

    # ── ยอดสาขา (จาก branch_sales) ──
    cash, transfer, coupon, branch_total, sale_id = _branch_sales_for(branch_id, date_str)
    st.markdown(f"#### ② ยอดที่สาขาบันทึก ของวัน {target_date} (จากบันทึกรายการขาย)")
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("💵 เงินสด", f"฿{cash:,.2f}")
    m2.metric("📲 เงินโอน", f"฿{transfer:,.2f}")
    m3.metric("🎟️ คูปอง", f"฿{coupon:,.2f}")
    m4.metric("รวม (สาขา)", f"฿{branch_total:,.2f}")
    if not sale_id:
        st.caption("⚠️ ยังไม่พบบันทึกรายการขายของสาขานี้ในวันดังกล่าว (ยอดสาขา = 0)")

    # ── DIFF ──
    diff = branch_total - pos_total
    if diff > 0.001:
        diff_flag = "DIFF +"
    elif diff < -0.001:
        diff_flag = "DIFF -"
    else:
        diff_flag = "OK"
    st.markdown("#### ③ ผลการเทียบ (สาขา − POS)")
    if diff_flag == "OK":
        st.success(f"✅ ยอดตรงกัน (สาขา = POS = ฿{branch_total:,.2f}) — ยืนยันได้เลย")
    elif diff_flag == "DIFF +":
        st.markdown(
            f"<div style='background:#1565C0;color:white;padding:12px;border-radius:8px;"
            f"font-size:18px;font-weight:bold;'>DIFF + : เงินจริงจากสาขา มากกว่า POS "
            f"฿{diff:,.2f}</div>", unsafe_allow_html=True)
    else:
        st.markdown(
            f"<div style='background:#C62828;color:white;padding:12px;border-radius:8px;"
            f"font-size:18px;font-weight:bold;'>DIFF − : เงินจริงจากสาขา น้อยกว่า POS "
            f"฿{abs(diff):,.2f}</div>", unsafe_allow_html=True)

    # ── ④ ตรวจยอดจากบรรจุภัณฑ์ที่ใช้ไปจริง (จากยอดที่ฝ่ายตรวจสอบนับ) ──
    st.markdown("#### ④ ตรวจยอดขายจากบรรจุภัณฑ์ที่ใช้ไปจริง (ของวัน D-1)")
    # D = วันที่คำนวณเงิน (วันที่เลือกด้านบน) → ตรวจบรรจุภัณฑ์ที่ใช้ไปจริงของวัน D-1
    # ใช้ไปจริงของ D-1 = ยอดที่ฝ่ายตรวจสอบนับเช้าวัน D-1 − นับเช้าวัน D  (ไม่ใช้ยอดที่สาขาแจ้ง)
    audit_used, dA, dB, foundA, foundB = _audit_used(branch_id, sales_date)
    st.caption(
        f"D = วันที่คำนวณเงิน = **{dB}** | ตรวจบรรจุภัณฑ์ที่ใช้ไปจริงของวัน **D-1 = {dA}**  \n"
        f"ยอดใช้ไปจริง = ยอดที่ฝ่ายตรวจสอบนับ **เช้าวัน {dA}** − นับ **เช้าวัน {dB}**  "
        f"(ไม่ใช้ยอดที่สาขาแจ้ง เพื่อกันการแจ้งเท็จ) | 1 ชนิดมีหลายราคา กรอก จำนวน+ราคา หลายช่อง "
        f"ผลรวมจำนวนต้องเท่ากับที่ใช้ไปจริง (ระบบดึงราคาให้ก่อน แก้ไขได้)")
    if not foundA or not foundB:
        miss = []
        if not foundA:
            miss.append(f"เช้าวัน {dA}")
        if not foundB:
            miss.append(f"เช้าวัน {dB}")
        st.warning(
            "⚠️ ยังไม่พบยอดตรวจนับของฝ่ายตรวจสอบ (" + " และ ".join(miss) + ") — "
            "ยอด 'ใช้ไปจริง' อาจไม่ครบ กรุณาให้ฝ่ายตรวจสอบบันทึกยอดคงเหลือก่อน")
    dprices = _pkg_default_prices()
    pkg_expected = 0.0
    for field, label, nprices in PKG_CHECK:
        af = AUDIT_MAP.get(field)          # คอลัมน์ยอดตรวจนับ (ถ้ามี)
        has_usage = af is not None
        used = _num(audit_used.get(af, 0)) if has_usage else None
        head = f"**{label}**"
        if has_usage:
            head += f" — ใช้ไปจริง (D-1) **{used:,.0f}**"
        else:
            head += " — (ไม่มีในการตรวจนับ กรอกจำนวนเอง)"
        st.markdown(head)
        sub = 0.0
        qsum = 0.0
        for i in range(nprices):
            cc = st.columns(2)
            # ราคาเริ่มต้น: ช่องแรกดึงจากระบบ (ถุงพิมพ์ลายช่องแรก = 0 แถมฟรี)
            if field == "yellow_premium_bag_qty" and i == 0:
                dp = 0.0
            else:
                dp = float(dprices.get(field, 0.0)) if i == 0 else 0.0
            q = _num(cc[0].number_input(f"จำนวน #{i+1} — {label}", min_value=0.0, step=1.0,
                                        key=f"pk_{field}_q{i}",
                                        label_visibility="collapsed" if i > 0 else "visible"))
            p = _num(cc[1].number_input(f"ราคา #{i+1} — {label}", min_value=0.0, step=1.0,
                                        value=dp, key=f"pk_{field}_p{i}",
                                        label_visibility="collapsed" if i > 0 else "visible"))
            sub += q * p
            qsum += q
        if has_usage and abs(qsum - used) > 0.001:
            st.warning(f"⚠️ {label}: ผลรวมจำนวนที่กรอก ({qsum:,.0f}) ต้องเท่ากับที่ใช้ไป ({used:,.0f})")
        st.caption(f"→ {label}: รวม ฿{sub:,.2f}")
        pkg_expected += sub

    # Topping (ไม่ใช้บรรจุภัณฑ์ — รับจำนวน + ราคา)
    st.markdown("**Topping (ไม่ใช้บรรจุภัณฑ์)**")
    tc = st.columns(2)
    top_q = _num(tc[0].number_input("จำนวน Topping", min_value=0.0, step=1.0, key="pk_topping_q"))
    top_p = _num(tc[1].number_input("ราคา Topping", min_value=0.0, step=1.0, key="pk_topping_p"))
    pkg_expected += top_q * top_p
    st.caption(f"→ Topping: รวม ฿{top_q*top_p:,.2f}")

    st.metric("💰 ยอดขายประมาณจากบรรจุภัณฑ์ + Topping", f"฿{pkg_expected:,.2f}")
    pdiff = branch_total - pkg_expected
    if abs(pdiff) < 0.001:
        st.caption("✅ ยอดจากบรรจุภัณฑ์ตรงกับยอดสาขา")
    else:
        st.caption(f"ℹ️ ต่างจากยอดสาขา {pdiff:+,.2f} บาท (ใช้ช่วยตรวจสอบความถูกต้อง)")

    # ── ⑤ เหตุผล/การแก้ปัญหา (เฉพาะเมื่อ DIFF) — แก้ไขภายหลังได้ ──
    diff_reason = str(prev.get("diff_reason", "")) if prev is not None else ""
    diff_solution = str(prev.get("diff_solution", "")) if prev is not None else ""
    if diff_flag != "OK":
        st.markdown("#### ⑤ กรณี DIFF — บันทึกเหตุผลและการแก้ปัญหา (แก้ไขภายหลังได้)")
        diff_reason = st.text_area("เหตุผลของการ DIFF", value=diff_reason, key="pr_reason", height=80)
        diff_solution = st.text_area("การแก้ปัญหา", value=diff_solution, key="pr_solution", height=80)

    created_by = st.text_input("👤 บันทึกโดย", key="pr_by")
    st.divider()
    btn_label = "💾 ยืนยัน/บันทึก" if diff_flag == "OK" else "💾 บันทึกผลตรวจ (DIFF)"
    if st.button(btn_label, type="primary", use_container_width=True, key="pr_save"):
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        payload = {
            "sales_date": date_str, "branch_id": branch_id,
            "channel_id": channel_id, "channel_name": channel_name,
            "pos_num_items": int(pos_num_items), "pos_total": pos_total,
            "branch_cash": cash, "branch_transfer": transfer, "branch_coupon": coupon,
            "branch_total": branch_total, "pkg_expected_total": round(pkg_expected, 2),
            "diff_amount": round(diff, 2), "diff_flag": diff_flag,
            "diff_reason": diff_reason.strip(), "diff_solution": diff_solution.strip(),
            "created_by": created_by.strip(), "updated_at": now,
        }
        try:
            if prev is not None:
                update_row(SHEET_MARKETING_POS_RECONCILE, "reconcile_id",
                           str(prev.get("reconcile_id")), payload)
            else:
                rid = next_id(rdf, "reconcile_id", "PR")
                payload["reconcile_id"] = rid
                payload["created_at"] = now
                append_row(SHEET_MARKETING_POS_RECONCILE, payload)
            st.success(f"✅ บันทึกผลตรวจสำเร็จ ({diff_flag})")
            st.rerun()
        except Exception as e:
            st.error(f"บันทึกไม่สำเร็จ: {e} (ตรวจว่ารัน roon_new_tables.sql แล้ว)")

    # ── ประวัติผลตรวจ ──
    st.divider()
    st.markdown("#### 📋 ประวัติผลตรวจ POS vs สาขา")
    if rdf is None or rdf.empty:
        st.info("ยังไม่มีประวัติ")
    else:
        disp = rdf.copy().sort_values("sales_date", ascending=False)
        show = pd.DataFrame({
            "วันที่": disp["sales_date"].astype(str),
            "สาขา": disp["branch_id"].astype(str),
            "ช่องทาง": disp.get("channel_name", "").astype(str),
            "POS": disp["pos_total"].map(lambda x: f"{_num(x):,.2f}"),
            "สาขา(฿)": disp["branch_total"].map(lambda x: f"{_num(x):,.2f}"),
            "ผล": disp["diff_flag"].astype(str),
            "เหตุผล DIFF": disp.get("diff_reason", "").astype(str),
        })
        st.dataframe(show, use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════
# TAB 3 : Reconcile เทียบยอด 3 ฝ่าย (เดิม)
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
    # ตัวหนังสือสีน้ำเงิน พื้นหลังสีเหลือง (ตามที่ ดร.วรรณ กำหนด)
    BG = "#FFF59D"   # เหลือง
    TX = "#0D47A1"   # น้ำเงิน
    header = "<tr>" + "".join(
        f"<th style='padding:8px;background:{TX};color:#FFFFFF;'>{h}</th>"
        for h in ["เปรียบเทียบ", "ฝ่าย A (บาท)", "ฝ่าย B (บาท)", "DIFF", "สถานะ"]
    ) + "</tr>"
    body = ""
    for label, a, b, diff in rows:
        is_diff = diff != 0
        td = f"padding:8px;background:{BG};color:{TX};"
        diff_txt = f"DIFF {diff:+,.2f}" if is_diff else "0"
        status_txt = "❌ DIFF" if is_diff else "✅ ตรง"
        cells  = f"<td style='{td}font-weight:bold;'>{label}</td>"
        cells += f"<td style='{td}text-align:right;'>฿{a:,.2f}</td>"
        cells += f"<td style='{td}text-align:right;'>฿{b:,.2f}</td>"
        cells += f"<td style='{td}text-align:center;font-weight:bold;'>{diff_txt}</td>"
        cells += f"<td style='{td}text-align:center;font-weight:bold;'>{status_txt}</td>"
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
