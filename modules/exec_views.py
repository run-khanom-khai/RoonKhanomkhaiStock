"""
exec_views.py  –  หน้าจอ "ดูอย่างเดียว" สำหรับผู้บริหาร (App ฝ่ายบริหาร)
ผู้พัฒนา: ดร.วรรณ (ดร.อภิวรรณ์ ดำแสงสวัสดิ์)

หลักการ:
  - ผู้บริหารดูรายงาน/ข้อมูลของทุกฝ่ายได้ แต่ "เพิ่ม/แก้ไข/ลบ ไม่ได้"
  - อ่านข้อมูลจากตารางตรง ๆ แล้วแสดงเป็นตาราง (แมปรหัส→ชื่อให้อ่านง่าย)
  - ไม่มีปุ่มบันทึก/แก้ไข/ลบ ใด ๆ ทั้งสิ้น
"""
import pandas as pd
import streamlit as st

from modules.excel_db import read_sheet
from config import (
    SHEET_BRANCHES, SHEET_ITEMS, SHEET_PRODUCTS, SHEET_EMPLOYEES,
    SHEET_BRANCH_GROUPS, SHEET_AREA_MASTER, SHEET_ITEM_CATEGORIES,
    SHEET_SALES_CHANNELS, SHEET_ROLES,
    SHEET_PAYROLL_PERIODS, SHEET_PAYROLL_RECORDS,
    SHEET_PRODUCTION_BATCHES, SHEET_PRODUCTION_MATERIAL_USED,
    SHEET_PURCHASE_ORDERS, SHEET_PURCHASE_ORDER_ITEMS,
    SHEET_STOCK_IN_TO_BRANCH, SHEET_STOCK_MOVEMENTS,
    SHEET_MATERIAL_DAILY, SHEET_MATERIAL_COST,
    SHEET_BRANCH_SALES, SHEET_BRANCH_SALES_DELIVERY,
    SHEET_BRANCH_STOCK_DAILY, SHEET_COUPONS,
    SHEET_AUDIT_STOCK_BALANCE,
    SHEET_BANK_ACCOUNTS, SHEET_BANK_TRANSACTIONS,
    SHEET_BRANCH_EXPENSES, SHEET_DAILY_SALES_ACCOUNTING,
    SHEET_MARKETING_DAILY_SALES, SHEET_MARKETING_DAILY_SALES_ITEMS,
    SHEET_SALES_RECONCILE, SHEET_MARKETING_POS_RECONCILE,
    SHEET_SALES_CHANNELS,
    SHEET_PETTY_CASH_REQUESTS, SHEET_PETTY_CASH_TRANSACTIONS,
)


# ══════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════
def _df(sheet):
    try:
        df = read_sheet(sheet)
        return df if df is not None else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


def _num(v):
    try:
        return float(str(v).replace(",", "").strip() or 0)
    except Exception:
        return 0.0


def _name_maps():
    """คืน dict แมป: item_id→ชื่อ, branch_id→ชื่อ, employee_id→ชื่อ"""
    items = _df(SHEET_ITEMS)
    branches = _df(SHEET_BRANCHES)
    emps = _df(SHEET_EMPLOYEES)
    im = (dict(zip(items["item_id"].astype(str), items["item_name"].astype(str)))
          if not items.empty and "item_id" in items.columns else {})
    bm = (dict(zip(branches["branch_id"].astype(str), branches["branch_name"].astype(str)))
          if not branches.empty and "branch_id" in branches.columns else {})
    em = {}
    if not emps.empty and "employee_id" in emps.columns:
        for _, r in emps.iterrows():
            nm = (str(r.get("first_name", "")).strip() + " " +
                  str(r.get("last_name", "")).strip()).strip()
            em[str(r.get("employee_id", ""))] = nm
    return im, bm, em


_ID_LABEL = {"item_id": "ชื่อรายการ", "branch_id": "ชื่อสาขา",
             "employee_id": "ชื่อพนักงาน"}


def _show(title, sheet, note=None, maxrows=1000, map_ids=True, drop_cols=None):
    """แสดงตารางแบบอ่านอย่างเดียว + แมปรหัสเป็นชื่อ
    map_ids=False → ไม่แทรกคอลัมน์ชื่อ (กรณีตารางมีคอลัมน์ชื่ออยู่แล้ว)
    drop_cols → รายชื่อคอลัมน์ที่ต้องการตัดออก
    """
    st.markdown(f"#### {title}")
    if note:
        st.caption(note)
    df = _df(sheet)
    if df.empty:
        st.info("— ยังไม่มีข้อมูล —")
        return
    show = df.copy()
    if drop_cols:
        show = show.drop(columns=[c for c in drop_cols if c in show.columns])
    if map_ids:
        im, bm, em = _name_maps()
        maps = {"item_id": im, "branch_id": bm, "employee_id": em}
        for col, mp in maps.items():
            if col in show.columns:
                newcol = _ID_LABEL[col]
                show.insert(show.columns.get_loc(col) + 1, newcol,
                            show[col].astype(str).map(mp).fillna(""))
    st.caption(f"ทั้งหมด {len(show):,} รายการ")
    st.dataframe(show.head(maxrows), use_container_width=True, hide_index=True)


def _kpi(cols_data):
    cols = st.columns(len(cols_data))
    for c, (label, val) in zip(cols, cols_data):
        c.metric(label, val)


# ══════════════════════════════════════════════════════════════
# SECTIONS (ดูอย่างเดียว)
# ══════════════════════════════════════════════════════════════
def view_master():
    st.title("🏪 ข้อมูลหลัก (Master Data) — ดูอย่างเดียว")
    t1, t2, t3 = st.tabs(["สาขา / สินค้า", "วัตถุดิบ-บรรจุภัณฑ์", "ตารางอ้างอิง"])
    with t1:
        # ตัดคอลัมน์ "ชื่อสาขา" ที่ซ้ำกับ branch_name (เป็นคอลัมน์จริงในตาราง)
        _show("🏪 สาขา (Branches)", SHEET_BRANCHES, map_ids=False,
              drop_cols=["ชื่อสาขา"])
        _show("🥚 สินค้าสำเร็จรูป (Products)", SHEET_PRODUCTS, map_ids=False,
              drop_cols=["ชื่อสินค้า", "ชื่อรายการ"])
    with t2:
        # ตัดคอลัมน์ชื่อที่ซ้ำ (item_name มีอยู่แล้ว) และตัด item_category_id
        _show("📦 วัตถุดิบ / บรรจุภัณฑ์ (Items)", SHEET_ITEMS,
              map_ids=False, drop_cols=["item_category_id", "ชื่อรายการ", "ชื่อสินค้า"])
    with t3:
        _show("🗂️ กลุ่มสาขา", SHEET_BRANCH_GROUPS)
        _show("🌏 พื้นที่ (Area)", SHEET_AREA_MASTER)
        _show("🏷️ หมวดหมู่สินค้า", SHEET_ITEM_CATEGORIES)
        _show("🛒 ช่องทางการขาย", SHEET_SALES_CHANNELS)
        _show("👤 บทบาท (Roles)", SHEET_ROLES)


def view_hr():
    st.title("👥 งานบุคคล (HR) — ดูอย่างเดียว")
    emps = _df(SHEET_EMPLOYEES)
    if emps.empty:
        st.info("— ยังไม่มีข้อมูลพนักงาน —")
        return

    def g(row, col):
        return str(row.get(col, "")) if col in emps.columns else ""

    # แยกไทย / ต่างด้าว (nationality ว่าง = ถือเป็นคนไทย)
    def _is_thai(row):
        nat = str(row.get("nationality", "")).strip()
        return (nat == "" or nat == "ไทย")

    im, bm, em = _name_maps()

    def _fullname(df):
        return (df.get("first_name", "").astype(str) + " " +
                df.get("last_name", "").astype(str)).str.strip()

    def _id_status(r):
        p = str(r.get("passport_no", "")).strip()
        m = str(r.get("mou_no", "")).strip()
        parts = []
        if p:
            parts.append(f"PASSPORT: {p}")
        if m:
            parts.append(f"MOU: {m}")
        return " | ".join(parts) if parts else "-"

    def _staff_cols(df, foreign=False):
        d = {
            "รหัส": df.get("employee_id", ""),
            "ชื่อ": _fullname(df),
            "ตำแหน่ง": df.get("position", ""),
            "ชื่อเล่น": df.get("nickname", ""),
            "อายุ": df.get("age", ""),
            "วันเกิด": df.get("birthdate", ""),
            "การศึกษา": df.get("education", ""),
            "เงินเดือน": df.get("salary", ""),
            "วันเริ่มทำงาน": df.get("start_date", ""),
            "เบอร์โทร": df.get("phone", ""),
            "ธนาคาร": df.get("bank_name", ""),
            "เลขที่บัญชี": df.get("bank_account_no", ""),
            "Promptpay": df.get("promptpay_no", ""),
        }
        if foreign:
            d["สัญชาติ"] = df.get("nationality", "")
            d["สถานะบัตร (PASSPORT/MOU)"] = df.apply(_id_status, axis=1)
        return pd.DataFrame(d)

    def _is_resigned(row):
        return str(row.get("status", "")).strip().lower() == "resigned" or \
               str(row.get("resign_date", "")).strip() != ""

    active = emps[~emps.apply(_is_resigned, axis=1)]

    t1, t2, t3, t4 = st.tabs([
        "🇹🇭 พนักงานสาขา (ไทย)", "🌏 พนักงานต่างด้าว",
        "🚪 พนักงานที่ลาออก", "💵 รอบการจ่ายเงินเดือน"])

    # ── 1) พนักงานสาขา (ไทย) — ไม่แก้ไข ──
    with t1:
        thai = active[active.apply(_is_thai, axis=1)]
        br_ids = sorted(thai["branch_id"].astype(str).str.strip().unique().tolist()) \
            if "branch_id" in thai.columns else []
        sel = st.selectbox("🏪 เลือกสาขา", ["ทั้งหมด"] + br_ids,
                           format_func=lambda k: k if k == "ทั้งหมด" else f"{k} – {bm.get(k, '')}",
                           key="hr_view_branch")
        show = thai
        if sel != "ทั้งหมด" and "branch_id" in show.columns:
            show = show[show["branch_id"].astype(str).str.strip() == sel]
        _kpi([("พนักงานไทยทั้งหมด", f"{len(thai):,}"), ("ในสาขาที่เลือก", f"{len(show):,}")])
        if show.empty:
            st.info("— ไม่มีข้อมูล —")
        else:
            st.dataframe(_staff_cols(show), use_container_width=True, hide_index=True)

    # ── 2) พนักงานต่างด้าว — เหมือนข้อ 1 + สัญชาติ + Passport/MOU ──
    with t2:
        foreign = active[~active.apply(_is_thai, axis=1)]
        _kpi([("พนักงานต่างด้าวทั้งหมด", f"{len(foreign):,}")])
        if foreign.empty:
            st.info("— ไม่มีพนักงานต่างด้าว —")
        else:
            st.dataframe(_staff_cols(foreign, foreign=True),
                         use_container_width=True, hide_index=True)

    # ── 3) พนักงานที่ลาออก ──
    with t3:
        resigned = emps[emps.apply(_is_resigned, axis=1)]
        _kpi([("พนักงานที่ลาออก", f"{len(resigned):,}")])
        if resigned.empty:
            st.info("— ไม่มีพนักงานที่ลาออก —")
        else:
            st.dataframe(pd.DataFrame({
                "รหัส": resigned.get("employee_id", ""),
                "ชื่อ": _fullname(resigned),
                "วันเริ่มงาน": resigned.get("start_date", ""),
                "วันลาออก": resigned.get("resign_date", ""),
                "สัญชาติ": resigned.get("nationality", ""),
                "เหตุผลที่ลาออก": resigned.get("resign_reason", ""),
            }), use_container_width=True, hide_index=True)

    # ── 4) รอบการจ่ายเงินเดือน ──
    with t4:
        _view_payroll_round(bm)


def _view_payroll_round(bm):
    import datetime as _dt
    st.markdown("#### 💵 รอบการจ่ายเงินเดือน")
    c1, c2 = st.columns(2)
    with c1:
        month = st.selectbox("เดือน", list(range(1, 13)),
                             index=_dt.date.today().month - 1,
                             format_func=lambda m: ["", "ม.ค.", "ก.พ.", "มี.ค.", "เม.ย.", "พ.ค.",
                                                    "มิ.ย.", "ก.ค.", "ส.ค.", "ก.ย.", "ต.ค.",
                                                    "พ.ย.", "ธ.ค."][m], key="pr_round_month")
    with c2:
        year_be = st.number_input("ปี (พ.ศ.)", min_value=2560, max_value=2600,
                                  value=_dt.date.today().year + 543, step=1, key="pr_round_year")

    # แผนที่กลุ่มสาขา (branch_id → branch_group_id) จากตาราง branches
    brdf = _df(SHEET_BRANCHES)
    grp_map = {}
    if not brdf.empty and "branch_id" in brdf.columns and "branch_group_id" in brdf.columns:
        grp_map = dict(zip(brdf["branch_id"].astype(str), brdf["branch_group_id"].astype(str)))

    periods = _df(SHEET_PAYROLL_PERIODS)
    recs = _df(SHEET_PAYROLL_RECORDS)
    emps = _df(SHEET_EMPLOYEES)

    pids = []
    if not periods.empty and "month" in periods.columns:
        pm = periods[(periods["month"].astype(str).str.zfill(2) == f"{int(month):02d}") &
                     (periods["year"].astype(str).isin([str(int(year_be)), str(int(year_be) - 543)]))]
        # แสดงรอบให้เลือก (ถ้ามีหลายรอบ)
        if not pm.empty:
            opt = ["ทุกรอบ"] + pm["payroll_period_id"].astype(str).tolist()
            sel_p = st.selectbox("รอบจ่าย", opt,
                                 format_func=lambda k: k if k == "ทุกรอบ" else
                                 f"{k} (รอบ {pm[pm['payroll_period_id'].astype(str)==k]['period_no'].values[0] if 'period_no' in pm.columns else ''}"
                                 f" • จ่าย {pm[pm['payroll_period_id'].astype(str)==k]['pay_date'].values[0] if 'pay_date' in pm.columns else ''})",
                                 key="pr_round_pid")
            pids = pm["payroll_period_id"].astype(str).tolist() if sel_p == "ทุกรอบ" else [sel_p]

    rows = []
    if pids and not recs.empty and "payroll_period_id" in recs.columns:
        rr = recs[recs["payroll_period_id"].astype(str).isin(pids)]
        emap = {}
        if not emps.empty and "employee_id" in emps.columns:
            for _, e in emps.iterrows():
                emap[str(e["employee_id"])] = e
        for _, r in rr.iterrows():
            e = emap.get(str(r.get("employee_id", "")), {})
            bid = str(e.get("branch_id", "")) if hasattr(e, "get") else ""
            nm = ((str(e.get("first_name", "")) + " " + str(e.get("last_name", ""))).strip()
                  if hasattr(e, "get") else str(r.get("employee_id", "")))
            pay = _num(r.get("net_income", 0)) or _num(r.get("wage_total", 0)) or \
                (_num(e.get("salary", 0)) if hasattr(e, "get") else 0)
            rows.append({"Branch_group_id": grp_map.get(bid, ""), "ชื่อสาขา": bm.get(bid, bid),
                         "_bid": bid, "ชื่อพนักงาน": nm, "เงินเดือน": pay})
    else:
        # ไม่มีข้อมูล payroll — แสดงรายชื่อพนักงานที่ยังทำงาน พร้อมเงินเดือนฐาน (ให้ดูภาพรวม)
        if not emps.empty:
            act = emps[emps.get("status", pd.Series([""] * len(emps))).astype(str).str.lower() != "resigned"] \
                if "status" in emps.columns else emps
            for _, e in act.iterrows():
                bid = str(e.get("branch_id", ""))
                rows.append({"Branch_group_id": grp_map.get(bid, ""), "ชื่อสาขา": bm.get(bid, bid),
                             "_bid": bid,
                             "ชื่อพนักงาน": (str(e.get("first_name", "")) + " " + str(e.get("last_name", ""))).strip(),
                             "เงินเดือน": _num(e.get("salary", 0))})
            st.caption("ℹ️ ยังไม่มีข้อมูลรอบเงินเดือนของเดือนนี้ — แสดงเงินเดือนฐานของพนักงานที่ยังทำงาน")

    if not rows:
        st.info("— ไม่มีข้อมูล —")
        return
    dfr = pd.DataFrame(rows).sort_values(["Branch_group_id", "_bid", "ชื่อพนักงาน"])
    total = dfr["เงินเดือน"].sum()
    dfr["เงินเดือน"] = dfr["เงินเดือน"].map(lambda x: f"{_num(x):,.2f}")
    st.dataframe(dfr.drop(columns=["_bid"]), use_container_width=True, hide_index=True)
    st.metric("รวมเงินเดือนทั้งหมด", f"฿{total:,.2f}")


def view_production():
    st.title("🏭 ฝ่ายผลิต (Production) — ดูอย่างเดียว")
    import datetime as _dt
    # ผู้บริหารดูผลผลิต 4 ชนิด + รวมวัตถุดิบที่ใช้ (ตามวันที่ผลิต)
    sel_date = st.date_input("📅 วันที่ผลิต", value=_dt.date.today(), key="exec_prod_date")
    ds = str(sel_date)[:10]
    bdf = _df(SHEET_PRODUCTION_BATCHES)
    st.markdown("#### 🥣 ผลผลิตของวันที่เลือก")
    OUT = [("finished_flour_big_bag", "แป้งสำเร็จรูป ถุงใหญ่"),
           ("finished_flour_small_bag", "แป้งสำเร็จรูป ถุงเล็ก"),
           ("ingredient_mix_big_bag", "ส่วนผสม ถุงใหญ่"),
           ("ingredient_mix_small_bag", "ส่วนผสม ถุงเล็ก")]
    batch_ids = []
    if not bdf.empty and "production_date" in bdf.columns:
        m = bdf[bdf["production_date"].astype(str).str[:10] == ds]
        if m.empty:
            st.caption(f"— ไม่มีการผลิตในวันที่ {sel_date} —")
        else:
            batch_ids = m["batch_id"].astype(str).tolist() if "batch_id" in m.columns else []
            cols = st.columns(4)
            for c, (f, lab) in zip(cols, OUT):
                tot = sum(_num(x) for x in m[f].tolist()) if f in m.columns else 0
                c.metric(lab, f"{tot:,.0f} ถุง")
    else:
        st.caption("— ยังไม่มีข้อมูลการผลิต —")

    # รวมวัตถุดิบที่ใช้ในการผลิต (แสดงเฉพาะผู้บริหาร)
    st.markdown("#### 🧪 รวมวัตถุดิบที่ใช้ในการผลิต (ของวันที่เลือก)")
    mu = _df(SHEET_PRODUCTION_MATERIAL_USED)
    if mu.empty or not batch_ids:
        st.caption("— ไม่มีข้อมูลวัตถุดิบที่ใช้ —")
    else:
        sub = mu[mu["batch_id"].astype(str).isin(batch_ids)] if "batch_id" in mu.columns else mu.iloc[0:0]
        if sub.empty:
            st.caption("— ไม่มีข้อมูลวัตถุดิบที่ใช้ —")
        else:
            names = {"RAW_FLOUR": "แป้ง", "RAW_SUGAR": "น้ำตาล", "RAW_SALT": "เกลือ"}
            agg = {}
            for _, r in sub.iterrows():
                iid = str(r.get("item_id", ""))
                agg[iid] = agg.get(iid, 0.0) + _num(r.get("qty_used", 0))
            st.dataframe(pd.DataFrame({
                "วัตถุดิบ": [names.get(k, k) for k in agg.keys()],
                "รวมที่ใช้ไป (กก.)": [f"{v:,.3f}" for v in agg.values()],
            }), use_container_width=True, hide_index=True)


def _stock_balance_df(branch=None):
    mv = _df(SHEET_STOCK_MOVEMENTS)
    if mv.empty or "item_id" not in mv.columns:
        return pd.DataFrame()
    d = mv.copy()
    if branch and branch != "ทั้งหมด":
        d = d[d["branch_id"].astype(str).str.strip() == branch]
    d["qty_in"] = pd.to_numeric(d.get("qty_in"), errors="coerce").fillna(0)
    d["qty_out"] = pd.to_numeric(d.get("qty_out"), errors="coerce").fillna(0)
    g = d.groupby(d["item_id"].astype(str)).agg(
        รับเข้า=("qty_in", "sum"), จ่ายออก=("qty_out", "sum")).reset_index()
    g["คงเหลือ"] = g["รับเข้า"] - g["จ่ายออก"]
    items = _df(SHEET_ITEMS)
    imap = (dict(zip(items["item_id"].astype(str), items["item_name"].astype(str)))
            if not items.empty and "item_id" in items.columns else {})
    mmap = {}
    if not items.empty and "min_stock" in items.columns:
        mmap = dict(zip(items["item_id"].astype(str),
                        items["min_stock"].map(_num)))
    g.insert(1, "ชื่อรายการ", g["item_id"].map(imap).fillna(g["item_id"]))
    g["ขั้นต่ำ (Min)"] = g["item_id"].map(mmap).fillna(0)
    g["ถึงขั้นต่ำ"] = g.apply(
        lambda r: "⚠️ ใช่" if (r["ขั้นต่ำ (Min)"] > 0 and
                                r["คงเหลือ"] <= r["ขั้นต่ำ (Min)"]) else "", axis=1)
    return g


def view_purchase():
    st.title("🛒 จัดซื้อ / สต๊อก (Purchase / Stock) — ดูอย่างเดียว")
    # ใช้หน้าจอแสดงผลชุดเดียวกับฝ่ายจัดซื้อ (อ่านอย่างเดียว ไม่มีการแก้ไข)
    try:
        import modules.purchase as P
    except Exception as e:
        st.error(f"โหลดโมดูลจัดซื้อไม่ได้: {e}")
        return
    t1, t2, t3 = st.tabs([
        "📅 รายการสั่งซื้อ (ตามวันที่)",
        "🚚 การเบิกเข้าสาขา (ตามวันที่)",
        "📊 สต๊อกคงเหลือ (เตือน min สีแดง)",
    ])
    with t1:
        P._render_purchase_view()
    with t2:
        P._render_stock_in_report()
    with t3:
        P._render_stock_balance()


def view_sales_pos():
    st.title("💵 รายได้ & ตรวจยอด (สาขา / POS / บรรจุภัณฑ์) — ดูอย่างเดียว")
    im, bm, em = _name_maps()
    ch = _df(SHEET_SALES_CHANNELS)
    ch_map = (dict(zip(ch["channel_id"].astype(str), ch["channel_name"].astype(str)))
              if not ch.empty and "channel_id" in ch.columns else {})

    c1, c2 = st.columns(2)
    with c1:
        import datetime as _dt
        sales_date = st.date_input("📅 วันที่", value=_dt.date.today(), key="vp_date")
    with c2:
        br_ids = sorted(list(bm.keys())) or [""]
        branch = st.selectbox("🏪 สาขา", br_ids,
                              format_func=lambda k: f"{k} – {bm.get(k,'')}" if k else "–",
                              key="vp_branch")
    ds = str(sales_date)

    # ── รายได้จากทุกช่องทางการขาย (จากยอดขายฝ่ายการตลาด) ──
    st.markdown("#### 📢 รายได้จากทุกช่องทางการขาย")
    md = _df(SHEET_MARKETING_DAILY_SALES)
    if md.empty or "sales_date" not in md.columns:
        st.info("— ยังไม่มีข้อมูลยอดขายการตลาด —")
    else:
        m = md[(md["sales_date"].astype(str).str[:10] == ds[:10]) &
               (md["branch_id"].astype(str) == str(branch))]
        if m.empty:
            st.caption("— ไม่มีข้อมูลของสาขา/วันที่นี้ —")
        else:
            rev = pd.DataFrame({
                "ช่องทาง": m["channel_id"].astype(str).map(lambda k: f"{k} – {ch_map.get(k, '')}"),
                "ยอดขาย (บาท)": m["total_sales"].map(lambda x: f"{_num(x):,.2f}"),
            })
            st.dataframe(rev, use_container_width=True, hide_index=True)
            st.metric("รวมรายได้ทุกช่องทาง", f"฿{m['total_sales'].map(_num).sum():,.2f}")

    # ── DIFF ทุกแบบ (สาขา / POS / บรรจุภัณฑ์) จากตารางตรวจยอด ──
    st.markdown("#### 🔍 ตรวจยอด: ยอดขายจริง(สาขา) vs POS vs บรรจุภัณฑ์")
    pr = _df(SHEET_MARKETING_POS_RECONCILE)
    if pr.empty or "sales_date" not in pr.columns:
        st.info("— ยังไม่มีข้อมูลการตรวจยอด (POS) —")
        return
    p = pr[(pr["sales_date"].astype(str) == ds) &
           (pr["branch_id"].astype(str) == str(branch))]
    if p.empty:
        st.caption("— ไม่มีข้อมูลตรวจยอดของสาขา/วันที่นี้ —")
        return
    for _, r in p.iterrows():
        chn = str(r.get("channel_name", "")) or str(r.get("channel_id", ""))
        branch_total = _num(r.get("branch_total", 0))
        pos_total = _num(r.get("pos_total", 0))
        pkg_total = _num(r.get("pkg_expected_total", 0))
        st.markdown(f"**ช่องทาง: {chn}**")
        k = st.columns(3)
        k[0].metric("💰 ยอดขายจริง (สาขา)", f"฿{branch_total:,.2f}")
        k[1].metric("🧾 ยอดจาก POS", f"฿{pos_total:,.2f}")
        k[2].metric("📦 ยอดจากบรรจุภัณฑ์", f"฿{pkg_total:,.2f}")
        d1 = branch_total - pos_total
        d2 = branch_total - pkg_total
        d3 = pos_total - pkg_total
        _kpi([("DIFF สาขา − POS", f"{d1:+,.2f}"),
              ("DIFF สาขา − บรรจุภัณฑ์", f"{d2:+,.2f}"),
              ("DIFF POS − บรรจุภัณฑ์", f"{d3:+,.2f}")])
        flag = str(r.get("diff_flag", ""))
        if flag and flag != "OK":
            st.markdown(
                f"<div style='background:#C62828;color:white;padding:8px;border-radius:6px;"
                f"font-weight:bold;'>สถานะ: {flag}</div>", unsafe_allow_html=True)
            if str(r.get("diff_reason", "")).strip():
                st.caption(f"เหตุผล DIFF: {r.get('diff_reason')}")
            if str(r.get("diff_solution", "")).strip():
                st.caption(f"การแก้ปัญหา: {r.get('diff_solution')}")
        else:
            st.success("สถานะ: ยอดตรง (OK)")
        st.divider()


def view_pnl():
    st.title("📈 กำไร-ขาดทุนสาขา (P&L) — ดูอย่างเดียว")
    st.caption("เลือกสาขาและเดือน/ปี เพื่อดูยอดสุทธิ (Net) = รายได้ − ค่าใช้จ่าย")
    im, bm, em = _name_maps()
    c1, c2, c3 = st.columns(3)
    with c1:
        br_ids = sorted(list(bm.keys())) or [""]
        branch = st.selectbox("🏪 สาขา", br_ids,
                              format_func=lambda k: f"{k} – {bm.get(k,'')}" if k else "–",
                              key="pnl_branch")
    import datetime as _dt
    with c2:
        month = st.selectbox("เดือน", list(range(1, 13)),
                             index=_dt.date.today().month - 1,
                             format_func=lambda m: ["", "ม.ค.", "ก.พ.", "มี.ค.", "เม.ย.", "พ.ค.",
                                                    "มิ.ย.", "ก.ค.", "ส.ค.", "ก.ย.", "ต.ค.",
                                                    "พ.ย.", "ธ.ค."][m], key="pnl_month")
    with c3:
        year_be = st.number_input("ปี (พ.ศ.)", min_value=2560, max_value=2600,
                                  value=_dt.date.today().year + 543, step=1, key="pnl_year")
    year_ce = int(year_be) - 543
    ym = f"{year_ce:04d}-{int(month):02d}"

    # ── รายได้: จาก branch_sales (เงินสด+โอน+คูปอง) ของสาขา/เดือน ──
    bs = _df(SHEET_BRANCH_SALES)
    revenue = 0.0
    if not bs.empty and "branch_id" in bs.columns:
        m = bs[(bs["branch_id"].astype(str) == str(branch)) &
               (bs["sale_date"].astype(str).str[:7] == ym)]
        for _, r in m.iterrows():
            revenue += (_num(r.get("cash_amount", 0)) + _num(r.get("transfer_amount", 0)) +
                        _num(r.get("coupon_amount", 0)))

    # ── ค่าใช้จ่าย: จาก branch_expenses (total_expense) ของสาขา/เดือน/ปี ──
    exp_df = _df(SHEET_BRANCH_EXPENSES)
    total_expense = 0.0
    exp_detail = {}
    EXP_FIELDS = [("hr_cost", "ค่าแรง/เงินเดือน"), ("cogs_cost", "ต้นทุนสินค้า"),
                  ("marketing_cost", "การตลาด"), ("water_cost", "ค่าน้ำ"),
                  ("electricity_cost", "ค่าไฟ"), ("rent_cost", "ค่าเช่า"),
                  ("mall_gp_cost", "GP ห้าง"), ("lineman_gp_cost", "GP LineMan"),
                  ("grab_gp_cost", "GP Grab"), ("transport_cost", "ค่าขนส่ง"),
                  ("operating_cost", "ค่าดำเนินการ"), ("other_cost", "อื่นๆ")]
    if not exp_df.empty and "branch_id" in exp_df.columns:
        me = exp_df[(exp_df["branch_id"].astype(str) == str(branch)) &
                    (exp_df.get("year", pd.Series([""] * len(exp_df))).astype(str) == str(year_ce)) &
                    (exp_df.get("month", pd.Series([""] * len(exp_df))).astype(str).str.zfill(2) == f"{int(month):02d}")]
        if me.empty:  # เผื่อ year เก็บเป็น พ.ศ.
            me = exp_df[(exp_df["branch_id"].astype(str) == str(branch)) &
                        (exp_df.get("year", pd.Series([""] * len(exp_df))).astype(str) == str(int(year_be))) &
                        (exp_df.get("month", pd.Series([""] * len(exp_df))).astype(str).str.zfill(2) == f"{int(month):02d}")]
        for _, r in me.iterrows():
            te = _num(r.get("total_expense", 0))
            if te <= 0:
                te = sum(_num(r.get(f, 0)) for f, _l in EXP_FIELDS)
            total_expense += te
            for f, lab in EXP_FIELDS:
                exp_detail[lab] = exp_detail.get(lab, 0.0) + _num(r.get(f, 0))

    net = revenue - total_expense
    st.divider()
    k = st.columns(3)
    k[0].metric("💰 รายได้ (Revenue)", f"฿{revenue:,.2f}")
    k[1].metric("💸 ค่าใช้จ่าย (Expense)", f"฿{total_expense:,.2f}")
    k[2].metric("📈 ยอดสุทธิ (Net)", f"฿{net:,.2f}",
                delta=("กำไร" if net >= 0 else "ขาดทุน"))

    if net >= 0:
        st.success(f"✅ สาขา {branch} – {bm.get(branch,'')} | เดือน {int(month)}/{int(year_be)} "
                   f"→ กำไรสุทธิ ฿{net:,.2f}")
    else:
        st.markdown(
            f"<div style='background:#C62828;color:white;padding:12px;border-radius:8px;"
            f"font-size:18px;font-weight:bold;'>ขาดทุนสุทธิ ฿{abs(net):,.2f}</div>",
            unsafe_allow_html=True)

    if exp_detail:
        st.markdown("#### รายละเอียดค่าใช้จ่าย")
        st.dataframe(pd.DataFrame({
            "รายการ": list(exp_detail.keys()),
            "จำนวนเงิน (บาท)": [f"{v:,.2f}" for v in exp_detail.values()],
        }), use_container_width=True, hide_index=True)
    st.caption("หมายเหตุ: รายได้คำนวณจากบันทึกรายการขายของสาขา (เงินสด+โอน+คูปอง) | "
               "ค่าใช้จ่ายจากเมนูค่าใช้จ่ายสาขา (Finance)")


def view_material():
    st.title("🧺 วัตถุดิบรายวัน / ต้นทุน — ดูอย่างเดียว")
    _show("🧺 วัตถุดิบรายวัน (Material Daily)", SHEET_MATERIAL_DAILY)
    _show("💲 ต้นทุนวัตถุดิบต่อหน่วย", SHEET_MATERIAL_COST)


def view_branch_ops():
    st.title("📊 ข้อมูลสาขา (ขาย/สต๊อก) — ดูอย่างเดียว")
    sales = _df(SHEET_BRANCH_SALES)
    if not sales.empty and "total_amount" in sales.columns:
        tot = sales["total_amount"].map(_num).sum()
        _kpi([("จำนวนบิลขาย", f"{len(sales):,}"),
              ("ยอดขายรวม", f"฿{tot:,.2f}")])
    t1, t2, t3 = st.tabs(["🧾 ยอดขายสาขา", "📦 สต๊อกสาขารายวัน", "🎟️ คูปอง"])
    with t1:
        _show("🧾 บันทึกรายการขายของสาขา", SHEET_BRANCH_SALES)
        _show("🛵 บรรจุภัณฑ์ที่ขาย (หน้าร้าน/Delivery)", SHEET_BRANCH_SALES_DELIVERY)
    with t2:
        _show("📦 บันทึกสต๊อกสาขารายวัน", SHEET_BRANCH_STOCK_DAILY)
    with t3:
        _show("🎟️ คูปอง", SHEET_COUPONS)


def view_audit():
    st.title("🔎 ฝ่ายตรวจสอบ (Audit) — ดูอย่างเดียว")
    im, bm, em = _name_maps()

    # ── ดูบรรจุภัณฑ์/วัตถุดิบ แยกตามชนิด — ทุกสาขา (+ ส่วนกลาง) ──
    st.markdown("#### 📦 บรรจุภัณฑ์ / วัตถุดิบ แยกตามชนิด (ทุกสาขา + ส่วนกลาง)")
    try:
        from modules.record_stock import STOCK_FIELDS, MATERIAL_FIELDS
        TYPES = [(k, lab, unit) for k, lab, unit in STOCK_FIELDS] + \
                [(k, lab, unit) for k, lab, unit in MATERIAL_FIELDS]
    except Exception:
        TYPES = []
    if not TYPES:
        st.info("— โหลดรายการชนิดไม่ได้ —")
    else:
        labels = [f"{lab} ({unit})" for _k, lab, unit in TYPES]
        sel_i = st.selectbox("เลือกชนิดบรรจุภัณฑ์/วัตถุดิบ", list(range(len(TYPES))),
                             format_func=lambda i: labels[i], key="au_type_sel")
        field, lab, unit = TYPES[sel_i]

        # ที่สาขา: ยอดล่าสุดจาก branch_stock_daily ต่อสาขา
        bsd = _df(SHEET_BRANCH_STOCK_DAILY)
        rows = []
        if not bsd.empty and "branch_id" in bsd.columns and field in bsd.columns:
            for bid, grp in bsd.groupby(bsd["branch_id"].astype(str).str.strip()):
                g2 = grp.sort_values("stock_date") if "stock_date" in grp.columns else grp
                last = g2.iloc[-1]
                rows.append({"สาขา": f"{bid} – {bm.get(bid, '')}",
                             f"คงเหลือ ({unit})": _num(last.get(field, 0)),
                             "วันที่นับล่าสุด": str(last.get("stock_date", ""))})
        st.markdown(f"**ชนิด: {lab} ({unit})** — ยอดคงเหลือที่สาขา (นับล่าสุด)")
        if rows:
            dfb = pd.DataFrame(rows).sort_values("สาขา")
            st.dataframe(dfb, use_container_width=True, hide_index=True)
            st.metric("รวมคงเหลือทุกสาขา", f"{sum(_num(r[f'คงเหลือ ({unit})']) for r in rows):,.0f} {unit}")
        else:
            st.caption("— ยังไม่มีข้อมูลการนับสต๊อกของสาขา —")

        # ที่ส่วนกลาง (ฝ่ายจัดซื้อ): จาก stock_movements CENTRAL (ถ้าชนิดนี้ผูกกับ item)
        st.markdown("**สต๊อกส่วนกลาง (ฝ่ายจัดซื้อ)**")
        mv = _df(SHEET_STOCK_MOVEMENTS)
        central = None
        if not mv.empty and "item_id" in mv.columns:
            im2 = im  # item_id → name
            # หา item ที่ชื่อใกล้เคียง label
            match_ids = [iid for iid, nm in im2.items() if lab and (lab in str(nm) or str(nm) in lab)]
            if match_ids:
                d = mv[(mv["branch_id"].astype(str).str.strip() == "CENTRAL") &
                       (mv["item_id"].astype(str).isin(match_ids))].copy()
                if not d.empty:
                    d["qty_in"] = pd.to_numeric(d["qty_in"], errors="coerce").fillna(0)
                    d["qty_out"] = pd.to_numeric(d["qty_out"], errors="coerce").fillna(0)
                    central = float(d["qty_in"].sum() - d["qty_out"].sum())
        if central is not None:
            st.metric(f"คงเหลือส่วนกลาง — {lab}", f"{central:,.0f} {unit}")
        else:
            st.caption("— ชนิดนี้ไม่ได้ติดตามที่สต๊อกส่วนกลาง (มีเฉพาะที่สาขา) —")

    st.divider()
    _show("🔎 ผลตรวจนับสต๊อกบรรจุภัณฑ์ (ประวัติ)", SHEET_AUDIT_STOCK_BALANCE)


def view_finance():
    st.title("💰 การเงินและบัญชี — ดูอย่างเดียว")
    _show("🏦 บัญชีธนาคาร", SHEET_BANK_ACCOUNTS)
    _show("💸 เงินเข้า / เงินออก", SHEET_BANK_TRANSACTIONS)
    _show("📋 ค่าใช้จ่ายสาขา", SHEET_BRANCH_EXPENSES)
    _show("📊 ยอดขายฝ่ายบัญชี", SHEET_DAILY_SALES_ACCOUNTING)


def view_marketing():
    st.title("📢 Marketing & Sales Reconcile — ดูอย่างเดียว")
    _show("📝 ยอดขายฝ่ายการตลาด", SHEET_MARKETING_DAILY_SALES)
    _show("📄 รายละเอียดยอดขายการตลาด", SHEET_MARKETING_DAILY_SALES_ITEMS)
    _show("🔍 Reconcile เทียบยอด 3 ฝ่าย", SHEET_SALES_RECONCILE)


def view_petty():
    st.title("💵 เงินสดย่อย — ดูอย่างเดียว")
    import datetime as _dt

    # ── รายการรอโอน (ตามวันที่) ──
    st.markdown("#### 💸 รายการรอโอน (Waiting Transfer)")
    as_of = st.date_input("📅 ณ วันที่", value=_dt.date.today(), key="petty_asof")
    req = _df(SHEET_PETTY_CASH_REQUESTS)
    if req.empty or "status" not in req.columns:
        st.info("— ยังไม่มีคำขอเบิก —")
    else:
        w = req[req["status"].astype(str).str.strip() == "waiting_transfer"].copy()
        if "request_date" in w.columns:
            w = w[w["request_date"].astype(str).str[:10] <= str(as_of)[:10]]
        if w.empty:
            st.success("✅ ไม่มีรายการรอโอน")
        else:
            disp = pd.DataFrame({
                "สาขา": (w.get("branch_id", "").astype(str) + " – " +
                        w.get("branch_name", "").astype(str)).str.strip(" –"),
                "ชื่อพนักงาน": w.get("employee_name", ""),
                "ยอดเงินรอโอน (บาท)": w.get("total_amount", "").map(lambda x: f"{_num(x):,.2f}"),
                "วันที่ขอ": w.get("request_date", ""),
            })
            st.dataframe(disp, use_container_width=True, hide_index=True)
            tot = w["total_amount"].map(_num).sum()
            k = st.columns(2)
            k[0].metric("รวมยอดรอโอนทั้งหมด", f"฿{tot:,.2f}")
            k[1].metric("จำนวนรายการรอโอน", f"{len(w):,} รายการ")

    st.divider()
    _show("🧾 คำขอเบิกเงินสดย่อย (ทั้งหมด)", SHEET_PETTY_CASH_REQUESTS)
    _show("💰 รายการโอน/จ่ายเงินสดย่อย", SHEET_PETTY_CASH_TRANSACTIONS)


# แมป section_key → ฟังก์ชัน
SECTIONS = {
    "view_master":      view_master,
    "view_hr":          view_hr,
    "view_production":  view_production,
    "view_purchase":    view_purchase,
    "view_material":    view_material,
    "view_branch_ops":  view_branch_ops,
    "view_audit":       view_audit,
    "view_finance":     view_finance,
    "view_marketing":   view_marketing,
    "view_petty":       view_petty,
    "view_sales_pos":   view_sales_pos,
    "view_pnl":         view_pnl,
}


def render(section_key):
    fn = SECTIONS.get(section_key)
    if fn is None:
        st.error("ไม่พบหน้าที่เลือก")
        return
    fn()
