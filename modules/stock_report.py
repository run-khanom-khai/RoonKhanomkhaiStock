"""
stock_report.py  –  รายงานสรุปสาขา (สำหรับฝ่ายจัดซื้อ/สต็อก) — ดาวน์โหลด Excel ได้
ผู้พัฒนา: ดร.วรรณ (ดร.อภิวรรณ์ ดำแสงสวัสดิ์)

รวมข้อมูลจาก: ที่สาขาบันทึก (branch_sales, branch_stock_daily) + ฝ่ายตรวจบรรจุภัณฑ์ (audit_stock_balance)
วันที่ในรายงาน = วันที่ขายจริง (เลือกสาขา + ช่วงวันที่ · รายวัน)

ราคาอ้างอิง (ตามที่ ดร.วรรณ กำหนด):
  - ถุง 10 ชิ้น = 70 บาท
  - กล่อง 20 ชิ้น = 130 บาท

รายงาน 2 แบบ:
  1) แบบละเอียด (mirror ไฟล์แนบ 1)
  2) แบบกระทบยอด DIFF (+/-) + ช่องการแก้ปัญหาจากฝ่าย Audit
"""
import io
import datetime
import pandas as pd
import streamlit as st

from config import (
    SHEET_BRANCHES, SHEET_BRANCH_SALES, SHEET_BRANCH_SALES_DELIVERY,
    SHEET_BRANCH_STOCK_DAILY, SHEET_AUDIT_STOCK_BALANCE,
    SHEET_SALE_AUDIT_RESOLUTION, SHEET_SALE_AUDIT_CORRECTION,
    SHEET_PRODUCTS,
)
from modules.excel_db import read_sheet

# รหัสสินค้าอ้างอิงราคา (ดึงจากตารางสินค้า products) — ปรับราคาที่แอปหลังบ้านได้
BAG_PRODUCT_ID = "R-0001"   # ขนมไข่ ถุง (10 ชิ้น)
BOX_PRODUCT_ID = "R-0002"   # ขนมไข่ กล่อง (20 ชิ้น)
BAG_PRICE_DEFAULT = 70.0    # ค่าสำรองถ้ายังไม่ตั้งราคาในตารางสินค้า
BOX_PRICE_DEFAULT = 130.0


def _product_price_map():
    """product_id -> ราคาขาย (ดึงจากตารางสินค้า products)"""
    out = {}
    try:
        df = read_sheet(SHEET_PRODUCTS)
    except Exception:
        df = None
    if df is None or df.empty or "product_id" not in df.columns:
        return out
    pcol = next((c for c in ("price", "selling_cost", "unit_price", "sell_price", "ราคาขาย")
                 if c in df.columns), None)
    if not pcol:
        return out
    for _, r in df.iterrows():
        pid = str(r.get("product_id", "")).strip()
        if pid:
            out[pid] = _num(r.get(pcol, 0))
    return out


def _bag_box_prices():
    """คืน (bag_price, box_price) จากตารางสินค้า — ถ้าไม่พบ/เป็น 0 ใช้ค่าสำรอง 70/130"""
    pm = _product_price_map()
    bag = pm.get(BAG_PRODUCT_ID, 0) or 0
    box = pm.get(BOX_PRODUCT_ID, 0) or 0
    bag = bag if bag > 0 else BAG_PRICE_DEFAULT
    box = box if box > 0 else BOX_PRICE_DEFAULT
    return bag, box


def _num(v):
    try:
        f = float(str(v).replace(",", "").strip() or 0)
        return 0.0 if f != f else f
    except Exception:
        return 0.0


def _d10(v):
    return str(v)[:10]


def _branch_map():
    df = read_sheet(SHEET_BRANCHES)
    out = {}
    if df is not None and not df.empty and "branch_id" in df.columns:
        for _, r in df.iterrows():
            out[str(r["branch_id"]).strip()] = str(r.get("branch_name", "")).strip()
    return out


def _rows_by_date(sheet, date_col, branch_id):
    """คืน dict {date10: [rows...]} ของสาขานั้น"""
    out = {}
    try:
        df = read_sheet(sheet)
    except Exception:
        return out
    if df is None or df.empty or "branch_id" not in df.columns or date_col not in df.columns:
        return out
    m = df[df["branch_id"].astype(str).str.strip() == str(branch_id)]
    for _, r in m.iterrows():
        out.setdefault(_d10(r[date_col]), []).append(r.to_dict())
    return out


def _sum_field(rows, field):
    return sum(_num(r.get(field, 0)) for r in rows)


def _last_field(rows, field):
    return _num(rows[-1].get(field, 0)) if rows else 0.0


def _audit_used(audit_by_date, d, field):
    """บรรจุภัณฑ์ที่ใช้ไปของวัน d (จากฝ่ายตรวจนับ) = ยอดวัน d − ยอดวัน d+1
    คืน (used, ok) ; ok=False ถ้าไม่มีข้อมูลตรวจนับครบ 2 วัน"""
    d1 = _d10(d)
    d2 = _d10(d + datetime.timedelta(days=1))
    r1 = audit_by_date.get(d1)
    r2 = audit_by_date.get(d2)
    if not r1 or not r2:
        return 0.0, False
    used = _last_field(r1, field) - _last_field(r2, field)
    return used, True


def _collect_notes(sales_rows, deliv_rows):
    notes = []
    for r in sales_rows:
        t = str(r.get("remark", "") or "").strip()
        if t and t.lower() not in ("nan", "none"):
            notes.append(t)
    for r in deliv_rows:
        ch = str(r.get("channel", "") or "").strip()
        t = str(r.get("remark", "") or "").strip()
        if t and t.lower() not in ("nan", "none"):
            notes.append(f"{ch}: {t}" if ch else t)
    return " / ".join(dict.fromkeys(notes))   # กันซ้ำ


def _resolution_text(branch_id, d):
    """สรุป 'การแก้ปัญหา' ของวัน d จากฝ่าย Audit (ชี้แจง + เงินคืน)"""
    parts = []
    d10 = _d10(d)
    try:
        rdf = read_sheet(SHEET_SALE_AUDIT_RESOLUTION)
        if rdf is not None and not rdf.empty and "branch_id" in rdf.columns:
            m = rdf[(rdf["branch_id"].astype(str) == str(branch_id)) &
                    (rdf["sale_date"].astype(str).str[:10] == d10)]
            if not m.empty:
                r = m.iloc[-1]
                who = str(r.get("called_who", "") or "").strip()
                how = str(r.get("how_fixed", "") or "").strip()
                seg = []
                if who: seg.append(f"โทร: {who}")
                if how: seg.append(f"แก้ไข: {how}")
                if seg: parts.append(" · ".join(seg))
    except Exception:
        pass
    try:
        cdf = read_sheet(SHEET_SALE_AUDIT_CORRECTION)
        if cdf is not None and not cdf.empty and "branch_id" in cdf.columns:
            m = cdf[(cdf["branch_id"].astype(str) == str(branch_id)) &
                    (cdf["sale_date"].astype(str).str[:10] == d10)]
            for _, r in m.iterrows():
                amt = _num(r.get("amount", 0))
                rs = str(r.get("reason", "") or "").strip()
                parts.append(f"เงินคืนบริษัท {amt:,.0f} บาท" + (f" ({rs})" if rs else ""))
    except Exception:
        pass
    return " | ".join(parts)


def _build_rows(branch_id, d_from, d_to):
    """สร้างข้อมูลรายวันของสาขาในช่วงวันที่ (list of dict)"""
    sales = _rows_by_date(SHEET_BRANCH_SALES, "sale_date", branch_id)
    stock = _rows_by_date(SHEET_BRANCH_STOCK_DAILY, "stock_date", branch_id)
    deliv_all = read_sheet(SHEET_BRANCH_SALES_DELIVERY)
    audit = _rows_by_date(SHEET_AUDIT_STOCK_BALANCE, "audit_date", branch_id)
    bag_price, box_price = _bag_box_prices()   # ราคาจากตารางสินค้า (อัปเดตได้)

    rows_detail, rows_diff = [], []
    day = d_from
    while day <= d_to:
        d10 = _d10(day)
        s = sales.get(d10, [])
        stk = stock.get(d10, [])
        # delivery ของ sale วันนั้น
        deliv_rows = []
        if deliv_all is not None and not deliv_all.empty and "sale_id" in deliv_all.columns and s:
            sids = [str(r.get("sale_id", "")) for r in s]
            deliv_rows = deliv_all[deliv_all["sale_id"].astype(str).isin(sids)].to_dict("records")

        # ─ ใช้จากฝ่ายตรวจนับ (D − D+1) ─
        boxes_used, ok_b = _audit_used(audit, day, "plastic_box_qty")
        bags_used, ok_g = _audit_used(audit, day, "paper_bag_qty")
        roon_used, _   = _audit_used(audit, day, "printed_carry_bag_qty")
        pkg_ok = ok_b or ok_g

        bag_money = bags_used * bag_price
        box_money = boxes_used * box_price
        pkg_total = bag_money + box_money

        cash = _sum_field(s, "cash_amount")
        transfer = _sum_field(s, "transfer_amount")
        sale_total = cash + transfer

        lost = pkg_total - sale_total      # ยอดหาย (บรรจุภัณฑ์ > เงิน = เงินขาด)
        over = sale_total - pkg_total      # ยอดเกิน
        note = _collect_notes(s, deliv_rows)

        has_any = bool(s or stk or pkg_ok)
        if not has_any:
            day += datetime.timedelta(days=1)
            continue

        rows_detail.append({
            "วันที่": d10,
            "ไข่ที่ใช้": _sum_field(s, "eggs_used"),
            "แป้งใหญ่ที่ใช้": _sum_field(s, "flour_finished_big_used"),
            "แป้งเล็กที่ใช้": _sum_field(s, "flour_finished_small_used"),
            "ส่วนผสมใหญ่ที่ใช้": _sum_field(s, "mix_big_used"),
            "ส่วนผสมเล็กที่ใช้": _sum_field(s, "mix_small_used"),
            "ไข่เหลือ": _last_field(stk, "egg_remaining"),
            "ถุงกระดาษคงเหลือ": _last_field(stk, "paper_bag_qty"),
            "กล่องคงเหลือ": _last_field(stk, "plastic_box_qty"),
            "ถุงหูหิ้ว Roon คงเหลือ": _last_field(stk, "printed_carry_bag_qty"),
            "สายคาดคงเหลือ": _last_field(stk, "band_qty"),
            "ไม้เสียบคงเหลือ": _last_field(stk, "skewer_pack_qty"),
            "ถุงร้อนคงเหลือ": _last_field(stk, "hot_bag_pack_qty"),
            "เนยยังไม่แกะ": _last_field(stk, "butter_unopened_qty"),
            "ขนมเหลือ(กล่อง)": _sum_field(s, "leftover_box_qty"),
            "ขนมเหลือ(ชิ้น)": _sum_field(s, "leftover_loose_pieces"),
            "กล่องที่ใช้": boxes_used if pkg_ok else "",
            "ถุงที่ใช้": bags_used if pkg_ok else "",
            "ถุงหูหิ้ว Roon ที่ใช้": roon_used if pkg_ok else "",
            "ยอดรวมจากถุงกระดาษ": bag_money if pkg_ok else "",
            "ยอดรวมจากกล่อง": box_money if pkg_ok else "",
            "เงินโอน": transfer,
            "เงินสด": cash,
            "เงินสด+เงินโอน": sale_total,
            "ยอดรวมจากการขาย": sale_total,
            "ยอดรวมจากบรรจุภัณฑ์": pkg_total if pkg_ok else "",
            "ยอดหาย": (lost if pkg_ok else ""),
            "ยอดเกิน": (over if pkg_ok else ""),
            "หมายเหตุ": note,
        })

        # ─ รายงานแบบ DIFF ─
        diff = over   # = เงินขาย − บรรจุภัณฑ์ (ลบ = ขาด/หาย, บวก = เกิน)
        diff_str = ""
        if pkg_ok and abs(diff) > 0.5:
            diff_str = f"{diff:+,.0f}"
        elif pkg_ok:
            diff_str = "0"
        rows_diff.append({
            "วันที่": d10,
            "ยอดรวมจากการขาย": sale_total,
            "ยอดรวมจากบรรจุภัณฑ์": pkg_total if pkg_ok else "",
            "DIFF (+/-)": diff_str,
            "การแก้ปัญหา (ฝ่าย Audit)": _resolution_text(branch_id, day),
            "หมายเหตุ": note,
        })
        day += datetime.timedelta(days=1)

    return rows_detail, rows_diff


def _to_excel_bytes(df, sheet_name="Report"):
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name[:31])
        ws = writer.sheets[sheet_name[:31]]
        # ปรับความกว้างคอลัมน์คร่าวๆ
        for i, col in enumerate(df.columns, 1):
            width = max(10, min(28, int(df[col].astype(str).map(len).max() if len(df) else 10) + 4,
                                len(str(col)) + 4))
            ws.column_dimensions[ws.cell(row=1, column=i).column_letter].width = width
    return buf.getvalue()


# ══════════════════════════════════════════════════════════════════════
# RENDER
# ══════════════════════════════════════════════════════════════════════
def render():
    st.subheader("📈 รายงานสรุปสาขา (ดาวน์โหลด Excel)")
    _bag_p, _box_p = _bag_box_prices()
    st.caption("รวมข้อมูลที่สาขาบันทึก + ฝ่ายตรวจบรรจุภัณฑ์ · วันที่ = วันที่ขายจริง · "
               f"ราคาดึงจากตารางสินค้าอัตโนมัติ → ถุง 10 ชิ้น = {_bag_p:,.0f} บาท / "
               f"กล่อง 20 ชิ้น = {_box_p:,.0f} บาท "
               "(ปรับราคาได้ที่แอปหลังบ้าน → สินค้าสำเร็จรูป)")

    bmap = _branch_map()
    if not bmap:
        st.warning("ยังไม่มีข้อมูลสาขา")
        return

    c1, c2, c3 = st.columns([2, 1, 1])
    with c1:
        branch_id = st.selectbox("🏪 เลือกสาขา", list(bmap.keys()),
                                 format_func=lambda b: f"{b} – {bmap[b]}", key="sr_branch")
    with c2:
        d_from = st.date_input("📅 ตั้งแต่วันที่",
                               value=datetime.date.today() - datetime.timedelta(days=7),
                               key="sr_from")
    with c3:
        d_to = st.date_input("ถึงวันที่", value=datetime.date.today(), key="sr_to")

    if d_from > d_to:
        st.error("ช่วงวันที่ไม่ถูกต้อง (วันเริ่มต้องไม่เกินวันสิ้นสุด)")
        return

    if (d_to - d_from).days > 120:
        st.warning("ช่วงวันที่ยาวเกิน 120 วัน — แนะนำให้เลือกช่วงสั้นลงเพื่อความเร็ว")

    rows_detail, rows_diff = _build_rows(branch_id, d_from, d_to)
    if not rows_detail:
        st.info("ไม่มีข้อมูลในช่วงวันที่ที่เลือก")
        return

    bname = bmap.get(branch_id, "")
    tag = f"{branch_id}_{d_from}_{d_to}"

    t1, t2 = st.tabs(["1️⃣ รายงานละเอียด", "2️⃣ รายงานกระทบยอด DIFF + การแก้ปัญหา"])

    with t1:
        df1 = pd.DataFrame(rows_detail)
        st.dataframe(df1, use_container_width=True, hide_index=True)
        st.download_button(
            "⬇️ ดาวน์โหลด Excel (รายงานละเอียด)",
            data=_to_excel_bytes(df1, "รายงานละเอียด"),
            file_name=f"ROON_report_detail_{tag}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="sr_dl1", use_container_width=True)

    with t2:
        st.caption("DIFF = ยอดขาย − ยอดบรรจุภัณฑ์ · ค่าติดลบ = เงินขาด(หาย) · ค่าบวก = เงินเกิน · "
                   "ช่อง 'การแก้ปัญหา' ดึงจากที่ฝ่าย Audit บันทึก (โทร/แก้ไข + เงินคืนบริษัท)")
        df2 = pd.DataFrame(rows_diff)

        def _hl(v):
            s = str(v)
            if s.startswith("-"):
                return "color:#C62828;font-weight:700"
            if s.startswith("+"):
                return "color:#2E7D32;font-weight:700"
            return ""
        try:
            styled = df2.style.applymap(_hl, subset=["DIFF (+/-)"])
            st.dataframe(styled, use_container_width=True, hide_index=True)
        except Exception:
            st.dataframe(df2, use_container_width=True, hide_index=True)

        st.download_button(
            "⬇️ ดาวน์โหลด Excel (กระทบยอด DIFF)",
            data=_to_excel_bytes(df2, "กระทบยอด DIFF"),
            file_name=f"ROON_report_diff_{tag}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="sr_dl2", use_container_width=True)

    st.divider()
    st.caption("ℹ️ หมายเหตุ: คอลัมน์ 'กล่อง/ถุงที่ใช้' จะว่างถ้ายังไม่มีข้อมูลฝ่ายตรวจนับครบ 2 วัน "
               "(วันขาย และเช้าวันถัดไป) · ยอดขาย = เงินสด + เงินโอน")
