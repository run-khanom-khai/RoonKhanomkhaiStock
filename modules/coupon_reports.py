"""
coupon_reports.py  –  รายงานคูปองสำหรับผู้บริหาร (Dashboard) — รอบ 20/8/2026
ผู้พัฒนา: ดร.วรรณ (ดร.อภิวรรณ์ ดำแสงสวัสดิ์)

6 รายงาน:
  1. คูปองออกทั้งหมด / ใช้แล้ว / หมดอายุ / ยกเลิก / ยังไม่ใช้
  2. คูปองที่ใช้แยกตามวัน–สาขา
  3. มูลค่าคูปอง: ใช้ไปแล้ว เทียบทั้งหมดในแคมเปญ (ตามช่วงวันที่)
  4. คูปองหมดอายุแต่ไม่ได้ใช้
  5. กระทบยอด: ยอดขายรวม–เงินรับจริง–คูปอง
  6. คูปองออกแล้วยังไม่หมดอายุ แต่ยังไม่ใช้
"""
import datetime
import pandas as pd
import streamlit as st

from config import (SHEET_COUPONS, SHEET_BRANCH_SALES_COUPONS,
                    SHEET_BRANCH_SALES, SHEET_BRANCHES)
from modules.excel_db import read_sheet


def _num(v):
    try:
        f = float(str(v).replace(",", "").strip() or 0)
        return 0.0 if f != f else f
    except Exception:
        return 0.0


def _parse_date(v):
    s = str(v).strip()
    if not s or s.lower() in ("nan", "none", "nat"):
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d"):
        try:
            return datetime.datetime.strptime(s[:10], fmt).date()
        except Exception:
            continue
    return None


def _is_expired(v):
    d = _parse_date(v)
    return d is not None and d < datetime.date.today()


def _is_used(status):
    return str(status).strip().lower() in ("used", "ใช้แล้ว")


def _is_cancelled(status):
    return str(status).strip().lower() in ("cancelled", "canceled", "ยกเลิก", "void")


def _branch_names():
    out = {}
    df = read_sheet(SHEET_BRANCHES)
    if df is not None and not df.empty and "branch_id" in df.columns:
        for _, r in df.iterrows():
            out[str(r["branch_id"]).strip()] = str(r.get("branch_name", "")).strip()
    return out


def _sale_date_map():
    """sale_id → sale_date (จาก branch_sales)"""
    out = {}
    df = read_sheet(SHEET_BRANCH_SALES)
    if df is not None and not df.empty and "sale_id" in df.columns:
        for _, r in df.iterrows():
            out[str(r["sale_id"]).strip()] = str(r.get("sale_date", ""))[:10]
    return out


def render():
    st.markdown(
        "<h1 style='color:#6A1B9A;font-size:1.7rem;font-weight:900;'>"
        "🎟️ รายงานคูปอง (ผู้บริหาร)</h1>", unsafe_allow_html=True)

    cdf = read_sheet(SHEET_COUPONS)
    if cdf is None or cdf.empty or "coupon_no" not in cdf.columns:
        st.info("ยังไม่มีข้อมูลคูปองในระบบ")
        return
    cdf = cdf.copy()

    tabs = st.tabs([
        "1️⃣ สรุปสถานะ", "2️⃣ ใช้ตามวัน-สาขา", "3️⃣ มูลค่าตามแคมเปญ",
        "4️⃣ หมดอายุไม่ได้ใช้", "5️⃣ กระทบยอด", "6️⃣ ยังไม่ใช้",
    ])

    status = cdf.get("status", pd.Series([""] * len(cdf)))
    exp = cdf.get("expire_date", pd.Series([""] * len(cdf)))
    used_mask = status.map(_is_used)
    cancel_mask = status.map(_is_cancelled)
    expired_mask = exp.map(_is_expired) & ~used_mask & ~cancel_mask
    active_mask = ~used_mask & ~cancel_mask & ~expired_mask
    amt = cdf.get("amount", pd.Series([0] * len(cdf))).map(_num)

    # ── 1. สรุปสถานะ ──
    with tabs[0]:
        st.subheader("คูปองออกทั้งหมด / ใช้แล้ว / หมดอายุ / ยกเลิก / ยังไม่ใช้")
        rows = [
            ("ออกทั้งหมด", len(cdf), amt.sum()),
            ("ใช้แล้ว", int(used_mask.sum()), amt[used_mask].sum()),
            ("หมดอายุ (ไม่ได้ใช้)", int(expired_mask.sum()), amt[expired_mask].sum()),
            ("ยกเลิก", int(cancel_mask.sum()), amt[cancel_mask].sum()),
            ("ยังไม่ใช้ (ใช้ได้)", int(active_mask.sum()), amt[active_mask].sum()),
        ]
        cols = st.columns(len(rows))
        for col, (label, n, v) in zip(cols, rows):
            col.metric(label, f"{n:,} ใบ", f"฿{v:,.0f}")
        st.dataframe(pd.DataFrame(
            [{"สถานะ": r[0], "จำนวน (ใบ)": r[1], "มูลค่า (บาท)": f"{r[2]:,.0f}"} for r in rows]),
            use_container_width=True, hide_index=True)
        st.caption("หมายเหตุ: 'แจกแล้ว' นับตามคูปองที่ออกในระบบ (ยังไม่มีการติดตามการแจกแยกต่างหาก)")

    # ── ข้อมูลการใช้ (branch_sales_coupons + วันที่จาก branch_sales) ──
    usage = read_sheet(SHEET_BRANCH_SALES_COUPONS)
    bnames = _branch_names()
    sdmap = _sale_date_map()
    use_df = pd.DataFrame()
    if usage is not None and not usage.empty and "coupon_no" in usage.columns:
        u = usage.copy()
        u["วันที่ใช้"] = u.get("sale_id", pd.Series([""] * len(u))).astype(str).map(
            lambda s: sdmap.get(s.strip(), ""))
        u["สาขา"] = u.get("branch_id", pd.Series([""] * len(u))).astype(str).map(
            lambda b: f"{b} – {bnames.get(b.strip(), '')}")
        u["มูลค่า"] = u.get("amount", pd.Series([0] * len(u))).map(_num)
        use_df = u

    # ── 2. ใช้ตามวัน-สาขา ──
    with tabs[1]:
        st.subheader("คูปองที่ใช้ — แยกตามวัน / สาขา")
        if use_df.empty:
            st.info("ยังไม่มีการใช้คูปอง")
        else:
            g = use_df.groupby(["วันที่ใช้", "สาขา"]).agg(
                จำนวนใบ=("coupon_no", "count"), มูลค่ารวม=("มูลค่า", "sum")).reset_index()
            g["มูลค่ารวม"] = g["มูลค่ารวม"].map(lambda x: f"{x:,.0f}")
            g = g.sort_values("วันที่ใช้", ascending=False)
            st.dataframe(g, use_container_width=True, hide_index=True)

    # ── 3. มูลค่าตามแคมเปญ (prefix) + ช่วงวันที่ ──
    with tabs[2]:
        st.subheader("มูลค่าคูปอง: ใช้ไปแล้ว เทียบ ทั้งหมดในแคมเปญ")
        st.caption("แคมเปญ = คำนำหน้าเลขคูปอง (เช่น RN-2609) · เลือกช่วงวันที่ออกคูปอง")
        c1, c2 = st.columns(2)
        d_from = c1.date_input("ตั้งแต่วันที่ (ออกคูปอง)",
                               value=datetime.date.today() - datetime.timedelta(days=90),
                               key="cr_from")
        d_to = c2.date_input("ถึงวันที่", value=datetime.date.today(), key="cr_to")
        tmp = cdf.copy()
        tmp["_issued"] = tmp.get("issued_at", pd.Series([""] * len(tmp))).map(_parse_date)
        tmp = tmp[tmp["_issued"].map(lambda d: d is not None and d_from <= d <= d_to)]
        if tmp.empty:
            st.info("ไม่มีคูปองที่ออกในช่วงวันที่นี้")
        else:
            def _campaign(cno):
                s = str(cno)
                return s.rsplit("-", 1)[0] if "-" in s else "(ไม่มีคำนำหน้า)"
            tmp["แคมเปญ"] = tmp["coupon_no"].map(_campaign)
            tmp["_amt"] = tmp.get("amount", pd.Series([0] * len(tmp))).map(_num)
            tmp["_used"] = tmp.get("status", pd.Series([""] * len(tmp))).map(_is_used)
            g = tmp.groupby("แคมเปญ").apply(lambda x: pd.Series({
                "ออก (ใบ)": len(x),
                "ใช้แล้ว (ใบ)": int(x["_used"].sum()),
                "มูลค่าทั้งหมด": x["_amt"].sum(),
                "มูลค่าที่ใช้ไป": x.loc[x["_used"], "_amt"].sum(),
            })).reset_index()
            g["% ใช้ไป (มูลค่า)"] = g.apply(
                lambda r: f"{(r['มูลค่าที่ใช้ไป']/r['มูลค่าทั้งหมด']*100) if r['มูลค่าทั้งหมด'] else 0:.1f}%",
                axis=1)
            for c in ["มูลค่าทั้งหมด", "มูลค่าที่ใช้ไป"]:
                g[c] = g[c].map(lambda x: f"{x:,.0f}")
            st.dataframe(g, use_container_width=True, hide_index=True)

    # ── 4. หมดอายุไม่ได้ใช้ ──
    with tabs[3]:
        st.subheader("คูปองหมดอายุ — แต่ไม่ได้ใช้")
        ex = cdf[expired_mask.values]
        if ex.empty:
            st.success("ไม่มีคูปองหมดอายุที่ไม่ได้ใช้ 🎉")
        else:
            disp = pd.DataFrame({
                "เลขคูปอง": ex["coupon_no"].astype(str),
                "มูลค่า": ex.get("amount", pd.Series([0] * len(ex))).map(lambda x: f"{_num(x):,.0f}"),
                "วันหมดอายุ": ex.get("expire_date", pd.Series([""] * len(ex))).astype(str),
                "ผู้อนุมัติ": ex.get("approver", pd.Series([""] * len(ex))).astype(str),
            })
            st.metric("รวมมูลค่าที่เสียโอกาส", f"฿{ex.get('amount', pd.Series()).map(_num).sum():,.0f}",
                      f"{len(ex):,} ใบ")
            st.dataframe(disp, use_container_width=True, hide_index=True)

    # ── 5. กระทบยอด ──
    with tabs[4]:
        st.subheader("กระทบยอด: ยอดขายรวม – เงินรับจริง – คูปอง")
        bs = read_sheet(SHEET_BRANCH_SALES)
        if bs is None or bs.empty:
            st.info("ยังไม่มีข้อมูลยอดขาย")
        else:
            c1, c2 = st.columns(2)
            f2 = c1.date_input("ตั้งแต่วันที่ (ขาย)",
                               value=datetime.date.today() - datetime.timedelta(days=30),
                               key="cr5_from")
            t2 = c2.date_input("ถึงวันที่", value=datetime.date.today(), key="cr5_to")
            b = bs.copy()
            b["_d"] = b.get("sale_date", pd.Series([""] * len(b))).map(_parse_date)
            b = b[b["_d"].map(lambda d: d is not None and f2 <= d <= t2)]
            if b.empty:
                st.info("ไม่มียอดขายในช่วงวันที่นี้")
            else:
                total = b.get("total_amount", pd.Series([0]*len(b))).map(_num).sum()
                cash = b.get("cash_amount", pd.Series([0]*len(b))).map(_num).sum()
                trans = b.get("transfer_amount", pd.Series([0]*len(b))).map(_num).sum()
                coup = b.get("coupon_amount", pd.Series([0]*len(b))).map(_num).sum()
                received = cash + trans
                m = st.columns(4)
                m[0].metric("ยอดขายรวม", f"฿{total:,.0f}")
                m[1].metric("เงินรับจริง (สด+โอน)", f"฿{received:,.0f}")
                m[2].metric("คูปอง/ส่วนลด", f"฿{coup:,.0f}")
                m[3].metric("ผลต่าง (รวม−รับ−คูปอง)", f"฿{total - received - coup:,.0f}")
                st.caption("ยอดขายรวม = เงินสด + เงินโอน + คูปอง · เงินรับจริง = เงินสด + เงินโอน · "
                           "คูปองถือเป็นส่วนลด (เงินที่ไม่ได้รับเป็นเงินสด)")

    # ── 6. ยังไม่ใช้ (ออกแล้วยังไม่หมดอายุ) ──
    with tabs[5]:
        st.subheader("คูปองออกแล้ว — ยังไม่หมดอายุ และยังไม่ใช้")
        av = cdf[active_mask.values]
        if av.empty:
            st.info("ไม่มีคูปองที่ยังใช้ได้")
        else:
            disp = pd.DataFrame({
                "เลขคูปอง": av["coupon_no"].astype(str),
                "มูลค่า": av.get("amount", pd.Series([0]*len(av))).map(lambda x: f"{_num(x):,.0f}"),
                "วันหมดอายุ": av.get("expire_date", pd.Series([""]*len(av))).astype(str).map(
                    lambda x: "" if x.lower() in ("nan", "none", "nat") else x),
                "ผู้อนุมัติ": av.get("approver", pd.Series([""]*len(av))).astype(str),
            })
            st.metric("รวมมูลค่าคงเหลือ (ยังใช้ได้)",
                      f"฿{av.get('amount', pd.Series()).map(_num).sum():,.0f}", f"{len(av):,} ใบ")
            st.dataframe(disp, use_container_width=True, hide_index=True)
