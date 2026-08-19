"""
coupon_manager.py  –  จัดการคูปอง / Promotion (ย้ายมาจาก App หลังบ้าน → Sale Audit รอบ 3)
ผู้พัฒนา: ดร.วรรณ (ดร.อภิวรรณ์ ดำแสงสวัสดิ์)

เพิ่มในรอบ 3:
  - field 'วันที่ Expire' (วันหมดอายุ) ของคูปอง
  - เมนูนี้อยู่ในแอป Sale Audit (เดิมอยู่แอปหลังบ้าน/การเงิน)
"""
import datetime
import pandas as pd
import streamlit as st

from config import (SHEET_COUPONS, SHEET_SALE_AUDIT_CONFIG,
                    COUPON_PREFIX_DEFAULT, COUPON_APPROVE_PW)
from modules.excel_db import read_sheet, append_row, delete_row, init_workbook


def _approve_pw():
    """รหัสผ่านอนุมัติคูปอง — อ่านจาก sale_audit_config (coupon_password) ถ้ามี ไม่งั้นใช้ค่าเริ่มต้น"""
    try:
        df = read_sheet(SHEET_SALE_AUDIT_CONFIG)
        if df is not None and not df.empty and "config_key" in df.columns:
            m = df[df["config_key"].astype(str) == "coupon_password"]
            if not m.empty:
                v = str(m.iloc[-1].get("config_value", "")).strip()
                if v:
                    return v
    except Exception:
        pass
    return COUPON_APPROVE_PW


def _num(v):
    try:
        return float(str(v).replace(",", "").strip() or 0)
    except Exception:
        return 0.0


def _parse_any_date(v):
    s = str(v).strip()
    if not s or s.lower() in ("nan", "none", "nat"):
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d"):
        try:
            return datetime.datetime.strptime(s[:10], fmt).date()
        except Exception:
            continue
    return None


def _is_expired(v) -> bool:
    d = _parse_any_date(v)
    if d is None:
        return False
    return d < datetime.date.today()


def _init():
    try:
        init_workbook()
    except Exception:
        pass
    try:
        df = read_sheet(SHEET_COUPONS)
    except Exception:
        df = pd.DataFrame()
    if df is None or df.empty:
        try:
            from modules.excel_db import write_sheet
            write_sheet(SHEET_COUPONS, pd.DataFrame(columns=[
                "coupon_no", "amount", "status", "expire_date", "approver",
                "used_branch_id", "used_sale_id", "used_at", "issued_at"]))
        except Exception:
            pass


def render():
    _init()
    st.markdown(
        "<h1 style='color:#6A1B9A;font-size:1.7rem;font-weight:900;'>"
        "🎟️ คูปอง / Promotion</h1>", unsafe_allow_html=True)
    st.caption("คูปองที่เพิ่มที่นี่ สาขาจะเห็นและใช้บันทึกเงินคูปองได้ "
               "(ใช้แล้ว/หมดอายุ ระบบจะไม่ให้ใช้อัตโนมัติ)")

    try:
        cdf = read_sheet(SHEET_COUPONS)
    except Exception:
        cdf = pd.DataFrame()

    today = datetime.date.today()

    # ── เพิ่มคูปองทีละใบ ──────────────────────────────────
    with st.form("form_add_coupon", clear_on_submit=True):
        st.markdown("#### ➕ เพิ่มคูปอง 1 ใบ")
        c1, c2, c3 = st.columns(3)
        with c1:
            coupon_no = st.text_input("เลขคูปอง *", key="cp_no")
        with c2:
            amount = st.number_input("มูลค่า (บาท) *", min_value=0.0, step=1.0,
                                     key="cp_amt")
        with c3:
            exp = st.date_input("วันที่ Expire (วันหมดอายุ)",
                                value=today + datetime.timedelta(days=30),
                                key="cp_exp")
        no_exp = st.checkbox("ไม่กำหนดวันหมดอายุ (ใช้ได้ตลอด)", key="cp_noexp")
        st.markdown("**🔑 ต้องผ่านการอนุมัติก่อนบันทึก**")
        ap1, ap2 = st.columns(2)
        with ap1:
            appr_pw = st.text_input("รหัสผ่านอนุมัติ *", type="password", key="cp_appw")
        with ap2:
            approver = st.text_input("ชื่อผู้อนุมัติ *", key="cp_appr",
                                     placeholder="เช่น ดร.วรรณ")
        add1 = st.form_submit_button("💾 บันทึกคูปอง", type="primary")
    if add1:
        cno = coupon_no.strip()
        exp_str = "" if no_exp else str(exp)
        if not cno:
            st.error("กรุณากรอกเลขคูปอง")
        elif appr_pw != _approve_pw():
            st.error("❌ รหัสผ่านอนุมัติไม่ถูกต้อง")
        elif not approver.strip():
            st.error("กรุณากรอกชื่อผู้อนุมัติ")
        elif not cdf.empty and "coupon_no" in cdf.columns and \
                (cdf["coupon_no"].astype(str).str.strip() == cno).any():
            st.error(f"มีเลขคูปอง {cno} อยู่แล้ว")
        else:
            now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            try:
                append_row(SHEET_COUPONS, {
                    "coupon_no": cno, "amount": amount, "status": "active",
                    "expire_date": exp_str, "approver": approver.strip(),
                    "used_branch_id": "", "used_sale_id": "", "used_at": "",
                    "issued_at": now})
                st.success(f"✅ เพิ่มคูปอง {cno} (฿{amount:,.0f}) โดย {approver.strip()} "
                           + (f"· หมดอายุ {exp_str}" if exp_str else "· ไม่มีวันหมดอายุ"))
                st.rerun()
            except Exception as e:
                st.error(f"บันทึกไม่สำเร็จ: {e} "
                         "(ตรวจว่ารัน roon_new_tables.sql แล้ว)")

    # ── ออกคูปองเป็นชุด ───────────────────────────────────
    with st.expander("➕➕ ออกคูปองเป็นชุด (auto-run เลข)"):
        with st.form("form_batch_coupon", clear_on_submit=False):
            b1, b2, b3, b4 = st.columns(4)
            with b1: prefix = st.text_input("คำนำหน้า", value=COUPON_PREFIX_DEFAULT,
                                            key="cpb_pre")
            with b2: start = st.number_input("เริ่มเลข", min_value=1, step=1,
                                             value=1, key="cpb_start")
            with b3: count = st.number_input("จำนวนใบ", min_value=1, max_value=500,
                                             step=1, value=10, key="cpb_count")
            with b4: bamt = st.number_input("มูลค่า/ใบ", min_value=0.0, step=1.0,
                                            key="cpb_amt")
            e1, e2 = st.columns(2)
            with e1:
                bexp = st.date_input("วันที่ Expire (ทั้งชุด)",
                                     value=today + datetime.timedelta(days=30),
                                     key="cpb_exp")
            with e2:
                b_noexp = st.checkbox("ไม่กำหนดวันหมดอายุ", key="cpb_noexp")
            digits = st.number_input("จำนวนหลักของเลข (เช่น 4 = 0001)",
                                     min_value=1, max_value=8, value=4,
                                     key="cpb_digits")
            st.markdown("**🔑 ต้องผ่านการอนุมัติก่อนบันทึก**")
            bp1, bp2 = st.columns(2)
            with bp1:
                b_appw = st.text_input("รหัสผ่านอนุมัติ *", type="password",
                                       key="cpb_appw")
            with bp2:
                b_appr = st.text_input("ชื่อผู้อนุมัติ *", key="cpb_appr",
                                       placeholder="เช่น ดร.วรรณ")
            addb = st.form_submit_button("💾 ออกคูปองเป็นชุด", type="primary")
        if addb:
            if b_appw != _approve_pw():
                st.error("❌ รหัสผ่านอนุมัติไม่ถูกต้อง")
                st.stop()
            if not b_appr.strip():
                st.error("กรุณากรอกชื่อผู้อนุมัติ")
                st.stop()
            existing = set()
            if not cdf.empty and "coupon_no" in cdf.columns:
                existing = set(cdf["coupon_no"].astype(str).str.strip())
            now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            bexp_str = "" if b_noexp else str(bexp)
            made, skipped = 0, 0
            for i in range(int(count)):
                cno = f"{prefix}{int(start)+i:0{int(digits)}d}"
                if cno in existing:
                    skipped += 1
                    continue
                try:
                    append_row(SHEET_COUPONS, {
                        "coupon_no": cno, "amount": bamt, "status": "active",
                        "expire_date": bexp_str, "approver": b_appr.strip(),
                        "used_branch_id": "", "used_sale_id": "", "used_at": "",
                        "issued_at": now})
                    made += 1
                except Exception as e:
                    st.error(f"หยุดที่ {cno}: {e}")
                    break
            st.success(f"✅ ออกคูปอง {made} ใบ"
                       + (f" (ข้ามซ้ำ {skipped} ใบ)" if skipped else ""))
            st.rerun()

    st.divider()
    # ── รายการคูปอง (ดู/ลบ) ───────────────────────────────
    st.markdown("#### 📋 คูปองในระบบ")
    if cdf is None or cdf.empty:
        st.info("ยังไม่มีคูปองในระบบ")
        return

    show = cdf.copy()
    st_l = show.get("status", pd.Series([""] * len(show))).astype(str).str.strip().str.lower()
    exp_series = show.get("expire_date", pd.Series([""] * len(show)))
    expired_mask = exp_series.map(_is_expired)
    used_mask = st_l.isin(["used", "ใช้แล้ว"]).values

    n_used = int(used_mask.sum())
    n_expired = int((expired_mask.values & ~used_mask).sum())
    n_active = int((~used_mask & ~expired_mask.values).sum())
    col = st.columns(4)
    col[0].metric("คูปองทั้งหมด", f"{len(show):,}")
    col[1].metric("ยังใช้ได้", f"{n_active:,}")
    col[2].metric("ใช้แล้ว", f"{n_used:,}")
    col[3].metric("หมดอายุ", f"{n_expired:,}")

    only = st.selectbox("กรอง", ["ทั้งหมด", "ยังใช้ได้", "ใช้แล้ว", "หมดอายุ"],
                        key="cp_filter")
    if only == "ยังใช้ได้":
        show = show[(~used_mask) & (~expired_mask.values)]
    elif only == "ใช้แล้ว":
        show = show[used_mask]
    elif only == "หมดอายุ":
        show = show[expired_mask.values & ~used_mask]

    def _status_label(row):
        if str(row.get("status", "")).strip().lower() in ("used", "ใช้แล้ว"):
            return "ใช้แล้ว"
        if _is_expired(row.get("expire_date", "")):
            return "หมดอายุ"
        return "ใช้ได้"

    disp = pd.DataFrame({
        "เลขคูปอง": show["coupon_no"].astype(str),
        "มูลค่า": show.get("amount", pd.Series([""] * len(show))).map(
            lambda x: f"{_num(x):,.0f}"),
        "วันหมดอายุ": show.get("expire_date", pd.Series([""] * len(show))).astype(str).map(
            lambda x: "" if x.lower() in ("nan", "none", "nat") else x),
        "ผู้อนุมัติ": show.get("approver", pd.Series([""] * len(show))).astype(str).map(
            lambda x: "" if x.lower() in ("nan", "none", "nat") else x),
        "สถานะ": show.apply(_status_label, axis=1) if not show.empty else [],
        "สาขาที่ใช้": show.get("used_branch_id", pd.Series([""] * len(show))).astype(str),
        "วันที่ใช้": show.get("used_at", pd.Series([""] * len(show))).astype(str),
    })
    st.dataframe(disp, use_container_width=True, hide_index=True)

    # ── ลบคูปอง (เฉพาะที่ยังไม่ถูกใช้) ──
    unused_ids = show[~show.get("status", pd.Series([""] * len(show)))
                      .astype(str).str.strip().str.lower()
                      .isin(["used", "ใช้แล้ว"]).values]["coupon_no"].astype(str).tolist() \
        if "coupon_no" in show.columns else []
    if unused_ids:
        d1, d2 = st.columns([3, 1])
        with d1:
            del_no = st.selectbox("เลือกคูปองที่จะลบ (เฉพาะที่ยังไม่ถูกใช้)",
                                  unused_ids, key="cp_del_sel")
        with d2:
            st.markdown("<div style='height:28px'></div>", unsafe_allow_html=True)
            if st.button("🗑️ ลบคูปอง", key="cp_del_btn", use_container_width=True):
                try:
                    delete_row(SHEET_COUPONS, "coupon_no", del_no)
                    st.success(f"ลบคูปอง {del_no} แล้ว")
                    st.rerun()
                except Exception as e:
                    st.error(f"ลบไม่สำเร็จ: {e}")
