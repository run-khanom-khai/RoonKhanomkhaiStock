"""
sale_audit.py  –  แอป Sale Audit (ตรวจสอบยอดขาย)
ผู้พัฒนา: ดร.วรรณ (ดร.อภิวรรณ์ ดำแสงสวัสดิ์)

3 เมนู:
  ① บันทึกเงินธนาคารรายวัน/สาขา (วันที่บันทึก = วันที่ขายจริง D)
      1.1 กลุ่ม Shopping Mall/Market — มีรหัสผ่านก่อนเข้า (สาขา key เอง)
      1.2 กลุ่มอื่น — ฝ่าย Sale Audit key
  ② แสดง+เทียบยอดบรรจุภัณฑ์ (ใช้ไป 3 วิธี / คงเหลือ 2 แหล่ง + DIFF)
  ③ สรุปเทียบเงิน 3 คอลัมน์ (เงินสาขาแจ้ง / เงินจากบรรจุภัณฑ์ / เงินเข้าธนาคารจริง)

นิยามวันที่:
  D   = วันขายจริง (สาขานับสต๊อกหลังปิดร้าน = ยอดปิดของ D)
  D+1 = ฝ่ายตรวจสอบนับเช้าวัน D+1 (ก่อนเปิดร้าน = ยอดปิดของ D)
  D-1 = ยอดปิดของวันก่อนหน้า
"""
import datetime
import base64
import pandas as pd
import streamlit as st

from config import (
    SHEET_BRANCHES, SHEET_BANK_ACCOUNTS, SHEET_PRODUCTS, SHEET_SALES_CHANNELS,
    SHEET_BRANCH_STOCK_DAILY, SHEET_AUDIT_STOCK_BALANCE,
    SHEET_BRANCH_SALES, SHEET_BRANCH_SALES_DELIVERY, SHEET_BRANCH_SALES_SLIPS,
    SHEET_MARKETING_DAILY_SALES, SHEET_MARKETING_DAILY_SALES_ITEMS,
    SHEET_SALE_BANK_INCOME, SHEET_SALE_AUDIT_CONFIG, SHEET_SALE_AUDIT_RESOLUTION,
    SALE_AUDIT_MALL_GROUPS, SALE_AUDIT_DEFAULT_PW, SALE_AUDIT_DELIVERY_CHANNELS,
    SALE_AUDIT_EGG_PRODUCT_IDS,
)
from modules.excel_db import read_sheet, append_row, update_row
from modules.record_stock import STOCK_FIELDS
from utils.id_generator import next_id


# ── บรรจุภัณฑ์ที่ก่อรายได้
#   (key, ชื่อ, คอลัมน์ audit/สต๊อก, คอลัมน์ delivery/ใช้, ราคาสำรอง, product_id สำหรับดึงราคา) ──
PKG_CANON = [
    ("box",   "ขนมไข่ กล่อง (20 ชิ้น)", "plastic_box_qty",      "box_qty",                130.0, "R-0002"),
    ("bag",   "ขนมไข่ ถุง (10 ชิ้น)",   "paper_bag_qty",         "bag_qty",                 70.0, "R-0001"),
    ("ybag",  "ถุงหูหิ้วกระดาษพิมพ์ลาย", "printed_carry_bag_qty", "yellow_premium_bag_qty",   0.0, ""),
    ("water", "แก้วน้ำ (เครื่องดื่ม)",   "water_cup_qty",         "water_cup_qty",            79.0, "R-0004"),
    ("icecup","แก้วไอศกรีม",            "ice_cream_cup_qty",     "",                         89.0, "R-0005"),
]
# แมป คอลัมน์สต๊อก → คอลัมน์ delivery (สำหรับ 'ยอดที่สาขา key')
STOCK_TO_DELIV = {c[2]: c[3] for c in PKG_CANON if c[2] and c[3]}


def _product_price_map():
    """product_id → ราคาขาย (ดึงจากตารางสินค้า products)"""
    out = {}
    try:
        df = read_sheet(SHEET_PRODUCTS)
    except Exception:
        df = pd.DataFrame()
    if df is None or df.empty or "product_id" not in df.columns:
        return out
    pcol = None
    for c in ("price", "selling_cost", "unit_price", "sell_price", "ราคาขาย"):
        if c in df.columns:
            pcol = c
            break
    for _, r in df.iterrows():
        pid = str(r.get("product_id", "")).strip()
        if pid:
            out[pid] = _num(r.get(pcol, 0)) if pcol else 0.0
    return out


def _products_info():
    """product_id → (ชื่อ, ราคา)"""
    out = {}
    try:
        df = read_sheet(SHEET_PRODUCTS)
    except Exception:
        df = pd.DataFrame()
    if df is None or df.empty or "product_id" not in df.columns:
        return out
    pcol = next((c for c in ("price", "selling_cost", "unit_price", "sell_price", "ราคาขาย")
                 if c in df.columns), None)
    for _, r in df.iterrows():
        pid = str(r.get("product_id", "")).strip()
        if pid:
            out[pid] = (str(r.get("product_name", "")).strip(),
                        _num(r.get(pcol, 0)) if pcol else 0.0)
    return out


def _delivery_channel_ids():
    """channel_id ของช่องทางที่ไม่รับเงินที่ร้าน (Delivery/Online)"""
    ids = set()
    try:
        df = read_sheet(SHEET_SALES_CHANNELS)
    except Exception:
        df = pd.DataFrame()
    if df is not None and not df.empty and "channel_id" in df.columns:
        for _, r in df.iterrows():
            nm = str(r.get("channel_name", ""))
            if any(k.lower() in nm.lower() for k in SALE_AUDIT_DELIVERY_CHANNELS):
                ids.add(str(r.get("channel_id", "")).strip())
    return ids


def _drink_sales(branch_id, D):
    """จำนวนขายแยกตามสินค้า (เฉพาะหน้าร้าน — ตัด Delivery/Online) จากยอดขายที่บันทึก
    คืน dict product_id → qty (ไม่รวมสินค้าขนมไข่ที่คิดเงินจากบรรจุภัณฑ์)"""
    try:
        ms = read_sheet(SHEET_MARKETING_DAILY_SALES)
        items = read_sheet(SHEET_MARKETING_DAILY_SALES_ITEMS)
    except Exception:
        return {}
    if ms is None or ms.empty or items is None or items.empty:
        return {}
    del_ids = _delivery_channel_ids()
    m = ms[(ms["branch_id"].astype(str).str.strip() == str(branch_id)) &
           (ms["sales_date"].astype(str).str[:10] == str(D)[:10])]
    if "channel_id" in m.columns and del_ids:
        m = m[~m["channel_id"].astype(str).str.strip().isin(del_ids)]
    sids = m["marketing_sales_id"].astype(str).tolist() if "marketing_sales_id" in m.columns else []
    if not sids:
        return {}
    it = items[items["marketing_sales_id"].astype(str).isin(sids)]
    agg = {}
    for _, r in it.iterrows():
        pid = str(r.get("product_id", "")).strip()
        if pid and pid not in SALE_AUDIT_EGG_PRODUCT_IDS:   # ตัดขนมไข่ (คิดจากบรรจุภัณฑ์แล้ว)
            agg[pid] = agg.get(pid, 0) + _num(r.get("qty_sold", 0))
    return agg


def _num(v):
    try:
        if v is None or v == "":
            return 0.0
        f = float(str(v).replace(",", ""))
        return 0.0 if f != f else f      # กัน NaN (NaN != NaN)
    except Exception:
        return 0.0


def _fmt(n):
    return f"{_num(n):,.0f}"


# ══════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════
def _branch_group_map():
    """branch_id → (branch_name, branch_group_id)"""
    df = read_sheet(SHEET_BRANCHES)
    out = {}
    if df is not None and not df.empty and "branch_id" in df.columns:
        for _, r in df.iterrows():
            out[str(r["branch_id"]).strip()] = (
                str(r.get("branch_name", "")).strip(),
                str(r.get("branch_group_id", "")).strip(),
            )
    return out


def _bank_for_branch(branch_id):
    """คืนข้อมูลบัญชีธนาคารของสาขา (จาก branches.bank_account_no → bank_accounts)"""
    bdf = read_sheet(SHEET_BRANCHES)
    acc_no = ""
    if bdf is not None and not bdf.empty and "branch_id" in bdf.columns:
        m = bdf[bdf["branch_id"].astype(str).str.strip() == str(branch_id)]
        if not m.empty and "bank_account_no" in m.columns:
            acc_no = str(m.iloc[-1].get("bank_account_no", "")).strip()
    if not acc_no:
        return None
    adf = read_sheet(SHEET_BANK_ACCOUNTS)
    if adf is not None and not adf.empty and "account_no" in adf.columns:
        mm = adf[adf["account_no"].astype(str).str.strip() == acc_no]
        if not mm.empty:
            return mm.iloc[-1].to_dict()
    return {"account_no": acc_no, "bank_name": "(ยังไม่พบสมุดบัญชีเลขนี้)",
            "bank_branch": "", "account_name": ""}


def _add_to_bank_balance(bank_account_id, delta):
    """บวกยอด (delta) เข้า current_balance ของบัญชีธนาคาร — คืนยอดคงเหลือใหม่ หรือ None"""
    if not bank_account_id or abs(_num(delta)) < 1e-9:
        return None
    adf = read_sheet(SHEET_BANK_ACCOUNTS)
    if adf is None or adf.empty or "bank_account_id" not in adf.columns:
        return None
    m = adf[adf["bank_account_id"].astype(str) == str(bank_account_id)]
    if m.empty:
        return None
    new_bal = _num(m.iloc[-1].get("current_balance", 0)) + _num(delta)
    try:
        update_row(SHEET_BANK_ACCOUNTS, "bank_account_id", str(bank_account_id),
                   {"current_balance": new_bal})
    except Exception:
        return None
    return new_bal


def _stock_row(sheet, date_col, date_val, branch_id):
    """คืน dict ของแถวสต๊อก (คงเหลือ) ตาม sheet/วันที่/สาขา — ว่างถ้าไม่มี"""
    try:
        df = read_sheet(sheet)
    except Exception:
        return {}
    if df is None or df.empty or "branch_id" not in df.columns or date_col not in df.columns:
        return {}
    m = df[(df["branch_id"].astype(str).str.strip() == str(branch_id)) &
           (df[date_col].astype(str).str[:10] == str(date_val)[:10])]
    return m.iloc[-1].to_dict() if not m.empty else {}


def _branch_keyed_used(branch_id, d_date, deliv_field, channels=None):
    """ผลรวมบรรจุภัณฑ์ที่สาขา key ใช้ไป (branch_sales_delivery) ของวัน D
    channels=None → ทุกช่องทาง (ยอดใช้ทั้งหมด) ; ระบุ list → เฉพาะช่องทางนั้น"""
    if not deliv_field:
        return 0.0
    sdf = read_sheet(SHEET_BRANCH_SALES)
    if sdf is None or sdf.empty:
        return 0.0
    sids = sdf[(sdf["branch_id"].astype(str).str.strip() == str(branch_id)) &
               (sdf["sale_date"].astype(str).str[:10] == str(d_date)[:10])]["sale_id"].astype(str).tolist()
    if not sids:
        return 0.0
    ddf = read_sheet(SHEET_BRANCH_SALES_DELIVERY)
    if ddf is None or ddf.empty or deliv_field not in ddf.columns:
        return 0.0
    d = ddf[ddf["sale_id"].astype(str).isin(sids)]
    if channels is not None and "channel" in d.columns:
        d = d[d["channel"].astype(str).str.strip().isin(channels)]
    return sum(_num(x) for x in d[deliv_field].tolist())


# ช่องทางที่บรรจุภัณฑ์ถูกใช้ไป (แสดงเป็นคอลัมน์ในตาราง 3.2) — รอบ 3
SALE_AUDIT_TABLE_CHANNELS = ["LineMan", "Grab", "Shopee", "TikTok", "อื่นๆ", "ชำรุด"]
# ช่องทางที่ 'ไม่รับเงินที่ร้าน' (นับเป็น Delivery/Online) — ไม่รวม 'ชำรุด'
SALE_AUDIT_NOMONEY_CHANNELS = ["LineMan", "Grab", "Shopee", "TikTok", "อื่นๆ"]


def _has_resolution(branch_id, D):
    """ฝ่าย Sale Audit ได้โทร/บันทึกการแก้ไขของสาขา+วัน D หรือยัง
    (มีข้อความ 'โทรหาใคร' หรือ 'แก้ไขอย่างไร' = ถือว่ามีการแก้ไข)"""
    try:
        rdf = read_sheet(SHEET_SALE_AUDIT_RESOLUTION)
    except Exception:
        return False
    if rdf is None or rdf.empty or "branch_id" not in rdf.columns:
        return False
    m = rdf[(rdf["branch_id"].astype(str) == str(branch_id)) &
            (rdf["sale_date"].astype(str).str[:10] == str(D)[:10])]
    if m.empty:
        return False
    r = m.iloc[-1]
    return bool(str(r.get("called_who", "")).strip() or
                str(r.get("how_fixed", "")).strip())


def _branch_delivery_notes(branch_id, D):
    """ดึง (channel, remark, photo_b64) จาก branch_sales_delivery ของวัน D
    เฉพาะแถวที่มีหมายเหตุหรือรูปภาพ — ใช้แสดงใต้ตาราง 3.2"""
    out = []
    sdf = read_sheet(SHEET_BRANCH_SALES)
    if sdf is None or sdf.empty:
        return out
    sids = sdf[(sdf["branch_id"].astype(str).str.strip() == str(branch_id)) &
               (sdf["sale_date"].astype(str).str[:10] == str(D)[:10])]["sale_id"].astype(str).tolist()
    if not sids:
        return out
    ddf = read_sheet(SHEET_BRANCH_SALES_DELIVERY)
    if ddf is None or ddf.empty or "sale_id" not in ddf.columns:
        return out
    d = ddf[ddf["sale_id"].astype(str).isin(sids)]
    for _, r in d.iterrows():
        rm = str(r.get("remark", "") or "").strip()
        ph = str(r.get("damage_photo", "") or "").strip()
        if rm or ph:
            out.append((str(r.get("channel", "")).strip(), rm, ph))
    return out


def _get_config(key, default=""):
    try:
        df = read_sheet(SHEET_SALE_AUDIT_CONFIG)
        if df is not None and not df.empty and "config_key" in df.columns:
            m = df[df["config_key"].astype(str) == key]
            if not m.empty:
                return str(m.iloc[-1].get("config_value", "")) or default
    except Exception:
        pass
    return default


# ══════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════
def render():
    st.title("🔍 Sale Audit — ตรวจสอบยอดขาย")
    st.caption("เทียบยอดขายจาก เงินสาขาแจ้ง · บรรจุภัณฑ์ที่ใช้จริง · เงินเข้าธนาคารจริง")

    t1, t2, t3, t4 = st.tabs([
        "① บันทึกเงินธนาคารรายวัน",
        "② เทียบยอดบรรจุภัณฑ์",
        "③ สรุปเทียบเงิน 3 ทาง",
        "④ คูปอง / Promotion",
    ])
    with t1:
        _render_bank_income()
    with t2:
        _render_pkg_compare()
    with t3:
        _render_money_summary()
    with t4:
        try:
            from modules import coupon_manager
            coupon_manager.render()
        except Exception as e:
            st.error(f"❌ ไม่สามารถโหลดเมนูคูปองได้: {e}")


# ══════════════════════════════════════════════════════════════════════
# ① บันทึกเงินธนาคารรายวัน/สาขา
# ══════════════════════════════════════════════════════════════════════
def _render_bank_income():
    st.subheader("① บันทึกเงินเข้าธนาคารรายวัน (วันที่บันทึก = วันที่ขายจริง)")
    s1, s2 = st.tabs([
        "1.1 กลุ่ม Shopping Mall / Market (มีรหัสผ่าน)",
        "1.2 กลุ่มอื่น (ฝ่าย Sale Audit)",
    ])
    with s1:
        _bank_income_form(only_groups=SALE_AUDIT_MALL_GROUPS, need_pw=True, key="pw11")
    with s2:
        _bank_income_form(only_groups=None, exclude_groups=SALE_AUDIT_MALL_GROUPS,
                          need_pw=False, key="pw12")


def _bank_income_form(only_groups=None, exclude_groups=None, need_pw=False, key="bi"):
    # ── รหัสผ่าน (เมนู 1.1) ──
    if need_pw:
        pw_needed = _get_config("mall_password", SALE_AUDIT_DEFAULT_PW)
        pw = st.text_input("🔑 รหัสผ่านเข้าเมนู", type="password", key=f"{key}_pw")
        if pw != pw_needed:
            st.info("กรุณากรอกรหัสผ่านให้ถูกต้องก่อนบันทึก")
            return

    bmap = _branch_group_map()
    # กรองสาขาตามกลุ่ม
    def _ok(gid):
        if only_groups is not None:
            return gid in only_groups
        if exclude_groups is not None:
            return gid not in exclude_groups
        return True
    opts = {b: v for b, v in bmap.items() if _ok(v[1])}
    if not opts:
        st.warning("ยังไม่มีสาขาในกลุ่มนี้")
        return

    c1, c2 = st.columns(2)
    with c1:
        branch_id = st.selectbox("🏪 เลือกสาขา", list(opts.keys()),
                                 format_func=lambda b: f"{b} – {opts[b][0]} ({opts[b][1]})",
                                 key=f"{key}_br")
    with c2:
        sale_date = st.date_input("📅 วันที่ขายจริง (วันที่บันทึกเงินเข้า)",
                                  value=datetime.date.today() - datetime.timedelta(days=1), key=f"{key}_dt")
    st.caption(f"กลุ่มสาขา: **{opts[branch_id][1]}**")

    # แสดงบัญชีธนาคารของสาขา
    bank = _bank_for_branch(branch_id)
    if bank:
        st.markdown(
            f"🏦 **ธนาคาร:** {bank.get('bank_name','-') or '-'} สาขา {bank.get('bank_branch','') or '-'} · "
            f"**เลขที่:** `{bank.get('account_no','')}` · **ชื่อบัญชี:** {bank.get('account_name','') or '-'}")
    else:
        st.warning("⚠️ สาขานี้ยังไม่ได้ผูกเลขที่บัญชีธนาคาร (ตั้งที่เมนู 'เพิ่ม สาขา/สินค้า → แก้ไขสาขา')")

    amount = st.number_input("💰 ยอดเงินขาย (เข้าธนาคาร)", min_value=0.0, step=1.0,
                             format="%.2f", key=f"{key}_amt")
    st.caption("💡 ยอดนี้จะถูกนำไปบวกเพิ่มใน 'ยอดเงินคงเหลือ (ยอดตั้งต้น)' ของบัญชีธนาคารนี้ด้วย")

    idf = read_sheet(SHEET_SALE_BANK_INCOME)   # อ่านไว้ก่อน (ใช้ทั้งบันทึก + ประวัติ)

    if st.button("💾 บันทึกเงินเข้าธนาคาร", type="primary", key=f"{key}_save"):
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # กันซ้ำ สาขา+วัน → อัปเดตแทน (เก็บยอดเดิมไว้คิดส่วนต่างที่ต้องบวกเข้าบัญชี)
        existing = None
        old_amt = 0.0
        if idf is not None and not idf.empty and "branch_id" in idf.columns:
            m = idf[(idf["branch_id"].astype(str) == str(branch_id)) &
                    (idf["sale_date"].astype(str).str[:10] == str(sale_date)[:10])]
            if not m.empty:
                existing = str(m.iloc[-1]["income_id"])
                old_amt = _num(m.iloc[-1].get("amount", 0))
        payload = {
            "sale_date": str(sale_date), "branch_id": branch_id,
            "branch_group_id": opts[branch_id][1],
            "bank_account_no": (bank or {}).get("account_no", ""),
            "amount": amount, "entered_by": st.session_state.get("dept_name", ""),
            "updated_at": now,
        }
        try:
            if existing:
                update_row(SHEET_SALE_BANK_INCOME, "income_id", existing, payload)
                st.success(f"✅ อัปเดตยอดเงินเข้าธนาคารแล้ว ({existing})")
            else:
                payload["income_id"] = next_id(idf, "income_id", "SBI")
                payload["created_at"] = now
                append_row(SHEET_SALE_BANK_INCOME, payload)
                st.success(f"✅ บันทึกยอดเงินเข้าธนาคารสำเร็จ ({payload['income_id']})")
            # บวกยอด (ส่วนต่าง) เข้ายอดคงเหลือของบัญชีธนาคาร
            new_bal = _add_to_bank_balance((bank or {}).get("bank_account_id", ""),
                                           _num(amount) - _num(old_amt))
            if new_bal is not None:
                st.info(f"🏦 ยอดคงเหลือบัญชีนี้ล่าสุด: ฿{new_bal:,.2f}")
            st.rerun()
        except Exception as e:
            st.error(f"บันทึกไม่สำเร็จ: {e} (รัน SQL sale_audit)")

    # ประวัติล่าสุด
    if idf is not None and not idf.empty:
        st.divider()
        st.markdown("##### 📋 ประวัติล่าสุด")
        show = idf.copy()
        show = show[show["branch_id"].astype(str) == str(branch_id)].sort_values("sale_date", ascending=False)
        if not show.empty:
            st.dataframe(pd.DataFrame({
                "วันขาย (D)": show["sale_date"].astype(str),
                "เลขบัญชี": show.get("bank_account_no", "").astype(str),
                "ยอดเงิน": show["amount"].map(lambda x: f"{_num(x):,.2f}"),
            }), use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════
# ② เทียบยอดบรรจุภัณฑ์
# ══════════════════════════════════════════════════════════════════════
def _render_pkg_compare():
    st.subheader("② เทียบยอดบรรจุภัณฑ์ (เลือกสาขา + วันขายจริง D)")
    bmap = _branch_group_map()
    if not bmap:
        st.warning("ยังไม่มีสาขาในระบบ")
        return
    c1, c2 = st.columns(2)
    with c1:
        branch_id = st.selectbox("🏪 สาขา", list(bmap.keys()),
                                 format_func=lambda b: f"{b} – {bmap[b][0]}", key="pc_br")
    with c2:
        D = st.date_input("📅 วันที่ขายจริง (D)", value=datetime.date.today() - datetime.timedelta(days=1), key="pc_d")
    Dm1 = D - datetime.timedelta(days=1)
    Dp1 = D + datetime.timedelta(days=1)
    st.caption(f"D = **{D}** · D-1 = {Dm1} · D+1 = {Dp1}")

    # แหล่งข้อมูล
    br_D   = _stock_row(SHEET_BRANCH_STOCK_DAILY, "stock_date", D,   branch_id)   # สาขาคงเหลือ D
    br_Dm1 = _stock_row(SHEET_BRANCH_STOCK_DAILY, "stock_date", Dm1, branch_id)   # สาขาคงเหลือ D-1
    au_D   = _stock_row(SHEET_AUDIT_STOCK_BALANCE, "audit_date", D,   branch_id)  # ตรวจสอบเช้า D
    au_Dp1 = _stock_row(SHEET_AUDIT_STOCK_BALANCE, "audit_date", Dp1, branch_id)  # ตรวจสอบเช้า D+1

    miss = []
    if not br_D:   miss.append(f"สาขาคงเหลือวัน {D}")
    if not br_Dm1: miss.append(f"สาขาคงเหลือวัน {Dm1}")
    if not au_D:   miss.append(f"ตรวจสอบเช้า {D}")
    if not au_Dp1: miss.append(f"ตรวจสอบเช้า {Dp1}")
    if miss:
        st.warning("⚠️ ยังไม่มีข้อมูลบางส่วน: " + " · ".join(miss) + " (ค่าที่ขาดจะถือเป็น 0)")

    # ── ส่วน A: จำนวนที่ใช้ไป (3 วิธี) ──
    st.markdown("#### 🅰️ จำนวนบรรจุภัณฑ์ที่ 'ใช้ไป' — เทียบ 3 วิธี")
    st.caption("(1) สาขา KEY ยอดใช้ของวัน D  |  (2) สาขาคงเหลือ (D-1)−(D)  |  (3) ฝ่ายตรวจสอบ เช้า D − เช้า D+1")
    hdr = ("<tr>" + "".join(
        f"<th style='padding:6px;background:#2b2723;color:#fff;'>{h}</th>"
        for h in ["บรรจุภัณฑ์", "(1) สาขา KEY", "(2) สาขา D-1−D", "DIFF 1–2",
                  "(3) ตรวจสอบ D−D+1", "DIFF 2–3"]) + "</tr>")
    rows_html = ""
    for key, label, unit in STOCK_FIELDS:
        used2 = _num(br_Dm1.get(key, 0)) - _num(br_D.get(key, 0))          # วิธี 2
        used3 = _num(au_D.get(key, 0)) - _num(au_Dp1.get(key, 0))          # วิธี 3
        deliv_f = STOCK_TO_DELIV.get(key)
        used1 = _branch_keyed_used(branch_id, D, deliv_f) if deliv_f else None
        v1 = "—" if used1 is None else _fmt(used1)
        d12 = "" if used1 is None else ("<span style='color:#C62828;font-weight:700;'>(DIFF)</span>"
                                        if abs(used1 - used2) > 0.5 else "<span style='color:#2E7D32;'>OK</span>")
        d23 = ("<span style='color:#C62828;font-weight:700;'>(DIFF)</span>"
               if abs(used2 - used3) > 0.5 else "<span style='color:#2E7D32;'>OK</span>")
        rows_html += (f"<tr><td style='padding:5px;'>{label}</td>"
                      f"<td style='padding:5px;text-align:center;'>{v1}</td>"
                      f"<td style='padding:5px;text-align:center;'>{_fmt(used2)}</td>"
                      f"<td style='padding:5px;text-align:center;'>{d12}</td>"
                      f"<td style='padding:5px;text-align:center;'>{_fmt(used3)}</td>"
                      f"<td style='padding:5px;text-align:center;'>{d23}</td></tr>")
    st.markdown(f"<table style='width:100%;border-collapse:collapse;border:1px solid #ddd;'>{hdr}{rows_html}</table>",
                unsafe_allow_html=True)

    # ── ส่วน B: จำนวนคงเหลือ (2 แหล่ง) ──
    st.markdown("#### 🅱️ จำนวนบรรจุภัณฑ์ 'คงเหลือ' — เทียบ 2 แหล่ง")
    st.caption("(1) คงเหลือวัน D ที่สาขา KEY  |  (2) คงเหลือวัน D+1 ที่ฝ่ายตรวจสอบแจ้ง (= ยอดปิดของ D)")
    hdr2 = ("<tr>" + "".join(
        f"<th style='padding:6px;background:#0D47A1;color:#fff;'>{h}</th>"
        for h in ["บรรจุภัณฑ์", "(1) คงเหลือ D — สาขา", "(2) คงเหลือ D+1 — ตรวจสอบ", "DIFF (1)−(2)"]) + "</tr>")
    rows2 = ""
    for key, label, unit in STOCK_FIELDS:
        r1 = _num(br_D.get(key, 0))
        r2 = _num(au_Dp1.get(key, 0))
        diff = r1 - r2
        dcell = (f"<span style='color:#C62828;font-weight:700;'>{diff:+,.0f} (DIFF)</span>"
                 if abs(diff) > 0.5 else "<span style='color:#2E7D32;'>0</span>")
        rows2 += (f"<tr><td style='padding:5px;'>{label}</td>"
                  f"<td style='padding:5px;text-align:center;'>{_fmt(r1)}</td>"
                  f"<td style='padding:5px;text-align:center;'>{_fmt(r2)}</td>"
                  f"<td style='padding:5px;text-align:center;'>{dcell}</td></tr>")
    st.markdown(f"<table style='width:100%;border-collapse:collapse;border:1px solid #ddd;'>{hdr2}{rows2}</table>",
                unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════
# ③ สรุปเทียบเงิน 3 ทาง
# ══════════════════════════════════════════════════════════════════════
def _branch_money(branch_id, D):
    df = read_sheet(SHEET_BRANCH_SALES)
    if df is None or df.empty or "branch_id" not in df.columns:
        return 0.0, 0.0, 0.0, 0.0
    m = df[(df["branch_id"].astype(str).str.strip() == str(branch_id)) &
           (df["sale_date"].astype(str).str[:10] == str(D)[:10])]
    if m.empty:
        return 0.0, 0.0, 0.0, 0.0
    cash = sum(_num(x) for x in m.get("cash_amount", pd.Series()).tolist())
    trans = sum(_num(x) for x in m.get("transfer_amount", pd.Series()).tolist())
    coup = sum(_num(x) for x in m.get("coupon_amount", pd.Series()).tolist())
    return cash, trans, coup, cash + trans + coup


def _render_money_summary():
    st.subheader("③ สรุปเทียบเงิน 3 ทาง (ต่อสาขา · วันขายจริง D)")
    bmap = _branch_group_map()
    if not bmap:
        st.warning("ยังไม่มีสาขาในระบบ")
        return
    c1, c2 = st.columns(2)
    with c1:
        branch_id = st.selectbox("🏪 สาขา", list(bmap.keys()),
                                 format_func=lambda b: f"{b} – {bmap[b][0]}", key="ms_br")
    with c2:
        D = st.date_input("📅 วันที่ขายจริง (D)", value=datetime.date.today() - datetime.timedelta(days=1), key="ms_d")
    Dp1 = D + datetime.timedelta(days=1)

    au_D   = _stock_row(SHEET_AUDIT_STOCK_BALANCE, "audit_date", D,   branch_id)
    au_Dp1 = _stock_row(SHEET_AUDIT_STOCK_BALANCE, "audit_date", Dp1, branch_id)

    # 3.1 เงินสาขาแจ้ง
    cash, trans, coup, money_branch = _branch_money(branch_id, D)

    # 3.2 เงิน = (ขนมไข่: จากบรรจุภัณฑ์กล่อง/ถุง) + (เครื่องดื่ม: แยกตามชนิดจากยอดขาย)
    pmap = _product_price_map()      # ราคาจากตารางสินค้า (อัตโนมัติ)

    # ── (ก) ขนมไข่ — คิดจากบรรจุภัณฑ์ที่ฝ่ายตรวจสอบนับ ──
    st.markdown("#### 3.2 (ก) ขนมไข่ — จากบรรจุภัณฑ์ที่ฝ่ายตรวจสอบนับ")
    st.caption("แต่ละช่องทาง = จำนวนบรรจุภัณฑ์ที่ใช้ไปของช่องทางนั้น · "
               "ยอดเงิน(บาท) = ใช้จริง − (LineMan+Grab+Shopee+TikTok+อื่นๆ+ชำรุด) · "
               "เป็นเงิน = ยอดเงิน(บาท) × ราคา")
    egg_money = 0.0
    no_money_total = 0.0     # ช่องทางไม่รับเงินที่ร้าน (ไม่รวมชำรุด)
    dmg_total = 0.0          # ชำรุด
    detail_rows = ""
    for key, label, af, df_field, price0, prod_id in PKG_CANON:
        if key not in ("box", "bag", "ybag"):   # เครื่องดื่ม (แก้ว) คิดแยกด้านล่าง
            continue
        used_audit = max(_num(au_D.get(af, 0)) - _num(au_Dp1.get(af, 0)), 0) if af else 0.0
        # จำนวนที่ใช้ไปแยกตามช่องทาง (จากที่สาขา key)
        ch_qty = {ch: (_branch_keyed_used(branch_id, D, df_field, channels=[ch]) if df_field else 0.0)
                  for ch in SALE_AUDIT_TABLE_CHANNELS}
        sum_ch = sum(ch_qty.values())
        nomoney = sum(ch_qty[c] for c in SALE_AUDIT_NOMONEY_CHANNELS)
        damaged = ch_qty.get("ชำรุด", 0.0)
        auto_price = _num(pmap.get(prod_id, 0)) if prod_id else 0.0
        default_price = auto_price if auto_price > 0 else float(price0)
        src = f" (สินค้า {prod_id})" if auto_price > 0 else ""
        price = st.number_input(f"ราคา/หน่วย — {label}{src}", min_value=0.0, step=1.0,
                                value=float(default_price), key=f"ms_price_{key}")
        payable = max(used_audit - sum_ch, 0)
        money = payable * price
        egg_money += money
        no_money_total += nomoney
        dmg_total += damaged
        cells = (f"<td style='padding:5px;'>{label}</td>"
                 f"<td style='padding:5px;text-align:center;'>{_fmt(used_audit)}</td>")
        for ch in SALE_AUDIT_TABLE_CHANNELS:
            color = "#C62828" if ch == "ชำรุด" else "#E65100"
            cells += (f"<td style='padding:5px;text-align:center;color:{color};'>"
                      f"{_fmt(ch_qty[ch])}</td>")
        cells += (f"<td style='padding:5px;text-align:center;font-weight:700;'>{_fmt(payable)}</td>"
                  f"<td style='padding:5px;text-align:center;'>{price:,.0f}</td>"
                  f"<td style='padding:5px;text-align:center;'>{money:,.2f}</td>")
        detail_rows += f"<tr>{cells}</tr>"
    hdr = ("<tr>" + "".join(
        f"<th style='padding:6px;background:#6A1B9A;color:#fff;font-size:.85rem;'>{h}</th>"
        for h in (["บรรจุภัณฑ์", "ใช้จริง(ตรวจสอบ)"] + SALE_AUDIT_TABLE_CHANNELS
                  + ["ยอดเงิน(บาท)", "ราคา", "เป็นเงิน"])) + "</tr>")
    st.markdown(f"<table style='width:100%;border-collapse:collapse;border:1px solid #ddd;font-size:.9rem;'>"
                f"{hdr}{detail_rows}</table>", unsafe_allow_html=True)

    # ── หมายเหตุ / รูปของชำรุด (จากที่สาขาบันทึกในช่องทาง) ──
    _notes = _branch_delivery_notes(branch_id, D)
    if _notes:
        st.markdown("###### 📝 หมายเหตุ / รูปของชำรุด (จากสาขา)")
        for ch, rm, ph in _notes:
            cc1, cc2 = st.columns([3, 1])
            with cc1:
                st.markdown(f"- **{ch}**: {rm if rm else '(ไม่มีหมายเหตุ)'}")
            with cc2:
                if ph:
                    try:
                        st.image(base64.b64decode(ph), width=110)
                    except Exception:
                        pass

    # ── (ข) เครื่องดื่ม/รายการอื่น — แยกตามชนิดจากยอดขายที่บันทึก × ราคาในระบบ ──
    st.markdown("#### 3.2 (ข) เครื่องดื่ม/รายการอื่น — แยกตามชนิด (จากยอดขายหน้าร้าน)")
    st.caption("ดึงจำนวนขายแต่ละชนิดจากยอดขายที่บันทึก (ตัด Delivery/Online) × ราคาในระบบ")
    pinfo = _products_info()
    drink_qty = _drink_sales(branch_id, D)
    drink_money = 0.0
    drows = ""
    for pid, qty in sorted(drink_qty.items()):
        name, price = pinfo.get(pid, (pid, 0.0))
        money = _num(qty) * _num(price)
        drink_money += money
        drows += (f"<tr><td style='padding:5px;'>{pid} – {name}</td>"
                  f"<td style='padding:5px;text-align:center;'>{_fmt(qty)}</td>"
                  f"<td style='padding:5px;text-align:center;'>{_num(price):,.0f}</td>"
                  f"<td style='padding:5px;text-align:center;font-weight:700;'>{money:,.2f}</td></tr>")
    if drows:
        dhdr = ("<tr>" + "".join(
            f"<th style='padding:6px;background:#00838F;color:#fff;'>{h}</th>"
            for h in ["สินค้า/เครื่องดื่ม", "จำนวนขาย", "ราคา", "เป็นเงิน"]) + "</tr>")
        st.markdown(f"<table style='width:100%;border-collapse:collapse;border:1px solid #ddd;'>{dhdr}{drows}</table>",
                    unsafe_allow_html=True)
    else:
        st.caption("— ยังไม่มียอดขายเครื่องดื่ม/รายการอื่นที่บันทึกไว้สำหรับวันนี้ —")

    total_pkg_money = egg_money + drink_money
    st.caption(f"ℹ️ ขนมไข่ {egg_money:,.2f} + เครื่องดื่ม/อื่นๆ {drink_money:,.2f} = **{total_pkg_money:,.2f}** · "
               f"บรรจุภัณฑ์ไม่รับเงิน (Delivery/Online) {no_money_total:,.0f} · เสียหาย {dmg_total:,.0f}")

    # 3.3 เงินเข้าธนาคารจริง
    money_bank = 0.0
    idf = read_sheet(SHEET_SALE_BANK_INCOME)
    if idf is not None and not idf.empty and "branch_id" in idf.columns:
        m = idf[(idf["branch_id"].astype(str) == str(branch_id)) &
                (idf["sale_date"].astype(str).str[:10] == str(D)[:10])]
        if not m.empty:
            money_bank = _num(m.iloc[-1].get("amount", 0))

    # ── ตารางเทียบ 3 คอลัมน์ ──
    st.markdown("#### 💰 ตารางเทียบเงิน 3 ทาง")

    # ── รอบ 3: เตือนสีแดง ถ้าฝ่าย Sale Audit ยังไม่โทร/บันทึกแก้ไข และเกิน 3 วัน ──
    try:
        _D_date = D if isinstance(D, datetime.date) else datetime.datetime.strptime(str(D)[:10], "%Y-%m-%d").date()
        _days_late = (datetime.date.today() - _D_date).days
    except Exception:
        _days_late = 0
    _resolved = _has_resolution(branch_id, D)
    _overdue = (_days_late > 3) and (not _resolved)
    _tbl_border = "#C62828" if _overdue else "#6A1B9A"
    if _overdue:
        st.markdown(
            "<div style='background:#C62828;color:#fff;padding:12px;border-radius:8px;"
            "font-weight:800;font-size:1.05rem;margin-bottom:8px;'>"
            f"🚨 เกินกำหนด {_days_late} วัน — ฝ่าย Sale Audit ยังไม่ได้โทร/บันทึกการแก้ไข "
            "ของวันขายนี้! กรุณาบันทึกการชี้แจง (เมนู ④ ด้านล่าง) โดยด่วน "
            "จนกว่าจะมีการแก้ไข ระบบจะเตือนสีแดงนี้ค้างไว้</div>",
            unsafe_allow_html=True)
    st.markdown(
        f"<table style='width:100%;border-collapse:collapse;border:2px solid {_tbl_border};'>"
        f"<tr>"
        f"<th style='padding:10px;background:#2E7D32;color:#fff;'>3.1 เงินจริงจากสาขาแจ้ง</th>"
        f"<th style='padding:10px;background:#6A1B9A;color:#fff;'>3.2 เงินจากบรรจุภัณฑ์ (ตรวจสอบ)</th>"
        f"<th style='padding:10px;background:#0D47A1;color:#fff;'>3.3 เงินเข้าธนาคารจริง (D)</th></tr>"
        f"<tr>"
        f"<td style='padding:14px;text-align:center;font-size:1.4rem;font-weight:800;'>฿{money_branch:,.2f}"
        f"<br><small style='color:#888;font-weight:400;'>โอน {trans:,.0f} · สด {cash:,.0f} · คูปอง {coup:,.0f}</small></td>"
        f"<td style='padding:14px;text-align:center;font-size:1.4rem;font-weight:800;'>฿{total_pkg_money:,.2f}"
        f"<br><small style='color:#888;font-weight:400;'>หัก Delivery/Online {no_money_total:,.0f} · เสียหาย {dmg_total:,.0f}</small></td>"
        f"<td style='padding:14px;text-align:center;font-size:1.4rem;font-weight:800;'>฿{money_bank:,.2f}</td></tr>"
        f"</table>", unsafe_allow_html=True)

    # แจ้ง DIFF
    diffs = []
    if abs(money_branch - money_bank) > 0.5:
        diffs.append(f"สาขาแจ้ง ≠ เงินเข้าธนาคาร ({money_branch - money_bank:+,.2f})")
    if abs(money_branch - total_pkg_money) > 0.5:
        diffs.append(f"สาขาแจ้ง ≠ บรรจุภัณฑ์ ({money_branch - total_pkg_money:+,.2f})")
    if diffs:
        st.markdown("<div style='background:#C62828;color:#fff;padding:10px;border-radius:8px;font-weight:700;'>"
                    "⚠️ พบส่วนต่าง (DIFF): " + " · ".join(diffs) + "</div>", unsafe_allow_html=True)
    else:
        st.success("✅ ยอดทั้ง 3 ทางตรงกัน")

    # ── รูปภาพที่สาขาแนบ (สลิป) + รูปเสียหายจากฝ่ายตรวจสอบ ──
    st.divider()
    st.markdown("##### 📎 รูปภาพประกอบ")
    sids = []
    sdf = read_sheet(SHEET_BRANCH_SALES)
    if sdf is not None and not sdf.empty:
        sids = sdf[(sdf["branch_id"].astype(str) == str(branch_id)) &
                   (sdf["sale_date"].astype(str).str[:10] == str(D)[:10])]["sale_id"].astype(str).tolist()
    slips = read_sheet(SHEET_BRANCH_SALES_SLIPS)
    shown = 0
    if slips is not None and not slips.empty and sids and "sale_id" in slips.columns:
        sl = slips[slips["sale_id"].astype(str).isin(sids)]
        cols = st.columns(4)
        for i, (_, r) in enumerate(sl.iterrows()):
            try:
                cols[i % 4].image(base64.b64decode(r["image_b64"]),
                                  caption=r.get("filename", "สลิปสาขา"), use_container_width=True)
                shown += 1
            except Exception:
                pass
    # รูปเสียหายจากฝ่ายตรวจสอบ (au_Dp1.damage_photo)
    dmg_photo = str(au_Dp1.get("damage_photo", "") or "")
    if dmg_photo:
        try:
            st.image(base64.b64decode(dmg_photo), caption="รูปบรรจุภัณฑ์เสียหาย (ฝ่ายตรวจสอบ)", width=280)
            shown += 1
        except Exception:
            pass
    if shown == 0:
        st.caption("— ยังไม่มีรูปภาพแนบ —")

    # ── ④ ฝ่าย Audit ชี้แจงการแก้ปัญหา (แก้ไขได้ · ลบไม่ได้) ──
    _render_resolution(branch_id, D)


def _render_resolution(branch_id, D):
    st.divider()
    st.markdown("#### ④ ฝ่าย Audit ชี้แจงการแก้ปัญหา (แก้ไขได้ · ลบไม่ได้)")
    rdf = read_sheet(SHEET_SALE_AUDIT_RESOLUTION)
    row = {}
    rid = None
    if rdf is not None and not rdf.empty and "branch_id" in rdf.columns:
        m = rdf[(rdf["branch_id"].astype(str) == str(branch_id)) &
                (rdf["sale_date"].astype(str).str[:10] == str(D)[:10])]
        if not m.empty:
            row = m.iloc[-1].to_dict()
            rid = str(row.get("resolution_id"))
            st.caption(f"📝 มีบันทึกชี้แจงแล้ว ({rid}) — แก้ไขเพิ่มเติมได้ (ลบไม่ได้)")

    c1, c2, c3 = st.columns([2, 1, 1])
    with c1:
        called_who = st.text_input("โทรหาใคร", value=str(row.get("called_who", "")), key=f"res_who_{branch_id}")
    with c2:
        cur_t = str(row.get("call_time", "")).strip()
        try:
            tval = datetime.time.fromisoformat(cur_t) if cur_t else datetime.datetime.now().time().replace(microsecond=0)
        except Exception:
            tval = datetime.datetime.now().time().replace(microsecond=0)
        call_time = st.time_input("เวลา", value=tval, key=f"res_time_{branch_id}")
    with c3:
        cur_d = str(row.get("call_date", "")).strip()
        try:
            dval = datetime.date.fromisoformat(cur_d[:10]) if cur_d else D
        except Exception:
            dval = D
        call_date = st.date_input("วันที่", value=dval, key=f"res_date_{branch_id}")

    how_fixed = st.text_area("แก้ไขอย่างไร", value=str(row.get("how_fixed", "")),
                             height=90, key=f"res_how_{branch_id}")

    # รูปภาพประกอบ (สูงสุด 5) — แสดงของเดิม + แนบใหม่ (แนบใหม่ = แทนที่)
    existing_photos = [str(row.get(f"photo{i}", "") or "") for i in range(1, 6)]
    existing_photos = [p for p in existing_photos if p]
    if existing_photos:
        st.caption(f"รูปที่แนบไว้ ({len(existing_photos)} รูป):")
        pc = st.columns(5)
        for i, p in enumerate(existing_photos):
            try:
                pc[i % 5].image(base64.b64decode(p), use_container_width=True)
            except Exception:
                pass
    new_files = st.file_uploader("📷 แนบรูปประกอบการแก้ปัญหา (สูงสุด 5 รูป — แนบใหม่ = แทนที่ของเดิม)",
                                 type=["png", "jpg", "jpeg"], accept_multiple_files=True,
                                 key=f"res_photos_{branch_id}")

    if st.button("💾 บันทึกการชี้แจง (แก้ไขได้ · ลบไม่ได้)", type="primary", key=f"res_save_{branch_id}"):
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        payload = {
            "sale_date": str(D), "branch_id": str(branch_id),
            "called_who": called_who.strip(), "call_time": str(call_time)[:5],
            "call_date": str(call_date), "how_fixed": how_fixed.strip(),
            "created_by": st.session_state.get("dept_name", ""), "updated_at": now,
        }
        # รูป: ถ้าแนบใหม่ → ใช้ชุดใหม่ (สูงสุด 5) ; ถ้าไม่แนบ → คงของเดิม
        if new_files:
            for i in range(1, 6):
                payload[f"photo{i}"] = ""
            for i, f in enumerate(new_files[:5], 1):
                try:
                    payload[f"photo{i}"] = base64.b64encode(f.read()).decode()
                except Exception:
                    payload[f"photo{i}"] = ""
        try:
            if rid:
                update_row(SHEET_SALE_AUDIT_RESOLUTION, "resolution_id", rid, payload)
                st.success("✅ อัปเดตการชี้แจงแล้ว")
            else:
                payload["resolution_id"] = next_id(rdf, "resolution_id", "RES")
                payload["created_at"] = now
                append_row(SHEET_SALE_AUDIT_RESOLUTION, payload)
                st.success(f"✅ บันทึกการชี้แจงสำเร็จ ({payload['resolution_id']})")
            st.rerun()
        except Exception as e:
            st.error(f"บันทึกไม่สำเร็จ: {e} (รัน roon_sale_audit_resolution.sql)")
