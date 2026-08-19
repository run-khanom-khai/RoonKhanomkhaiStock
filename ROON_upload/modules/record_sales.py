"""
record_sales.py  –  เมนู "บันทึกรายการขาย" (รอบที่ 1)
ผู้พัฒนา: ดร.วรรณ (ดร.อภิวรรณ์ ดำแสงสวัสดิ์)

โครงสร้างตามที่ ดร.วรรณ กำหนด:
  5.1 บันทึกวันที่
  5.2 บันทึกยอดขาย 3 ยอด
      5.2.1 เงินสด (บาท) + แนบสลิปโอน ≤ 3 รูป (ใส่ทีหลังได้)
      5.2.2 เงินโอน (บาท)
      5.2.3 เงินจากคูปอง (บาท) — เลขที่คูปองหลายเลขได้
             ตรวจจากตารางคูปอง (file คูปอง) ถ้าไม่มี/ถูกใช้แล้ว = บันทึกไม่ได้
             ยอดเงินคูปอง = รวมมูลค่าจากตารางคูปอง (กันสาขาโกงเงิน)
      5.2.4 จำนวนชนิดการขายที่สาขาไม่ได้รับเงิน (Grab / LineMan / อื่นๆ)
             แต่ละช่อง: ขนมไข่กล่อง, ขนมไข่ถุง, ถุงหูหิ้วกระดาษพิมพ์ลาย
  - แก้ไข / ลบ ได้ (ลบต้องยืนยันก่อน)
  - ระบบคำนวณยอดรวม (เงินสด + เงินโอน + เงินคูปอง)
"""
import io
import base64
import datetime
import pandas as pd
import streamlit as st

from config import (
    SHEET_COUPONS,
    SHEET_PRODUCTS,
    SHEET_BRANCH_SALES,
    SHEET_BRANCH_SALES_COUPONS,
    SHEET_BRANCH_SALES_SLIPS,
    SHEET_BRANCH_SALES_DELIVERY,
)
from modules.excel_db import (
    read_sheet, write_sheet, init_workbook, append_row, update_row, delete_row,
)
from utils.id_generator import next_id


FRONT_CHANNEL     = "หน้าร้าน"
DELIVERY_CHANNELS = ["Grab", "LineMan", "อื่นๆ"]
ALL_PKG_CHANNELS  = [FRONT_CHANNEL] + DELIVERY_CHANNELS
MAX_SLIPS = 3

# นิยามชนิดบรรจุภัณฑ์: key -> (label, unit)
PKG_FIELD_DEFS = {
    "box_qty":                ("ขนมไข่ชนิดกล่อง",        "กล่อง"),
    "bag_qty":                ("ชนิดถุง",                 "ถุง"),
    "yellow_premium_bag_qty": ("ถุงหูหิ้วกระดาษพิมพ์ลาย", "ถุง"),
    "drip_box_qty":           ("กล่องดริป",              "กล่อง"),
    "water_cup_qty":          ("แก้วน้ำ",                 "ใบ"),
    "ice_cream_cup_qty":      ("แก้วไอศครีม",             "ใบ"),
    "ice_cream_ring_qty":     ("วงแหวนรองถ้วยไอศครีม",   "แผ่น"),
}
# หน้าร้าน: 7 ชนิด (ครบ รวมแก้วไอศครีม + วงแหวนรองถ้วยไอศครีม)
# Delivery: 5 ชนิด (ไม่มีแก้วไอศครีม + วงแหวน)
FRONT_FIELDS    = ["box_qty", "bag_qty", "yellow_premium_bag_qty",
                   "drip_box_qty", "water_cup_qty",
                   "ice_cream_cup_qty", "ice_cream_ring_qty"]
DELIVERY_FIELDS = ["box_qty", "bag_qty", "yellow_premium_bag_qty",
                   "drip_box_qty", "water_cup_qty"]
ALL_PKG_FIELDS  = FRONT_FIELDS   # ชุดรวมครบทุกชนิด (ใช้เก็บข้อมูล + สรุป)

# ป้ายกำกับสรุป (ครบ 7 ชนิด ตามที่ ดร.วรรณ กำหนด) — key -> label ที่ใช้ในสรุป
PKG_FIELDS = [
    ("box_qty",                "กล่อง"),
    ("bag_qty",                "ถุงกระดาษขาว"),
    ("yellow_premium_bag_qty", "ถุงหูหิ้วกระดาษพิมพ์ลาย"),
    ("drip_box_qty",           "กล่องดริป"),
    ("water_cup_qty",          "แก้วน้ำ"),
    ("ice_cream_cup_qty",      "แก้วไอศครีม"),
    ("ice_cream_ring_qty",     "วงแหวนรองถ้วยไอศครีม"),
]


# ══════════════════════════════════════════════════════════════
# SCHEMAS
# ══════════════════════════════════════════════════════════════
SHEET_SCHEMAS = {
    SHEET_BRANCH_SALES: [
        "sale_id", "sale_date", "branch_id",
        "cash_amount", "transfer_amount", "coupon_amount", "total_amount",
        # ตีแป้ง — วัตถุดิบที่ใช้ไป
        "eggs_used",
        "flour_finished_big_used", "flour_finished_small_used",
        "mix_big_used", "mix_small_used",
        # ขนมไข่คงเหลือ
        "leftover_box_qty", "leftover_loose_pieces", "leftover_total_pieces",
        "box_unit_price", "leftover_value",
        "remark", "status", "created_at", "updated_at",
    ],
    SHEET_BRANCH_SALES_COUPONS: [
        "id", "sale_id", "branch_id", "coupon_no", "amount",
    ],
    SHEET_BRANCH_SALES_SLIPS: [
        "id", "sale_id", "branch_id", "filename", "image_b64", "uploaded_at",
    ],
    SHEET_BRANCH_SALES_DELIVERY: [
        "id", "sale_id", "branch_id", "channel",
        "box_qty", "bag_qty", "yellow_premium_bag_qty", "drip_box_qty",
        "water_cup_qty", "ice_cream_cup_qty", "ice_cream_ring_qty",
    ],
    # ตารางคูปองแม่ (HQ เติมในรอบ 3) — สร้าง header เผื่อไว้
    SHEET_COUPONS: [
        "coupon_no", "amount", "status",
        "used_branch_id", "used_sale_id", "used_at", "issued_at",
    ],
}


def _init_sheets():
    if st.session_state.get("_init_rs"):
        return
    init_workbook()
    for sheet_name, columns in SHEET_SCHEMAS.items():
        try:
            df = read_sheet(sheet_name)
        except Exception:
            df = pd.DataFrame()
        # สร้าง header เฉพาะกรณีตารางว่างจริง ๆ (กันลบข้อมูลเดิม)
        if df is None or df.empty:
            try:
                write_sheet(sheet_name, pd.DataFrame(columns=columns))
            except Exception:
                pass
    st.session_state["_init_rs"] = True


# ══════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════
def _num(v, default=0.0):
    try:
        return float(str(v).replace(",", "").strip() or 0)
    except Exception:
        return default


def _int(v, default=0):
    try:
        return int(float(str(v).replace(",", "").strip() or 0))
    except Exception:
        return default


def _encode_image(uploaded_file) -> str:
    """แปลงรูปเป็น base64 (ย่อขนาดเพื่อไม่ให้ฐานข้อมูลใหญ่เกิน)"""
    raw = uploaded_file.getvalue()
    try:
        from PIL import Image
        img = Image.open(io.BytesIO(raw))
        img = img.convert("RGB")
        # ย่อด้านยาวสุดไม่เกิน 1000 px
        max_side = 1000
        w, h = img.size
        if max(w, h) > max_side:
            if w >= h:
                nw, nh = max_side, int(h * max_side / w)
            else:
                nw, nh = int(w * max_side / h), max_side
            img = img.resize((nw, nh))
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=70)
        raw = buf.getvalue()
    except Exception:
        pass  # ไม่มี PIL หรือไฟล์ไม่ใช่รูป — เก็บ raw
    return base64.b64encode(raw).decode()


def _lookup_coupon(coupon_no: str):
    """คืน (found, amount, status_msg) จากตารางคูปองแม่"""
    coupon_no = str(coupon_no).strip()
    if not coupon_no:
        return (False, 0.0, "ว่าง")
    try:
        cdf = read_sheet(SHEET_COUPONS)
    except Exception:
        cdf = pd.DataFrame()
    if cdf is None or cdf.empty or "coupon_no" not in cdf.columns:
        return (False, 0.0, "ยังไม่มีข้อมูลคูปองในระบบ")
    row = cdf[cdf["coupon_no"].astype(str).str.strip() == coupon_no]
    if row.empty:
        return (False, 0.0, "ไม่พบเลขคูปองนี้")
    status = str(row.iloc[0].get("status", "")).strip().lower()
    if status in ("used", "ใช้แล้ว"):
        return (False, 0.0, "คูปองนี้ถูกใช้ไปแล้ว")
    amount = _num(row.iloc[0].get("amount", 0))
    return (True, amount, "ใช้ได้")


def _validate_coupons(raw_text: str, exclude_sale_id: str = ""):
    """
    รับข้อความเลขคูปอง (บรรทัดละ 1 เลข) คืน:
      valid_list = [{"coupon_no":..., "amount":...}]
      errors     = [ข้อความ error]
      total      = รวมมูลค่า
    exclude_sale_id: ตอนแก้ไข ให้ไม่ต้องเช็คคูปองของ sale เดิม (ถือว่าใช้ได้)
    """
    lines = [l.strip() for l in str(raw_text).splitlines() if l.strip()]
    valid, errors = [], []
    seen = set()

    # คูปองที่ sale เดิมเคยใช้ (กรณีแก้ไข) — อนุญาตให้ใช้ซ้ำได้
    own_coupons = set()
    if exclude_sale_id:
        try:
            scdf = read_sheet(SHEET_BRANCH_SALES_COUPONS)
            if not scdf.empty and "sale_id" in scdf.columns:
                own = scdf[scdf["sale_id"].astype(str) == str(exclude_sale_id)]
                own_coupons = set(own["coupon_no"].astype(str).str.strip())
        except Exception:
            pass

    for cno in lines:
        if cno in seen:
            errors.append(f"เลข {cno}: ซ้ำในรายการนี้")
            continue
        seen.add(cno)

        if cno in own_coupons:
            # คูปองเดิมของ sale นี้ — หา amount จากตารางแม่
            _f, amt, _m = _lookup_coupon(cno)
            if amt == 0:
                # เผื่อ status ถูก mark used อยู่ — ดึง amount จากตารางแม่ตรง ๆ
                try:
                    cdf = read_sheet(SHEET_COUPONS)
                    r = cdf[cdf["coupon_no"].astype(str).str.strip() == cno]
                    if not r.empty:
                        amt = _num(r.iloc[0].get("amount", 0))
                except Exception:
                    pass
            valid.append({"coupon_no": cno, "amount": amt})
            continue

        ok, amt, msg = _lookup_coupon(cno)
        if ok:
            valid.append({"coupon_no": cno, "amount": amt})
        else:
            errors.append(f"เลข {cno}: {msg}")

    total = sum(c["amount"] for c in valid)
    return valid, errors, total


def _mark_coupons_used(valid_coupons, branch_id, sale_id):
    used_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for c in valid_coupons:
        try:
            update_row(SHEET_COUPONS, "coupon_no", c["coupon_no"], {
                "status":        "used",
                "used_branch_id": branch_id,
                "used_sale_id":   sale_id,
                "used_at":        used_at,
            })
        except Exception:
            pass


def _release_coupons(sale_id):
    """คืนคูปองของ sale นี้ให้กลับเป็น active (ตอนลบ/แก้ไข)"""
    try:
        scdf = read_sheet(SHEET_BRANCH_SALES_COUPONS)
    except Exception:
        return
    if scdf.empty or "sale_id" not in scdf.columns:
        return
    own = scdf[scdf["sale_id"].astype(str) == str(sale_id)]
    for cno in own["coupon_no"].astype(str):
        try:
            update_row(SHEET_COUPONS, "coupon_no", cno.strip(), {
                "status":         "active",
                "used_branch_id": "",
                "used_sale_id":   "",
                "used_at":        "",
            })
        except Exception:
            pass


def _delete_children(sale_id):
    """ลบ coupon / slip / delivery ที่ผูกกับ sale นี้"""
    for sheet in (SHEET_BRANCH_SALES_COUPONS, SHEET_BRANCH_SALES_SLIPS,
                  SHEET_BRANCH_SALES_DELIVERY):
        try:
            df = read_sheet(sheet)
            if df.empty or "sale_id" not in df.columns:
                continue
            keep = df[df["sale_id"].astype(str) != str(sale_id)]
            write_sheet(sheet, keep)
        except Exception:
            pass


def _slip_count(sale_id) -> int:
    try:
        df = read_sheet(SHEET_BRANCH_SALES_SLIPS)
        if df.empty or "sale_id" not in df.columns:
            return 0
        return int((df["sale_id"].astype(str) == str(sale_id)).sum())
    except Exception:
        return 0


def _save_slips(uploaded_files, branch_id, sale_id, existing=0):
    """เก็บสลิป (base64) — รวมไม่เกิน MAX_SLIPS"""
    if not uploaded_files:
        return 0
    room = MAX_SLIPS - existing
    saved = 0
    uploaded_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for uf in uploaded_files[:max(0, room)]:
        try:
            b64 = _encode_image(uf)
            sdf = read_sheet(SHEET_BRANCH_SALES_SLIPS)
            sid = next_id(sdf, "id", "SLP")
            append_row(SHEET_BRANCH_SALES_SLIPS, {
                "id":          sid,
                "sale_id":     sale_id,
                "branch_id":   branch_id,
                "filename":    getattr(uf, "name", "slip.jpg"),
                "image_b64":   b64,
                "uploaded_at": uploaded_at,
            })
            saved += 1
        except Exception:
            pass
    return saved


# ══════════════════════════════════════════════════════════════
# MAIN RENDER
# ══════════════════════════════════════════════════════════════
def render():
    _init_sheets()

    # ── ชื่อเมนูใหญ่ ตรงกลางด้านบน ──────────────────────────
    st.markdown(
        "<h1 style='text-align:center;color:#E65100;font-size:2rem;"
        "font-weight:900;margin-bottom:2px;'>🧾 บันทึกรายการขาย</h1>"
        "<p style='text-align:center;color:#999;margin-top:0;'>"
        "บันทึกยอดขายประจำวัน — เงินสด / เงินโอน / คูปอง และยอด Delivery</p>",
        unsafe_allow_html=True,
    )
    st.divider()

    branch_id = str(st.session_state.get("locked_branch_id", "")).strip()
    if not branch_id:
        st.error("❌ ไม่พบรหัสสาขา กรุณาเข้าสู่ระบบใหม่")
        return

    # ① วันที่ — ถ้าวันที่นั้นมีข้อมูลแล้ว จะดึงขึ้นมาให้แก้ไข/แนบสลิป/ลบ ได้เลย
    st.subheader("① วันที่")
    sale_date = st.date_input("📅 วันที่ขาย", value=datetime.date.today(),
                              key="rs_date")
    existing = _find_sale(branch_id, str(sale_date))
    if existing is not None:
        st.info(f"📄 พบข้อมูลของวันที่ {sale_date} แล้ว — แก้ไข / แนบสลิป / ลบ ได้ด้านล่าง")
    else:
        st.caption("➕ ยังไม่มีข้อมูลของวันที่นี้ — กรอกเพื่อบันทึกใหม่")

    _render_form(branch_id, str(sale_date), existing)


# ══════════════════════════════════════════════════════════════
# ตัวช่วยค้นหา/ดึงข้อมูลเดิม
# ══════════════════════════════════════════════════════════════
def _find_sale(branch_id, date_str):
    """หา sale ของสาขา+วันที่ (ถ้ามีหลายอัน เอาอันล่าสุด) — คืน Series หรือ None"""
    try:
        df = read_sheet(SHEET_BRANCH_SALES)
    except Exception:
        return None
    if df is None or df.empty or "branch_id" not in df.columns:
        return None
    m = df[(df["branch_id"].astype(str).str.strip() == str(branch_id)) &
           (df["sale_date"].astype(str) == str(date_str))]
    if m.empty:
        return None
    if "created_at" in m.columns:
        m = m.sort_values("created_at")
    return m.iloc[-1]


def _get_slips_df(sale_id):
    try:
        sdf = read_sheet(SHEET_BRANCH_SALES_SLIPS)
        if not sdf.empty and "sale_id" in sdf.columns:
            return sdf[sdf["sale_id"].astype(str) == str(sale_id)]
    except Exception:
        pass
    return pd.DataFrame()


def _get_coupons_text(sale_id):
    try:
        scdf = read_sheet(SHEET_BRANCH_SALES_COUPONS)
        if not scdf.empty and "sale_id" in scdf.columns:
            my = scdf[scdf["sale_id"].astype(str) == str(sale_id)]
            return "\n".join(my["coupon_no"].astype(str))
    except Exception:
        pass
    return ""


def _get_delivery_defaults(sale_id):
    d = {}
    try:
        ddf = read_sheet(SHEET_BRANCH_SALES_DELIVERY)
        if not ddf.empty and "sale_id" in ddf.columns:
            my = ddf[ddf["sale_id"].astype(str) == str(sale_id)]
            for _, r in my.iterrows():
                d[str(r["channel"])] = r.to_dict()
    except Exception:
        pass
    return d


# ══════════════════════════════════════════════════════════════
# ฟอร์มเดียว — บันทึกใหม่ / แก้ไข / ลบ (ตามวันที่ที่เลือก)
# ══════════════════════════════════════════════════════════════
def _render_form(branch_id, sale_date_str, existing):
    is_edit = existing is not None
    ex = existing.to_dict() if is_edit else {}
    sale_id = str(ex.get("sale_id", "")) if is_edit else ""
    suffix = sale_id if is_edit else f"new_{sale_date_str}"

    st.divider()
    st.subheader("② ยอดขาย")
    col1, col2 = st.columns(2)
    cash_amount = col1.number_input(
        "💵 เงินสด (บาท)", min_value=0.0, step=1.0, format="%.2f",
        value=_num(ex.get("cash_amount", 0)), key=f"rs_cash_{suffix}")
    transfer_amount = col2.number_input(
        "📲 เงินโอน (บาท)", min_value=0.0, step=1.0, format="%.2f",
        value=_num(ex.get("transfer_amount", 0)), key=f"rs_transfer_{suffix}")

    # ── สลิปการโอน ─────────────────────────────────────────
    st.markdown("**🧾 สลิปการโอน** (ไม่เกิน 3 รูป — แนบเพิ่ม/มาใส่ทีหลังได้)")
    existing_slips = _get_slips_df(sale_id) if is_edit else pd.DataFrame()
    if not existing_slips.empty:
        scols = st.columns(min(3, len(existing_slips)))
        for i, (_, sr) in enumerate(existing_slips.iterrows()):
            try:
                scols[i % len(scols)].image(
                    base64.b64decode(sr["image_b64"]),
                    caption=sr.get("filename", ""), use_container_width=True)
            except Exception:
                pass
    room = MAX_SLIPS - len(existing_slips)
    if room > 0:
        slip_files = st.file_uploader(
            f"แนบสลิป (เพิ่มได้อีก {room} รูป)", type=["png", "jpg", "jpeg"],
            accept_multiple_files=True, key=f"rs_slips_{suffix}")
    else:
        slip_files = None
        st.caption("🧾 แนบสลิปครบ 3 รูปแล้ว")

    st.divider()
    # ── คูปอง ──────────────────────────────────────────────
    st.markdown("**🎟️ เงินจากคูปอง** — พิมพ์เลขคูปอง (บรรทัดละ 1 เลข)")
    coupon_text = st.text_area(
        "เลขคูปอง", value=(_get_coupons_text(sale_id) if is_edit else ""),
        key=f"rs_coupons_{suffix}", height=90, placeholder="เช่น\nRC-0001\nRC-0002")
    valid_coupons, coupon_errors, coupon_amount = _validate_coupons(
        coupon_text, exclude_sale_id=sale_id)
    if coupon_text.strip():
        if valid_coupons:
            st.success("คูปองที่ใช้ได้: " + ", ".join(
                f"{c['coupon_no']} (฿{c['amount']:,.0f})" for c in valid_coupons))
        for e in coupon_errors:
            st.error(f"❌ {e}")
    st.metric("🎟️ เงินจากคูปอง (คำนวณจากระบบ)", f"฿{coupon_amount:,.2f}")

    st.divider()
    # ── บรรจุภัณฑ์ที่ขายได้ ─────────────────────────────────
    st.subheader("③ จำนวนบรรจุภัณฑ์ที่ขายได้ (หน้าร้าน + Delivery)")
    d_defaults = _get_delivery_defaults(sale_id) if is_edit else None
    delivery_rows = _packaging_inputs(prefix=f"rs_pkg_{suffix}", defaults=d_defaults)
    _summ = _pkg_summary(delivery_rows)
    st.markdown("##### 📊 สรุปจำนวนบรรจุภัณฑ์รวม")
    for i in range(0, len(PKG_FIELDS), 4):
        chunk = PKG_FIELDS[i:i + 4]
        sc = st.columns(len(chunk))
        for col, (field, label) in zip(sc, chunk):
            col.metric(label, f"{_summ[field]:,} {PKG_FIELD_DEFS[field][1]}")

    st.divider()
    # ── ตีแป้ง & ขนมไข่คงเหลือ ─────────────────────────────
    st.subheader("④ ตีแป้ง & ขนมไข่คงเหลือ")
    leftover = _leftover_inputs(prefix=f"rs_lo_{suffix}", defaults=ex)

    st.divider()
    # ── ยอดรวม ─────────────────────────────────────────────
    total_amount = cash_amount + transfer_amount + coupon_amount
    st.subheader("⑤ ยอดรวม")
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("💵 เงินสด", f"฿{cash_amount:,.2f}")
    m2.metric("📲 เงินโอน", f"฿{transfer_amount:,.2f}")
    m3.metric("🎟️ คูปอง", f"฿{coupon_amount:,.2f}")
    m4.metric("💰 รวมทั้งหมด", f"฿{total_amount:,.2f}")

    remark = st.text_input("📝 หมายเหตุ (ถ้ามี)",
                           value=str(ex.get("remark", "")), key=f"rs_remark_{suffix}")

    st.divider()
    # ── ปุ่มบันทึก / ลบ ────────────────────────────────────
    if is_edit:
        bcol1, bcol2 = st.columns(2)
        save_label = "💾 บันทึกการแก้ไข"
    else:
        bcol1 = st.container()
        bcol2 = None
        save_label = "💾 บันทึกรายการขาย"

    save_clicked = bcol1.button(save_label, type="primary",
                                use_container_width=True, key=f"rs_save_{suffix}")

    if save_clicked:
        if coupon_text.strip() and coupon_errors:
            st.error("❌ มีเลขคูปองที่ใช้ไม่ได้ กรุณาแก้ไขก่อนบันทึก")
            return
        if not is_edit and total_amount <= 0 and not any(
            any(_int(r.get(f, 0)) for f in ALL_PKG_FIELDS) for r in delivery_rows
        ) and not any(_int(leftover.get(k, 0)) for k in
                      BATTER_KEYS + ["leftover_box_qty", "leftover_loose_pieces"]):
            st.error("❌ ยังไม่มีข้อมูลให้บันทึก")
            return

        if is_edit:
            _save_edit(
                sale_id=sale_id, branch_id=branch_id, sale_date=sale_date_str,
                cash_amount=cash_amount, transfer_amount=transfer_amount,
                coupon_amount=coupon_amount, total_amount=total_amount,
                remark=remark, valid_coupons=valid_coupons,
                delivery_rows=delivery_rows, leftover=leftover)
            n = _save_slips(slip_files, branch_id, sale_id,
                            existing=len(existing_slips))
            st.success("✅ บันทึกการแก้ไขสำเร็จ" +
                       (f" | เพิ่มสลิป {n} รูป" if n else ""))
            st.rerun()
        else:
            _save_new(
                branch_id=branch_id, sale_date=sale_date_str,
                cash_amount=cash_amount, transfer_amount=transfer_amount,
                coupon_amount=coupon_amount, total_amount=total_amount,
                remark=remark, valid_coupons=valid_coupons,
                delivery_rows=delivery_rows, slip_files=slip_files,
                leftover=leftover)
            st.rerun()

    # ปุ่มลบ (เฉพาะกรณีมีข้อมูลอยู่แล้ว)
    if is_edit and bcol2 is not None:
        if st.session_state.get(f"rs_del_{suffix}"):
            bcol2.warning("⚠️ ยืนยันลบข้อมูลของวันนี้?")
            dc1, dc2 = bcol2.columns(2)
            if dc1.button("✅ ยืนยันลบ", key=f"rs_delyes_{suffix}",
                          type="primary", use_container_width=True):
                _delete_sale(sale_id)
                st.session_state.pop(f"rs_del_{suffix}", None)
                st.success("ลบข้อมูลแล้ว")
                st.rerun()
            if dc2.button("ยกเลิก", key=f"rs_delno_{suffix}",
                          use_container_width=True):
                st.session_state.pop(f"rs_del_{suffix}", None)
                st.rerun()
        else:
            if bcol2.button("🗑️ ลบข้อมูลวันนี้", use_container_width=True,
                            key=f"rs_delbtn_{suffix}"):
                st.session_state[f"rs_del_{suffix}"] = True
                st.rerun()


# ══════════════════════════════════════════════════════════════
# บันทึกรายการใหม่
# ══════════════════════════════════════════════════════════════
PCS_PER_BOX = 20          # ขนมไข่ 1 กล่อง = 20 ชิ้น
DEFAULT_BOX_PRICE = 130.0  # ราคาต่อกล่องสำรอง (ถ้าอ่านจากตาราง products ไม่ได้)


def _box_price():
    """อ่าน 'ราคาขายชนิดกล่อง' จากตาราง products (สินค้าขนมไข่ชนิดกล่อง 20 ชิ้น)
    คืน (price, source) — source บอกที่มาของราคาเพื่อให้ตรวจสอบได้
    """
    try:
        df = read_sheet(SHEET_PRODUCTS)
        if df is not None and not df.empty and "price" in df.columns:
            name = df.get("product_name", pd.Series([""] * len(df))).astype(str)
            ptype = df.get("product_type", pd.Series([""] * len(df))).astype(str)
            # หาสินค้า 'ขนมไข่ชนิดกล่อง' — ชื่อมี 20 หรือ กล่อง และเป็นประเภทขนมไข่
            cand = df[(name.str.contains("20") | name.str.contains("กล่อง"))]
            egg = cand[ptype.loc[cand.index].str.contains("ขนมไข่")]
            if not egg.empty:
                cand = egg
            if not cand.empty:
                price = _num(cand.iloc[0].get("price", 0))
                if price > 0:
                    return price, f"ตาราง products: {cand.iloc[0].get('product_name','')}"
    except Exception:
        pass
    return DEFAULT_BOX_PRICE, "ค่าเริ่มต้น (ยังไม่พบราคาในตาราง products)"


# ตีแป้ง — วัตถุดิบที่ใช้ไป: (key, label, unit)
BATTER_FIELDS = [
    ("eggs_used",                 "จำนวนไข่ที่ใช้ไป",       "ฟอง"),
    ("flour_finished_big_used",   "แป้งสำเร็จรูปถุงใหญ่",   "ถุง"),
    ("flour_finished_small_used", "แป้งสำเร็จรูปถุงเล็ก",   "ถุง"),
    ("mix_big_used",              "ส่วนผสมถุงใหญ่",         "ถุง"),
    ("mix_small_used",            "ส่วนผสมถุงเล็ก",         "ถุง"),
]
BATTER_KEYS = [k for k, _l, _u in BATTER_FIELDS]


def _leftover_inputs(prefix, defaults=None):
    """ช่องกรอก ตีแป้ง (วัตถุดิบที่ใช้ไป) + ขนมไข่คงเหลือ
    คืน dict ค่าที่กรอกทั้งหมด
    """
    d = defaults or {}
    vals = {}

    # ── ตีแป้ง : วัตถุดิบที่ใช้ไป ────────────────────────────
    st.markdown("**🥚 ตีแป้ง — วัตถุดิบที่ใช้ไป**")
    for i in range(0, len(BATTER_FIELDS), 3):
        chunk = BATTER_FIELDS[i:i + 3]
        cols = st.columns(len(chunk))
        for col, (k, label, unit) in zip(cols, chunk):
            vals[k] = col.number_input(
                f"{label} ({unit})", min_value=0, step=1,
                value=_int(d.get(k, 0)), key=f"{prefix}_{k}")

    # ── ขนมไข่คงเหลือ (ขึ้นบรรทัดใหม่) ──────────────────────
    st.markdown("**🍰 ขนมไข่คงเหลือ (ยังขายได้แต่ยังไม่ได้ขาย)**")
    c1, c2 = st.columns(2)
    vals["leftover_box_qty"] = c1.number_input(
        "จำนวนกล่อง (กล่องละ 20 ชิ้น)", min_value=0, step=1,
        value=_int(d.get("leftover_box_qty", 0)), key=f"{prefix}_lbox")
    vals["leftover_loose_pieces"] = c2.number_input(
        "ไม่ใส่บรรจุภัณฑ์ (ชิ้น)", min_value=0, step=1,
        value=_int(d.get("leftover_loose_pieces", 0)), key=f"{prefix}_lloose")

    vals["leftover_total_pieces"] = (vals["leftover_box_qty"] * PCS_PER_BOX
                                     + vals["leftover_loose_pieces"])
    vals["box_unit_price"] = 0
    vals["leftover_value"] = 0
    return vals


def _pkg_channel_input(ch, prefix, defaults, fields, expanded=False, boxed=False):
    """ช่องกรอกบรรจุภัณฑ์ 1 ช่องทาง (ตามรายการ fields) — คืน dict ครบทุกชนิด"""
    k = ch.replace(" ", "_")
    d = (defaults or {}).get(ch, {})
    icon = {"หน้าร้าน": "🏪", "Grab": "🛵", "LineMan": "🛵"}.get(ch, "📦")

    def _render():
        vals = {}
        # จัดเป็นแถวละไม่เกิน 4 ช่อง เพื่อไม่ให้แคบเกินไป
        for i in range(0, len(fields), 4):
            chunk = fields[i:i + 4]
            cols = st.columns(len(chunk))
            for col, fld in zip(cols, chunk):
                label, unit = PKG_FIELD_DEFS[fld]
                with col:
                    vals[fld] = st.number_input(
                        f"{label} ({unit}) – {ch}", min_value=0, step=1,
                        value=_int(d.get(fld, 0)), key=f"{prefix}_{fld}_{k}")
        return vals

    if boxed:
        st.markdown(f"**{icon} {ch}**")
        vals = _render()
    else:
        with st.expander(f"{icon} {ch}", expanded=expanded):
            vals = _render()

    row = {"channel": ch}
    for fld in ALL_PKG_FIELDS:      # เก็บครบทุกชนิด (ที่ไม่มีในช่องทางนี้ = 0)
        row[fld] = vals.get(fld, 0)
    return row


def _packaging_inputs(prefix, defaults=None):
    """สร้างช่องกรอกบรรจุภัณฑ์ทั้ง หน้าร้าน + Grab/LineMan/อื่นๆ — คืน list of dict"""
    rows = []
    # หน้าร้าน (5 ชนิด, แสดงเต็ม ไม่พับ)
    st.markdown("#### 🏪 หน้าร้าน")
    st.caption("⚠️ ถ้าสาขาไหนไม่มีบางบรรจุภัณฑ์ ให้กดผ่าน (ปล่อยเป็น 0) ได้เลย")
    rows.append(_pkg_channel_input(FRONT_CHANNEL, prefix, defaults,
                                   FRONT_FIELDS, boxed=True))
    # Delivery (7 ชนิด)
    st.markdown("#### 🛵 Delivery (Grab / LineMan / อื่นๆ)")
    for ch in DELIVERY_CHANNELS:
        rows.append(_pkg_channel_input(ch, prefix, defaults,
                                       DELIVERY_FIELDS, expanded=(ch == "Grab")))
    return rows


def _pkg_summary(rows):
    """สรุปยอดบรรจุภัณฑ์รวม (หน้าร้าน + Delivery) แยกตามชนิด (ตามรายการสรุป)"""
    return {field: sum(_int(r.get(field, 0)) for r in rows)
            for field, _label in PKG_FIELDS}


def _delivery_row_dict(did, sale_id, branch_id, r):
    """สร้าง dict สำหรับบันทึกลงตาราง delivery — เก็บครบทุกชนิดบรรจุภัณฑ์"""
    d = {"id": did, "sale_id": sale_id, "branch_id": branch_id,
         "channel": r["channel"]}
    for fld in ALL_PKG_FIELDS:
        d[fld] = r.get(fld, 0)
    return d


def _save_new(**kw):
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sales_df = read_sheet(SHEET_BRANCH_SALES)
    sale_id = next_id(sales_df, "sale_id", "SAL")

    _lo = kw.get("leftover", {}) or {}
    append_row(SHEET_BRANCH_SALES, {
        "sale_id":         sale_id,
        "sale_date":       kw["sale_date"],
        "branch_id":       kw["branch_id"],
        "cash_amount":     kw["cash_amount"],
        "transfer_amount": kw["transfer_amount"],
        "coupon_amount":   kw["coupon_amount"],
        "total_amount":    kw["total_amount"],
        "eggs_used":                 _lo.get("eggs_used", 0),
        "flour_finished_big_used":   _lo.get("flour_finished_big_used", 0),
        "flour_finished_small_used": _lo.get("flour_finished_small_used", 0),
        "mix_big_used":              _lo.get("mix_big_used", 0),
        "mix_small_used":            _lo.get("mix_small_used", 0),
        "leftover_box_qty":      _lo.get("leftover_box_qty", 0),
        "leftover_loose_pieces": _lo.get("leftover_loose_pieces", 0),
        "leftover_total_pieces": _lo.get("leftover_total_pieces", 0),
        "box_unit_price":        _lo.get("box_unit_price", 0),
        "leftover_value":        _lo.get("leftover_value", 0),
        "remark":          kw["remark"],
        "status":          "submitted",
        "created_at":      now,
        "updated_at":      now,
    })

    # คูปอง
    for c in kw["valid_coupons"]:
        cdf = read_sheet(SHEET_BRANCH_SALES_COUPONS)
        cid = next_id(cdf, "id", "SC")
        append_row(SHEET_BRANCH_SALES_COUPONS, {
            "id":        cid,
            "sale_id":   sale_id,
            "branch_id": kw["branch_id"],
            "coupon_no": c["coupon_no"],
            "amount":    c["amount"],
        })
    _mark_coupons_used(kw["valid_coupons"], kw["branch_id"], sale_id)

    # Delivery
    for r in kw["delivery_rows"]:
        ddf = read_sheet(SHEET_BRANCH_SALES_DELIVERY)
        did = next_id(ddf, "id", "DV")
        append_row(SHEET_BRANCH_SALES_DELIVERY,
                   _delivery_row_dict(did, sale_id, kw["branch_id"], r))

    # สลิป
    n_slip = _save_slips(kw["slip_files"], kw["branch_id"], sale_id, existing=0)

    st.success(
        f"✅ บันทึกสำเร็จ! เลขที่: **{sale_id}** | วันที่ {kw['sale_date']} | "
        f"ยอดรวม ฿{kw['total_amount']:,.2f}" +
        (f" | แนบสลิป {n_slip} รูป" if n_slip else "")
    )
    st.balloons()


# ══════════════════════════════════════════════════════════════
# รายการที่บันทึกไว้ (แก้ไข / ลบ)
# ══════════════════════════════════════════════════════════════
def _save_edit(**kw):
    sale_id = kw["sale_id"]
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    _lo = kw.get("leftover", {}) or {}
    update_row(SHEET_BRANCH_SALES, "sale_id", sale_id, {
        "sale_date":       kw["sale_date"],
        "cash_amount":     kw["cash_amount"],
        "transfer_amount": kw["transfer_amount"],
        "coupon_amount":   kw["coupon_amount"],
        "total_amount":    kw["total_amount"],
        "eggs_used":                 _lo.get("eggs_used", 0),
        "flour_finished_big_used":   _lo.get("flour_finished_big_used", 0),
        "flour_finished_small_used": _lo.get("flour_finished_small_used", 0),
        "mix_big_used":              _lo.get("mix_big_used", 0),
        "mix_small_used":            _lo.get("mix_small_used", 0),
        "leftover_box_qty":      _lo.get("leftover_box_qty", 0),
        "leftover_loose_pieces": _lo.get("leftover_loose_pieces", 0),
        "leftover_total_pieces": _lo.get("leftover_total_pieces", 0),
        "box_unit_price":        _lo.get("box_unit_price", 0),
        "leftover_value":        _lo.get("leftover_value", 0),
        "remark":          kw["remark"],
        "updated_at":      now,
    })

    # คูปอง: คืนของเดิม → ลบ mapping เดิม → ใส่ใหม่ → mark used
    _release_coupons(sale_id)
    try:
        scdf = read_sheet(SHEET_BRANCH_SALES_COUPONS)
        if not scdf.empty and "sale_id" in scdf.columns:
            write_sheet(SHEET_BRANCH_SALES_COUPONS,
                        scdf[scdf["sale_id"].astype(str) != sale_id])
    except Exception:
        pass
    for c in kw["valid_coupons"]:
        cdf = read_sheet(SHEET_BRANCH_SALES_COUPONS)
        cid = next_id(cdf, "id", "SC")
        append_row(SHEET_BRANCH_SALES_COUPONS, {
            "id": cid, "sale_id": sale_id, "branch_id": kw["branch_id"],
            "coupon_no": c["coupon_no"], "amount": c["amount"],
        })
    _mark_coupons_used(kw["valid_coupons"], kw["branch_id"], sale_id)

    # Delivery: ลบเดิม → ใส่ใหม่
    try:
        ddf = read_sheet(SHEET_BRANCH_SALES_DELIVERY)
        if not ddf.empty and "sale_id" in ddf.columns:
            write_sheet(SHEET_BRANCH_SALES_DELIVERY,
                        ddf[ddf["sale_id"].astype(str) != sale_id])
    except Exception:
        pass
    for r in kw["delivery_rows"]:
        ddf = read_sheet(SHEET_BRANCH_SALES_DELIVERY)
        did = next_id(ddf, "id", "DV")
        append_row(SHEET_BRANCH_SALES_DELIVERY,
                   _delivery_row_dict(did, sale_id, kw["branch_id"], r))


def _delete_sale(sale_id):
    _release_coupons(sale_id)   # คืนคูปองก่อน
    _delete_children(sale_id)   # ลบ coupon/slip/delivery
    try:
        delete_row(SHEET_BRANCH_SALES, "sale_id", sale_id)
    except Exception:
        pass


def _parse_date(v):
    try:
        return datetime.datetime.strptime(str(v)[:10], "%Y-%m-%d").date()
    except Exception:
        return datetime.date.today()
