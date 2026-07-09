"""
branch_report.py  –  Branch Daily Report (รอบที่ 2)
หน้าจอกรอกรายงานปิดยอดรายวันสำหรับพนักงานสาขา
"""
import datetime
import streamlit as st
import pandas as pd

from config import (
    SHEET_BRANCHES,
    SHEET_BRANCH_DAILY_REPORTS,
    SHEET_BRANCH_FRONT_SALES_PKG,
    SHEET_BRANCH_DRINK_SALES,
    SHEET_BRANCH_MATERIAL_BALANCE,
    SHEET_BRANCH_PACKAGING_BALANCE,
    SHEET_DELIVERY_PACKAGING_SALES,
    SHEET_BRANCH_OTHER_STOCK_BALANCE,
    SHEET_BRANCH_SPECIAL_REMARK,
    SHEET_BRANCH_SALES_RECHECK,
)
from modules.excel_db import (
    read_sheet, write_sheet, init_workbook, append_row, read_sheet
)
from utils.id_generator import next_id


# ══════════════════════════════════════════════════════════════════════
# INIT SHEETS — สร้าง header ถ้า Sheet ยังว่างอยู่
# ══════════════════════════════════════════════════════════════════════
SHEET_SCHEMAS = {
    SHEET_BRANCH_DAILY_REPORTS: [
        "branch_report_id", "report_date", "branch_id", "staff_id",
        "statement_amount", "cash_amount", "transfer_amount",
        "other_income_amount", "lineman_amount", "grab_amount",
        "total_received", "remark", "submitted_at", "status",
    ],
    SHEET_BRANCH_FRONT_SALES_PKG: [
        "front_packaging_id", "branch_report_id",
        "paper_bag_qty", "yellow_bag_qty", "plastic_box_qty", "drink_cup_qty",
        "paper_bag_price", "yellow_bag_price", "plastic_box_price", "drink_cup_price",
        "expected_sales_amount",
    ],
    SHEET_BRANCH_DRINK_SALES: [
        "drink_sale_id", "branch_report_id",
        "thai_tea_slushy_qty", "lemon_tea_slushy_qty",
        "iced_thai_tea_qty", "iced_lemon_tea_qty",
        "total_drink_qty",
    ],
    SHEET_BRANCH_MATERIAL_BALANCE: [
        "branch_material_balance_id", "branch_report_id",
        "finished_flour_big_bag_qty", "finished_flour_small_bag_qty",
        "ingredient_mix_big_bag_qty", "ingredient_mix_small_bag_qty",
        "egg_qty", "opened_butter_qty", "mixed_tea_gallon_qty",
        "unmixed_tea_gallon_qty", "tea_base_ml_qty",
        "sweetened_condensed_milk_qty", "evaporated_milk_qty",
        "honey_ml_qty", "remark",
    ],
    SHEET_BRANCH_PACKAGING_BALANCE: [
        "branch_packaging_balance_id", "branch_report_id",
        "paper_bag_qty", "plastic_box_qty", "drink_cup_qty",
        "cup_lid_qty", "band_qty", "skewer_pack_qty",
        "hot_bag_pack_qty", "printed_carry_bag_qty",
        "plastic_carry_bag_7x15_qty", "plastic_carry_bag_8x16_qty",
        "remark",
    ],
    SHEET_DELIVERY_PACKAGING_SALES: [
        "delivery_packaging_id", "branch_report_id",
        "channel_name", "plastic_box_qty", "paper_bag_qty",
        "drink_cup_qty", "remark",
    ],
    SHEET_BRANCH_OTHER_STOCK_BALANCE: [
        "other_stock_balance_id", "branch_report_id",
        "empty_gallon_qty", "straw_pack_qty", "detergent_bag_qty",
        "dishwashing_liquid_bag_qty", "bleach_bottle_qty",
        "tissue_roll_qty", "black_garbage_bag_m_box_qty",
        "black_glove_s_box_qty", "clear_glove_box_qty",
        "hair_cover_pack_qty", "hair_cover_photo",
        "slip_roll_qty", "black_mask_box_qty",
        "toothpick_pack_qty", "unused_butter_piping_bag_qty",
        "remark",
    ],
    SHEET_BRANCH_SPECIAL_REMARK: [
        "special_remark_id", "branch_report_id",
        "free_gift_or_promotion_remark", "needed_purchase_remark",
    ],
    SHEET_BRANCH_SALES_RECHECK: [
        "sales_recheck_id", "branch_report_id",
        "expected_sales_amount", "branch_reported_sales",
        "diff_amount", "status", "remark",
    ],
}


def _init_report_sheets():
    """สร้าง header row ใน Sheet ที่ยังว่างอยู่"""
    init_workbook()
    for sheet_name, columns in SHEET_SCHEMAS.items():
        df = read_sheet(sheet_name)
        if df.empty or list(df.columns) != columns:
            empty_df = pd.DataFrame(columns=columns)
            write_sheet(sheet_name, empty_df)


# ══════════════════════════════════════════════════════════════════════
# MAIN RENDER
# ══════════════════════════════════════════════════════════════════════
def render():
    _init_report_sheets()
    st.title("📊 Branch Daily Report")
    st.caption("กรอกรายงานปิดยอดประจำวันสำหรับพนักงานสาขา")

    tab_new, tab_history = st.tabs(["📝 กรอกรายงานใหม่", "📋 ประวัติรายงาน"])

    with tab_new:
        _render_new_report()
    with tab_history:
        _render_history()


# ══════════════════════════════════════════════════════════════════════
# SECTION: กรอกรายงานใหม่
# ══════════════════════════════════════════════════════════════════════
def _render_new_report():
    # ── เลือกวันที่และสาขา ──────────────────────────────────────────
    st.subheader("① เลือกวันที่และสาขา")
    col1, col2, col3 = st.columns(3)
    with col1:
        report_date = st.date_input("📅 วันที่รายงาน", value=datetime.date.today())
    with col2:
        branches_df = read_sheet(SHEET_BRANCHES)
        if branches_df.empty:
            st.warning("⚠️ ยังไม่มีข้อมูลสาขา กรุณาเพิ่มสาขาใน Master Data ก่อน")
            return
        branch_opts = dict(zip(branches_df["branch_id"], branches_df["branch_name"]))
        branch_id = st.selectbox(
            "🏪 สาขา",
            options=list(branch_opts.keys()),
            format_func=lambda k: f"{k} – {branch_opts[k]}"
        )
    with col3:
        # ดึงรายชื่อพนักงานของสาขาที่เลือก
        from config import SHEET_EMPLOYEES
        emp_df = read_sheet(SHEET_EMPLOYEES)
        emp_options = ["-- กรอกชื่อเอง --"]
        if not emp_df.empty and "branch_id" in emp_df.columns:
            branch_emps = emp_df[
                emp_df["branch_id"].astype(str).str.strip() == str(branch_id).strip()
            ]
            if not branch_emps.empty:
                emp_options = (
                    branch_emps["first_name"].astype(str) + " " +
                    branch_emps["last_name"].astype(str)
                ).tolist()
                emp_options = ["-- เลือกพนักงาน --"] + emp_options
        sel_emp = st.selectbox("👤 ชื่อผู้กรอก", emp_options, key="br_emp_sel")
        if sel_emp in ["-- เลือกพนักงาน --","-- กรอกชื่อเอง --"]:
            staff_id = st.text_input("พิมพ์ชื่อผู้กรอก", key="br_staff_manual")
        else:
            staff_id = sel_emp

    # ตรวจว่ารายงานวันนี้ของสาขานี้มีอยู่แล้วหรือยัง
    existing_df = read_sheet(SHEET_BRANCH_DAILY_REPORTS)
    already_exists = False
    existing_report_id = None
    if not existing_df.empty:
        mask = (
            (existing_df["report_date"].astype(str) == str(report_date)) &
            (existing_df["branch_id"].astype(str) == str(branch_id))
        )
        if mask.any():
            already_exists = True
            existing_report_id = existing_df[mask]["branch_report_id"].values[0]
            st.warning(
                f"⚠️ มีรายงานของสาขานี้ในวันที่ {report_date} แล้ว "
                f"(ID: {existing_report_id}) — ข้อมูลที่กรอกใหม่จะ **เพิ่มเติม** ไม่ใช่แทนที่"
            )

    st.divider()

    # ── ② ยอดเงิน ────────────────────────────────────────────────────
    st.subheader("② ยอดเงินรับประจำวัน")
    col1, col2, col3 = st.columns(3)
    with col1:
        statement_amount      = st.number_input("💳 ยอด Statement (บาท)",     min_value=0.0, step=1.0, format="%.2f")
        cash_amount           = st.number_input("💵 ยอดเงินสด (บาท)",         min_value=0.0, step=1.0, format="%.2f")
    with col2:
        transfer_amount       = st.number_input("📲 ยอดโอน (บาท)",            min_value=0.0, step=1.0, format="%.2f")
        other_income_amount   = st.number_input("💰 รายได้อื่น ๆ (บาท)",       min_value=0.0, step=1.0, format="%.2f")
    with col3:
        lineman_amount        = st.number_input("🛵 Line Man (บาท)",           min_value=0.0, step=1.0, format="%.2f")
        grab_amount           = st.number_input("🟢 Grab (บาท)",              min_value=0.0, step=1.0, format="%.2f")

    total_received = (
        statement_amount + cash_amount + transfer_amount +
        other_income_amount + lineman_amount + grab_amount
    )
    st.metric("💰 ยอดรวมทั้งหมด (total_received)", f"฿{total_received:,.2f}")
    daily_remark = st.text_input("📝 หมายเหตุรายวัน")

    st.divider()

    # ── ③ บรรจุภัณฑ์หน้าร้าน ─────────────────────────────────────────
    st.subheader("③ บรรจุภัณฑ์ที่ขายหน้าร้าน (Front Packaging)")
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**จำนวน (ชิ้น)**")
        paper_bag_qty        = st.number_input("ถุงกระดาษขาว",     min_value=0, step=1, key="pb_qty")
        yellow_bag_qty       = st.number_input("ถุงกระดาษเหลือง",  min_value=0, step=1, key="yb_qty")
        plastic_box_qty      = st.number_input("กล่องพลาสติก",     min_value=0, step=1, key="plb_qty")
        drink_cup_qty        = st.number_input("แก้วเครื่องดื่ม",  min_value=0, step=1, key="dc_qty")
    with col2:
        st.markdown("**ราคาต่อชิ้น (บาท)**")
        paper_bag_price      = st.number_input("ราคาถุงกระดาษขาว",    min_value=0.0, step=0.5, value=0.0, format="%.2f", key="pb_price")
        yellow_bag_price     = st.number_input("ราคาถุงกระดาษเหลือง", min_value=0.0, step=0.5, value=0.0, format="%.2f", key="yb_price")
        plastic_box_price    = st.number_input("ราคากล่องพลาสติก",    min_value=0.0, step=0.5, value=0.0, format="%.2f", key="plb_price")
        drink_cup_price      = st.number_input("ราคาแก้วเครื่องดื่ม", min_value=0.0, step=0.5, value=0.0, format="%.2f", key="dc_price")

    expected_sales_amount = (
        paper_bag_qty   * paper_bag_price +
        yellow_bag_qty  * yellow_bag_price +
        plastic_box_qty * plastic_box_price +
        drink_cup_qty   * drink_cup_price
    )
    st.metric("📦 ยอดขายที่ควรเป็น (expected_sales_amount)", f"฿{expected_sales_amount:,.2f}")

    st.divider()

    # ── ④ เครื่องดื่ม ─────────────────────────────────────────────────
    st.subheader("④ รายละเอียดเครื่องดื่มที่ขาย")
    col1, col2 = st.columns(2)
    with col1:
        thai_tea_slushy_qty   = st.number_input("🧋 ชาไทยปั่น",    min_value=0, step=1)
        lemon_tea_slushy_qty  = st.number_input("🍋 มะนาวปั่น",    min_value=0, step=1)
    with col2:
        iced_thai_tea_qty     = st.number_input("🥤 ชาไทยเย็น",    min_value=0, step=1)
        iced_lemon_tea_qty    = st.number_input("🍵 มะนาวเย็น",    min_value=0, step=1)

    total_drink_qty = thai_tea_slushy_qty + lemon_tea_slushy_qty + iced_thai_tea_qty + iced_lemon_tea_qty
    st.metric("🥤 รวมเครื่องดื่มทั้งหมด (total_drink_qty)", f"{total_drink_qty} แก้ว")

    st.divider()

    # ── ⑤ วัตถุดิบคงเหลือ ────────────────────────────────────────────
    st.subheader("⑤ วัตถุดิบคงเหลือ (Material Balance)")
    col1, col2, col3 = st.columns(3)
    with col1:
        finished_flour_big_bag_qty    = st.number_input("แป้งสำเร็จรูป ถุงใหญ่",     min_value=0, step=1)
        finished_flour_small_bag_qty  = st.number_input("แป้งสำเร็จรูป ถุงเล็ก",     min_value=0, step=1)
        ingredient_mix_big_bag_qty    = st.number_input("ส่วนผสม ถุงใหญ่",           min_value=0, step=1)
        ingredient_mix_small_bag_qty  = st.number_input("ส่วนผสม ถุงเล็ก",           min_value=0, step=1)
        egg_qty                       = st.number_input("ไข่ (ฟอง)",                  min_value=0, step=1)
    with col2:
        opened_butter_qty             = st.number_input("เนยที่เปิดแล้ว",             min_value=0, step=1)
        mixed_tea_gallon_qty          = st.number_input("ชาที่ผสมแล้ว (แกลลอน)",     min_value=0, step=1)
        unmixed_tea_gallon_qty        = st.number_input("ชาที่ยังไม่ผสม (แกลลอน)",   min_value=0, step=1)
        tea_base_ml_qty               = st.number_input("หัวเชื้อชา (มล.)",           min_value=0, step=1)
    with col3:
        sweetened_condensed_milk_qty  = st.number_input("นมข้นหวาน",                  min_value=0, step=1)
        evaporated_milk_qty           = st.number_input("นมข้นจืด",                   min_value=0, step=1)
        honey_ml_qty                  = st.number_input("น้ำผึ้ง (มล.)",              min_value=0, step=1)
    material_remark = st.text_input("หมายเหตุ (วัตถุดิบ)", key="mat_remark")

    st.divider()

    # ── ⑥ บรรจุภัณฑ์คงเหลือ ──────────────────────────────────────────
    st.subheader("⑥ บรรจุภัณฑ์คงเหลือ (Packaging Balance)")
    col1, col2, col3 = st.columns(3)
    with col1:
        pkg_paper_bag_qty            = st.number_input("ถุงกระดาษ",            min_value=0, step=1, key="pkgpb")
        pkg_plastic_box_qty          = st.number_input("กล่องพลาสติก",          min_value=0, step=1, key="pkgplb")
        pkg_drink_cup_qty            = st.number_input("แก้วเครื่องดื่ม",       min_value=0, step=1, key="pkgdc")
        pkg_cup_lid_qty              = st.number_input("ฝาแก้ว",                min_value=0, step=1)
    with col2:
        pkg_band_qty                 = st.number_input("ยางรัด",                min_value=0, step=1)
        pkg_skewer_pack_qty          = st.number_input("ไม้เสียบ (แพ็ก)",       min_value=0, step=1)
        pkg_hot_bag_pack_qty         = st.number_input("ถุงร้อน (แพ็ก)",        min_value=0, step=1)
        pkg_printed_carry_bag_qty    = st.number_input("ถุงหิ้วพิมพ์ลาย",       min_value=0, step=1)
    with col3:
        pkg_plastic_carry_7x15_qty   = st.number_input("ถุงพลาสติก 7x15",      min_value=0, step=1)
        pkg_plastic_carry_8x16_qty   = st.number_input("ถุงพลาสติก 8x16",      min_value=0, step=1)
    packaging_remark = st.text_input("หมายเหตุ (บรรจุภัณฑ์)", key="pkg_remark")

    st.divider()

    # ── ⑦ Delivery (Line Man / Grab / Other) ─────────────────────────
    st.subheader("⑦ ยอดขาย Delivery แยกช่อง")
    st.caption("บรรจุภัณฑ์ที่ใช้ในแต่ละช่องทาง Delivery")

    delivery_rows = []
    for channel in ["Line Man", "Grab", "Other"]:
        with st.expander(f"🚴 {channel}", expanded=(channel == "Line Man")):
            c1, c2, c3 = st.columns(3)
            k = channel.replace(" ", "_").lower()
            with c1:
                dlv_plastic = st.number_input(f"กล่องพลาสติก – {channel}", min_value=0, step=1, key=f"dlv_plb_{k}")
            with c2:
                dlv_paper   = st.number_input(f"ถุงกระดาษ – {channel}",    min_value=0, step=1, key=f"dlv_pb_{k}")
            with c3:
                dlv_cup     = st.number_input(f"แก้ว – {channel}",         min_value=0, step=1, key=f"dlv_dc_{k}")
            dlv_remark  = st.text_input(f"หมายเหตุ – {channel}", key=f"dlv_rem_{k}")
            delivery_rows.append({
                "channel_name":     channel,
                "plastic_box_qty":  dlv_plastic,
                "paper_bag_qty":    dlv_paper,
                "drink_cup_qty":    dlv_cup,
                "remark":           dlv_remark,
            })

    st.divider()

    # ── ⑧ ของใช้อื่น ๆ คงเหลือ ──────────────────────────────────────
    st.subheader("⑧ ของใช้อื่น ๆ คงเหลือ (Other Stock Balance)")
    col1, col2, col3 = st.columns(3)
    with col1:
        empty_gallon_qty              = st.number_input("ถังเปล่า",                   min_value=0, step=1)
        straw_pack_qty                = st.number_input("หลอดดูด (แพ็ก)",             min_value=0, step=1)
        detergent_bag_qty             = st.number_input("ผงซักฟอก (ถุง)",             min_value=0, step=1)
        dishwashing_liquid_bag_qty    = st.number_input("น้ำยาล้างจาน (ถุง)",         min_value=0, step=1)
        bleach_bottle_qty             = st.number_input("น้ำยาฟอกขาว (ขวด)",          min_value=0, step=1)
    with col2:
        tissue_roll_qty               = st.number_input("ทิชชู่ม้วน",                 min_value=0, step=1)
        black_garbage_bag_m_box_qty   = st.number_input("ถุงดำ M (กล่อง)",            min_value=0, step=1)
        black_glove_s_box_qty         = st.number_input("ถุงมือดำ S (กล่อง)",         min_value=0, step=1)
        clear_glove_box_qty           = st.number_input("ถุงมือใส (กล่อง)",           min_value=0, step=1)
        hair_cover_pack_qty           = st.number_input("ตาข่ายคลุมผม (แพ็ก)",        min_value=0, step=1)
    with col3:
        slip_roll_qty                 = st.number_input("กระดาษสลิป (ม้วน)",          min_value=0, step=1)
        black_mask_box_qty            = st.number_input("หน้ากากดำ (กล่อง)",           min_value=0, step=1)
        toothpick_pack_qty            = st.number_input("ไม้จิ้มฟัน (แพ็ก)",          min_value=0, step=1)
        unused_butter_piping_bag_qty  = st.number_input("ถุงบีบเนยที่ยังไม่ใช้",       min_value=0, step=1)
    hair_cover_photo = st.text_input("รูปภาพตาข่ายคลุมผม (URL / ชื่อไฟล์)", key="hc_photo")
    other_stock_remark = st.text_input("หมายเหตุ (ของใช้อื่น ๆ)", key="os_remark")

    st.divider()

    # ── ⑨ หมายเหตุโปรโมชั่น / ของที่ต้องซื้อเพิ่ม ───────────────────
    st.subheader("⑨ หมายเหตุพิเศษ")
    col1, col2 = st.columns(2)
    with col1:
        free_gift_remark = st.text_area("🎁 โปรโมชั่น / ของแถม / แจก", height=100)
    with col2:
        needed_purchase_remark = st.text_area("🛒 รายการที่ต้องสั่งซื้อเพิ่ม", height=100)

    st.divider()

    # ── ⑩ Sales Recheck Summary ──────────────────────────────────────
    st.subheader("⑩ ตรวจสอบยอดขาย (Sales Recheck)")
    diff_amount = total_received - expected_sales_amount

    col1, col2, col3 = st.columns(3)
    col1.metric("📦 ยอดที่ควรเป็น (Expected)",  f"฿{expected_sales_amount:,.2f}")
    col2.metric("💰 ยอดที่สาขาแจ้ง (Received)", f"฿{total_received:,.2f}")
    col3.metric("📊 ส่วนต่าง (Diff)",           f"฿{diff_amount:,.2f}",
                delta=f"{diff_amount:+.2f}")

    if diff_amount == 0:
        st.success("✅ ยอดตรง — ข้อมูลถูกต้อง")
        recheck_status = "OK"
    else:
        st.markdown(
            f"""
            <div style="
                background:#FF0000;color:white;padding:18px;border-radius:8px;
                font-size:24px;font-weight:bold;text-align:center;
            ">
            ⚠️ DIFF — ยอดไม่ตรง : ฿{diff_amount:+,.2f}
            </div>
            """,
            unsafe_allow_html=True,
        )
        recheck_status = "DIFF"

    recheck_remark = st.text_input("หมายเหตุ (ตรวจสอบยอด)", key="recheck_remark")

    st.divider()

    # ── บันทึก ─────────────────────────────────────────────────────────
    if st.button("💾 บันทึกรายงานประจำวัน", type="primary", use_container_width=True):
        if not staff_id.strip():
            st.error("กรุณากรอกรหัสพนักงาน / ชื่อผู้กรอก")
            return

        _save_report(
            report_date=str(report_date),
            branch_id=branch_id,
            staff_id=staff_id.strip(),
            # ยอดเงิน
            statement_amount=statement_amount,
            cash_amount=cash_amount,
            transfer_amount=transfer_amount,
            other_income_amount=other_income_amount,
            lineman_amount=lineman_amount,
            grab_amount=grab_amount,
            total_received=total_received,
            daily_remark=daily_remark,
            # บรรจุภัณฑ์หน้าร้าน
            paper_bag_qty=paper_bag_qty,
            plastic_box_qty=plastic_box_qty,
            drink_cup_qty=drink_cup_qty,
            paper_bag_price=paper_bag_price,
            plastic_box_price=plastic_box_price,
            drink_cup_price=drink_cup_price,
            expected_sales_amount=expected_sales_amount,
            # เครื่องดื่ม
            thai_tea_slushy_qty=thai_tea_slushy_qty,
            lemon_tea_slushy_qty=lemon_tea_slushy_qty,
            iced_thai_tea_qty=iced_thai_tea_qty,
            iced_lemon_tea_qty=iced_lemon_tea_qty,
            total_drink_qty=total_drink_qty,
            # วัตถุดิบ
            finished_flour_big_bag_qty=finished_flour_big_bag_qty,
            finished_flour_small_bag_qty=finished_flour_small_bag_qty,
            ingredient_mix_big_bag_qty=ingredient_mix_big_bag_qty,
            ingredient_mix_small_bag_qty=ingredient_mix_small_bag_qty,
            egg_qty=egg_qty,
            opened_butter_qty=opened_butter_qty,
            mixed_tea_gallon_qty=mixed_tea_gallon_qty,
            unmixed_tea_gallon_qty=unmixed_tea_gallon_qty,
            tea_base_ml_qty=tea_base_ml_qty,
            sweetened_condensed_milk_qty=sweetened_condensed_milk_qty,
            evaporated_milk_qty=evaporated_milk_qty,
            honey_ml_qty=honey_ml_qty,
            material_remark=material_remark,
            # บรรจุภัณฑ์คงเหลือ
            pkg_paper_bag_qty=pkg_paper_bag_qty,
            pkg_plastic_box_qty=pkg_plastic_box_qty,
            pkg_drink_cup_qty=pkg_drink_cup_qty,
            pkg_cup_lid_qty=pkg_cup_lid_qty,
            pkg_band_qty=pkg_band_qty,
            pkg_skewer_pack_qty=pkg_skewer_pack_qty,
            pkg_hot_bag_pack_qty=pkg_hot_bag_pack_qty,
            pkg_printed_carry_bag_qty=pkg_printed_carry_bag_qty,
            pkg_plastic_carry_7x15_qty=pkg_plastic_carry_7x15_qty,
            pkg_plastic_carry_8x16_qty=pkg_plastic_carry_8x16_qty,
            packaging_remark=packaging_remark,
            # delivery
            delivery_rows=delivery_rows,
            # other stock
            empty_gallon_qty=empty_gallon_qty,
            straw_pack_qty=straw_pack_qty,
            detergent_bag_qty=detergent_bag_qty,
            dishwashing_liquid_bag_qty=dishwashing_liquid_bag_qty,
            bleach_bottle_qty=bleach_bottle_qty,
            tissue_roll_qty=tissue_roll_qty,
            black_garbage_bag_m_box_qty=black_garbage_bag_m_box_qty,
            black_glove_s_box_qty=black_glove_s_box_qty,
            clear_glove_box_qty=clear_glove_box_qty,
            hair_cover_pack_qty=hair_cover_pack_qty,
            hair_cover_photo=hair_cover_photo,
            slip_roll_qty=slip_roll_qty,
            black_mask_box_qty=black_mask_box_qty,
            toothpick_pack_qty=toothpick_pack_qty,
            unused_butter_piping_bag_qty=unused_butter_piping_bag_qty,
            other_stock_remark=other_stock_remark,
            # หมายเหตุพิเศษ
            free_gift_remark=free_gift_remark,
            needed_purchase_remark=needed_purchase_remark,
            # recheck
            diff_amount=diff_amount,
            recheck_status=recheck_status,
            recheck_remark=recheck_remark,
        )


# ══════════════════════════════════════════════════════════════════════
# SAVE FUNCTION
# ══════════════════════════════════════════════════════════════════════
def _save_report(**kw):
    submitted_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # ─ 1. branch_daily_reports ──────────────────────────────────────
    master_df = read_sheet(SHEET_BRANCH_DAILY_REPORTS)
    report_id = next_id(master_df, "branch_report_id", "RPT")
    append_row(SHEET_BRANCH_DAILY_REPORTS, {
        "branch_report_id":    report_id,
        "report_date":         kw["report_date"],
        "branch_id":           kw["branch_id"],
        "staff_id":            kw["staff_id"],
        "statement_amount":    kw["statement_amount"],
        "cash_amount":         kw["cash_amount"],
        "transfer_amount":     kw["transfer_amount"],
        "other_income_amount": kw["other_income_amount"],
        "lineman_amount":      kw["lineman_amount"],
        "grab_amount":         kw["grab_amount"],
        "total_received":      kw["total_received"],
        "remark":              kw["daily_remark"],
        "submitted_at":        submitted_at,
        "status":              "submitted",
    })

    # ─ 2. branch_front_sales_packaging ──────────────────────────────
    fp_df = read_sheet(SHEET_BRANCH_FRONT_SALES_PKG)
    fp_id = next_id(fp_df, "front_packaging_id", "FP")
    append_row(SHEET_BRANCH_FRONT_SALES_PKG, {
        "front_packaging_id":    fp_id,
        "branch_report_id":      report_id,
        "paper_bag_qty":         kw["paper_bag_qty"],
        "plastic_box_qty":       kw["plastic_box_qty"],
        "drink_cup_qty":         kw["drink_cup_qty"],
        "paper_bag_price":       kw["paper_bag_price"],
        "plastic_box_price":     kw["plastic_box_price"],
        "drink_cup_price":       kw["drink_cup_price"],
        "expected_sales_amount": kw["expected_sales_amount"],
    })

    # ─ 3. branch_drink_sales_detail ─────────────────────────────────
    dr_df = read_sheet(SHEET_BRANCH_DRINK_SALES)
    dr_id = next_id(dr_df, "drink_sale_id", "DRK")
    append_row(SHEET_BRANCH_DRINK_SALES, {
        "drink_sale_id":        dr_id,
        "branch_report_id":     report_id,
        "thai_tea_slushy_qty":  kw["thai_tea_slushy_qty"],
        "lemon_tea_slushy_qty": kw["lemon_tea_slushy_qty"],
        "iced_thai_tea_qty":    kw["iced_thai_tea_qty"],
        "iced_lemon_tea_qty":   kw["iced_lemon_tea_qty"],
        "total_drink_qty":      kw["total_drink_qty"],
    })

    # ─ 4. branch_material_balance ────────────────────────────────────
    mb_df = read_sheet(SHEET_BRANCH_MATERIAL_BALANCE)
    mb_id = next_id(mb_df, "branch_material_balance_id", "MB")
    append_row(SHEET_BRANCH_MATERIAL_BALANCE, {
        "branch_material_balance_id":    mb_id,
        "branch_report_id":              report_id,
        "finished_flour_big_bag_qty":    kw["finished_flour_big_bag_qty"],
        "finished_flour_small_bag_qty":  kw["finished_flour_small_bag_qty"],
        "ingredient_mix_big_bag_qty":    kw["ingredient_mix_big_bag_qty"],
        "ingredient_mix_small_bag_qty":  kw["ingredient_mix_small_bag_qty"],
        "egg_qty":                       kw["egg_qty"],
        "opened_butter_qty":             kw["opened_butter_qty"],
        "mixed_tea_gallon_qty":          kw["mixed_tea_gallon_qty"],
        "unmixed_tea_gallon_qty":        kw["unmixed_tea_gallon_qty"],
        "tea_base_ml_qty":               kw["tea_base_ml_qty"],
        "sweetened_condensed_milk_qty":  kw["sweetened_condensed_milk_qty"],
        "evaporated_milk_qty":           kw["evaporated_milk_qty"],
        "honey_ml_qty":                  kw["honey_ml_qty"],
        "remark":                        kw["material_remark"],
    })

    # ─ 5. branch_packaging_balance ───────────────────────────────────
    pb_df = read_sheet(SHEET_BRANCH_PACKAGING_BALANCE)
    pb_id = next_id(pb_df, "branch_packaging_balance_id", "PB")
    append_row(SHEET_BRANCH_PACKAGING_BALANCE, {
        "branch_packaging_balance_id": pb_id,
        "branch_report_id":            report_id,
        "paper_bag_qty":               kw["pkg_paper_bag_qty"],
        "plastic_box_qty":             kw["pkg_plastic_box_qty"],
        "drink_cup_qty":               kw["pkg_drink_cup_qty"],
        "cup_lid_qty":                 kw["pkg_cup_lid_qty"],
        "band_qty":                    kw["pkg_band_qty"],
        "skewer_pack_qty":             kw["pkg_skewer_pack_qty"],
        "hot_bag_pack_qty":            kw["pkg_hot_bag_pack_qty"],
        "printed_carry_bag_qty":       kw["pkg_printed_carry_bag_qty"],
        "plastic_carry_bag_7x15_qty":  kw["pkg_plastic_carry_7x15_qty"],
        "plastic_carry_bag_8x16_qty":  kw["pkg_plastic_carry_8x16_qty"],
        "remark":                      kw["packaging_remark"],
    })

    # ─ 6. delivery_packaging_sales (3 rows: Line Man / Grab / Other) ─
    dlv_df = read_sheet(SHEET_DELIVERY_PACKAGING_SALES)
    for drow in kw["delivery_rows"]:
        dlv_id = next_id(read_sheet(SHEET_DELIVERY_PACKAGING_SALES), "delivery_packaging_id", "DLV")
        append_row(SHEET_DELIVERY_PACKAGING_SALES, {
            "delivery_packaging_id": dlv_id,
            "branch_report_id":      report_id,
            "channel_name":          drow["channel_name"],
            "plastic_box_qty":       drow["plastic_box_qty"],
            "paper_bag_qty":         drow["paper_bag_qty"],
            "drink_cup_qty":         drow["drink_cup_qty"],
            "remark":                drow["remark"],
        })

    # ─ 7. branch_other_stock_balance ─────────────────────────────────
    os_df = read_sheet(SHEET_BRANCH_OTHER_STOCK_BALANCE)
    os_id = next_id(os_df, "other_stock_balance_id", "OS")
    append_row(SHEET_BRANCH_OTHER_STOCK_BALANCE, {
        "other_stock_balance_id":         os_id,
        "branch_report_id":               report_id,
        "empty_gallon_qty":               kw["empty_gallon_qty"],
        "straw_pack_qty":                 kw["straw_pack_qty"],
        "detergent_bag_qty":              kw["detergent_bag_qty"],
        "dishwashing_liquid_bag_qty":     kw["dishwashing_liquid_bag_qty"],
        "bleach_bottle_qty":              kw["bleach_bottle_qty"],
        "tissue_roll_qty":                kw["tissue_roll_qty"],
        "black_garbage_bag_m_box_qty":    kw["black_garbage_bag_m_box_qty"],
        "black_glove_s_box_qty":          kw["black_glove_s_box_qty"],
        "clear_glove_box_qty":            kw["clear_glove_box_qty"],
        "hair_cover_pack_qty":            kw["hair_cover_pack_qty"],
        "hair_cover_photo":               kw["hair_cover_photo"],
        "slip_roll_qty":                  kw["slip_roll_qty"],
        "black_mask_box_qty":             kw["black_mask_box_qty"],
        "toothpick_pack_qty":             kw["toothpick_pack_qty"],
        "unused_butter_piping_bag_qty":   kw["unused_butter_piping_bag_qty"],
        "remark":                         kw["other_stock_remark"],
    })

    # ─ 8. branch_special_remark ──────────────────────────────────────
    sr_df = read_sheet(SHEET_BRANCH_SPECIAL_REMARK)
    sr_id = next_id(sr_df, "special_remark_id", "SR")
    append_row(SHEET_BRANCH_SPECIAL_REMARK, {
        "special_remark_id":              sr_id,
        "branch_report_id":               report_id,
        "free_gift_or_promotion_remark":  kw["free_gift_remark"],
        "needed_purchase_remark":         kw["needed_purchase_remark"],
    })

    # ─ 9. branch_sales_recheck ───────────────────────────────────────
    rc_df = read_sheet(SHEET_BRANCH_SALES_RECHECK)
    rc_id = next_id(rc_df, "sales_recheck_id", "RC")
    append_row(SHEET_BRANCH_SALES_RECHECK, {
        "sales_recheck_id":        rc_id,
        "branch_report_id":        report_id,
        "expected_sales_amount":   kw["expected_sales_amount"],
        "branch_reported_sales":   kw["total_received"],
        "diff_amount":             kw["diff_amount"],
        "status":                  kw["recheck_status"],
        "remark":                  kw["recheck_remark"],
    })

    st.success(
        f"✅ บันทึกรายงานสำเร็จ! Report ID: **{report_id}** | "
        f"วันที่: {kw['report_date']} | สาขา: {kw['branch_id']}"
    )
    st.balloons()


# ══════════════════════════════════════════════════════════════════════
# SECTION: ประวัติรายงาน
# ══════════════════════════════════════════════════════════════════════
def _render_history():
    st.subheader("📋 ประวัติรายงานประจำวัน")

    df = read_sheet(SHEET_BRANCH_DAILY_REPORTS)
    if df.empty:
        st.info("ยังไม่มีรายงาน")
        return

    # Filter
    col1, col2 = st.columns(2)
    with col1:
        branches_df = read_sheet(SHEET_BRANCHES)
        branch_opts = {"ทั้งหมด": "ทั้งหมด"}
        if not branches_df.empty:
            branch_opts.update(dict(zip(branches_df["branch_id"], branches_df["branch_name"])))
        sel_branch = st.selectbox("กรองตามสาขา", options=list(branch_opts.keys()),
                                  format_func=lambda k: branch_opts[k])
    with col2:
        sel_date = st.date_input("กรองตามวันที่ (ว่างไว้ = ทั้งหมด)",
                                  value=None, key="hist_date")

    df_show = df.copy()
    if sel_branch != "ทั้งหมด":
        df_show = df_show[df_show["branch_id"].astype(str) == sel_branch]
    if sel_date:
        df_show = df_show[df_show["report_date"].astype(str) == str(sel_date)]

    if df_show.empty:
        st.info("ไม่พบรายงานตามเงื่อนไขที่เลือก")
        return

    st.dataframe(df_show, use_container_width=True)

    # ── Recheck Summary แบบมีสี ──────────────────────────────────────
    st.subheader("🔍 สรุปการตรวจสอบยอด (Sales Recheck)")
    rc_df = read_sheet(SHEET_BRANCH_SALES_RECHECK)
    if not rc_df.empty:
        # เชื่อมกับ report date
        merged = rc_df.merge(
            df[["branch_report_id", "report_date", "branch_id"]],
            on="branch_report_id", how="left"
        )
        if sel_branch != "ทั้งหมด":
            merged = merged[merged["branch_id"].astype(str) == sel_branch]
        if sel_date:
            merged = merged[merged["report_date"].astype(str) == str(sel_date)]

        for _, row in merged.iterrows():
            try:
                diff = float(row["diff_amount"])
            except Exception:
                diff = 0
            status = str(row.get("status", ""))
            date_str = row.get("report_date", "")
            branch_str = row.get("branch_id", "")
            rpt_id = row.get("branch_report_id", "")

            if diff == 0 or status == "OK":
                st.success(
                    f"✅ {date_str} | สาขา {branch_str} | {rpt_id} | "
                    f"ยอดตรง ฿{float(row['expected_sales_amount']):,.2f}"
                )
            else:
                st.markdown(
                    f"""
                    <div style="
                        background:#FF0000;color:white;padding:12px;border-radius:6px;
                        font-size:18px;font-weight:bold;margin-bottom:6px;
                    ">
                    ⚠️ DIFF &nbsp;|&nbsp; {date_str} &nbsp;|&nbsp; สาขา {branch_str}
                    &nbsp;|&nbsp; {rpt_id}
                    &nbsp;|&nbsp; ส่วนต่าง: ฿{diff:+,.2f}
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
