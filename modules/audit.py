"""
audit.py  –  ระบบ Audit ตรวจสอบสาขา (รอบที่ 3)
ฝ่ายตรวจสอบกรอกข้อมูลจริง → เปรียบเทียบกับข้อมูลสาขา → แสดง DIFF สีแดง
"""
import datetime
import streamlit as st
import pandas as pd

from config import (
    SHEET_BRANCHES,
    SHEET_BRANCH_DAILY_REPORTS,
    SHEET_BRANCH_PACKAGING_BALANCE,
    SHEET_AUDIT_SESSIONS,
    SHEET_AUDIT_PACKAGING_BALANCE,
    SHEET_AUDIT_PACKAGING_DIFF,
    SHEET_TRUE_STOCK_BALANCE,
    SHEET_DAILY_STOCK_USAGE,
    SHEET_DAILY_PACKAGING_COST,
)
from modules.excel_db import (
    read_sheet, write_sheet, append_row, init_workbook
)
from utils.id_generator import next_id

# ──────────────────────────────────────────────────────────────────────
# PACKAGING ITEMS ที่ตรวจนับ  (label → branch_col, audit_col)
# ──────────────────────────────────────────────────────────────────────
PKG_FIELDS = [
    ("ถุงกระดาษ",           "paper_bag_qty",              "paper_bag_audit_qty"),
    ("กล่องพลาสติก",         "plastic_box_qty",            "plastic_box_audit_qty"),
    ("แก้วเครื่องดื่ม",       "drink_cup_qty",              "drink_cup_audit_qty"),
    ("ฝาแก้ว",               "cup_lid_qty",                "cup_lid_audit_qty"),
    ("ยางรัด",               "band_qty",                   "band_audit_qty"),
    ("ไม้เสียบ (แพ็ก)",      "skewer_pack_qty",            "skewer_pack_audit_qty"),
    ("ถุงร้อน (แพ็ก)",       "hot_bag_pack_qty",           "hot_bag_pack_audit_qty"),
    ("ถุงหิ้วพิมพ์ลาย",       "printed_carry_bag_qty",      "printed_carry_bag_audit_qty"),
    ("ถุงพลาสติก (แพ็ก)",    "plastic_carry_bag_8x16_qty", "plastic_carry_bag_pack_audit_qty"),
]

# ══════════════════════════════════════════════════════════════════════
# INIT SHEETS
# ══════════════════════════════════════════════════════════════════════
AUDIT_SCHEMAS = {
    SHEET_AUDIT_SESSIONS: [
        "audit_id", "audit_date", "audit_for_date", "branch_id",
        "auditor_id", "audit_time", "overall_status",
        "behavior_remark", "created_at",
    ],
    SHEET_AUDIT_PACKAGING_BALANCE: [
        "audit_packaging_balance_id", "audit_id", "branch_report_id",
        "paper_bag_audit_qty", "plastic_box_audit_qty", "drink_cup_audit_qty",
        "cup_lid_audit_qty", "band_audit_qty", "skewer_pack_audit_qty",
        "hot_bag_pack_audit_qty", "printed_carry_bag_audit_qty",
        "plastic_carry_bag_pack_audit_qty",
        "photo_proof", "remark",
    ],
    SHEET_AUDIT_PACKAGING_DIFF: [
        "audit_diff_id", "audit_id", "branch_report_id",
        "item_name", "branch_qty", "audit_qty",
        "diff_qty", "display_status", "display_color", "remark",
    ],
    SHEET_TRUE_STOCK_BALANCE: [
        "true_stock_id", "stock_date", "branch_id", "item_id",
        "audit_qty", "stock_in_qty", "sold_or_used_qty",
        "true_remaining_qty", "source_audit_id", "remark",
    ],
    SHEET_DAILY_STOCK_USAGE: [
        "stock_usage_id", "usage_date", "branch_id", "item_id",
        "opening_qty", "stock_in_qty", "true_remaining_qty",
        "used_qty", "unit", "remark",
    ],
    SHEET_DAILY_PACKAGING_COST: [
        "packaging_cost_id", "report_date", "branch_id",
        "paper_bag_qty", "plastic_box_qty", "drink_cup_qty",
        "paper_bag_unit_cost", "plastic_box_unit_cost", "drink_cup_unit_cost",
        "total_packaging_cost",
    ],
}


def _init_audit_sheets():
    init_workbook()
    for sheet_name, columns in AUDIT_SCHEMAS.items():
        df = read_sheet(sheet_name)
        if df.empty or list(df.columns) != columns:
            write_sheet(sheet_name, pd.DataFrame(columns=columns))


# ══════════════════════════════════════════════════════════════════════
# MAIN RENDER
# ══════════════════════════════════════════════════════════════════════
def render():
    _init_audit_sheets()
    st.title("🔎 Audit — ตรวจสอบสาขา")
    st.caption("ฝ่ายตรวจสอบกรอกข้อมูลจริง • ข้อมูล Audit ถือเป็นข้อมูลที่ถูกต้อง")

    tab_audit, tab_diff, tab_history = st.tabs([
        "📝 กรอกข้อมูล Audit",
        "📊 ดู DIFF รายงาน",
        "📋 ประวัติ Audit",
    ])

    with tab_audit:
        _render_audit_form()
    with tab_diff:
        _render_diff_viewer()
    with tab_history:
        _render_audit_history()


# ══════════════════════════════════════════════════════════════════════
# TAB 1: กรอกข้อมูล Audit
# ══════════════════════════════════════════════════════════════════════
def _render_audit_form():
    st.subheader("① เลือกวันที่และสาขา")
    col1, col2, col3 = st.columns(3)
    with col1:
        audit_date     = st.date_input("📅 วันที่ตรวจ (audit_date)",         value=datetime.date.today())
    with col2:
        audit_for_date = st.date_input("📋 วันที่ที่ตรวจสอบ (audit_for_date)", value=datetime.date.today())
    with col3:
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

    col1, col2 = st.columns(2)
    with col1:
        auditor_id = st.text_input("👤 รหัสผู้ตรวจ / ชื่อผู้ตรวจ")
    with col2:
        audit_time = st.time_input("⏰ เวลาที่ตรวจ", value=datetime.datetime.now().time())

    # ── ดึง branch_report_id ──────────────────────────────────────────
    st.divider()
    rpt_df = read_sheet(SHEET_BRANCH_DAILY_REPORTS)
    branch_report_id = None
    branch_pkg_row   = None

    if not rpt_df.empty:
        mask = (
            (rpt_df["report_date"].astype(str) == str(audit_for_date)) &
            (rpt_df["branch_id"].astype(str)   == str(branch_id))
        )
        matched = rpt_df[mask]
        if not matched.empty:
            branch_report_id = matched.iloc[0]["branch_report_id"]
            st.success(f"✅ พบรายงานสาขา: **{branch_report_id}** (วันที่ {audit_for_date})")

            # ดึงข้อมูล packaging ที่สาขากรอก
            pkg_df = read_sheet(SHEET_BRANCH_PACKAGING_BALANCE)
            if not pkg_df.empty:
                pkg_mask = pkg_df["branch_report_id"].astype(str) == str(branch_report_id)
                if pkg_mask.any():
                    branch_pkg_row = pkg_df[pkg_mask].iloc[0]
        else:
            st.warning(
                f"⚠️ ไม่พบรายงานสาขา {branch_id} วันที่ {audit_for_date} "
                f"— สามารถกรอก Audit ได้ แต่จะไม่มีข้อมูลสาขาเทียบ"
            )
    else:
        st.info("ยังไม่มีรายงานสาขาในระบบ")

    # ── ② กรอกจำนวนตรวจนับจริง (Packaging) ─────────────────────────
    st.subheader("② กรอกจำนวนตรวจนับจริง (Packaging Balance)")

    # สร้าง input พร้อมแสดงค่าที่สาขากรอกเพื่ออ้างอิง
    audit_values = {}
    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        st.markdown("**รายการ**")
    with col2:
        st.markdown("**สาขากรอก**")
    with col3:
        st.markdown("**Audit กรอก (ตัวจริง)**")

    for label, branch_col, audit_col in PKG_FIELDS:
        branch_val = 0
        if branch_pkg_row is not None:
            try:
                branch_val = int(float(branch_pkg_row.get(branch_col, 0)))
            except Exception:
                branch_val = 0

        c1, c2, c3 = st.columns([2, 1, 1])
        with c1:
            st.write(label)
        with c2:
            st.markdown(f"<div style='padding:8px;color:#666'>{branch_val}</div>",
                        unsafe_allow_html=True)
        with c3:
            audit_values[audit_col] = st.number_input(
                label, min_value=0, step=1,
                key=f"audit_{audit_col}", label_visibility="collapsed"
            )

    photo_proof  = st.text_input("📸 หลักฐานภาพถ่าย (URL / ชื่อไฟล์)", key="photo_proof")
    audit_remark = st.text_input("📝 หมายเหตุ Audit", key="audit_pkg_remark")

    st.divider()

    # ── ③ ต้นทุนบรรจุภัณฑ์ ───────────────────────────────────────────
    st.subheader("③ ต้นทุนบรรจุภัณฑ์ (Packaging Cost)")
    col1, col2, col3 = st.columns(3)
    with col1:
        paper_bag_unit_cost    = st.number_input("ราคาต่อถุงกระดาษ (บาท)",     min_value=0.0, step=0.5, format="%.2f")
    with col2:
        plastic_box_unit_cost  = st.number_input("ราคาต่อกล่องพลาสติก (บาท)",  min_value=0.0, step=0.5, format="%.2f")
    with col3:
        drink_cup_unit_cost    = st.number_input("ราคาต่อแก้วเครื่องดื่ม (บาท)", min_value=0.0, step=0.5, format="%.2f")

    paper_bag_audit_qty   = audit_values.get("paper_bag_audit_qty",   0)
    plastic_box_audit_qty = audit_values.get("plastic_box_audit_qty", 0)
    drink_cup_audit_qty   = audit_values.get("drink_cup_audit_qty",   0)
    total_pkg_cost = (
        paper_bag_audit_qty   * paper_bag_unit_cost +
        plastic_box_audit_qty * plastic_box_unit_cost +
        drink_cup_audit_qty   * drink_cup_unit_cost
    )
    st.metric("💰 ต้นทุนบรรจุภัณฑ์รวม", f"฿{total_pkg_cost:,.2f}")

    st.divider()

    # ── ④ True Stock (สำหรับ packaging หลัก 3 รายการ) ───────────────
    st.subheader("④ ข้อมูล True Stock")
    st.caption("ตัวเลขจาก Audit ถือเป็น Stock ที่แท้จริง")
    col1, col2 = st.columns(2)
    with col1:
        stock_in_qty_paper   = st.number_input("ถุงกระดาษ – เข้า Stock วันนี้ (stock_in)",    min_value=0, step=1, key="si_paper")
        stock_in_qty_plastic = st.number_input("กล่องพลาสติก – เข้า Stock วันนี้ (stock_in)", min_value=0, step=1, key="si_plastic")
        stock_in_qty_cup     = st.number_input("แก้วเครื่องดื่ม – เข้า Stock วันนี้ (stock_in)", min_value=0, step=1, key="si_cup")
    with col2:
        sold_paper   = st.number_input("ถุงกระดาษ – ขาย/ใช้ไป (sold_or_used)",    min_value=0, step=1, key="sold_paper")
        sold_plastic = st.number_input("กล่องพลาสติก – ขาย/ใช้ไป (sold_or_used)", min_value=0, step=1, key="sold_plastic")
        sold_cup     = st.number_input("แก้วเครื่องดื่ม – ขาย/ใช้ไป (sold_or_used)", min_value=0, step=1, key="sold_cup")

    true_remaining_paper   = paper_bag_audit_qty   + stock_in_qty_paper   - sold_paper
    true_remaining_plastic = plastic_box_audit_qty + stock_in_qty_plastic - sold_plastic
    true_remaining_cup     = drink_cup_audit_qty   + stock_in_qty_cup     - sold_cup

    col1, col2, col3 = st.columns(3)
    col1.metric("ถุงกระดาษ คงเหลือจริง",    true_remaining_paper)
    col2.metric("กล่องพลาสติก คงเหลือจริง", true_remaining_plastic)
    col3.metric("แก้ว คงเหลือจริง",          true_remaining_cup)

    st.divider()

    # ── ⑤ หมายเหตุพฤติกรรม ──────────────────────────────────────────
    st.subheader("⑤ หมายเหตุพฤติกรรมพนักงานสาขา")
    behavior_remark = st.text_area(
        "บันทึกพฤติกรรม / ความผิดปกติ / ข้อสังเกต",
        height=120, key="behavior_remark"
    )

    # ── Preview DIFF ──────────────────────────────────────────────────
    st.subheader("⑥ ตัวอย่าง DIFF ก่อนบันทึก")
    _preview_diff(branch_pkg_row, audit_values)

    st.divider()

    # ── บันทึก ────────────────────────────────────────────────────────
    if st.button("💾 บันทึก Audit", type="primary", use_container_width=True):
        if not auditor_id.strip():
            st.error("กรุณากรอกรหัสผู้ตรวจ")
            return
        _save_audit(
            audit_date=str(audit_date),
            audit_for_date=str(audit_for_date),
            branch_id=branch_id,
            auditor_id=auditor_id.strip(),
            audit_time=str(audit_time),
            behavior_remark=behavior_remark,
            branch_report_id=branch_report_id,
            branch_pkg_row=branch_pkg_row,
            audit_values=audit_values,
            photo_proof=photo_proof,
            audit_remark=audit_remark,
            # packaging cost
            paper_bag_unit_cost=paper_bag_unit_cost,
            plastic_box_unit_cost=plastic_box_unit_cost,
            drink_cup_unit_cost=drink_cup_unit_cost,
            total_pkg_cost=total_pkg_cost,
            # true stock
            stock_in_paper=stock_in_qty_paper,
            stock_in_plastic=stock_in_qty_plastic,
            stock_in_cup=stock_in_qty_cup,
            sold_paper=sold_paper,
            sold_plastic=sold_plastic,
            sold_cup=sold_cup,
            true_remaining_paper=true_remaining_paper,
            true_remaining_plastic=true_remaining_plastic,
            true_remaining_cup=true_remaining_cup,
        )


# ══════════════════════════════════════════════════════════════════════
# PREVIEW DIFF TABLE
# ══════════════════════════════════════════════════════════════════════
def _preview_diff(branch_pkg_row, audit_values):
    rows = []
    for label, branch_col, audit_col in PKG_FIELDS:
        branch_qty = 0
        if branch_pkg_row is not None:
            try:
                branch_qty = int(float(branch_pkg_row.get(branch_col, 0)))
            except Exception:
                branch_qty = 0
        audit_qty = int(audit_values.get(audit_col, 0))
        diff_qty  = branch_qty - audit_qty
        rows.append({
            "รายการ":          label,
            "สาขากรอก":        branch_qty,
            "Audit กรอก":      audit_qty,
            "DIFF":            diff_qty,
            "สถานะ":           "✅ ตรง" if diff_qty == 0 else "❌ DIFF",
        })

    df_preview = pd.DataFrame(rows)

    # สร้าง HTML ตาราง เพื่อแสดงสีแดง
    html = _build_diff_html_table(df_preview)
    st.markdown(html, unsafe_allow_html=True)


def _build_diff_html_table(df: pd.DataFrame) -> str:
    header = "<tr>" + "".join(
        f"<th style='padding:8px;background:#1e1e1e;color:white;'>{c}</th>"
        for c in df.columns
    ) + "</tr>"

    rows_html = ""
    for _, r in df.iterrows():
        diff = r["DIFF"]
        if diff == 0:
            row_style = "background:#d4edda;"
            diff_cell = f"<td style='padding:8px;color:green;font-weight:bold;text-align:center;'>{diff}</td>"
        else:
            row_style = "background:#fff3cd;"
            diff_cell = (
                f"<td style='padding:8px;background:#FF0000;color:white;"
                f"font-size:16px;font-weight:bold;text-align:center;'>"
                f"DIFF {diff:+d}</td>"
            )
        status_cell = (
            "<td style='padding:8px;color:green;text-align:center;'>✅ ตรง</td>"
            if diff == 0 else
            "<td style='padding:8px;background:#FF0000;color:white;font-weight:bold;text-align:center;'>❌ DIFF</td>"
        )
        cells = ""
        for col in ["รายการ", "สาขากรอก", "Audit กรอก"]:
            cells += f"<td style='padding:8px;{row_style}'>{r[col]}</td>"
        cells += diff_cell + status_cell
        rows_html += f"<tr>{cells}</tr>"

    return f"""
    <table style='border-collapse:collapse;width:100%;font-size:14px;'>
      <thead>{header}</thead>
      <tbody>{rows_html}</tbody>
    </table>
    """


# ══════════════════════════════════════════════════════════════════════
# SAVE AUDIT
# ══════════════════════════════════════════════════════════════════════
def _save_audit(**kw):
    created_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # ─ 1. audit_sessions ─────────────────────────────────────────────
    ses_df    = read_sheet(SHEET_AUDIT_SESSIONS)
    audit_id  = next_id(ses_df, "audit_id", "AUD")

    # สรุป overall_status จาก diff
    has_diff = False
    if kw["branch_pkg_row"] is not None:
        for _, branch_col, audit_col in PKG_FIELDS:
            try:
                bv = int(float(kw["branch_pkg_row"].get(branch_col, 0)))
            except Exception:
                bv = 0
            av = int(kw["audit_values"].get(audit_col, 0))
            if bv != av:
                has_diff = True
                break

    overall_status = "DIFF" if has_diff else "OK"

    append_row(SHEET_AUDIT_SESSIONS, {
        "audit_id":        audit_id,
        "audit_date":      kw["audit_date"],
        "audit_for_date":  kw["audit_for_date"],
        "branch_id":       kw["branch_id"],
        "auditor_id":      kw["auditor_id"],
        "audit_time":      kw["audit_time"],
        "overall_status":  overall_status,
        "behavior_remark": kw["behavior_remark"],
        "created_at":      created_at,
    })

    # ─ 2. audit_packaging_balance ────────────────────────────────────
    apb_df = read_sheet(SHEET_AUDIT_PACKAGING_BALANCE)
    apb_id  = next_id(apb_df, "audit_packaging_balance_id", "APB")
    av = kw["audit_values"]
    append_row(SHEET_AUDIT_PACKAGING_BALANCE, {
        "audit_packaging_balance_id":       apb_id,
        "audit_id":                         audit_id,
        "branch_report_id":                 kw["branch_report_id"] or "",
        "paper_bag_audit_qty":              av.get("paper_bag_audit_qty",   0),
        "plastic_box_audit_qty":            av.get("plastic_box_audit_qty", 0),
        "drink_cup_audit_qty":              av.get("drink_cup_audit_qty",   0),
        "cup_lid_audit_qty":                av.get("cup_lid_audit_qty",     0),
        "band_audit_qty":                   av.get("band_audit_qty",        0),
        "skewer_pack_audit_qty":            av.get("skewer_pack_audit_qty", 0),
        "hot_bag_pack_audit_qty":           av.get("hot_bag_pack_audit_qty",0),
        "printed_carry_bag_audit_qty":      av.get("printed_carry_bag_audit_qty", 0),
        "plastic_carry_bag_pack_audit_qty": av.get("plastic_carry_bag_pack_audit_qty", 0),
        "photo_proof":                      kw["photo_proof"],
        "remark":                           kw["audit_remark"],
    })

    # ─ 3. audit_packaging_diff ───────────────────────────────────────
    for label, branch_col, audit_col in PKG_FIELDS:
        branch_qty = 0
        if kw["branch_pkg_row"] is not None:
            try:
                branch_qty = int(float(kw["branch_pkg_row"].get(branch_col, 0)))
            except Exception:
                branch_qty = 0
        audit_qty = int(av.get(audit_col, 0))
        diff_qty  = branch_qty - audit_qty
        status    = "match" if diff_qty == 0 else "diff"
        color     = "green"  if diff_qty == 0 else "red"

        diff_df = read_sheet(SHEET_AUDIT_PACKAGING_DIFF)
        diff_id = next_id(diff_df, "audit_diff_id", "ADIFF")
        append_row(SHEET_AUDIT_PACKAGING_DIFF, {
            "audit_diff_id":    diff_id,
            "audit_id":         audit_id,
            "branch_report_id": kw["branch_report_id"] or "",
            "item_name":        label,
            "branch_qty":       branch_qty,
            "audit_qty":        audit_qty,
            "diff_qty":         diff_qty,
            "display_status":   status,
            "display_color":    color,
            "remark":           "",
        })

    # ─ 4. true_stock_balance (3 รายการหลัก) ─────────────────────────
    item_map = [
        ("PKG_PAPER",   "ถุงกระดาษ",       av.get("paper_bag_audit_qty",   0), kw["stock_in_paper"],   kw["sold_paper"],   kw["true_remaining_paper"]),
        ("PKG_PLASTIC", "กล่องพลาสติก",     av.get("plastic_box_audit_qty", 0), kw["stock_in_plastic"], kw["sold_plastic"], kw["true_remaining_plastic"]),
        ("PKG_CUP",     "แก้วเครื่องดื่ม",   av.get("drink_cup_audit_qty",   0), kw["stock_in_cup"],     kw["sold_cup"],     kw["true_remaining_cup"]),
    ]
    for item_id, item_label, audit_qty, stock_in, sold, true_remaining in item_map:
        ts_df = read_sheet(SHEET_TRUE_STOCK_BALANCE)
        ts_id = next_id(ts_df, "true_stock_id", "TS")
        append_row(SHEET_TRUE_STOCK_BALANCE, {
            "true_stock_id":      ts_id,
            "stock_date":         kw["audit_for_date"],
            "branch_id":          kw["branch_id"],
            "item_id":            item_id,
            "audit_qty":          audit_qty,
            "stock_in_qty":       stock_in,
            "sold_or_used_qty":   sold,
            "true_remaining_qty": true_remaining,
            "source_audit_id":    audit_id,
            "remark":             item_label,
        })

    # ─ 5. daily_stock_usage ──────────────────────────────────────────
    # opening_qty = หาจาก true_stock ของวันก่อนหน้า (ถ้ามี) ไม่มีก็ใส่ 0
    ts_df_all = read_sheet(SHEET_TRUE_STOCK_BALANCE)
    for item_id, item_label, audit_qty, stock_in, sold, true_remaining in item_map:
        opening = 0
        if not ts_df_all.empty:
            prev = ts_df_all[
                (ts_df_all["branch_id"].astype(str) == str(kw["branch_id"])) &
                (ts_df_all["item_id"].astype(str)   == item_id) &
                (ts_df_all["stock_date"].astype(str) < kw["audit_for_date"])
            ]
            if not prev.empty:
                try:
                    opening = int(float(prev.sort_values("stock_date").iloc[-1]["true_remaining_qty"]))
                except Exception:
                    opening = 0

        used_qty = opening + stock_in - true_remaining

        su_df = read_sheet(SHEET_DAILY_STOCK_USAGE)
        su_id = next_id(su_df, "stock_usage_id", "SU")
        append_row(SHEET_DAILY_STOCK_USAGE, {
            "stock_usage_id":    su_id,
            "usage_date":        kw["audit_for_date"],
            "branch_id":         kw["branch_id"],
            "item_id":           item_id,
            "opening_qty":       opening,
            "stock_in_qty":      stock_in,
            "true_remaining_qty": true_remaining,
            "used_qty":          used_qty,
            "unit":              "ชิ้น",
            "remark":            item_label,
        })

    # ─ 6. daily_packaging_cost ───────────────────────────────────────
    pc_df = read_sheet(SHEET_DAILY_PACKAGING_COST)
    pc_id = next_id(pc_df, "packaging_cost_id", "PC")
    append_row(SHEET_DAILY_PACKAGING_COST, {
        "packaging_cost_id":      pc_id,
        "report_date":            kw["audit_for_date"],
        "branch_id":              kw["branch_id"],
        "paper_bag_qty":          av.get("paper_bag_audit_qty",   0),
        "plastic_box_qty":        av.get("plastic_box_audit_qty", 0),
        "drink_cup_qty":          av.get("drink_cup_audit_qty",   0),
        "paper_bag_unit_cost":    kw["paper_bag_unit_cost"],
        "plastic_box_unit_cost":  kw["plastic_box_unit_cost"],
        "drink_cup_unit_cost":    kw["drink_cup_unit_cost"],
        "total_packaging_cost":   kw["total_pkg_cost"],
    })

    # ─ แสดงผลลัพธ์ ────────────────────────────────────────────────────
    if overall_status == "OK":
        st.success(f"✅ บันทึก Audit สำเร็จ! Audit ID: **{audit_id}** | สถานะ: ตรงทุกรายการ")
    else:
        st.markdown(
            f"""
            <div style="background:#FF0000;color:white;padding:16px;border-radius:8px;
                        font-size:20px;font-weight:bold;text-align:center;">
            ⚠️ บันทึก Audit สำเร็จ — พบ DIFF! &nbsp;|&nbsp; Audit ID: {audit_id}
            </div>
            """,
            unsafe_allow_html=True,
        )
    st.balloons()


# ══════════════════════════════════════════════════════════════════════
# TAB 2: ดู DIFF รายงาน
# ══════════════════════════════════════════════════════════════════════
def _render_diff_viewer():
    st.subheader("📊 ตารางเปรียบเทียบ สาขา vs Audit (DIFF)")

    diff_df = read_sheet(SHEET_AUDIT_PACKAGING_DIFF)
    ses_df  = read_sheet(SHEET_AUDIT_SESSIONS)

    if diff_df.empty:
        st.info("ยังไม่มีข้อมูล Audit DIFF")
        return

    # filter
    col1, col2 = st.columns(2)
    with col1:
        if not ses_df.empty:
            audit_ids = ["ทั้งหมด"] + ses_df["audit_id"].tolist()
            sel_audit = st.selectbox("เลือก Audit ID", audit_ids)
        else:
            sel_audit = "ทั้งหมด"
    with col2:
        show_diff_only = st.checkbox("แสดงเฉพาะรายการที่ DIFF", value=False)

    df_show = diff_df.copy()
    if sel_audit != "ทั้งหมด":
        df_show = df_show[df_show["audit_id"].astype(str) == sel_audit]
    if show_diff_only:
        df_show = df_show[df_show["display_status"].astype(str) == "diff"]

    if df_show.empty:
        st.success("✅ ไม่มีรายการ DIFF ในช่วงที่เลือก")
        return

    # แสดงตารางพร้อมสี
    html = _build_diff_detail_table(df_show)
    st.markdown(html, unsafe_allow_html=True)

    # สรุปจำนวน
    total   = len(df_show)
    ok_cnt  = len(df_show[df_show["display_status"] == "match"])
    dif_cnt = len(df_show[df_show["display_status"] == "diff"])
    c1, c2, c3 = st.columns(3)
    c1.metric("รายการทั้งหมด", total)
    c2.metric("✅ ตรง",         ok_cnt)
    c3.metric("❌ DIFF",        dif_cnt)


def _build_diff_detail_table(df: pd.DataFrame) -> str:
    cols = ["audit_id", "branch_report_id", "item_name",
            "branch_qty", "audit_qty", "diff_qty", "display_status"]
    labels = ["Audit ID", "Report ID", "รายการ",
              "สาขากรอก", "Audit กรอก", "DIFF", "สถานะ"]

    header = "<tr>" + "".join(
        f"<th style='padding:8px;background:#1e1e1e;color:white;'>{l}</th>"
        for l in labels
    ) + "</tr>"

    rows_html = ""
    for _, r in df.iterrows():
        is_diff = str(r.get("display_status", "")) == "diff"
        try:
            diff_val = int(float(r["diff_qty"]))
        except Exception:
            diff_val = 0

        row_bg  = "#fff3cd" if is_diff else "#d4edda"
        diff_td = (
            f"<td style='padding:8px;background:#FF0000;color:white;"
            f"font-size:15px;font-weight:bold;text-align:center;'>DIFF {diff_val:+d}</td>"
            if is_diff else
            f"<td style='padding:8px;color:green;text-align:center;'>0</td>"
        )
        status_td = (
            "<td style='padding:8px;background:#FF0000;color:white;"
            "font-weight:bold;text-align:center;'>❌ DIFF</td>"
            if is_diff else
            "<td style='padding:8px;color:green;text-align:center;'>✅ ตรง</td>"
        )
        cells = ""
        for col in ["audit_id", "branch_report_id", "item_name", "branch_qty", "audit_qty"]:
            cells += f"<td style='padding:8px;background:{row_bg};'>{r.get(col,'')}</td>"
        cells += diff_td + status_td
        rows_html += f"<tr>{cells}</tr>"

    return f"""
    <table style='border-collapse:collapse;width:100%;font-size:13px;'>
      <thead>{header}</thead>
      <tbody>{rows_html}</tbody>
    </table>
    """


# ══════════════════════════════════════════════════════════════════════
# TAB 3: ประวัติ Audit
# ══════════════════════════════════════════════════════════════════════
def _render_audit_history():
    st.subheader("📋 ประวัติ Audit Sessions")

    ses_df = read_sheet(SHEET_AUDIT_SESSIONS)
    if ses_df.empty:
        st.info("ยังไม่มีประวัติ Audit")
        return

    # แสดงตารางพร้อมไฮไลต์แถว DIFF
    html_rows = ""
    for _, r in ses_df.iterrows():
        is_diff = str(r.get("overall_status", "")) == "DIFF"
        row_bg  = "#fff3cd" if is_diff else "#d4edda"
        status_td = (
            "<td style='padding:8px;background:#FF0000;color:white;"
            "font-size:14px;font-weight:bold;text-align:center;'>⚠️ DIFF</td>"
            if is_diff else
            "<td style='padding:8px;color:green;font-weight:bold;text-align:center;'>✅ OK</td>"
        )
        cells = "".join(
            f"<td style='padding:8px;background:{row_bg};'>{r.get(c,'')}</td>"
            for c in ["audit_id", "audit_date", "audit_for_date",
                      "branch_id", "auditor_id", "audit_time"]
        )
        cells += status_td
        cells += f"<td style='padding:8px;background:{row_bg};'>{r.get('behavior_remark','')}</td>"
        html_rows += f"<tr>{cells}</tr>"

    header_labels = ["Audit ID", "วันที่ตรวจ", "ตรวจสำหรับวัน",
                     "สาขา", "ผู้ตรวจ", "เวลา", "สถานะ", "หมายเหตุพฤติกรรม"]
    header = "<tr>" + "".join(
        f"<th style='padding:8px;background:#1e1e1e;color:white;'>{h}</th>"
        for h in header_labels
    ) + "</tr>"

    st.markdown(
        f"<table style='border-collapse:collapse;width:100%;font-size:13px;'>"
        f"<thead>{header}</thead><tbody>{html_rows}</tbody></table>",
        unsafe_allow_html=True,
    )

    st.divider()

    # True Stock Summary
    st.subheader("📦 True Stock Balance (ข้อมูลจาก Audit)")
    ts_df = read_sheet(SHEET_TRUE_STOCK_BALANCE)
    if not ts_df.empty:
        st.dataframe(ts_df, use_container_width=True)
    else:
        st.info("ยังไม่มีข้อมูล True Stock")

    # Daily Packaging Cost
    st.subheader("💰 Daily Packaging Cost")
    pc_df = read_sheet(SHEET_DAILY_PACKAGING_COST)
    if not pc_df.empty:
        st.dataframe(pc_df, use_container_width=True)
    else:
        st.info("ยังไม่มีข้อมูลต้นทุนบรรจุภัณฑ์")
