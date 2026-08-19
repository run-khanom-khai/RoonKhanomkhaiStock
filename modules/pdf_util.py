"""
pdf_util.py  –  ตัวช่วยสร้างไฟล์ PDF ภาษาไทย (ใช้ฟอนต์ Loma ที่แนบมากับโปรเจกต์)
ผู้พัฒนา: ดร.วรรณ (ดร.อภิวรรณ์ ดำแสงสวัสดิ์)

ใช้ fpdf2 + ฟอนต์ไทย (assets/fonts/Loma.otf) เพื่อให้แสดงภาษาไทยได้ถูกต้อง
เรียกใช้:  pdf_bytes = make_table_pdf(title, meta_lines, columns, rows, summary_lines)
"""
import os

try:
    from config import BASE_DIR
except Exception:
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

_FONT_REG  = os.path.join(BASE_DIR, "assets", "fonts", "Loma.otf")
_FONT_BOLD = os.path.join(BASE_DIR, "assets", "fonts", "Loma-Bold.otf")


def _new_pdf(orientation="P"):
    from fpdf import FPDF
    from fpdf.enums import XPos, YPos
    pdf = FPDF(orientation=orientation, unit="mm", format="A4")
    pdf._X_LM, pdf._Y_NEXT = XPos.LMARGIN, YPos.NEXT
    pdf.set_auto_page_break(auto=True, margin=12)
    # ลงทะเบียนฟอนต์ไทย (ถ้าหาไฟล์ไม่เจอ จะ fallback เป็น Helvetica — ไทยจะเพี้ยน)
    try:
        pdf.add_font("Loma", "", _FONT_REG)
        if os.path.exists(_FONT_BOLD):
            pdf.add_font("Loma", "B", _FONT_BOLD)
        else:
            pdf.add_font("Loma", "B", _FONT_REG)
        pdf._roon_font = "Loma"
    except Exception:
        pdf._roon_font = "Helvetica"
    pdf.add_page()
    return pdf


def make_table_pdf(title, meta_lines, columns, rows,
                   summary_lines=None, col_widths=None,
                   col_align=None, orientation="L"):
    """สร้าง PDF ตารางภาษาไทย → คืน bytes

    title        : หัวเรื่องใหญ่ (str)
    meta_lines   : list[str] บรรทัดข้อมูลหัวเอกสาร (เช่น สาขา/วันที่)
    columns      : list[str] ชื่อคอลัมน์
    rows         : list[list] ข้อมูลแต่ละแถว (ตรงกับ columns)
    summary_lines: list[str] บรรทัดสรุปท้ายตาราง (เช่น ยอดรวม)
    col_widths   : list[float] ความกว้างแต่ละคอลัมน์ (มม.) — ถ้าไม่ระบุ แบ่งเท่ากัน
    col_align    : list[str] การจัดวาง 'L'/'C'/'R' ต่อคอลัมน์
    orientation  : 'P' แนวตั้ง / 'L' แนวนอน
    """
    pdf = _new_pdf(orientation)
    font = pdf._roon_font
    epw = pdf.w - pdf.l_margin - pdf.r_margin

    # ── หัวเรื่อง ──
    pdf.set_font(font, "B", 16)
    pdf.multi_cell(epw, 9, title, align="C", new_x=pdf._X_LM, new_y=pdf._Y_NEXT)
    pdf.ln(1)

    # ── ข้อมูลหัวเอกสาร ──
    if meta_lines:
        pdf.set_font(font, "", 11)
        for line in meta_lines:
            pdf.multi_cell(epw, 6, str(line), align="L",
                           new_x=pdf._X_LM, new_y=pdf._Y_NEXT)
    pdf.ln(2)

    n = len(columns)
    if not col_widths:
        col_widths = [epw / n] * n
    else:
        # ปรับสเกลให้พอดีหน้ากระดาษ
        s = sum(col_widths)
        col_widths = [w * epw / s for w in col_widths]
    if not col_align:
        col_align = ["L"] * n

    # ── หัวตาราง ──
    pdf.set_font(font, "B", 10)
    pdf.set_fill_color(30, 30, 30)
    pdf.set_text_color(255, 255, 255)
    for w, c in zip(col_widths, columns):
        pdf.cell(w, 8, str(c), border=1, align="C", fill=True)
    pdf.ln()

    # ── ข้อมูล ──
    pdf.set_font(font, "", 10)
    pdf.set_text_color(0, 0, 0)
    fill = False
    for row in rows:
        if fill:
            pdf.set_fill_color(240, 240, 240)
        for w, val, al in zip(col_widths, row, col_align):
            pdf.cell(w, 7, str(val), border=1, align=al, fill=fill)
        pdf.ln()
        fill = not fill

    # ── สรุปท้ายตาราง ──
    if summary_lines:
        pdf.ln(3)
        pdf.set_font(font, "B", 12)
        for line in summary_lines:
            pdf.multi_cell(epw, 7, str(line), align="R",
                           new_x=pdf._X_LM, new_y=pdf._Y_NEXT)

    out = pdf.output()
    return bytes(out)


def make_delivery_note_pdf(doc_no, send_date, branch_label, preparer, remark, rows):
    """ใบส่งมอบและตรวจรับวัตถุดิบ/บรรจุภัณฑ์ (ฝ่ายจัดซื้อ → สาขา)
    rows: list ของ (รหัส, ประเภท, รายการ, จำนวน, หน่วย)
    คอลัมน์: ลำดับ | รหัส | ประเภท | รายการ | ส่งของ(☐) | จำนวน | หน่วย | รับแล้ว(☐)
    ช่อง 'ส่งของ' สำหรับผู้จัดของติ๊ก • ช่อง 'รับแล้ว' สำหรับผู้รับติ๊ก
    """
    pdf = _new_pdf("P")
    font = pdf._roon_font
    epw = pdf.w - pdf.l_margin - pdf.r_margin
    NAVY = (26, 35, 82)

    # ── โลโก้ + หัวเรื่อง ──
    logo = os.path.join(BASE_DIR, "logo_roon.png")
    y0 = pdf.get_y()
    if os.path.exists(logo):
        try:
            pdf.image(logo, x=pdf.l_margin, y=y0, w=32)
        except Exception:
            pass
    pdf.set_xy(pdf.l_margin + 36, y0 + 1)
    pdf.set_font(font, "B", 17); pdf.set_text_color(*NAVY)
    pdf.cell(epw - 36, 9, "ใบส่งมอบและตรวจรับวัตถุดิบ/บรรจุภัณฑ์",
             align="C", new_x=pdf._X_LM, new_y=pdf._Y_NEXT)
    pdf.set_x(pdf.l_margin + 36)
    pdf.set_font(font, "", 11); pdf.set_text_color(90, 90, 90)
    pdf.cell(epw - 36, 6, "ฝ่ายจัดซื้อ - ส่งมอบให้สาขา",
             align="C", new_x=pdf._X_LM, new_y=pdf._Y_NEXT)
    pdf.ln(5)
    pdf.set_draw_color(*NAVY); pdf.set_line_width(0.6)
    pdf.line(pdf.l_margin, pdf.get_y(), pdf.w - pdf.r_margin, pdf.get_y())
    pdf.ln(3)

    # ── ข้อมูลหัวเอกสาร ──
    def meta_pair(l1, v1, l2, v2):
        pdf.set_font(font, "B", 11); pdf.set_text_color(30, 30, 30); pdf.cell(32, 7, l1)
        pdf.set_font(font, "", 11); pdf.cell(58, 7, str(v1))
        if l2:
            pdf.set_font(font, "B", 11); pdf.cell(26, 7, l2)
            pdf.set_font(font, "", 11); pdf.cell(0, 7, str(v2))
        pdf.ln(7)
    meta_pair("เลขที่เอกสาร:", doc_no, "วันที่ส่ง:", send_date)
    meta_pair("สาขาปลายทาง:", branch_label, "", "")
    meta_pair("ผู้จัดทำรายการ:", preparer or "ฝ่ายจัดซื้อ", "หมายเหตุ:", remark or "-")
    pdf.ln(1)

    # ── แถบคำแนะนำ ──
    pdf.set_fill_color(250, 247, 235); pdf.set_text_color(90, 70, 20); pdf.set_font(font, "", 10.5)
    pdf.multi_cell(epw, 8,
                   "คำแนะนำ: ผู้จัดของติ๊กช่อง 'ส่งของ'  •  ผู้รับตรวจนับจำนวน/สภาพสินค้า แล้วติ๊กช่อง 'รับแล้ว' ทีละรายการ",
                   fill=True, new_x=pdf._X_LM, new_y=pdf._Y_NEXT)
    pdf.ln(2)

    # ── ตาราง ──
    cols = ["ลำดับ", "รหัส", "ประเภท", "รายการ", "ส่งของ", "จำนวน", "หน่วย", "รับแล้ว"]
    W = [12, 22, 26, 58, 16, 20, 16, 16]      # รวม = 186 = epw (A4 P margin 12)
    aligns = ["C", "C", "L", "L", "C", "R", "C", "C"]

    pdf.set_font(font, "B", 10.5); pdf.set_fill_color(*NAVY); pdf.set_text_color(255, 255, 255)
    pdf.set_draw_color(*NAVY); pdf.set_line_width(0.2)
    for c, w in zip(cols, W):
        pdf.cell(w, 9, c, border=1, align="C", fill=True)
    pdf.ln()

    pdf.set_font(font, "", 10); pdf.set_text_color(30, 30, 30)
    for i, r in enumerate(rows, 1):
        code, typ, name, qty, unit = (list(r) + ["", "", "", "", ""])[:5]
        h = 8.5
        vals = [str(i), str(code), str(typ), str(name), None, str(qty), str(unit), None]
        for v, w, a in zip(vals, W, aligns):
            x = pdf.get_x(); y = pdf.get_y()
            pdf.set_draw_color(210, 205, 190); pdf.set_line_width(0.2)
            pdf.cell(w, h, "" if v is None else v, border=1, align=a)
            if v is None:      # วาดช่องติ๊ก (checkbox)
                s = 4.2
                pdf.set_draw_color(*NAVY); pdf.set_line_width(0.45)
                pdf.rect(x + (w - s) / 2, y + (h - s) / 2, s, s)
        pdf.ln()

    pdf.ln(3)
    pdf.set_font(font, "B", 12); pdf.set_text_color(*NAVY)
    pdf.cell(0, 8, f"รวมจำนวนรายการที่ส่งมอบ: {len(rows)} รายการ",
             align="R", new_x=pdf._X_LM, new_y=pdf._Y_NEXT)
    pdf.set_font(font, "", 9); pdf.set_text_color(120, 110, 90)
    pdf.multi_cell(epw, 6,
                   "กรณีจำนวนไม่ครบ สินค้าชำรุด หรือรายการไม่ตรง กรุณาระบุรายละเอียดก่อนลงนามรับของ",
                   new_x=pdf._X_LM, new_y=pdf._Y_NEXT)

    return bytes(pdf.output())
