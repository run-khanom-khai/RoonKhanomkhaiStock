import re

import pandas as pd


def next_id(df: pd.DataFrame, id_col: str, prefix: str = "") -> str:
    """สร้าง ID ถัดไปแบบ prefix + เลขลำดับ (อย่างน้อย 3 หลัก)

    ดึงเลขลำดับสูงสุดจากข้อมูล 'ทั้งหมด' แล้ว +1
    - รองรับรหัสที่ยาวเกิน 3 หลัก (เช่น DV1397 → DV1398)
    - ตัด prefix ออกเฉพาะส่วนหน้า และดึงเฉพาะตัวเลขอย่างปลอดภัย
    หมายเหตุ: ต้องส่ง df ที่อ่านมา 'ครบทุกแถว' (ดู read_sheet ที่ทำ pagination)
    มิฉะนั้นเลขสูงสุดจะเพี้ยนและเกิดรหัสซ้ำได้
    """
    if df is None or df.empty or id_col not in df.columns:
        return f"{prefix}001"
    existing = df[id_col].dropna().astype(str)
    nums = []
    for val in existing:
        s = val.strip()
        # ตัด prefix ออกเฉพาะส่วนหน้า (ไม่ใช่ทุกตำแหน่ง)
        if prefix and s.startswith(prefix):
            s = s[len(prefix):]
        # ดึงเฉพาะตัวเลขที่เหลือ
        digits = re.sub(r"\D", "", s)
        if digits:
            try:
                nums.append(int(digits))
            except ValueError:
                pass
    next_num = (max(nums) + 1) if nums else 1
    return f"{prefix}{next_num:03d}"
