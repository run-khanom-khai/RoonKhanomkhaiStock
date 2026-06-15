import pandas as pd


def next_id(df: pd.DataFrame, id_col: str, prefix: str = "") -> str:
    """สร้าง ID ถัดไปแบบ prefix + เลขลำดับ 3 หลัก"""
    if df.empty or id_col not in df.columns:
        return f"{prefix}001"
    existing = df[id_col].dropna().astype(str)
    nums = []
    for val in existing:
        part = val.replace(prefix, "")
        if part.isdigit():
            nums.append(int(part))
    next_num = (max(nums) + 1) if nums else 1
    return f"{prefix}{next_num:03d}"
