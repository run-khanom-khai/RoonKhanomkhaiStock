"""
branch_lock.py – ตัวเลือกสาขาแบบล็อกได้
ถ้า session มี locked_branch_id → แสดงอ่านอย่างเดียว
ถ้าไม่มี (ผู้บริหาร/Audit) → ทำงานเป็น selectbox ปกติ
"""

import streamlit as st


def branch_selector(branches_df, label="🏪 สาขา", key=None):
    if branches_df is None or branches_df.empty:
        st.warning("⚠️ ยังไม่มีข้อมูลสาขา")
        return None

    opts = dict(
        zip(
            branches_df["branch_id"].astype(str),
            branches_df["branch_name"].astype(str),
        )
    )

    locked = str(st.session_state.get("locked_branch_id", ""))

    # ── โหมดล็อก (พนักงานสาขา) ──────────────────────────────
    if locked and locked in opts:
        st.markdown(
            f"<div style='background:#FFF3E0;border:2px solid #FF6B35;"
            f"border-radius:8px;padding:10px 14px;'>"
            f"<small style='color:#888;'>{label}</small><br>"
            f"<b style='font-size:1.1rem;color:#E65100;'>"
            f"{locked} – {opts[locked]}</b></div>",
            unsafe_allow_html=True,
        )
        return locked

    # ── โหมดปกติ (ผู้บริหาร / Audit) ────────────────────────
    return st.selectbox(
        label,
        options=list(opts.keys()),
        format_func=lambda k: f"{k} – {opts[k]}",
        key=key,
    )
