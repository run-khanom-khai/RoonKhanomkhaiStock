"""
dashboard.py  –  Dashboard พร้อมกราฟ Plotly (ปรับปรุง)
"""
import io, datetime, base64
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from config import (
    SHEET_BRANCHES, SHEET_BRANCH_GROUPS, SHEET_ITEMS,
    SHEET_BRANCH_DAILY_REPORTS, SHEET_BRANCH_EXPENSES,
    SHEET_AUDIT_SESSIONS, SHEET_AUDIT_PACKAGING_DIFF,
    SHEET_STOCK_MOVEMENTS, SHEET_BANK_ACCOUNTS,
    SHEET_PAYROLL_RECORDS, SHEET_EMPLOYEES, SHEET_PRODUCTS,
    SHEET_DAILY_SALES_ACCOUNTING,
)
from modules.excel_db import read_sheet

BRAND_COLOR  = "#FF6B35"
COLORS_MAIN  = ["#FF6B35","#1976D2","#2E7D32","#7B1FA2","#E53935",
                 "#FF8F00","#0097A7","#5D4037","#37474F","#AD1457"]

# ─────────────────────────────────────────────────────────────
def _num(df, col):
    if col not in df.columns: return pd.Series([0]*len(df))
    return pd.to_numeric(df[col], errors="coerce").fillna(0)

def _branches_lookup():
    df = read_sheet(SHEET_BRANCHES)
    if df.empty: return {}, {}
    bname  = dict(zip(df["branch_id"], df["branch_name"]))
    bgroup = dict(zip(df["branch_id"], df.get("branch_group_id", pd.Series(dtype=str))))
    return bname, bgroup

def _bg_lookup():
    df = read_sheet(SHEET_BRANCH_GROUPS)
    if df.empty: return {}
    return dict(zip(df["branch_group_id"], df["branch_group_name"]))

def _h1(text, color=BRAND_COLOR):
    st.markdown(
        f"<h1 style='color:{color};font-size:1.8rem;font-weight:800;"
        f"border-left:6px solid {color};padding-left:12px;margin-bottom:4px;'>{text}</h1>",
        unsafe_allow_html=True)

def _h2(text, color="#1976D2"):
    st.markdown(
        f"<h2 style='color:{color};font-size:1.25rem;font-weight:700;"
        f"border-bottom:2px solid {color};padding-bottom:3px;margin-top:18px;'>{text}</h2>",
        unsafe_allow_html=True)

def _kpi(label, value, color=BRAND_COLOR, sub=""):
    st.markdown(
        f"<div style='background:linear-gradient(135deg,{color}22,{color}0a);"
        f"border:2px solid {color};border-radius:10px;padding:12px 16px;text-align:center;margin:4px 0;'>"
        f"<div style='color:{color};font-size:0.8rem;font-weight:600;'>{label}</div>"
        f"<div style='color:{color};font-size:1.6rem;font-weight:800;'>{value}</div>"
        f"{'<div style=color:#666;font-size:0.72rem;>'+sub+'</div>' if sub else ''}"
        f"</div>", unsafe_allow_html=True)

def _no_data(msg="ยังไม่มีข้อมูล — กรุณากรอกข้อมูลก่อน"):
    st.markdown(
        f"<div style='background:#f5f5f5;border:2px dashed #ccc;border-radius:8px;"
        f"padding:30px;text-align:center;color:#999;font-size:1rem;'>"
        f"📊 {msg}</div>", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════
def render():
    _h1("📈 Dashboard — ROON KHANOMKHAI", BRAND_COLOR)
    st.caption(f"ข้อมูล ณ {datetime.date.today().strftime('%d/%m/%Y')} | ออกแบบโดย ดร.อภิวรรณ์ ดำแสงสวัสดิ์")

    tabs = st.tabs([
        "🏆 ยอดขาย", "💸 ค่าใช้จ่าย", "📦 Stock",
        "🔎 Audit/ความผิดพลาด", "👥 HR/เงินเดือน",
        "🏦 การเงิน", "📤 Export PowerBI",
    ])
    with tabs[0]: _tab_sales()
    with tabs[1]: _tab_expenses()
    with tabs[2]: _tab_stock()
    with tabs[3]: _tab_audit()
    with tabs[4]: _tab_hr()
    with tabs[5]: _tab_finance()
    with tabs[6]: _tab_export()


# ══════════════════════════════════════════════════════════════
# TAB 1 : ยอดขาย
# ══════════════════════════════════════════════════════════════
def _tab_sales():
    _h1("🏆 วิเคราะห์ยอดขาย", BRAND_COLOR)
    rpt_df = read_sheet(SHEET_BRANCH_DAILY_REPORTS)
    exp_df = read_sheet(SHEET_BRANCH_EXPENSES)
    bname, bgroup = _branches_lookup()
    bg_dict = _bg_lookup()

    if rpt_df.empty:
        _no_data("ยังไม่มีข้อมูล Branch Daily Report"); return

    for c in ["total_received","cash_amount","transfer_amount",
              "statement_amount","lineman_amount","grab_amount","other_income_amount"]:
        rpt_df[c] = _num(rpt_df, c)

    rpt_df["branch_name"]  = rpt_df["branch_id"].map(bname).fillna(rpt_df["branch_id"])
    rpt_df["branch_group"] = rpt_df["branch_id"].map(bgroup).map(bg_dict).fillna("ไม่ระบุ")

    total_sales = rpt_df["total_received"].sum()
    num_reports = len(rpt_df)
    num_branches = rpt_df["branch_id"].nunique()

    # ── KPI Row ──────────────────────────────────────────────
    c1,c2,c3,c4 = st.columns(4)
    with c1: _kpi("💰 ยอดขายรวม",    f"฿{total_sales:,.0f}", BRAND_COLOR)
    with c2: _kpi("📅 รายงาน",       f"{num_reports} วัน",   "#1976D2")
    with c3: _kpi("🏪 สาขา",         f"{num_branches} สาขา", "#2E7D32")
    with c4: _kpi("📊 เฉลี่ย/วัน",   f"฿{total_sales/max(num_reports,1):,.0f}", "#7B1FA2")

    st.divider()

    # ── กราฟ 1: ยอดขายแยกสาขา (Bar) ────────────────────────
    _h2("📊 ยอดขายแยกสาขา")
    branch_s = rpt_df.groupby("branch_name")["total_received"].sum().reset_index()
    branch_s = branch_s.sort_values("total_received", ascending=False)
    if not branch_s.empty:
        fig = px.bar(branch_s, x="branch_name", y="total_received",
                     color="total_received", color_continuous_scale="Oranges",
                     labels={"branch_name":"สาขา","total_received":"ยอดขาย (฿)"},
                     text_auto=".2s")
        fig.update_layout(showlegend=False, plot_bgcolor="white", paper_bgcolor="white",
                          coloraxis_showscale=False, height=350)
        fig.update_traces(textfont_color="black")
        st.plotly_chart(fig, use_container_width=True)

    col1, col2 = st.columns(2)

    # ── กราฟ 2: ยอดขายรายวัน (Area) ────────────────────────
    with col1:
        _h2("📈 ยอดขายรายวัน")
        daily = rpt_df.groupby("report_date")["total_received"].sum().reset_index()
        daily.columns = ["วันที่","ยอดขาย"]
        daily = daily.sort_values("วันที่")
        if not daily.empty:
            fig2 = px.area(daily, x="วันที่", y="ยอดขาย",
                           color_discrete_sequence=[BRAND_COLOR],
                           labels={"ยอดขาย":"ยอดขาย (฿)"})
            fig2.update_layout(plot_bgcolor="white", paper_bgcolor="white", height=300)
            st.plotly_chart(fig2, use_container_width=True)

    # ── กราฟ 3: ช่องทางการขาย (Donut) ──────────────────────
    with col2:
        _h2("🍩 สัดส่วนช่องทางขาย")
        ch_labels = ["เงินสด","โอน","Statement","Line Man","Grab","อื่น ๆ"]
        ch_values = [
            rpt_df["cash_amount"].sum(),
            rpt_df["transfer_amount"].sum(),
            rpt_df["statement_amount"].sum(),
            rpt_df["lineman_amount"].sum(),
            rpt_df["grab_amount"].sum(),
            rpt_df["other_income_amount"].sum(),
        ]
        ch_df = pd.DataFrame({"ช่องทาง": ch_labels, "ยอด": ch_values})
        ch_df = ch_df[ch_df["ยอด"] > 0]
        if not ch_df.empty:
            fig3 = px.pie(ch_df, names="ช่องทาง", values="ยอด",
                          hole=0.45, color_discrete_sequence=COLORS_MAIN)
            fig3.update_traces(textfont_color="black", textinfo="percent+label")
            fig3.update_layout(height=300, showlegend=True,
                               legend=dict(font=dict(color="black")))
            st.plotly_chart(fig3, use_container_width=True)

    # ── กราฟ 4: กลุ่มสาขา (Donut) ──────────────────────────
    col3, col4 = st.columns(2)
    with col3:
        _h2("🍩 ยอดขายแยกกลุ่มสาขา")
        grp_s = rpt_df.groupby("branch_group")["total_received"].sum().reset_index()
        if not grp_s.empty:
            fig4 = px.pie(grp_s, names="branch_group", values="total_received",
                          hole=0.4, color_discrete_sequence=COLORS_MAIN)
            fig4.update_traces(textfont_color="black", textinfo="percent+label")
            fig4.update_layout(height=300, legend=dict(font=dict(color="black")))
            st.plotly_chart(fig4, use_container_width=True)

    # ── กราฟ 5: Sales vs Expense per branch ─────────────────
    with col4:
        _h2("📊 ยอดขาย vs ค่าใช้จ่าย vs กำไร")
        if not exp_df.empty:
            exp_df["total_expense"] = _num(exp_df,"total_expense")
            exp_agg = exp_df.groupby("branch_id")["total_expense"].sum().reset_index()
            br_agg  = rpt_df.groupby("branch_id")["total_received"].sum().reset_index()
            merged  = br_agg.merge(exp_agg, on="branch_id", how="left").fillna(0)
            merged["กำไร"] = merged["total_received"] - merged["total_expense"]
            merged["branch_name"] = merged["branch_id"].map(bname).fillna(merged["branch_id"])
            fig5 = go.Figure()
            fig5.add_trace(go.Bar(name="ยอดขาย", x=merged["branch_name"],
                                   y=merged["total_received"], marker_color="#FF6B35"))
            fig5.add_trace(go.Bar(name="ค่าใช้จ่าย", x=merged["branch_name"],
                                   y=merged["total_expense"], marker_color="#E53935"))
            fig5.add_trace(go.Bar(name="กำไร", x=merged["branch_name"],
                                   y=merged["กำไร"], marker_color="#2E7D32"))
            fig5.update_layout(barmode="group", plot_bgcolor="white", paper_bgcolor="white",
                                height=300, legend=dict(font=dict(color="black")),
                                font=dict(color="black"))
            st.plotly_chart(fig5, use_container_width=True)
        else:
            _no_data("ยังไม่มีข้อมูลค่าใช้จ่าย")


# ══════════════════════════════════════════════════════════════
# TAB 2 : ค่าใช้จ่าย
# ══════════════════════════════════════════════════════════════
def _tab_expenses():
    _h1("💸 วิเคราะห์ค่าใช้จ่าย", "#E53935")
    exp_df = read_sheet(SHEET_BRANCH_EXPENSES)
    rpt_df = read_sheet(SHEET_BRANCH_DAILY_REPORTS)
    bname, _ = _branches_lookup()

    if exp_df.empty:
        _no_data("ยังไม่มีข้อมูลค่าใช้จ่าย"); return

    cost_cols = ["hr_cost","marketing_cost","water_cost","electricity_cost",
                 "rent_cost","accounting_cost","transport_cost",
                 "mall_gp_cost","lineman_gp_cost","grab_gp_cost",
                 "operating_cost","cogs_cost","other_cost","total_expense"]
    for c in cost_cols:
        exp_df[c] = _num(exp_df, c)
    exp_df["branch_name"] = exp_df["branch_id"].map(bname).fillna(exp_df["branch_id"])

    # KPI
    c1,c2,c3 = st.columns(3)
    with c1: _kpi("💸 ค่าใช้จ่ายรวม", f"฿{exp_df['total_expense'].sum():,.0f}", "#E53935")
    with c2: _kpi("👥 HR รวม",         f"฿{exp_df['hr_cost'].sum():,.0f}",      "#1976D2")
    with c3: _kpi("🏠 ค่าเช่ารวม",     f"฿{exp_df['rent_cost'].sum():,.0f}",   "#7B1FA2")

    st.divider()
    col1, col2 = st.columns(2)

    # ── กราฟ สัดส่วนค่าใช้จ่าย (Donut) ─────────────────────
    with col1:
        _h2("🍩 สัดส่วนประเภทค่าใช้จ่าย")
        cost_labels = ["HR","การตลาด","น้ำ","ไฟ","เช่า","บัญชี",
                       "ขนส่ง","GP Mall","GP LineMan","GP Grab","Operating","COGS","อื่น ๆ"]
        cost_keys   = ["hr_cost","marketing_cost","water_cost","electricity_cost",
                       "rent_cost","accounting_cost","transport_cost",
                       "mall_gp_cost","lineman_gp_cost","grab_gp_cost",
                       "operating_cost","cogs_cost","other_cost"]
        cost_vals   = [exp_df[k].sum() for k in cost_keys]
        c_df = pd.DataFrame({"ประเภท":cost_labels,"มูลค่า":cost_vals})
        c_df = c_df[c_df["มูลค่า"] > 0]
        if not c_df.empty:
            fig = px.pie(c_df, names="ประเภท", values="มูลค่า",
                         hole=0.45, color_discrete_sequence=COLORS_MAIN)
            fig.update_traces(textfont_color="black", textinfo="percent+label")
            fig.update_layout(height=350, legend=dict(font=dict(color="black")))
            st.plotly_chart(fig, use_container_width=True)

    # ── กราฟ ค่าใช้จ่าย Stacked Bar ─────────────────────────
    with col2:
        _h2("📊 ค่าใช้จ่ายแยกสาขา")
        agg = exp_df.groupby("branch_name")[["hr_cost","rent_cost","electricity_cost",
                                              "cogs_cost","marketing_cost","other_cost"]].sum().reset_index()
        if not agg.empty:
            fig2 = px.bar(agg, x="branch_name",
                          y=["hr_cost","rent_cost","electricity_cost","cogs_cost","marketing_cost","other_cost"],
                          barmode="stack",
                          labels={"branch_name":"สาขา","value":"฿","variable":"ประเภท"},
                          color_discrete_sequence=COLORS_MAIN)
            fig2.update_layout(plot_bgcolor="white", paper_bgcolor="white", height=350,
                               legend=dict(font=dict(color="black")), font=dict(color="black"))
            st.plotly_chart(fig2, use_container_width=True)

    # ── กราฟ ค่าไฟ ค่าเช่า GP (Bar กลุ่ม) ─────────────────
    _h2("⚡ ค่าไฟ / ค่าเช่า / GP รายสาขา")
    agg2 = exp_df.groupby("branch_name")[["electricity_cost","rent_cost",
                                           "mall_gp_cost","lineman_gp_cost","grab_gp_cost"]].sum().reset_index()
    if not agg2.empty:
        fig3 = px.bar(agg2, x="branch_name",
                      y=["electricity_cost","rent_cost","mall_gp_cost","lineman_gp_cost","grab_gp_cost"],
                      barmode="group",
                      labels={"branch_name":"สาขา","value":"฿","variable":"รายการ"},
                      color_discrete_sequence=["#FF8F00","#7B1FA2","#1976D2","#E65100","#2E7D32"])
        fig3.update_layout(plot_bgcolor="white", paper_bgcolor="white", height=320,
                           legend=dict(font=dict(color="black")), font=dict(color="black"))
        st.plotly_chart(fig3, use_container_width=True)

    # ── กราฟ Sales vs Expense ratio ─────────────────────────
    if not rpt_df.empty:
        _h2("📊 ประสิทธิภาพสาขา (ยอดขาย vs ค่าใช้จ่าย)")
        rpt_df["total_received"] = _num(rpt_df,"total_received")
        rpt_df["branch_name"]   = rpt_df["branch_id"].map(bname).fillna(rpt_df["branch_id"])
        s_agg = rpt_df.groupby("branch_name")["total_received"].sum().reset_index()
        e_agg = exp_df.groupby("branch_name")["total_expense"].sum().reset_index()
        eff   = s_agg.merge(e_agg, on="branch_name", how="outer").fillna(0)
        eff["กำไร"]   = eff["total_received"] - eff["total_expense"]
        eff["กำไร%"]  = (eff["กำไร"] / eff["total_received"].replace(0,1) * 100).round(1)
        fig4 = px.scatter(eff, x="total_received", y="total_expense",
                          text="branch_name", size="กำไร".replace("-","").split(".")[0]
                          if False else None,
                          color="กำไร%", color_continuous_scale="RdYlGn",
                          labels={"total_received":"ยอดขาย (฿)","total_expense":"ค่าใช้จ่าย (฿)"})
        fig4.update_traces(textposition="top center", textfont_color="black")
        fig4.update_layout(height=350, plot_bgcolor="white", paper_bgcolor="white",
                           coloraxis_colorbar=dict(tickfont=dict(color="black")))
        st.plotly_chart(fig4, use_container_width=True)


# ══════════════════════════════════════════════════════════════
# TAB 3 : Stock
# ══════════════════════════════════════════════════════════════
def _tab_stock():
    _h1("📦 Stock Control", "#388E3C")
    mv_df    = read_sheet(SHEET_STOCK_MOVEMENTS)
    items_df = read_sheet(SHEET_ITEMS)
    bname, _ = _branches_lookup()

    if mv_df.empty:
        _no_data("ยังไม่มีข้อมูล Stock Movement"); return

    mv_df["qty_in"]   = _num(mv_df,"qty_in")
    mv_df["qty_out"]  = _num(mv_df,"qty_out")
    mv_df["unit_cost"]= _num(mv_df,"unit_cost")

    items_d = dict(zip(items_df["item_id"], items_df["item_name"])) if not items_df.empty else {}
    minst_d = {}
    if not items_df.empty:
        for _, r in items_df.iterrows():
            try: minst_d[r["item_id"]] = float(r.get("min_stock",0))
            except: minst_d[r["item_id"]] = 0

    bal = mv_df.groupby("item_id").agg(
        qty_in=("qty_in","sum"), qty_out=("qty_out","sum"),
        avg_cost=("unit_cost","mean")
    ).reset_index()
    bal["คงเหลือ"]   = bal["qty_in"] - bal["qty_out"]
    bal["ชื่อ"]       = bal["item_id"].map(items_d).fillna(bal["item_id"])
    bal["min_stock"]  = bal["item_id"].map(minst_d).fillna(0)
    bal["มูลค่า"]     = bal["คงเหลือ"] * bal["avg_cost"]
    bal["สถานะ"]       = bal.apply(
        lambda r: "🔴 ต่ำกว่าขั้นต่ำ" if r["คงเหลือ"] < r["min_stock"] else "🟢 ปกติ", axis=1)

    c1,c2,c3 = st.columns(3)
    with c1: _kpi("📦 Items ทั้งหมด", len(bal), "#1976D2")
    with c2: _kpi("🔴 ต่ำกว่าขั้นต่ำ", len(bal[bal["คงเหลือ"]<bal["min_stock"]]), "#E53935")
    with c3: _kpi("💰 มูลค่า Stock รวม", f"฿{bal['มูลค่า'].sum():,.0f}", "#2E7D32")

    st.divider()
    col1, col2 = st.columns(2)

    with col1:
        _h2("📊 Stock คงเหลือแยก Item")
        fig = px.bar(bal.sort_values("คงเหลือ",ascending=False),
                     x="ชื่อ", y="คงเหลือ",
                     color="สถานะ",
                     color_discrete_map={"🔴 ต่ำกว่าขั้นต่ำ":"#E53935","🟢 ปกติ":"#2E7D32"},
                     text_auto=True)
        fig.update_layout(plot_bgcolor="white", paper_bgcolor="white", height=350,
                          legend=dict(font=dict(color="black")), font=dict(color="black"))
        fig.update_traces(textfont_color="black")
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        _h2("💰 มูลค่า Stock แยก Item (฿)")
        bal_mv = bal[bal["มูลค่า"] > 0].sort_values("มูลค่า", ascending=False)
        if not bal_mv.empty:
            fig2 = px.pie(bal_mv, names="ชื่อ", values="มูลค่า",
                          hole=0.4, color_discrete_sequence=COLORS_MAIN)
            fig2.update_traces(textfont_color="black", textinfo="percent+label")
            fig2.update_layout(height=350, legend=dict(font=dict(color="black")))
            st.plotly_chart(fig2, use_container_width=True)

    # Stock by branch
    _h2("🏪 Stock คงเหลือแยกสาขา")
    br_bal = mv_df.groupby(["branch_id","item_id"]).agg(
        qty_in=("qty_in","sum"), qty_out=("qty_out","sum"),
        avg_cost=("unit_cost","mean")
    ).reset_index()
    br_bal["คงเหลือ"] = br_bal["qty_in"] - br_bal["qty_out"]
    br_bal["มูลค่า"]  = br_bal["คงเหลือ"] * br_bal["avg_cost"]
    br_bal["สาขา"]    = br_bal["branch_id"].map(bname).fillna(br_bal["branch_id"])
    br_agg = br_bal.groupby("สาขา")["มูลค่า"].sum().reset_index()
    if not br_agg.empty:
        fig3 = px.bar(br_agg.sort_values("มูลค่า",ascending=False),
                      x="สาขา", y="มูลค่า",
                      color="มูลค่า", color_continuous_scale="Blues",
                      text_auto=".2s",
                      labels={"มูลค่า":"มูลค่า Stock (฿)"})
        fig3.update_layout(
            plot_bgcolor="white",
            paper_bgcolor="white",
            height=320,
            coloraxis_showscale=False,
            font=dict(color="black", size=12),
            xaxis=dict(tickfont=dict(color="black"), title_font=dict(color="black")),
            yaxis=dict(tickfont=dict(color="black"), title_font=dict(color="black")),
        )
        fig3.update_traces(textfont_color="black")
        st.plotly_chart(fig3, use_container_width=True)


# ══════════════════════════════════════════════════════════════
# TAB 4 : Audit / ความผิดพลาด
# ══════════════════════════════════════════════════════════════
def _tab_audit():
    _h1("🔎 Audit & ความผิดพลาดสาขา", "#E53935")
    diff_df = read_sheet(SHEET_AUDIT_PACKAGING_DIFF)
    ses_df  = read_sheet(SHEET_AUDIT_SESSIONS)
    bname, _ = _branches_lookup()

    total_a  = len(ses_df) if not ses_df.empty else 0
    diff_a   = 0
    if not ses_df.empty:
        diff_a = len(ses_df[ses_df["overall_status"].astype(str)=="DIFF"])

    c1,c2,c3 = st.columns(3)
    with c1: _kpi("🔎 Audit ทั้งหมด",  total_a,            "#1976D2")
    with c2: _kpi("⚠️ มี DIFF",        diff_a,  "#E53935")
    with c3: _kpi("✅ ผ่าน",           total_a - diff_a,   "#2E7D32")

    # ── ประวัติ Audit Sessions ──────────────────────────────────
    if not ses_df.empty:
        _h2("📋 ประวัติ Audit Sessions")
        show_ses = ses_df.copy()
        if "branch_id" in show_ses.columns:
            show_ses["ชื่อสาขา"] = show_ses["branch_id"].map(bname).fillna(show_ses["branch_id"])
        display_cols = [c for c in [
            "audit_id","audit_date","audit_for_date","ชื่อสาขา",
            "overall_status","behavior_remark","auditor_id"
        ] if c in show_ses.columns or c == "ชื่อสาขา"]
        st.dataframe(
            show_ses[[c for c in display_cols if c in show_ses.columns]],
            use_container_width=True
        )

    if diff_df.empty:
        st.divider(); _no_data("ยังไม่มีข้อมูล DIFF"); return

    diff_df["diff_qty"]   = _num(diff_df,"diff_qty")
    diff_df["branch_qty"] = _num(diff_df,"branch_qty")
    diff_df["audit_qty"]  = _num(diff_df,"audit_qty")
    diff_only = diff_df[diff_df["display_status"].astype(str)=="diff"].copy()

    st.divider()
    col1, col2 = st.columns(2)

    with col1:
        _h2("📊 จำนวนครั้งที่ DIFF แยกตาม Item")
        if not diff_only.empty:
            item_cnt = diff_only.groupby("item_name").agg(
                ครั้ง=("diff_qty","count"),
                รวมส่วนต่าง=("diff_qty","sum")
            ).reset_index()
            fig = px.bar(item_cnt.sort_values("ครั้ง",ascending=False),
                         x="item_name", y="ครั้ง",
                         color="รวมส่วนต่าง", color_continuous_scale="Reds",
                         text_auto=True,
                         labels={"item_name":"รายการ","ครั้ง":"จำนวนครั้งที่ DIFF"})
            fig.update_layout(
                plot_bgcolor="white", paper_bgcolor="white", height=320,
                coloraxis_showscale=False, font=dict(color="black", size=12),
                xaxis=dict(tickfont=dict(color="black"), title_font=dict(color="black")),
                yaxis=dict(tickfont=dict(color="black"), title_font=dict(color="black")),
            )
            fig.update_traces(textfont_color="black")
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        _h2("🍩 สัดส่วน DIFF แยก Item")
        if not diff_only.empty:
            fig2 = px.pie(diff_only, names="item_name",
                          values="diff_qty".replace("diff_qty","diff_qty")
                          if False else "diff_qty",
                          hole=0.45, color_discrete_sequence=COLORS_MAIN)
            fig2.update_traces(textfont_color="black", textinfo="percent+label")
            fig2.update_layout(
                height=320, paper_bgcolor="white",
                legend=dict(font=dict(color="black")),
                font=dict(color="black"),
            )
            st.plotly_chart(fig2, use_container_width=True)

    # ── DIFF by branch ───────────────────────────────────────
    if not ses_df.empty and not diff_only.empty:
        _h2("🏪 ความผิดพลาดแยกสาขา")
        merged = diff_only.merge(
            ses_df[["audit_id","branch_id"]].drop_duplicates(),
            on="audit_id", how="left"
        )
        merged["สาขา"] = merged["branch_id"].map(bname).fillna(merged["branch_id"])
        br_diff = merged.groupby("สาขา").agg(
            ครั้งที่DIFF=("diff_qty","count"),
            รวมส่วนต่าง=("diff_qty","sum")
        ).reset_index()
        fig3 = px.bar(br_diff.sort_values("ครั้งที่DIFF",ascending=False),
                      x="สาขา", y="ครั้งที่DIFF",
                      color="รวมส่วนต่าง", color_continuous_scale="OrRd",
                      text_auto=True,
                      labels={"ครั้งที่DIFF":"จำนวนครั้ง"})
        fig3.update_layout(
            plot_bgcolor="white",
            paper_bgcolor="white",
            height=300,
            coloraxis_showscale=False,
            font=dict(color="black", size=12),
            xaxis=dict(tickfont=dict(color="black"), title_font=dict(color="black")),
            yaxis=dict(tickfont=dict(color="black"), title_font=dict(color="black")),
        )
        fig3.update_traces(textfont_color="black")
        st.plotly_chart(fig3, use_container_width=True)


# ══════════════════════════════════════════════════════════════
# TAB 5 : HR / เงินเดือน
# ══════════════════════════════════════════════════════════════
def _tab_hr():
    _h1("👥 HR & เงินเดือน", "#1976D2")
    pr_df  = read_sheet(SHEET_PAYROLL_RECORDS)
    emp_df = read_sheet(SHEET_EMPLOYEES)
    rpt_df = read_sheet(SHEET_BRANCH_DAILY_REPORTS)
    bname, _ = _branches_lookup()

    if pr_df.empty:
        _no_data("ยังไม่มีข้อมูลเงินเดือน"); return

    pay_cols = ["wage_total","diligence_allowance","position_allowance",
                "marketing_share","gross_income","net_income","social_security",
                "leave_deduction","late_deduction"]
    for c in pay_cols:
        pr_df[c] = _num(pr_df, c)

    # merge ชื่อพนักงาน
    if not emp_df.empty:
        emp_df["full_name"] = emp_df["first_name"] + " " + emp_df["last_name"]
        pr_df = pr_df.merge(emp_df[["employee_id","full_name","branch_id","position"]],
                            on="employee_id", how="left")
        pr_df["branch_name"] = pr_df["branch_id"].map(bname).fillna("")
    else:
        pr_df["full_name"] = pr_df["employee_id"]
        pr_df["branch_name"] = ""

    c1,c2,c3,c4 = st.columns(4)
    with c1: _kpi("💵 ค่าแรงรวม",    f"฿{pr_df['wage_total'].sum():,.0f}",  "#1976D2")
    with c2: _kpi("💰 รายได้รวม",    f"฿{pr_df['gross_income'].sum():,.0f}", BRAND_COLOR)
    with c3: _kpi("🏦 ประกันสังคม", f"฿{pr_df['social_security'].sum():,.0f}","#7B1FA2")
    with c4: _kpi("✅ รายได้สุทธิ", f"฿{pr_df['net_income'].sum():,.0f}",    "#2E7D32")

    st.divider()
    col1, col2 = st.columns(2)

    with col1:
        _h2("📊 รายได้รวมแยกพนักงาน")
        emp_pay = pr_df.groupby("full_name")[["wage_total","diligence_allowance",
                                               "position_allowance","net_income"]].sum().reset_index()
        fig = px.bar(emp_pay.sort_values("net_income",ascending=False),
                     x="full_name",
                     y=["wage_total","diligence_allowance","position_allowance"],
                     barmode="stack",
                     labels={"full_name":"พนักงาน","value":"฿","variable":"ประเภท"},
                     color_discrete_sequence=["#1976D2","#FF8F00","#2E7D32"])
        fig.update_layout(plot_bgcolor="white", paper_bgcolor="white", height=350,
                          legend=dict(font=dict(color="black")), font=dict(color="black"))
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        _h2("🍩 สัดส่วนรายจ่าย HR")
        hr_labels = ["ค่าแรง","เบี้ยขยัน","ค่าตำแหน่ง","ส่วนแบ่งการตลาด"]
        hr_vals   = [pr_df["wage_total"].sum(), pr_df["diligence_allowance"].sum(),
                     pr_df["position_allowance"].sum(), pr_df["marketing_share"].sum()]
        hr_df = pd.DataFrame({"ประเภท":hr_labels,"มูลค่า":hr_vals})
        hr_df = hr_df[hr_df["มูลค่า"] > 0]
        if not hr_df.empty:
            fig2 = px.pie(hr_df, names="ประเภท", values="มูลค่า",
                          hole=0.45, color_discrete_sequence=COLORS_MAIN)
            fig2.update_traces(textfont_color="black", textinfo="percent+label")
            fig2.update_layout(height=350, legend=dict(font=dict(color="black")))
            st.plotly_chart(fig2, use_container_width=True)

    # ── ค่าจ้าง vs ยอดขาย ─────────────────────────────────
    if not rpt_df.empty and "branch_name" in pr_df.columns:
        _h2("📊 ค่าจ้างแยกสาขา เทียบยอดขาย")
        rpt_df["total_received"] = _num(rpt_df,"total_received")
        rpt_df["branch_name"]   = rpt_df["branch_id"].map(bname).fillna("")
        sales_br = rpt_df.groupby("branch_name")["total_received"].sum().reset_index()
        hr_br    = pr_df.groupby("branch_name")["gross_income"].sum().reset_index()
        comp = sales_br.merge(hr_br, on="branch_name", how="outer").fillna(0)
        comp.columns = ["สาขา","ยอดขาย","ค่าจ้าง"]
        comp = comp[comp["สาขา"] != ""]
        if not comp.empty:
            fig3 = go.Figure()
            fig3.add_trace(go.Bar(name="ยอดขาย", x=comp["สาขา"],
                                   y=comp["ยอดขาย"], marker_color=BRAND_COLOR))
            fig3.add_trace(go.Bar(name="ค่าจ้าง", x=comp["สาขา"],
                                   y=comp["ค่าจ้าง"], marker_color="#1976D2"))
            fig3.update_layout(barmode="group", plot_bgcolor="white", paper_bgcolor="white", height=320,
                               legend=dict(font=dict(color="black")), font=dict(color="black"))
            st.plotly_chart(fig3, use_container_width=True)


# ══════════════════════════════════════════════════════════════
# TAB 6 : การเงิน
# ══════════════════════════════════════════════════════════════
def _tab_finance():
    _h1("🏦 การเงิน & ธนาคาร", "#7B1FA2")
    ba_df = read_sheet(SHEET_BANK_ACCOUNTS)

    if ba_df.empty:
        _no_data("ยังไม่มีข้อมูลบัญชีธนาคาร"); return

    ba_df["current_balance"] = _num(ba_df,"current_balance")
    total_cash = ba_df["current_balance"].sum()

    c1,c2 = st.columns(2)
    with c1: _kpi("💰 ยอดเงินรวมทุกบัญชี", f"฿{total_cash:,.2f}", "#7B1FA2")
    with c2: _kpi("🏦 จำนวนบัญชี",          len(ba_df), "#1976D2")

    st.divider()
    col1, col2 = st.columns(2)

    with col1:
        _h2("🍩 เงินคงเหลือแยกธนาคาร (Donut)")
        active = ba_df[ba_df["current_balance"] > 0].copy()
        active["label"] = active["bank_name"] + " " + active["account_no"].str[-4:]
        if not active.empty:
            fig = px.pie(active, names="label", values="current_balance",
                         hole=0.50, color_discrete_sequence=COLORS_MAIN)
            fig.update_traces(textfont_color="black",
                              textinfo="percent+label+value",
                              texttemplate="%{label}<br>฿%{value:,.0f}<br>%{percent}")
            fig.update_layout(height=380, legend=dict(font=dict(color="black")))
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        _h2("📊 ยอดเงินแยกธนาคาร (Bar)")
        ba_df["label"] = ba_df["bank_name"] + "\n" + ba_df["account_no"].str[-4:]
        fig2 = px.bar(ba_df.sort_values("current_balance",ascending=False),
                      x="label", y="current_balance",
                      color="current_balance", color_continuous_scale="Purples",
                      text_auto=".2s",
                      labels={"label":"ธนาคาร","current_balance":"ยอดคงเหลือ (฿)"})
        fig2.update_layout(plot_bgcolor="white", paper_bgcolor="white", height=380,
                           coloraxis_showscale=False, font=dict(color="black"))
        fig2.update_traces(textfont_color="black")
        st.plotly_chart(fig2, use_container_width=True)

    # แสดงตาราง
    _h2("📋 รายละเอียดบัญชีธนาคาร")
    st.dataframe(ba_df[["bank_name","bank_branch","account_no","account_name","current_balance"]],
                 use_container_width=True)


# ══════════════════════════════════════════════════════════════
# TAB 7 : Export PowerBI
# ══════════════════════════════════════════════════════════════
def _tab_export():
    _h1("📤 Export สำหรับ Power BI", "#7B1FA2")
    # แก้สีอักษรให้เป็นดำ
    st.markdown(
        "<div style='background:#E3F2FD;border:2px solid #1976D2;border-radius:8px;"
        "padding:14px;margin-bottom:16px;color:#000000;'>"
        "<b style='color:#1976D2;font-size:1rem;'>📌 วิธีใช้กับ Power BI</b>"
        "<ol style='color:#000000;margin-top:8px;'>"
        "<li>ดาวน์โหลดไฟล์ <code style='background:#ddd;padding:2px 6px;border-radius:3px;'>"
        "PowerBI_Export_YYYY-MM-DD.xlsx</code></li>"
        "<li>เปิด Power BI Desktop → <b>Get Data → Excel</b></li>"
        "<li>เลือกไฟล์ที่ดาวน์โหลด → เลือก Sheets ที่ต้องการ</li>"
        "<li>สร้าง Relationship ระหว่าง <code style='background:#ddd;padding:2px 6px;border-radius:3px;'>"
        "branch_id</code> ของแต่ละ View</li>"
        "<li>สร้าง Dashboard ตามต้องการ</li>"
        "</ol></div>",
        unsafe_allow_html=True,
    )

    bname, bgroup = _branches_lookup()
    bg_dict = _bg_lookup()
    views   = {}

    # view_executive_dashboard
    rpt_df = read_sheet(SHEET_BRANCH_DAILY_REPORTS)
    exp_df = read_sheet(SHEET_BRANCH_EXPENSES)
    if not rpt_df.empty:
        for c in ["total_received","cash_amount","transfer_amount",
                  "lineman_amount","grab_amount","statement_amount"]:
            rpt_df[c] = _num(rpt_df,c)
        rpt_df["branch_name"]  = rpt_df["branch_id"].map(bname).fillna("")
        rpt_df["branch_group"] = rpt_df["branch_id"].map(bgroup).map(bg_dict).fillna("")
        exp_agg = pd.DataFrame()
        if not exp_df.empty:
            exp_df["total_expense"] = _num(exp_df,"total_expense")
            exp_agg = exp_df.groupby("branch_id")["total_expense"].sum().reset_index()
        rows = []
        for _, r in rpt_df.iterrows():
            bid = r["branch_id"]
            exp_val = 0.0
            if not exp_agg.empty:
                m = exp_agg[exp_agg["branch_id"]==bid]
                if not m.empty: exp_val = float(m.iloc[0]["total_expense"])
            s = float(r["total_received"])
            rows.append({"date":r["report_date"],"branch_id":bid,
                         "branch_name":r["branch_name"],"branch_group":r["branch_group"],
                         "total_sales":s,"total_expense":exp_val,"gross_profit":s-exp_val,
                         "cash_amount":float(r["cash_amount"]),
                         "transfer_amount":float(r["transfer_amount"]),
                         "delivery_amount":float(r["lineman_amount"])+float(r["grab_amount"]),
                         "status":r.get("status","")})
        views["view_executive_dashboard"] = pd.DataFrame(rows)
    else:
        views["view_executive_dashboard"] = pd.DataFrame()

    # view_stock_control
    mv_df = read_sheet(SHEET_STOCK_MOVEMENTS)
    items_df = read_sheet(SHEET_ITEMS)
    items_d = dict(zip(items_df["item_id"],items_df["item_name"])) if not items_df.empty else {}
    if not mv_df.empty:
        mv_df["qty_in"]  = _num(mv_df,"qty_in")
        mv_df["qty_out"] = _num(mv_df,"qty_out")
        sc = mv_df.groupby(["branch_id","item_id"]).agg(
            qty_in=("qty_in","sum"),qty_out=("qty_out","sum")).reset_index()
        sc["remaining_qty"] = sc["qty_in"]-sc["qty_out"]
        sc["branch_name"]   = sc["branch_id"].map(bname).fillna(sc["branch_id"])
        sc["item_name"]     = sc["item_id"].map(items_d).fillna(sc["item_id"])
        sc["stock_date"]    = str(datetime.date.today())
        views["view_stock_control"] = sc
    else:
        views["view_stock_control"] = pd.DataFrame()

    # view_fraud_audit
    diff_df = read_sheet(SHEET_AUDIT_PACKAGING_DIFF)
    ses_df  = read_sheet(SHEET_AUDIT_SESSIONS)
    if not diff_df.empty and not ses_df.empty:
        merged = diff_df.merge(
            ses_df[["audit_id","audit_date","audit_for_date","branch_id","behavior_remark"]],
            on="audit_id", how="left")
        merged["diff_qty"]   = _num(merged,"diff_qty")
        merged["branch_name"]= merged["branch_id"].map(bname).fillna("")
        views["view_fraud_audit_dashboard"] = merged
    else:
        views["view_fraud_audit_dashboard"] = pd.DataFrame()

    # Preview
    for vname, vdf in views.items():
        with st.expander(f"📄 {vname} ({len(vdf)} rows)"):
            st.dataframe(vdf, use_container_width=True)

    st.divider()
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        for vname, vdf in views.items():
            if vdf.empty:
                pd.DataFrame({"no_data":[]}).to_excel(w,index=False,sheet_name=vname[:31])
            else:
                vdf.to_excel(w, index=False, sheet_name=vname[:31])
    st.download_button(
        "📥 ดาวน์โหลด PowerBI_Export.xlsx",
        data=buf.getvalue(),
        file_name=f"PowerBI_Export_{datetime.date.today()}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        type="primary", use_container_width=True,
    )
