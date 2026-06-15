"""
dashboard.py  –  Dashboard + Export for Power BI (รอบที่ 9)
"""
import io
import datetime
import streamlit as st
import pandas as pd

from config import (
    SHEET_BRANCHES, SHEET_BRANCH_GROUPS, SHEET_ITEMS,
    SHEET_BRANCH_DAILY_REPORTS, SHEET_BRANCH_EXPENSES,
    SHEET_AUDIT_SESSIONS, SHEET_AUDIT_PACKAGING_DIFF,
    SHEET_STOCK_MOVEMENTS, SHEET_TRUE_STOCK_BALANCE,
    SHEET_AUDIT_PACKAGING_BALANCE,
    SHEET_DAILY_SALES_ACCOUNTING, SHEET_SALES_RECONCILE,
    SHEET_PAYROLL_RECORDS, SHEET_EMPLOYEES,
)
from modules.excel_db import read_sheet

# ─────────────────────────────────────────────────────────────
# STYLE HELPERS
# ─────────────────────────────────────────────────────────────
def _h1(text, color="#FF6B35"):
    st.markdown(
        f"<h1 style='color:{color};font-size:2rem;font-weight:800;"
        f"border-left:6px solid {color};padding-left:12px;margin-bottom:4px;'>{text}</h1>",
        unsafe_allow_html=True,
    )

def _h2(text, color="#1976D2"):
    st.markdown(
        f"<h2 style='color:{color};font-size:1.4rem;font-weight:700;"
        f"border-bottom:3px solid {color};padding-bottom:4px;margin-top:20px;'>{text}</h2>",
        unsafe_allow_html=True,
    )

def _h3(text, color="#388E3C"):
    st.markdown(
        f"<h3 style='color:{color};font-size:1.1rem;font-weight:600;margin-top:14px;'>{text}</h3>",
        unsafe_allow_html=True,
    )

def _kpi(label, value, sub="", color="#1976D2"):
    st.markdown(
        f"""<div style='background:linear-gradient(135deg,{color}22,{color}11);
        border:2px solid {color};border-radius:10px;padding:14px 18px;text-align:center;'>
        <div style='color:{color};font-size:0.85rem;font-weight:600;'>{label}</div>
        <div style='color:{color};font-size:1.7rem;font-weight:800;'>{value}</div>
        {'<div style="color:#888;font-size:0.75rem;">'+sub+'</div>' if sub else ''}
        </div>""",
        unsafe_allow_html=True,
    )

def _diff_banner(msg, is_diff=True):
    if is_diff:
        st.markdown(
            f"<div style='background:#FF0000;color:white;padding:12px;border-radius:8px;"
            f"font-size:18px;font-weight:bold;text-align:center;margin:6px 0;'>⚠️ {msg}</div>",
            unsafe_allow_html=True,
        )
    else:
        st.success(f"✅ {msg}")

# ─────────────────────────────────────────────────────────────
# DATA HELPERS
# ─────────────────────────────────────────────────────────────
def _num(df, col):
    if col not in df.columns:
        return pd.Series([0]*len(df))
    return pd.to_numeric(df[col], errors="coerce").fillna(0)

def _branches_lookup():
    df = read_sheet(SHEET_BRANCHES)
    if df.empty:
        return {}, {}
    bname = dict(zip(df["branch_id"], df["branch_name"]))
    bgroup = dict(zip(df["branch_id"], df.get("branch_group_id", pd.Series())))
    return bname, bgroup

def _bg_lookup():
    df = read_sheet(SHEET_BRANCH_GROUPS)
    if df.empty:
        return {}
    return dict(zip(df["branch_group_id"], df["branch_group_name"]))

# ─────────────────────────────────────────────────────────────
# MAIN RENDER
# ─────────────────────────────────────────────────────────────
def render():
    st.set_page_config  # already set in app.py — skip

    _h1("📈 ROON KHANOMKHAI — Dashboard", "#FF6B35")
    st.caption(f"ข้อมูล ณ วันที่: {datetime.date.today().strftime('%d/%m/%Y')}")

    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "🏆 Executive",
        "🏪 Branch Performance",
        "📦 Stock Control",
        "🔎 Fraud & Audit",
        "📤 Export Power BI",
    ])
    with tab1: _tab_executive()
    with tab2: _tab_branch_performance()
    with tab3: _tab_stock_control()
    with tab4: _tab_fraud_audit()
    with tab5: _tab_export()


# ══════════════════════════════════════════════════════════════
# TAB 1 : EXECUTIVE DASHBOARD
# ══════════════════════════════════════════════════════════════
def _tab_executive():
    _h2("🏆 Executive Dashboard", "#FF6B35")

    rpt_df = read_sheet(SHEET_BRANCH_DAILY_REPORTS)
    exp_df = read_sheet(SHEET_BRANCH_EXPENSES)
    bname, _ = _branches_lookup()

    # ── KPI Summary ──────────────────────────────────────────
    if not rpt_df.empty:
        rpt_df["total_received"] = _num(rpt_df, "total_received")
        total_sales   = rpt_df["total_received"].sum()
        total_records = len(rpt_df)
    else:
        total_sales, total_records = 0.0, 0

    total_expense = 0.0
    if not exp_df.empty:
        exp_df["total_expense"] = _num(exp_df, "total_expense")
        total_expense = exp_df["total_expense"].sum()

    gross_profit = total_sales - total_expense
    avg_daily    = total_sales / max(total_records, 1)

    c1,c2,c3,c4 = st.columns(4)
    with c1: _kpi("💰 ยอดขายรวม",     f"฿{total_sales:,.0f}",   f"{total_records} รายงาน", "#FF6B35")
    with c2: _kpi("💸 ค่าใช้จ่ายรวม", f"฿{total_expense:,.0f}", "",                         "#E53935")
    with c3: _kpi("📊 กำไรรวม",       f"฿{gross_profit:,.0f}",  "",                         "#2E7D32")
    with c4: _kpi("📅 เฉลี่ยต่อวัน",   f"฿{avg_daily:,.0f}",     "",                         "#1565C0")

    st.divider()

    # ── ยอดขายแยกสาขา ────────────────────────────────────────
    _h3("ยอดขายแยกสาขา")
    if not rpt_df.empty:
        branch_sales = rpt_df.groupby("branch_id")["total_received"].sum().reset_index()
        branch_sales["branch_name"] = branch_sales["branch_id"].map(bname).fillna(branch_sales["branch_id"])
        branch_sales = branch_sales.sort_values("total_received", ascending=False)

        # top branch
        if not branch_sales.empty:
            top = branch_sales.iloc[0]
            st.markdown(
                f"<div style='background:linear-gradient(135deg,#FF6B3522,#FF6B3511);"
                f"border:2px solid #FF6B35;border-radius:10px;padding:12px;text-align:center;'>"
                f"<span style='color:#FF6B35;font-size:1rem;font-weight:700;'>🥇 สาขาขายดีที่สุด: "
                f"{top['branch_name']} — ฿{top['total_received']:,.0f}</span></div>",
                unsafe_allow_html=True,
            )

        st.dataframe(
            branch_sales.rename(columns={"branch_id":"สาขา ID","branch_name":"ชื่อสาขา",
                                          "total_received":"ยอดขายรวม (฿)"}),
            use_container_width=True
        )
    else:
        st.info("ยังไม่มีข้อมูลยอดขาย")

    st.divider()

    # ── ยอดขายแยกช่องทาง ────────────────────────────────────
    _h3("ยอดขายแยกช่องทาง")
    if not rpt_df.empty:
        rpt_df["cash_amount"]    = _num(rpt_df, "cash_amount")
        rpt_df["transfer_amount"]= _num(rpt_df, "transfer_amount")
        rpt_df["lineman_amount"] = _num(rpt_df, "lineman_amount")
        rpt_df["grab_amount"]    = _num(rpt_df, "grab_amount")
        rpt_df["statement_amount"] = _num(rpt_df, "statement_amount")

        ch_data = {
            "💵 เงินสด":        rpt_df["cash_amount"].sum(),
            "📲 โอน":           rpt_df["transfer_amount"].sum(),
            "💳 Statement":     rpt_df["statement_amount"].sum(),
            "🛵 Line Man":      rpt_df["lineman_amount"].sum(),
            "🟢 Grab":          rpt_df["grab_amount"].sum(),
        }
        cols = st.columns(len(ch_data))
        colors = ["#1976D2","#7B1FA2","#0097A7","#E65100","#2E7D32"]
        for i,(label,val) in enumerate(ch_data.items()):
            with cols[i]:
                _kpi(label, f"฿{val:,.0f}", "", colors[i])
    else:
        st.info("ยังไม่มีข้อมูล")

    st.divider()

    # ── ยอดขายรายวัน ────────────────────────────────────────
    _h3("ยอดขายรายวัน (ตาราง)")
    if not rpt_df.empty:
        daily = rpt_df.groupby("report_date")["total_received"].sum().reset_index()
        daily.columns = ["วันที่","ยอดขาย (฿)"]
        daily["ยอดขาย (฿)"] = daily["ยอดขาย (฿)"].round(2)
        st.dataframe(daily.sort_values("วันที่",ascending=False), use_container_width=True)


# ══════════════════════════════════════════════════════════════
# TAB 2 : BRANCH PERFORMANCE
# ══════════════════════════════════════════════════════════════
def _tab_branch_performance():
    _h2("🏪 Branch Performance", "#1976D2")

    rpt_df = read_sheet(SHEET_BRANCH_DAILY_REPORTS)
    exp_df = read_sheet(SHEET_BRANCH_EXPENSES)
    bname, _ = _branches_lookup()

    if rpt_df.empty and exp_df.empty:
        st.info("ยังไม่มีข้อมูล")
        return

    # รวมยอดขายต่อสาขา
    sales_by_branch = {}
    if not rpt_df.empty:
        rpt_df["total_received"] = _num(rpt_df, "total_received")
        for bid, grp in rpt_df.groupby("branch_id"):
            sales_by_branch[bid] = grp["total_received"].sum()

    # รวมค่าใช้จ่ายต่อสาขา
    perf_rows = []
    if not exp_df.empty:
        cost_cols = ["hr_cost","marketing_cost","rent_cost","water_cost","electricity_cost","cogs_cost","total_expense"]
        for c in cost_cols:
            exp_df[c] = _num(exp_df, c)
        for bid, grp in exp_df.groupby("branch_id"):
            total_sales  = sales_by_branch.get(bid, 0)
            hr_cost      = grp["hr_cost"].sum()
            mkt_cost     = grp["marketing_cost"].sum()
            rent_cost    = grp["rent_cost"].sum()
            util_cost    = (grp["water_cost"] + grp["electricity_cost"]).sum()
            cogs_cost    = grp["cogs_cost"].sum()
            total_exp    = grp["total_expense"].sum()
            net_profit   = total_sales - total_exp
            def pct(cost): return round(cost/total_sales*100,1) if total_sales>0 else 0
            status = "✅ ดี" if net_profit > 0 else ("⚠️ คุ้มทุน" if net_profit == 0 else "❌ ขาดทุน")
            perf_rows.append({
                "branch_id":      bid,
                "ชื่อสาขา":       bname.get(bid, bid),
                "ยอดขาย":         total_sales,
                "HR":             hr_cost,
                "การตลาด":        mkt_cost,
                "ค่าเช่า":        rent_cost,
                "สาธารณูปโภค":   util_cost,
                "COGS":           cogs_cost,
                "ค่าใช้จ่ายรวม": total_exp,
                "กำไรสุทธิ":      net_profit,
                "เช่า%":         pct(rent_cost),
                "HR%":           pct(hr_cost),
                "COGS%":         pct(cogs_cost),
                "สถานะ":          status,
            })
    elif sales_by_branch:
        for bid, total_sales in sales_by_branch.items():
            perf_rows.append({
                "branch_id": bid, "ชื่อสาขา": bname.get(bid,bid),
                "ยอดขาย": total_sales,
                "HR":0,"การตลาด":0,"ค่าเช่า":0,"สาธารณูปโภค":0,"COGS":0,
                "ค่าใช้จ่ายรวม":0,"กำไรสุทธิ":total_sales,
                "เช่า%":0,"HR%":0,"COGS%":0,"สถานะ":"ℹ️ ยังไม่มีค่าใช้จ่าย",
            })

    if not perf_rows:
        st.info("ยังไม่มีข้อมูลเพียงพอ")
        return

    perf_df = pd.DataFrame(perf_rows).drop(columns=["branch_id"])
    st.dataframe(perf_df, use_container_width=True)

    st.divider()
    _h3("📊 เปรียบเทียบสาขา")
    if len(perf_rows) > 0:
        summary_df = pd.DataFrame(perf_rows)[["ชื่อสาขา","ยอดขาย","ค่าใช้จ่ายรวม","กำไรสุทธิ"]]
        # แสดงตาราง highlight กำไร
        def color_profit(val):
            try:
                v = float(val)
                if v > 0:   return "color: green; font-weight: bold"
                elif v < 0: return "color: red; font-weight: bold"
                return ""
            except:
                return ""
        styled = summary_df.style.map(color_profit, subset=["กำไรสุทธิ"])
        st.dataframe(styled, use_container_width=True)


# ══════════════════════════════════════════════════════════════
# TAB 3 : STOCK CONTROL
# ══════════════════════════════════════════════════════════════
def _tab_stock_control():
    _h2("📦 Stock Control", "#388E3C")

    mv_df   = read_sheet(SHEET_STOCK_MOVEMENTS)
    items_df = read_sheet(SHEET_ITEMS)
    bname, _ = _branches_lookup()

    items_dict    = dict(zip(items_df["item_id"], items_df["item_name"])) if not items_df.empty else {}
    min_stock_dict = {}
    cat_dict       = {}
    unit_dict      = {}
    if not items_df.empty:
        for _, r in items_df.iterrows():
            try: min_stock_dict[r["item_id"]] = float(r.get("min_stock",0))
            except: min_stock_dict[r["item_id"]] = 0
            cat_dict[r["item_id"]]  = r.get("item_category_id","")
            unit_dict[r["item_id"]] = r.get("unit","")

    if mv_df.empty:
        st.info("ยังไม่มีข้อมูล Stock Movement")
        return

    mv_df["qty_in"]  = _num(mv_df,"qty_in")
    mv_df["qty_out"] = _num(mv_df,"qty_out")

    all_branches = sorted(mv_df["branch_id"].dropna().unique().tolist())
    sel_branch   = st.selectbox("🏪 เลือกสาขา", ["ทั้งหมด"] + all_branches,
                                 format_func=lambda k: f"{k} – {bname.get(k,k)}" if k != "ทั้งหมด" else "ทั้งหมด")

    df_f = mv_df if sel_branch == "ทั้งหมด" else mv_df[mv_df["branch_id"]==sel_branch]
    bal  = df_f.groupby("item_id").agg(
        qty_in=("qty_in","sum"), qty_out=("qty_out","sum")
    ).reset_index()
    bal["คงเหลือ"]   = bal["qty_in"] - bal["qty_out"]
    bal["ชื่อ"]       = bal["item_id"].map(items_dict).fillna(bal["item_id"])
    bal["หมวดหมู่"]   = bal["item_id"].map(cat_dict).fillna("")
    bal["หน่วย"]      = bal["item_id"].map(unit_dict).fillna("")
    bal["min_stock"]  = bal["item_id"].map(min_stock_dict).fillna(0)
    bal["สถานะ"]       = bal.apply(
        lambda r: "🔴 ต่ำกว่าขั้นต่ำ" if r["คงเหลือ"] < r["min_stock"]
        else ("🟡 ใกล้หมด" if r["คงเหลือ"] < r["min_stock"]*1.5 else "🟢 ปกติ"), axis=1
    )

    # ── KPI ──────────────────────────────────────────────────
    low_items = bal[bal["คงเหลือ"] < bal["min_stock"]]
    c1,c2,c3 = st.columns(3)
    with c1: _kpi("📦 รายการทั้งหมด", len(bal), "",        "#1976D2")
    with c2: _kpi("🔴 ต่ำกว่าขั้นต่ำ", len(low_items), "รายการ", "#E53935")
    with c3: _kpi("🟢 ปกติ", len(bal)-len(low_items), "รายการ",  "#2E7D32")

    if not low_items.empty:
        _diff_banner(f"มี {len(low_items)} รายการที่ต่ำกว่า min_stock ต้องสั่งซื้อด่วน!", True)

    st.divider()
    _h3("📋 ตาราง Stock คงเหลือ")

    show_low = st.checkbox("🔴 แสดงเฉพาะรายการต่ำกว่าขั้นต่ำ")
    display  = low_items if show_low else bal

    # สร้าง HTML table พร้อมสี
    html = _stock_html_table(display)
    st.markdown(html, unsafe_allow_html=True)


def _stock_html_table(df):
    cols   = ["item_id","ชื่อ","หมวดหมู่","คงเหลือ","min_stock","หน่วย","สถานะ"]
    labels = ["Item ID","ชื่อ","หมวด","คงเหลือ","ขั้นต่ำ","หน่วย","สถานะ"]
    header = "<tr>" + "".join(
        f"<th style='padding:8px;background:#1e3a5f;color:white;font-size:13px;'>{l}</th>"
        for l in labels
    ) + "</tr>"
    rows = ""
    for _, r in df.iterrows():
        low    = r["คงเหลือ"] < r["min_stock"]
        bg     = "#ffe0e0" if low else ("#fff8dc" if r["คงเหลือ"] < r["min_stock"]*1.5 else "#e8f5e9")
        qty_td = (f"<td style='padding:8px;background:#FF0000;color:white;"
                  f"font-weight:bold;text-align:center;'>{r['คงเหลือ']:.0f}</td>"
                  if low else
                  f"<td style='padding:8px;color:#2E7D32;text-align:center;font-weight:bold;'>{r['คงเหลือ']:.0f}</td>")
        cells  = "".join(
            f"<td style='padding:8px;background:{bg};font-size:12px;'>{r.get(c,'')}</td>"
            for c in ["item_id","ชื่อ","หมวดหมู่"]
        )
        cells += qty_td
        cells += f"<td style='padding:8px;background:{bg};text-align:center;'>{r['min_stock']:.0f}</td>"
        cells += f"<td style='padding:8px;background:{bg};text-align:center;'>{r.get('หน่วย','')}</td>"
        cells += f"<td style='padding:8px;background:{bg};text-align:center;font-size:13px;'>{r.get('สถานะ','')}</td>"
        rows   += f"<tr>{cells}</tr>"
    return (f"<table style='border-collapse:collapse;width:100%;'>"
            f"<thead>{header}</thead><tbody>{rows}</tbody></table>")


# ══════════════════════════════════════════════════════════════
# TAB 4 : FRAUD & AUDIT DASHBOARD
# ══════════════════════════════════════════════════════════════
def _tab_fraud_audit():
    _h2("🔎 Fraud & Audit Dashboard", "#E53935")
    st.caption("ข้อมูลจากฝ่ายตรวจสอบเท่านั้น — ใช้เป็นข้อมูลจริง")

    diff_df = read_sheet(SHEET_AUDIT_PACKAGING_DIFF)
    ses_df  = read_sheet(SHEET_AUDIT_SESSIONS)
    bname, _ = _branches_lookup()

    # ── KPI ──────────────────────────────────────────────────
    total_audits = len(ses_df) if not ses_df.empty else 0
    diff_sessions = 0
    if not ses_df.empty:
        diff_sessions = len(ses_df[ses_df["overall_status"].astype(str) == "DIFF"])

    total_diff_items = 0
    if not diff_df.empty:
        total_diff_items = len(diff_df[diff_df["display_status"].astype(str) == "diff"])

    c1,c2,c3 = st.columns(3)
    with c1: _kpi("🔎 Audit ทั้งหมด",       total_audits,      "ครั้ง",   "#1976D2")
    with c2: _kpi("⚠️ Sessions ที่ DIFF",    diff_sessions,     "ครั้ง",   "#E53935")
    with c3: _kpi("❌ รายการที่ไม่ตรง",       total_diff_items,  "รายการ", "#FF6B35")

    if diff_sessions > 0:
        _diff_banner(f"พบ {diff_sessions} ครั้ง ที่ข้อมูลสาขาไม่ตรงกับ Audit!", True)
    else:
        _diff_banner("ข้อมูลทุก Session ตรงกับ Audit", False)

    st.divider()

    # ── ตาราง DIFF แยกรายการ ─────────────────────────────────
    _h3("📊 รายการที่ DIFF แยกตาม Item")
    if not diff_df.empty:
        diff_only = diff_df[diff_df["display_status"].astype(str) == "diff"].copy()
        if not diff_only.empty:
            diff_only["diff_qty"] = _num(diff_only, "diff_qty")
            # สรุปตาม item_name
            item_sum = diff_only.groupby("item_name").agg(
                จำนวนครั้งที่DIFF=("diff_qty","count"),
                รวมส่วนต่าง=("diff_qty","sum"),
            ).reset_index()
            item_sum.columns = ["รายการ","จำนวนครั้งที่ DIFF","รวมส่วนต่าง"]
            st.dataframe(item_sum, use_container_width=True)
            st.divider()

            _h3("🔴 รายการ DIFF ทั้งหมด (ต้องตรวจสอบ)")
            html = _diff_detail_html(diff_only, bname)
            st.markdown(html, unsafe_allow_html=True)
        else:
            st.success("✅ ไม่มีรายการ DIFF")
    else:
        st.info("ยังไม่มีข้อมูล Audit DIFF")

    st.divider()

    # ── พฤติกรรมพนักงาน ──────────────────────────────────────
    _h3("👁️ รายงานพฤติกรรมพนักงาน")
    if not ses_df.empty:
        behavior = ses_df[ses_df["behavior_remark"].astype(str).str.strip() != ""]
        if not behavior.empty:
            for _, r in behavior.iterrows():
                is_diff = str(r.get("overall_status","")) == "DIFF"
                color   = "#FF0000" if is_diff else "#FF8F00"
                st.markdown(
                    f"<div style='background:{color}22;border-left:4px solid {color};"
                    f"padding:10px;border-radius:4px;margin:4px 0;'>"
                    f"<b style='color:{color};'>{r.get('audit_date','')} | สาขา: {bname.get(str(r.get('branch_id','')),'?')} | {r.get('auditor_id','')}</b><br>"
                    f"<span>{r.get('behavior_remark','')}</span></div>",
                    unsafe_allow_html=True,
                )
        else:
            st.info("ไม่มีรายงานพฤติกรรมพนักงาน")


def _diff_detail_html(df, bname):
    header = "<tr>" + "".join(
        f"<th style='padding:8px;background:#B71C1C;color:white;font-size:12px;'>{h}</th>"
        for h in ["Audit ID","Report ID","รายการ","สาขากรอก","Audit กรอก","ส่วนต่าง","สถานะ"]
    ) + "</tr>"
    rows = ""
    for _, r in df.iterrows():
        try: diff = int(float(r["diff_qty"]))
        except: diff = 0
        cells  = "".join(
            f"<td style='padding:6px;background:#fff3cd;font-size:12px;'>{r.get(c,'')}</td>"
            for c in ["audit_id","branch_report_id","item_name"]
        )
        cells += "".join(
            f"<td style='padding:6px;background:#fff3cd;text-align:center;font-size:12px;'>{r.get(c,'')}</td>"
            for c in ["branch_qty","audit_qty"]
        )
        cells += (f"<td style='padding:6px;background:#FF0000;color:white;"
                  f"font-weight:bold;text-align:center;font-size:14px;'>DIFF {diff:+d}</td>")
        cells += "<td style='padding:6px;background:#FF0000;color:white;font-weight:bold;text-align:center;'>❌</td>"
        rows  += f"<tr>{cells}</tr>"
    return (f"<table style='border-collapse:collapse;width:100%;'>"
            f"<thead>{header}</thead><tbody>{rows}</tbody></table>")


# ══════════════════════════════════════════════════════════════
# TAB 5 : EXPORT POWER BI
# ══════════════════════════════════════════════════════════════
def _tab_export():
    _h2("📤 Export Views สำหรับ Power BI", "#7B1FA2")
    st.info("📌 ไฟล์ Excel ที่ Export พร้อมนำเข้า Power BI ทันที — แต่ละ Sheet คือ 1 View")

    bname, bgroup = _branches_lookup()
    bg_dict = _bg_lookup()

    # ── Build Views ──────────────────────────────────────────
    views = {}

    # 1. view_executive_dashboard
    rpt_df = read_sheet(SHEET_BRANCH_DAILY_REPORTS)
    exp_df = read_sheet(SHEET_BRANCH_EXPENSES)
    if not rpt_df.empty:
        rpt_df["total_received"]   = _num(rpt_df,"total_received")
        rpt_df["cash_amount"]      = _num(rpt_df,"cash_amount")
        rpt_df["transfer_amount"]  = _num(rpt_df,"transfer_amount")
        rpt_df["lineman_amount"]   = _num(rpt_df,"lineman_amount")
        rpt_df["grab_amount"]      = _num(rpt_df,"grab_amount")
        rpt_df["branch_name"]      = rpt_df["branch_id"].map(bname).fillna("")
        rpt_df["branch_group"]     = rpt_df["branch_id"].map(bgroup).map(bg_dict).fillna("")

        # merge expense
        exp_agg = pd.DataFrame()
        if not exp_df.empty:
            exp_df["total_expense"] = _num(exp_df,"total_expense")
            exp_agg = exp_df.groupby("branch_id")["total_expense"].sum().reset_index()

        exec_rows = []
        for _, r in rpt_df.iterrows():
            bid = r["branch_id"]
            exp_val = 0.0
            if not exp_agg.empty:
                m = exp_agg[exp_agg["branch_id"]==bid]
                if not m.empty: exp_val = float(m.iloc[0]["total_expense"])
            sales = float(r["total_received"])
            exec_rows.append({
                "date": r["report_date"], "branch_id": bid,
                "branch_name": r["branch_name"], "branch_group": r["branch_group"],
                "total_sales": sales, "total_expense": exp_val,
                "gross_profit": sales - exp_val,
                "cash_amount": float(r["cash_amount"]),
                "transfer_amount": float(r["transfer_amount"]),
                "delivery_amount": float(r["lineman_amount"]) + float(r["grab_amount"]),
                "status": r.get("status",""),
            })
        views["view_executive_dashboard"] = pd.DataFrame(exec_rows)
    else:
        views["view_executive_dashboard"] = pd.DataFrame(columns=[
            "date","branch_id","branch_name","branch_group","total_sales","total_expense",
            "gross_profit","cash_amount","transfer_amount","delivery_amount","status"
        ])

    # 2. view_branch_performance
    if not exp_df.empty:
        exp_df["hr_cost"]          = _num(exp_df,"hr_cost")
        exp_df["marketing_cost"]   = _num(exp_df,"marketing_cost")
        exp_df["rent_cost"]        = _num(exp_df,"rent_cost")
        exp_df["water_cost"]       = _num(exp_df,"water_cost")
        exp_df["electricity_cost"] = _num(exp_df,"electricity_cost")
        exp_df["cogs_cost"]        = _num(exp_df,"cogs_cost")
        exp_df["total_expense"]    = _num(exp_df,"total_expense")
        exp_df["branch_name"]      = exp_df["branch_id"].map(bname).fillna("")

        sales_agg = {}
        if not rpt_df.empty:
            rpt_df["total_received"] = _num(rpt_df,"total_received")
            for bid, grp in rpt_df.groupby("branch_id"):
                sales_agg[bid] = grp["total_received"].sum()

        bp_rows = []
        for _, r in exp_df.iterrows():
            bid   = r["branch_id"]
            sales = sales_agg.get(bid, 0)
            exp   = float(r["total_expense"])
            net   = sales - exp
            def pct(c): return round(float(r[c])/sales*100,2) if sales>0 else 0
            bp_rows.append({
                "month": r.get("month",""), "year": r.get("year",""),
                "branch_id": bid, "branch_name": r["branch_name"],
                "total_sales": sales, "hr_cost": float(r["hr_cost"]),
                "marketing_cost": float(r["marketing_cost"]),
                "rent_cost": float(r["rent_cost"]),
                "utility_cost": float(r["water_cost"]) + float(r["electricity_cost"]),
                "cogs_cost": float(r["cogs_cost"]),
                "total_expense": exp, "net_profit": net,
                "rent_percent_of_sales": pct("rent_cost"),
                "hr_percent_of_sales":   pct("hr_cost"),
                "cogs_percent_of_sales": pct("cogs_cost"),
                "performance_status": "profit" if net > 0 else ("break_even" if net == 0 else "loss"),
            })
        views["view_branch_performance"] = pd.DataFrame(bp_rows)
    else:
        views["view_branch_performance"] = pd.DataFrame(columns=[
            "month","year","branch_id","branch_name","total_sales","hr_cost","marketing_cost",
            "rent_cost","utility_cost","cogs_cost","total_expense","net_profit",
            "rent_percent_of_sales","hr_percent_of_sales","cogs_percent_of_sales","performance_status"
        ])

    # 3. view_stock_control
    mv_df    = read_sheet(SHEET_STOCK_MOVEMENTS)
    items_df = read_sheet(SHEET_ITEMS)
    items_d  = dict(zip(items_df["item_id"], items_df["item_name"])) if not items_df.empty else {}
    minst_d  = {}
    unit_d   = {}
    cat_d    = {}
    if not items_df.empty:
        for _, r in items_df.iterrows():
            try: minst_d[r["item_id"]] = float(r.get("min_stock",0))
            except: minst_d[r["item_id"]] = 0
            unit_d[r["item_id"]] = r.get("unit","")
            cat_d[r["item_id"]]  = r.get("item_category_id","")

    if not mv_df.empty:
        mv_df["qty_in"]  = _num(mv_df,"qty_in")
        mv_df["qty_out"] = _num(mv_df,"qty_out")
        sc_rows = []
        for (bid, iid), grp in mv_df.groupby(["branch_id","item_id"]):
            rem = grp["qty_in"].sum() - grp["qty_out"].sum()
            ms  = minst_d.get(iid, 0)
            sc_rows.append({
                "date": str(datetime.date.today()),
                "branch_id": bid, "branch_name": bname.get(bid,bid),
                "item_id": iid, "item_name": items_d.get(iid,iid),
                "item_category": cat_d.get(iid,""),
                "remaining_qty": rem, "min_stock": ms,
                "unit": unit_d.get(iid,""),
                "stock_status": "low" if rem < ms else ("warning" if rem < ms*1.5 else "ok"),
            })
        views["view_stock_control"] = pd.DataFrame(sc_rows)
    else:
        views["view_stock_control"] = pd.DataFrame(columns=[
            "date","branch_id","branch_name","item_id","item_name","item_category",
            "remaining_qty","min_stock","unit","stock_status"
        ])

    # 4. view_fraud_audit_dashboard
    diff_df = read_sheet(SHEET_AUDIT_PACKAGING_DIFF)
    ses_df  = read_sheet(SHEET_AUDIT_SESSIONS)
    if not diff_df.empty and not ses_df.empty:
        merged = diff_df.merge(
            ses_df[["audit_id","audit_date","audit_for_date","branch_id","behavior_remark"]],
            on="audit_id", how="left"
        )
        merged["diff_qty"]  = _num(merged,"diff_qty")
        merged["branch_qty"] = _num(merged,"branch_qty")
        merged["audit_qty"]  = _num(merged,"audit_qty")
        fraud_rows = []
        for _, r in merged.iterrows():
            diff_val = float(r["diff_qty"])
            bid = str(r.get("branch_id",""))
            fraud_rows.append({
                "audit_date":       r.get("audit_date",""),
                "audit_for_date":   r.get("audit_for_date",""),
                "branch_id":        bid,
                "branch_name":      bname.get(bid,bid),
                "item_name":        r.get("item_name",""),
                "branch_qty":       float(r["branch_qty"]),
                "audit_qty":        float(r["audit_qty"]),
                "difference_qty":   diff_val,
                "difference_value": 0.0,
                "cash_difference":  0.0,
                "status":           r.get("display_status",""),
                "behavior_remark":  r.get("behavior_remark",""),
            })
        views["view_fraud_audit_dashboard"] = pd.DataFrame(fraud_rows)
    else:
        views["view_fraud_audit_dashboard"] = pd.DataFrame(columns=[
            "audit_date","audit_for_date","branch_id","branch_name","item_name",
            "branch_qty","audit_qty","difference_qty","difference_value",
            "cash_difference","status","behavior_remark"
        ])

    # ── Preview ──────────────────────────────────────────────
    for view_name, view_df in views.items():
        with st.expander(f"📄 {view_name} ({len(view_df)} rows)", expanded=False):
            st.dataframe(view_df, use_container_width=True)

    st.divider()

    # ── Export ────────────────────────────────────────────────
    _h3("⬇️ ดาวน์โหลด Export (Power BI Ready)")
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        for view_name, view_df in views.items():
            sheet_name = view_name[:31]  # Excel max 31 chars
            view_df.to_excel(writer, index=False, sheet_name=sheet_name)
    st.download_button(
        label="📥 ดาวน์โหลด PowerBI_Export.xlsx (ทุก View)",
        data=buf.getvalue(),
        file_name=f"PowerBI_Export_{datetime.date.today()}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        type="primary",
        use_container_width=True,
    )

    st.markdown("""
    <div style='background:#1a237e11;border:2px solid #1a237e;border-radius:8px;padding:14px;margin-top:16px;'>
    <b style='color:#1a237e;font-size:1rem;'>📌 วิธีใช้กับ Power BI</b><br>
    <ol style='color:#333;margin-top:8px;'>
    <li>ดาวน์โหลดไฟล์ <code>PowerBI_Export_YYYY-MM-DD.xlsx</code></li>
    <li>เปิด Power BI Desktop → <b>Get Data → Excel</b></li>
    <li>เลือกไฟล์ที่ดาวน์โหลด → เลือก Sheets ที่ต้องการ</li>
    <li>สร้าง Relationship ระหว่าง <code>branch_id</code> ของแต่ละ View</li>
    <li>สร้าง Dashboard ตามต้องการ</li>
    </ol>
    </div>
    """, unsafe_allow_html=True)
