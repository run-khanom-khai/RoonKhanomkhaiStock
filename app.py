import streamlit as st
import sqlite3
import pandas as pd
import io
from datetime import date, timedelta
import plotly.graph_objects as go

DB_PATH = "run_sales.db"

def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def init_db():
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS inventory (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entry_date TEXT NOT NULL,
            item_type TEXT NOT NULL,
            qty REAL NOT NULL,
            entry_kind TEXT NOT NULL,
            note TEXT
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS sales (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sale_date TEXT NOT NULL,
            branch_code TEXT NOT NULL,
            actual_cash REAL NOT NULL,
            box10_used REAL NOT NULL DEFAULT 0,
            box20_used REAL NOT NULL DEFAULT 0,
            box10_price REAL NOT NULL DEFAULT 70,
            box20_price REAL NOT NULL DEFAULT 130,
            drink_thaitea_used REAL NOT NULL DEFAULT 0,
            drink_milky_used REAL NOT NULL DEFAULT 0,
            drink_bright_used REAL NOT NULL DEFAULT 0,
            drink_honey_used REAL NOT NULL DEFAULT 0,
            drink_thaitea_price REAL NOT NULL DEFAULT 89,
            drink_milky_price REAL NOT NULL DEFAULT 79,
            drink_bright_price REAL NOT NULL DEFAULT 79,
            drink_honey_price REAL NOT NULL DEFAULT 89,
            shopbag_used REAL NOT NULL DEFAULT 0,
            shopbag_price REAL NOT NULL DEFAULT 15,
            note TEXT
        )
    """)
    existing = [row[1] for row in c.execute("PRAGMA table_info(sales)").fetchall()]
    new_cols = {
        "box10_used":"REAL NOT NULL DEFAULT 0","box20_used":"REAL NOT NULL DEFAULT 0",
        "box10_price":"REAL NOT NULL DEFAULT 70","box20_price":"REAL NOT NULL DEFAULT 130",
        "drink_thaitea_used":"REAL NOT NULL DEFAULT 0","drink_milky_used":"REAL NOT NULL DEFAULT 0",
        "drink_bright_used":"REAL NOT NULL DEFAULT 0","drink_honey_used":"REAL NOT NULL DEFAULT 0",
        "drink_thaitea_price":"REAL NOT NULL DEFAULT 89","drink_milky_price":"REAL NOT NULL DEFAULT 79",
        "drink_bright_price":"REAL NOT NULL DEFAULT 79","drink_honey_price":"REAL NOT NULL DEFAULT 89",
        "shopbag_used":"REAL NOT NULL DEFAULT 0","shopbag_price":"REAL NOT NULL DEFAULT 15",
    }
    for col, typedef in new_cols.items():
        if col not in existing:
            try:
                c.execute(f"ALTER TABLE sales ADD COLUMN {col} {typedef}")
            except Exception:
                pass
    conn.commit()
    conn.close()

init_db()

BRANCHES = [f"{i:03d}" for i in range(1, 26)]
ITEM_TYPES = ["กล่องใส (ขนมไข่ 10 ชิ้น)","ถุงกระดาษ (ขนมไข่ 20 ชิ้น)","แก้วเครื่องดื่ม","ถุงช็อปปิ้ง ROON"]

PRODUCTS = [
    ("รุนขนมไข่ 10 ชิ้น (กล่องใส)",         "box10_used",         "box10_price",         70.0),
    ("รุนขนมไข่ 20 ชิ้น (ถุงกระดาษ)",        "box20_used",         "box20_price",        130.0),
    ("Premium Thai Tea Slushy",               "drink_thaitea_used", "drink_thaitea_price",  89.0),
    ("The Milky Royal Tea",                   "drink_milky_used",   "drink_milky_price",    79.0),
    ("The Brightest Day",                     "drink_bright_used",  "drink_bright_price",   79.0),
    ("Premium Honey Lime Slushy Tea",         "drink_honey_used",   "drink_honey_price",    89.0),
    ("ROON's Shopping Bag",                   "shopbag_used",       "shopbag_price",        15.0),
]

def load_inventory():
    conn = get_conn()
    df = pd.read_sql("SELECT * FROM inventory ORDER BY entry_date DESC", conn)
    conn.close()
    return df

def load_sales():
    conn = get_conn()
    df = pd.read_sql("SELECT * FROM sales ORDER BY sale_date DESC", conn)
    conn.close()
    return df

def calc_expected(row):
    total = 0.0
    for _, uc, pc, _ in PRODUCTS:
        if uc in row.index and pc in row.index:
            total += row[uc] * row[pc]
    return total

def get_stock_balance():
    df = load_inventory()
    result = {}
    for item in ITEM_TYPES:
        sub = df[df["item_type"] == item]
        result[item] = sub[sub["entry_kind"]=="นำเข้า"]["qty"].sum() - sub[sub["entry_kind"]=="ของเสีย"]["qty"].sum()
    return result

st.set_page_config(page_title="โปรแกรมตรวจเช็คยอดขายกับบรรจุภัณฑ์", page_icon="🥚", layout="wide", initial_sidebar_state="expanded")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Sarabun:wght@300;400;600;700&family=Prompt:wght@400;600;700&display=swap');
html,body,[class*="css"]{font-family:'Sarabun',sans-serif}
.main-title{font-family:'Prompt',sans-serif;font-size:1.6rem;font-weight:700;color:#B45309;letter-spacing:-0.02em;margin-bottom:.1rem}
.sub-title{font-size:.85rem;color:#92400E;margin-bottom:1.2rem}
section[data-testid="stSidebar"]{background:linear-gradient(180deg,#FEF3C7 0%,#FFFBF0 100%);border-right:1px solid #FDE68A}
.stButton>button{background:#B45309!important;color:white!important;border:none!important;border-radius:8px!important;font-family:'Sarabun',sans-serif!important;font-weight:600!important}
.stButton>button:hover{background:#92400E!important}
.section-header{font-family:'Prompt',sans-serif;font-size:1rem;font-weight:600;color:#78350F;border-left:4px solid #F59E0B;padding-left:10px;margin:1rem 0 .8rem}
.info-box{background:#FEF3C7;border:1px solid #FCD34D;border-radius:8px;padding:.7rem 1rem;font-size:.85rem;color:#78350F;margin-bottom:1rem}
.drink-label{background:#E0F2FE;border:1px solid #BAE6FD;border-radius:6px;padding:3px 8px;font-size:.78rem;color:#0369A1;margin-bottom:4px;display:inline-block}
</style>
""", unsafe_allow_html=True)

with st.sidebar:
    st.markdown("### 🥚 รุนขนมไข่ไส้เนย")
    st.markdown("**สงขลา | 25 สาขา**")
    st.divider()
    menu = st.radio("เมนู",[
        "📊 แดชบอร์ด","📦 จัดการสต็อกบรรจุภัณฑ์",
        "💰 บันทึกยอดขายรายวัน","📋 รายงานตรวจสอบ","📈 กราฟวิเคราะห์"
    ], label_visibility="collapsed")
    st.divider()
    st.markdown("**คงเหลือปัจจุบัน**")
    for k,v in get_stock_balance().items():
        color="#065F46" if v>50 else "#991B1B"
        lbl=k[:22]+"..." if len(k)>22 else k
        st.markdown(f"<span style='font-size:.76rem'>{lbl}: <b style='color:{color}'>{v:,.0f}</b></span>",unsafe_allow_html=True)

st.markdown('<div class="main-title">🥚 โปรแกรมตรวจเช็คยอดขายกับบรรจุภัณฑ์</div>',unsafe_allow_html=True)
st.markdown('<div class="sub-title">ร้านรุนขนมไข่ไส้เนย สงขลา · 25 สาขา · บันทึกถาวรด้วย SQLite</div>',unsafe_allow_html=True)

# ── DASHBOARD ──
if menu == "📊 แดชบอร์ด":
    sales_df = load_sales()
    total_sales = sales_df["actual_cash"].sum() if len(sales_df) else 0
    total_exp = sales_df.apply(calc_expected,axis=1).sum() if len(sales_df) else 0
    diff_rows = (abs(sales_df["actual_cash"]-sales_df.apply(calc_expected,axis=1))>0.01).sum() if len(sales_df) else 0
    c1,c2,c3,c4 = st.columns(4)
    c1.metric("ยอดขายรวม (บาท)",f"{total_sales:,.0f}")
    c2.metric("ยอดระบบรวม (บาท)",f"{total_exp:,.0f}")
    c3.metric("ส่วนต่างรวม (บาท)",f"{total_sales-total_exp:+,.0f}")
    c4.metric("รายการที่มีส่วนต่าง",f"{diff_rows} รายการ")
    st.divider()
    if len(sales_df)==0:
        st.info("ยังไม่มีข้อมูลยอดขายค่ะ กรุณาบันทึกผ่านเมนู 'บันทึกยอดขายรายวัน'")
    else:
        df2=sales_df.copy()
        df2["expected"]=df2.apply(calc_expected,axis=1)
        df2["diff"]=abs(df2["actual_cash"]-df2["expected"])
        top5=df2.groupby("branch_code")["diff"].sum().sort_values(ascending=False).head(5).reset_index()
        top5.columns=["สาขา","ส่วนต่างสะสม (บาท)"]
        col1,col2=st.columns(2)
        with col1:
            st.markdown('<div class="section-header">5 สาขาที่มีส่วนต่างสูงสุด</div>',unsafe_allow_html=True)
            st.dataframe(top5,use_container_width=True,hide_index=True)
        with col2:
            st.markdown('<div class="section-header">สต็อกคงเหลือ</div>',unsafe_allow_html=True)
            bal=get_stock_balance()
            fig=go.Figure(go.Bar(x=[k[:18] for k in bal.keys()],y=list(bal.values()),
                marker_color=["#F59E0B","#B45309","#0891B2","#6B7280"],
                text=[f"{v:,.0f}" for v in bal.values()],textposition="outside"))
            fig.update_layout(height=260,margin=dict(t=20,b=60,l=10,r=10),
                paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)",
                yaxis_title="จำนวน",font=dict(family="Sarabun"),xaxis_tickangle=-30)
            st.plotly_chart(fig,use_container_width=True)

# ── INVENTORY ──
elif menu == "📦 จัดการสต็อกบรรจุภัณฑ์":
    st.markdown('<div class="section-header">บันทึกรายการบรรจุภัณฑ์</div>',unsafe_allow_html=True)
    tab1,tab2=st.tabs(["➕ บันทึกรายการ","📋 ประวัติรายการ"])
    with tab1:
        c1,c2=st.columns(2)
        with c1:
            entry_kind=st.selectbox("ประเภทรายการ",["นำเข้า","ของเสีย"])
            entry_date=st.date_input("วันที่",value=date.today())
        with c2:
            item_type=st.selectbox("ชนิดบรรจุภัณฑ์",ITEM_TYPES)
            qty=st.number_input("จำนวน (ชิ้น/แก้ว/ใบ)",min_value=0.0,step=1.0,format="%.0f")
        note=st.text_input("หมายเหตุ (ไม่บังคับ)")
        if st.button("💾 บันทึกรายการ",use_container_width=True):
            if qty<=0:
                st.error("กรุณาใส่จำนวนให้ถูกต้องค่ะ")
            else:
                conn=get_conn()
                conn.execute("INSERT INTO inventory (entry_date,item_type,qty,entry_kind,note) VALUES (?,?,?,?,?)",
                    (str(entry_date),item_type,qty,entry_kind,note))
                conn.commit();conn.close()
                st.success(f"✅ บันทึกแล้ว! {entry_kind} · {item_type} · {qty:,.0f} ชิ้น")
                st.rerun()
    with tab2:
        df=load_inventory()
        if len(df)==0:
            st.info("ยังไม่มีรายการค่ะ")
        else:
            show=df[["entry_date","item_type","entry_kind","qty","note"]].copy()
            show.columns=["วันที่","ชนิด","ประเภท","จำนวน","หมายเหตุ"]
            st.dataframe(show,use_container_width=True,hide_index=True)
            buf=io.BytesIO()
            with pd.ExcelWriter(buf,engine="openpyxl") as w:
                show.to_excel(w,index=False,sheet_name="สต็อก")
            st.download_button("📥 Export Excel",data=buf.getvalue(),file_name="inventory.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# ── DAILY SALES ──
elif menu == "💰 บันทึกยอดขายรายวัน":
    st.markdown('<div class="section-header">บันทึกยอดขายรายวัน</div>',unsafe_allow_html=True)
    tab1,tab2=st.tabs(["➕ บันทึกรายการ","📋 ประวัติยอดขาย"])
    with tab1:
        c1,c2=st.columns(2)
        with c1:
            sale_date=st.date_input("วันที่",value=date.today())
            branch_code=st.selectbox("รหัสสาขา",BRANCHES,format_func=lambda x:f"สาขา {x}")
        with c2:
            actual_cash=st.number_input("ยอดเงินที่สาขาแจ้ง (บาท)",min_value=0.0,step=10.0,format="%.2f")

        st.markdown("---")
        st.markdown("#### 📦 ขนมไข่")
        c1,c2,c3,c4=st.columns(4)
        with c1:
            box10_used=st.number_input("กล่องใส 10 ชิ้น (กล่อง)",min_value=0.0,step=1.0,format="%.0f",key="u_b10")
        with c2:
            box10_price=st.number_input("ราคา/กล่อง (บาท)",value=70.0,min_value=0.0,step=5.0,key="p_b10")
        with c3:
            box20_used=st.number_input("ถุงกระดาษ 20 ชิ้น (ถุง)",min_value=0.0,step=1.0,format="%.0f",key="u_b20")
        with c4:
            box20_price=st.number_input("ราคา/ถุง (บาท)",value=130.0,min_value=0.0,step=5.0,key="p_b20")

        st.markdown("#### 🧋 เครื่องดื่ม")
        d1,d2,d3,d4=st.columns(4)
        with d1:
            st.markdown('<div class="drink-label">Thai Tea Slushy ☕</div>',unsafe_allow_html=True)
            drink_thaitea_used=st.number_input("จำนวน (แก้ว)",min_value=0.0,step=1.0,format="%.0f",key="u_tt")
            drink_thaitea_price=st.number_input("ราคา/แก้ว",value=89.0,min_value=0.0,step=1.0,key="p_tt")
        with d2:
            st.markdown('<div class="drink-label">Milky Royal Tea 🍵</div>',unsafe_allow_html=True)
            drink_milky_used=st.number_input("จำนวน (แก้ว)",min_value=0.0,step=1.0,format="%.0f",key="u_mk")
            drink_milky_price=st.number_input("ราคา/แก้ว",value=79.0,min_value=0.0,step=1.0,key="p_mk")
        with d3:
            st.markdown('<div class="drink-label">The Brightest Day 🍋</div>',unsafe_allow_html=True)
            drink_bright_used=st.number_input("จำนวน (แก้ว)",min_value=0.0,step=1.0,format="%.0f",key="u_br")
            drink_bright_price=st.number_input("ราคา/แก้ว",value=79.0,min_value=0.0,step=1.0,key="p_br")
        with d4:
            st.markdown('<div class="drink-label">Honey Lime Slushy 🍯</div>',unsafe_allow_html=True)
            drink_honey_used=st.number_input("จำนวน (แก้ว)",min_value=0.0,step=1.0,format="%.0f",key="u_hn")
            drink_honey_price=st.number_input("ราคา/แก้ว",value=89.0,min_value=0.0,step=1.0,key="p_hn")

        st.markdown("#### 🛒 ถุงช็อปปิ้ง")
        sb1,sb2,_,_=st.columns(4)
        with sb1:
            shopbag_used=st.number_input("ROON's Shopping Bag (ใบ)",min_value=0.0,step=1.0,format="%.0f",key="u_sb")
        with sb2:
            shopbag_price=st.number_input("ราคา/ใบ (บาท)",value=15.0,min_value=0.0,step=1.0,key="p_sb")

        note=st.text_input("หมายเหตุ")
        st.markdown("---")

        expected=(box10_used*box10_price+box20_used*box20_price+
                  drink_thaitea_used*drink_thaitea_price+drink_milky_used*drink_milky_price+
                  drink_bright_used*drink_bright_price+drink_honey_used*drink_honey_price+
                  shopbag_used*shopbag_price)
        diff=actual_cash-expected
        diff_color="#991B1B" if abs(diff)>0.01 else "#065F46"
        st.markdown(f"""<div class="info-box">
        📊 ยอดระบบ: <b>{expected:,.2f} บาท</b> &nbsp;|&nbsp;
        ยอดสาขาแจ้ง: <b>{actual_cash:,.2f} บาท</b> &nbsp;|&nbsp;
        ส่วนต่าง: <b style="color:{diff_color}">{diff:+,.2f} บาท</b>
        </div>""",unsafe_allow_html=True)

        if st.button("💾 บันทึกยอดขาย",use_container_width=True):
            conn=get_conn()
            conn.execute("""INSERT INTO sales (
                sale_date,branch_code,actual_cash,
                box10_used,box20_used,box10_price,box20_price,
                drink_thaitea_used,drink_milky_used,drink_bright_used,drink_honey_used,
                drink_thaitea_price,drink_milky_price,drink_bright_price,drink_honey_price,
                shopbag_used,shopbag_price,note
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",(
                str(sale_date),branch_code,actual_cash,
                box10_used,box20_used,box10_price,box20_price,
                drink_thaitea_used,drink_milky_used,drink_bright_used,drink_honey_used,
                drink_thaitea_price,drink_milky_price,drink_bright_price,drink_honey_price,
                shopbag_used,shopbag_price,note))
            conn.commit();conn.close()
            st.success(f"✅ บันทึกแล้ว! สาขา {branch_code} · {sale_date}")
            st.rerun()

    with tab2:
        df=load_sales()
        if len(df)==0:
            st.info("ยังไม่มีรายการค่ะ")
        else:
            df2=df.copy()
            df2["ยอดระบบ"]=df2.apply(calc_expected,axis=1)
            df2["ส่วนต่าง"]=df2["actual_cash"]-df2["ยอดระบบ"]
            df2["สถานะ"]=df2["ส่วนต่าง"].apply(lambda x:"PASS" if abs(x)<0.01 else "DIFF")
            show=df2[["sale_date","branch_code","actual_cash","ยอดระบบ","ส่วนต่าง",
                      "box10_used","box20_used","drink_thaitea_used","drink_milky_used",
                      "drink_bright_used","drink_honey_used","shopbag_used","สถานะ"]].copy()
            show.columns=["วันที่","สาขา","ยอดสาขา","ยอดระบบ","ส่วนต่าง",
                          "กล่อง10","ถุง20","ThaiTea","Milky","Bright","Honey","ถุงช็อป","สถานะ"]
            st.dataframe(show,use_container_width=True,hide_index=True)

# ── REPORT ──
elif menu == "📋 รายงานตรวจสอบ":
    st.markdown('<div class="section-header">รายงานตรวจเช็คยอดขายกับบรรจุภัณฑ์</div>',unsafe_allow_html=True)
    sales_df=load_sales()
    inv_df=load_inventory()
    if len(sales_df)==0:
        st.info("ยังไม่มีข้อมูลยอดขายค่ะ")
    else:
        c1,c2,c3=st.columns(3)
        with c1: filter_branch=st.selectbox("กรองสาขา",["ทุกสาขา"]+BRANCHES)
        with c2: date_from=st.date_input("จากวันที่",value=date.today()-timedelta(days=30))
        with c3: date_to=st.date_input("ถึงวันที่",value=date.today())
        df=sales_df.copy()
        df["sale_date"]=pd.to_datetime(df["sale_date"])
        df=df[(df["sale_date"]>=pd.Timestamp(date_from))&(df["sale_date"]<=pd.Timestamp(date_to))]
        if filter_branch!="ทุกสาขา": df=df[df["branch_code"]==filter_branch]
        df["expected"]=df.apply(calc_expected,axis=1)
        df["diff_amt"]=df["actual_cash"]-df["expected"]
        df["status"]=df["diff_amt"].apply(lambda x:"PASS" if abs(x)<0.01 else "DIFF")
        if len(inv_df)>0:
            waste_total=inv_df[inv_df["entry_kind"]=="ของเสีย"]["qty"].sum()
            diff_count=(abs(df["diff_amt"])>0.01).sum()
            if diff_count>0 and waste_total>0:
                st.markdown(f"""<div class="info-box">⚠️ พบ <b>{diff_count} รายการ</b> ที่ยอดไม่ตรงกัน
                และมีบรรจุภัณฑ์เสีย <b>{waste_total:,.0f} ชิ้น</b> —
                อาจเพราะพนักงานลืมนับบรรจุภัณฑ์เสียออกจากยอดค่ะ</div>""",unsafe_allow_html=True)
        display=df[["sale_date","branch_code","actual_cash","expected","diff_amt",
                    "box10_used","box20_used","drink_thaitea_used","drink_milky_used",
                    "drink_bright_used","drink_honey_used","shopbag_used","status"]].copy()
        display.columns=["วันที่","สาขา","ยอดสาขา","ยอดระบบ","ส่วนต่าง",
                         "กล่อง10","ถุง20","ThaiTea","Milky","Bright","Honey","ถุงช็อป","สถานะ"]
        display["วันที่"]=display["วันที่"].dt.strftime("%d/%m/%Y")
        def style_row(row):
            c="#FFF5F5" if row["สถานะ"]=="DIFF" else "#F0FFF4"
            return [f"background-color:{c}"]*len(row)
        def style_status(val):
            return "color:#991B1B;font-weight:bold" if val=="DIFF" else "color:#065F46;font-weight:bold"
        styled=display.style.apply(style_row,axis=1).applymap(style_status,subset=["สถานะ"])
        st.dataframe(styled,use_container_width=True,hide_index=True)
        buf=io.BytesIO()
        with pd.ExcelWriter(buf,engine="openpyxl") as w:
            display.to_excel(w,index=False,sheet_name="รายงาน")
        st.download_button("📥 Export รายงาน Excel",data=buf.getvalue(),
            file_name=f"report_{date_from}_{date_to}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# ── CHARTS ──
elif menu == "📈 กราฟวิเคราะห์":
    st.markdown('<div class="section-header">กราฟวิเคราะห์ยอดขายและส่วนต่าง</div>',unsafe_allow_html=True)
    sales_df=load_sales()
    if len(sales_df)==0:
        st.info("ยังไม่มีข้อมูลยอดขายค่ะ")
    else:
        df=sales_df.copy()
        df["expected"]=df.apply(calc_expected,axis=1)
        df["diff_amt"]=df["actual_cash"]-df["expected"]
        df["sale_date"]=pd.to_datetime(df["sale_date"])
        tab1,tab2,tab3,tab4=st.tabs(["ยอดขายรายสาขา","ส่วนต่างรายสาขา","แนวโน้มรายวัน","สัดส่วนสินค้า"])
        with tab1:
            bs=df.groupby("branch_code")[["actual_cash","expected"]].sum().reset_index()
            fig=go.Figure()
            fig.add_trace(go.Bar(name="ยอดสาขาแจ้ง",x=bs["branch_code"],y=bs["actual_cash"],marker_color="#F59E0B"))
            fig.add_trace(go.Bar(name="ยอดระบบ",x=bs["branch_code"],y=bs["expected"],marker_color="#B45309"))
            fig.update_layout(barmode="group",height=420,xaxis_title="สาขา",yaxis_title="บาท",
                font=dict(family="Sarabun"),paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig,use_container_width=True)
        with tab2:
            bd=df.groupby("branch_code")["diff_amt"].sum().reset_index()
            colors=["#991B1B" if v<0 else "#065F46" for v in bd["diff_amt"]]
            fig2=go.Figure(go.Bar(x=bd["branch_code"],y=bd["diff_amt"],marker_color=colors,
                text=bd["diff_amt"].apply(lambda x:f"{x:+,.0f}"),textposition="outside"))
            fig2.update_layout(height=420,xaxis_title="สาขา",yaxis_title="บาท",
                font=dict(family="Sarabun"),paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig2,use_container_width=True)
        with tab3:
            daily=df.groupby("sale_date")[["actual_cash","expected"]].sum().reset_index()
            fig3=go.Figure()
            fig3.add_trace(go.Scatter(x=daily["sale_date"],y=daily["actual_cash"],name="ยอดสาขาแจ้ง",line=dict(color="#F59E0B",width=2.5)))
            fig3.add_trace(go.Scatter(x=daily["sale_date"],y=daily["expected"],name="ยอดระบบ",line=dict(color="#B45309",width=2.5,dash="dash")))
            fig3.update_layout(height=380,xaxis_title="วันที่",yaxis_title="บาท",
                font=dict(family="Sarabun"),paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig3,use_container_width=True)
        with tab4:
            pt={
                "กล่องใส 10 ชิ้น":(df["box10_used"]*df["box10_price"]).sum(),
                "ถุงกระดาษ 20 ชิ้น":(df["box20_used"]*df["box20_price"]).sum(),
                "Thai Tea Slushy":(df["drink_thaitea_used"]*df["drink_thaitea_price"]).sum(),
                "Milky Royal Tea":(df["drink_milky_used"]*df["drink_milky_price"]).sum(),
                "Brightest Day":(df["drink_bright_used"]*df["drink_bright_price"]).sum(),
                "Honey Lime Slushy":(df["drink_honey_used"]*df["drink_honey_price"]).sum(),
                "Shopping Bag":(df["shopbag_used"]*df["shopbag_price"]).sum(),
            }
            fig4=go.Figure(go.Pie(labels=list(pt.keys()),values=list(pt.values()),
                marker_colors=["#F59E0B","#B45309","#0891B2","#0E7490","#06B6D4","#67E8F9","#6B7280"],
                hole=0.4,textinfo="label+percent"))
            fig4.update_layout(height=420,font=dict(family="Sarabun"),paper_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig4,use_container_width=True)
