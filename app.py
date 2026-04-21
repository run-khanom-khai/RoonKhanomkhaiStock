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
            branch_code TEXT NOT NULL DEFAULT '000',
            item_type TEXT NOT NULL,
            qty REAL NOT NULL,
            entry_kind TEXT NOT NULL,
            note TEXT
        )
    """)
    existing_inv = [row[1] for row in c.execute("PRAGMA table_info(inventory)").fetchall()]
    if "branch_code" not in existing_inv:
        c.execute("ALTER TABLE inventory ADD COLUMN branch_code TEXT NOT NULL DEFAULT '000'")

    c.execute("""
        CREATE TABLE IF NOT EXISTS sales (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sale_date TEXT NOT NULL,
            branch_code TEXT NOT NULL,
            actual_cash REAL NOT NULL DEFAULT 0,
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
            lineman_box10 REAL NOT NULL DEFAULT 0,
            lineman_box20 REAL NOT NULL DEFAULT 0,
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
        "lineman_box10":"REAL NOT NULL DEFAULT 0","lineman_box20":"REAL NOT NULL DEFAULT 0",
    }
    for col, typedef in new_cols.items():
        if col not in existing:
            try:
                c.execute(f"ALTER TABLE sales ADD COLUMN {col} {typedef}")
            except:
                pass
    conn.commit()
    conn.close()

init_db()

BRANCHES = [f"{i:03d}" for i in range(1, 26)]
ITEM_TYPES = [
    "กล่องใส (ขนมไข่ 10 ชิ้น)",
    "ถุงกระดาษ (ขนมไข่ 20 ชิ้น)",
    "แก้ว Thai Tea Slushy",
    "แก้ว Milky Royal Tea",
    "แก้ว Brightest Day",
    "แก้ว Honey Lime Slushy",
    "ถุงช็อปปิ้ง ROON",
]
ITEM_UNIT = {
    "กล่องใส (ขนมไข่ 10 ชิ้น)": "กล่อง",
    "ถุงกระดาษ (ขนมไข่ 20 ชิ้น)": "ถุง",
    "แก้ว Thai Tea Slushy": "แก้ว",
    "แก้ว Milky Royal Tea": "แก้ว",
    "แก้ว Brightest Day": "แก้ว",
    "แก้ว Honey Lime Slushy": "แก้ว",
    "ถุงช็อปปิ้ง ROON": "ใบ",
}
PRODUCTS = [
    ("กล่องใส 10 ชิ้น",   "box10_used",         "box10_price",        70.0, "กล่องใส (ขนมไข่ 10 ชิ้น)", "กล่อง"),
    ("ถุงกระดาษ 20 ชิ้น", "box20_used",         "box20_price",       130.0, "ถุงกระดาษ (ขนมไข่ 20 ชิ้น)", "ถุง"),
    ("Thai Tea Slushy",    "drink_thaitea_used", "drink_thaitea_price", 89.0, "แก้ว Thai Tea Slushy", "แก้ว"),
    ("Milky Royal Tea",    "drink_milky_used",   "drink_milky_price",   79.0, "แก้ว Milky Royal Tea", "แก้ว"),
    ("Brightest Day",      "drink_bright_used",  "drink_bright_price",  79.0, "แก้ว Brightest Day", "แก้ว"),
    ("Honey Lime Slushy",  "drink_honey_used",   "drink_honey_price",   89.0, "แก้ว Honey Lime Slushy", "แก้ว"),
    ("Shopping Bag",       "shopbag_used",       "shopbag_price",       15.0, "ถุงช็อปปิ้ง ROON", "ใบ"),
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
    for _, uc, pc, _, _, _ in PRODUCTS:
        if uc in row.index and pc in row.index:
            total += row[uc] * row[pc]
    return total

def get_stock_balance():
    inv_df = load_inventory()
    sales_df = load_sales()
    result = {}
    used_map = {
        "กล่องใส (ขนมไข่ 10 ชิ้น)": ("box10_used", "lineman_box10"),
        "ถุงกระดาษ (ขนมไข่ 20 ชิ้น)": ("box20_used", "lineman_box20"),
        "แก้ว Thai Tea Slushy": ("drink_thaitea_used", None),
        "แก้ว Milky Royal Tea": ("drink_milky_used", None),
        "แก้ว Brightest Day": ("drink_bright_used", None),
        "แก้ว Honey Lime Slushy": ("drink_honey_used", None),
        "ถุงช็อปปิ้ง ROON": ("shopbag_used", None),
    }
    for item in ITEM_TYPES:
        sub = inv_df[inv_df["item_type"] == item] if len(inv_df) > 0 else pd.DataFrame()
        incoming = sub[sub["entry_kind"] == "นำเข้า"]["qty"].sum() if len(sub) > 0 else 0
        waste = sub[sub["entry_kind"] == "ของเสีย"]["qty"].sum() if len(sub) > 0 else 0
        used = 0
        if len(sales_df) > 0 and item in used_map:
            col, lm_col = used_map[item]
            if col in sales_df.columns:
                used += sales_df[col].sum()
            if lm_col and lm_col in sales_df.columns:
                used += sales_df[lm_col].sum()
        result[item] = incoming - waste - used
    return result

def get_inv_duplicate(entry_date, branch_code, item_type):
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM inventory WHERE entry_date=? AND branch_code=? AND item_type=? AND entry_kind='นำเข้า'",
        (str(entry_date), branch_code, item_type)
    ).fetchall()
    conn.close()
    return rows

def get_sale_duplicate(sale_date, branch_code):
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM sales WHERE sale_date=? AND branch_code=?",
        (str(sale_date), branch_code)
    ).fetchall()
    conn.close()
    return rows

# ── PAGE CONFIG ──
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
.warn-box{background:#FEE2E2;border:1px solid #FECACA;border-radius:8px;padding:.7rem 1rem;font-size:.85rem;color:#991B1B;margin-bottom:1rem}
.diff-big{font-size:2rem;font-weight:700;color:#991B1B}
.pass-big{font-size:2rem;font-weight:700;color:#065F46}
.report-card{background:var(--background-color,#fff);border:1px solid #FDE68A;border-radius:12px;padding:1rem 1.25rem;margin-bottom:12px}
.report-branch{font-family:'Prompt',sans-serif;font-size:1.1rem;font-weight:600;color:#78350F;margin-bottom:8px}
.diff-pos{font-size:1.3rem;font-weight:700;color:#065F46}
.diff-neg{font-size:1.3rem;font-weight:700;color:#991B1B}
.diff-zero{font-size:1.3rem;font-weight:700;color:#888780}
.lm-box{background:#EFF6FF;border:1px solid #BFDBFE;border-radius:8px;padding:.5rem .8rem;font-size:.82rem;color:#1E40AF;margin-top:6px}
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
    st.markdown("**สต็อกคงเหลือ**")
    bal = get_stock_balance()
    for k,v in bal.items():
        color="#065F46" if v>50 else "#991B1B"
        unit = ITEM_UNIT.get(k,"ชิ้น")
        lbl = k[:20]+"..." if len(k)>20 else k
        st.markdown(f"<span style='font-size:.76rem'>{lbl}: <b style='color:{color}'>{v:,.0f} {unit}</b></span>",unsafe_allow_html=True)

st.markdown('<div class="main-title">🥚 โปรแกรมตรวจเช็คยอดขายกับบรรจุภัณฑ์</div>',unsafe_allow_html=True)
st.markdown('<div class="sub-title">ร้านรุนขนมไข่ไส้เนย สงขลา · 25 สาขา · บันทึกถาวรด้วย SQLite</div>',unsafe_allow_html=True)

# ══════════════════════════════════════
# DASHBOARD
# ══════════════════════════════════════
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
        st.info("ยังไม่มีข้อมูลยอดขายค่ะ")
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
            short_labels=["กล่อง10","ถุง20","ThaiTea","Milky","Bright","Honey","ถุงช็อป"]
            fig=go.Figure(go.Bar(x=short_labels,y=list(bal.values()),
                marker_color=["#F59E0B","#B45309","#0891B2","#0E7490","#06B6D4","#67E8F9","#6B7280"],
                text=[f"{v:,.0f}" for v in bal.values()],textposition="outside"))
            fig.update_layout(height=260,margin=dict(t=20,b=40,l=10,r=10),
                paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)",font=dict(family="Sarabun"))
            st.plotly_chart(fig,use_container_width=True)

# ══════════════════════════════════════
# INVENTORY
# ══════════════════════════════════════
elif menu == "📦 จัดการสต็อกบรรจุภัณฑ์":
    st.markdown('<div class="section-header">บันทึกรายการบรรจุภัณฑ์ — ทุกชนิดในหน้าเดียว</div>',unsafe_allow_html=True)
    tab1,tab2 = st.tabs(["➕ บันทึกรายการ","📋 ประวัติรายการ"])
    with tab1:
        c1,c2,c3 = st.columns(3)
        with c1: entry_kind = st.selectbox("ประเภทรายการ",["นำเข้า","ของเสีย"])
        with c2: entry_date = st.date_input("วันที่",value=date.today())
        with c3: branch_inv = st.selectbox("สาขา",BRANCHES,format_func=lambda x:f"สาขา {x}",key="inv_branch")
        st.markdown("---")
        st.markdown("**กรอกจำนวนบรรจุภัณฑ์แต่ละชนิด**")
        qty_inputs = {}
        bal = get_stock_balance()
        cols_row1 = st.columns(4)
        cols_row2 = st.columns(3)
        all_cols = list(cols_row1) + list(cols_row2)
        for i, item in enumerate(ITEM_TYPES):
            unit = ITEM_UNIT[item]
            stock_left = bal.get(item, 0)
            dup = get_inv_duplicate(entry_date, branch_inv, item)
            with all_cols[i]:
                if dup and entry_kind == "นำเข้า":
                    st.markdown(f"<span style='color:#991B1B;font-size:11px'>⚠️ ซ้ำ! มีแล้ว {dup[0][4]:,.0f} {unit}</span>",unsafe_allow_html=True)
                short_name = item.replace("(ขนมไข่ 10 ชิ้น)","10ชิ้น").replace("(ขนมไข่ 20 ชิ้น)","20ชิ้น").replace("ถุงช็อปปิ้ง ","")
                qty_inputs[item] = st.number_input(
                    f"{short_name} ({unit}) เหลือ {stock_left:,.0f}",
                    min_value=0.0,step=1.0,format="%.0f",key=f"inv_{i}",
                    value=float(dup[0][4]) if dup and entry_kind=="นำเข้า" else 0.0
                )
        note_inv = st.text_input("หมายเหตุ",key="inv_note")
        st.markdown("---")
        if st.button("💾 บันทึกรายการทั้งหมด",use_container_width=True):
            saved=0; updated=0
            conn=get_conn()
            for item,qty in qty_inputs.items():
                if qty>0:
                    dup=get_inv_duplicate(entry_date,branch_inv,item)
                    if dup and entry_kind=="นำเข้า":
                        conn.execute("UPDATE inventory SET qty=?,note=? WHERE id=?",(qty,note_inv,dup[0][0]))
                        updated+=1
                    else:
                        conn.execute("INSERT INTO inventory (entry_date,branch_code,item_type,qty,entry_kind,note) VALUES (?,?,?,?,?,?)",
                            (str(entry_date),branch_inv,item,qty,entry_kind,note_inv))
                        saved+=1
            conn.commit();conn.close()
            if saved>0: st.success(f"✅ บันทึกใหม่ {saved} รายการ")
            if updated>0: st.warning(f"🔄 อัปเดตซ้ำ {updated} รายการ")
            st.rerun()
    with tab2:
        df=load_inventory()
        if len(df)==0:
            st.info("ยังไม่มีรายการค่ะ")
        else:
            col_f1,col_f2=st.columns(2)
            with col_f1: f_branch=st.selectbox("กรองสาขา",["ทุกสาขา"]+BRANCHES,key="inv_f_branch")
            with col_f2: f_kind=st.selectbox("กรองประเภท",["ทั้งหมด","นำเข้า","ของเสีย"],key="inv_f_kind")
            show=df.copy()
            if f_branch!="ทุกสาขา": show=show[show["branch_code"]==f_branch]
            if f_kind!="ทั้งหมด": show=show[show["entry_kind"]==f_kind]
            show=show[["entry_date","branch_code","item_type","entry_kind","qty","note"]].copy()
            show.columns=["วันที่","สาขา","ชนิด","ประเภท","จำนวน","หมายเหตุ"]
            st.dataframe(show,use_container_width=True,hide_index=True)
            buf=io.BytesIO()
            with pd.ExcelWriter(buf,engine="openpyxl") as w:
                show.to_excel(w,index=False,sheet_name="สต็อก")
            st.download_button("📥 Export Excel",data=buf.getvalue(),file_name="inventory.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# ══════════════════════════════════════
# DAILY SALES
# ══════════════════════════════════════
elif menu == "💰 บันทึกยอดขายรายวัน":
    st.markdown('<div class="section-header">บันทึกยอดขายรายวัน</div>',unsafe_allow_html=True)
    tab1,tab2 = st.tabs(["➕ บันทึกรายการ","📋 ประวัติยอดขาย"])
    with tab1:
        c1,c2=st.columns(2)
        with c1: sale_date=st.date_input("วันที่",value=date.today())
        with c2: branch_code=st.selectbox("รหัสสาขา",BRANCHES,format_func=lambda x:f"สาขา {x}")

        dup_sale=get_sale_duplicate(sale_date,branch_code)
        if dup_sale:
            st.markdown(f"""<div class="warn-box">⚠️ <b>บันทึกซ้ำ!</b> สาขา {branch_code} วันที่ {sale_date}
            มีข้อมูลอยู่แล้วค่ะ — แก้ไขได้เลย แล้วกดบันทึกเพื่ออัปเดตค่ะ</div>""",unsafe_allow_html=True)
            prev=dup_sale[0]
        else:
            prev=None

        actual_cash=st.number_input("ยอดเงินที่สาขาแจ้ง (บาท)",min_value=0.0,step=10.0,format="%.2f",
            value=float(prev[3]) if prev else 0.0)

        bal=get_stock_balance()
        st.markdown("---")
        st.markdown("#### 📦 ขนมไข่ (ยอดสาขา)")
        c1,c2,c3,c4=st.columns(4)
        with c1:
            s_box10=bal.get("กล่องใส (ขนมไข่ 10 ชิ้น)",0)
            box10_used=st.number_input(f"กล่องใส 10 ชิ้น (เหลือ {s_box10:,.0f} กล่อง)",min_value=0.0,step=1.0,format="%.0f",key="u_b10",value=float(prev[4]) if prev else 0.0)
        with c2:
            box10_price=st.number_input("ราคา/กล่อง (บาท)",value=float(prev[6]) if prev else 70.0,min_value=0.0,step=5.0,key="p_b10")
        with c3:
            s_box20=bal.get("ถุงกระดาษ (ขนมไข่ 20 ชิ้น)",0)
            box20_used=st.number_input(f"ถุงกระดาษ 20 ชิ้น (เหลือ {s_box20:,.0f} ถุง)",min_value=0.0,step=1.0,format="%.0f",key="u_b20",value=float(prev[5]) if prev else 0.0)
        with c4:
            box20_price=st.number_input("ราคา/ถุง (บาท)",value=float(prev[7]) if prev else 130.0,min_value=0.0,step=5.0,key="p_b20")

        # Line Man section
        st.markdown('<div class="lm-box">🛵 <b>Line Man</b> — ตัดสต็อกเท่านั้น ไม่คำนวณยอดเงิน (ยอดเงินเข้าบริษัทโดยตรง)</div>',unsafe_allow_html=True)
        lm1,lm2,_,_=st.columns(4)
        with lm1:
            lineman_box10=st.number_input(f"Line Man กล่องใส (เหลือ {s_box10:,.0f} กล่อง)",min_value=0.0,step=1.0,format="%.0f",key="lm_b10",
                value=float(prev[19]) if prev and len(prev)>19 else 0.0)
        with lm2:
            lineman_box20=st.number_input(f"Line Man ถุงกระดาษ (เหลือ {s_box20:,.0f} ถุง)",min_value=0.0,step=1.0,format="%.0f",key="lm_b20",
                value=float(prev[20]) if prev and len(prev)>20 else 0.0)

        st.markdown("#### 🧋 เครื่องดื่ม")
        d1,d2,d3,d4=st.columns(4)
        with d1:
            s_tt=bal.get("แก้ว Thai Tea Slushy",0)
            drink_thaitea_used=st.number_input(f"Thai Tea Slushy (เหลือ {s_tt:,.0f} แก้ว)",min_value=0.0,step=1.0,format="%.0f",key="u_tt",value=float(prev[8]) if prev else 0.0)
            drink_thaitea_price=st.number_input("ราคา/แก้ว",value=float(prev[12]) if prev else 89.0,min_value=0.0,step=1.0,key="p_tt")
        with d2:
            s_mk=bal.get("แก้ว Milky Royal Tea",0)
            drink_milky_used=st.number_input(f"Milky Royal Tea (เหลือ {s_mk:,.0f} แก้ว)",min_value=0.0,step=1.0,format="%.0f",key="u_mk",value=float(prev[9]) if prev else 0.0)
            drink_milky_price=st.number_input("ราคา/แก้ว",value=float(prev[13]) if prev else 79.0,min_value=0.0,step=1.0,key="p_mk")
        with d3:
            s_br=bal.get("แก้ว Brightest Day",0)
            drink_bright_used=st.number_input(f"Brightest Day (เหลือ {s_br:,.0f} แก้ว)",min_value=0.0,step=1.0,format="%.0f",key="u_br",value=float(prev[10]) if prev else 0.0)
            drink_bright_price=st.number_input("ราคา/แก้ว",value=float(prev[14]) if prev else 79.0,min_value=0.0,step=1.0,key="p_br")
        with d4:
            s_hn=bal.get("แก้ว Honey Lime Slushy",0)
            drink_honey_used=st.number_input(f"Honey Lime (เหลือ {s_hn:,.0f} แก้ว)",min_value=0.0,step=1.0,format="%.0f",key="u_hn",value=float(prev[11]) if prev else 0.0)
            drink_honey_price=st.number_input("ราคา/แก้ว",value=float(prev[15]) if prev else 89.0,min_value=0.0,step=1.0,key="p_hn")

        st.markdown("#### 🛒 ถุงช็อปปิ้ง")
        sb1,sb2,_,_=st.columns(4)
        with sb1:
            s_sb=bal.get("ถุงช็อปปิ้ง ROON",0)
            shopbag_used=st.number_input(f"Shopping Bag (เหลือ {s_sb:,.0f} ใบ)",min_value=0.0,step=1.0,format="%.0f",key="u_sb",value=float(prev[16]) if prev else 0.0)
        with sb2:
            shopbag_price=st.number_input("ราคา/ใบ (บาท)",value=float(prev[17]) if prev else 15.0,min_value=0.0,step=1.0,key="p_sb")

        note=st.text_input("หมายเหตุ",value=str(prev[18]) if prev and prev[18] else "")
        st.markdown("---")

        expected=(box10_used*box10_price+box20_used*box20_price+
                  drink_thaitea_used*drink_thaitea_price+drink_milky_used*drink_milky_price+
                  drink_bright_used*drink_bright_price+drink_honey_used*drink_honey_price+
                  shopbag_used*shopbag_price)
        diff=actual_cash-expected
        is_diff=abs(diff)>0.01

        col_r1,col_r2,col_r3,col_r4=st.columns(4)
        with col_r1:
            st.markdown(f"<div style='font-size:13px;color:#92400E'>ยอดสาขาแจ้ง</div><div style='font-size:1.8rem;font-weight:700'>{actual_cash:,.2f} ฿</div>",unsafe_allow_html=True)
        with col_r2:
            st.markdown(f"<div style='font-size:13px;color:#92400E'>ยอดระบบคำนวณ</div><div style='font-size:1.8rem;font-weight:700'>{expected:,.2f} ฿</div>",unsafe_allow_html=True)
        with col_r3:
            diff_cls="diff-big" if is_diff else "pass-big"
            st.markdown(f"<div style='font-size:13px;color:#92400E'>ส่วนต่าง</div><div class='{diff_cls}'>{diff:+,.2f} ฿</div>",unsafe_allow_html=True)
        with col_r4:
            if is_diff:
                st.markdown("<div style='background:#FEE2E2;border-radius:8px;padding:14px;text-align:center'><span style='font-size:1.6rem;font-weight:700;color:#991B1B'>❌ DIFF</span></div>",unsafe_allow_html=True)
            else:
                st.markdown("<div style='background:#D1FAE5;border-radius:8px;padding:14px;text-align:center'><span style='font-size:1.6rem;font-weight:700;color:#065F46'>✅ PASS</span></div>",unsafe_allow_html=True)

        st.markdown("")
        if st.button("💾 บันทึกยอดขาย",use_container_width=True):
            conn=get_conn()
            vals=(actual_cash,box10_used,box20_used,box10_price,box20_price,
                  drink_thaitea_used,drink_milky_used,drink_bright_used,drink_honey_used,
                  drink_thaitea_price,drink_milky_price,drink_bright_price,drink_honey_price,
                  shopbag_used,shopbag_price,lineman_box10,lineman_box20,note)
            if dup_sale:
                conn.execute("""UPDATE sales SET actual_cash=?,box10_used=?,box20_used=?,box10_price=?,box20_price=?,
                    drink_thaitea_used=?,drink_milky_used=?,drink_bright_used=?,drink_honey_used=?,
                    drink_thaitea_price=?,drink_milky_price=?,drink_bright_price=?,drink_honey_price=?,
                    shopbag_used=?,shopbag_price=?,lineman_box10=?,lineman_box20=?,note=? WHERE id=?""",
                    vals+(dup_sale[0][0],))
                st.success(f"🔄 อัปเดตแล้ว! สาขา {branch_code} · {sale_date}")
            else:
                conn.execute("""INSERT INTO sales (sale_date,branch_code,actual_cash,
                    box10_used,box20_used,box10_price,box20_price,
                    drink_thaitea_used,drink_milky_used,drink_bright_used,drink_honey_used,
                    drink_thaitea_price,drink_milky_price,drink_bright_price,drink_honey_price,
                    shopbag_used,shopbag_price,lineman_box10,lineman_box20,note)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (str(sale_date),branch_code)+vals)
                st.success(f"✅ บันทึกแล้ว! สาขา {branch_code} · {sale_date}")
            conn.commit();conn.close()
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
            lm10=df2["lineman_box10"] if "lineman_box10" in df2.columns else 0
            lm20=df2["lineman_box20"] if "lineman_box20" in df2.columns else 0
            show=df2[["sale_date","branch_code","actual_cash","ยอดระบบ","ส่วนต่าง",
                      "box10_used","box20_used","drink_thaitea_used","drink_milky_used",
                      "drink_bright_used","drink_honey_used","shopbag_used","สถานะ"]].copy()
            show.columns=["วันที่","สาขา","ยอดสาขา","ยอดระบบ","ส่วนต่าง","กล่อง10","ถุง20","ThaiTea","Milky","Bright","Honey","ถุงช็อป","สถานะ"]
            st.dataframe(show,use_container_width=True,hide_index=True)

# ══════════════════════════════════════
# REPORT
# ══════════════════════════════════════
elif menu == "📋 รายงานตรวจสอบ":
    st.markdown('<div class="section-header">รายงานตรวจเช็คยอดขายรายวัน — แยกสาขา</div>',unsafe_allow_html=True)
    sales_df=load_sales()

    report_date=st.date_input("เลือกวันที่ต้องการดูรายงาน",value=date.today())

    if len(sales_df)==0:
        st.markdown("<div class='warn-box'>ไม่มีข้อมูลในระบบค่ะ</div>",unsafe_allow_html=True)
    else:
        df=sales_df.copy()
        df["sale_date"]=pd.to_datetime(df["sale_date"])
        df=df[df["sale_date"]==pd.Timestamp(report_date)]

        if len(df)==0:
            st.markdown(f"<div class='warn-box'>⚠️ ไม่มีข้อมูล วันที่ {report_date.strftime('%d/%m/%Y')} ค่ะ</div>",unsafe_allow_html=True)
        else:
            df["expected"]=df.apply(calc_expected,axis=1)
            df["diff_amt"]=df["actual_cash"]-df["expected"]

            st.markdown(f"**วันที่ {report_date.strftime('%d/%m/%Y')} — พบข้อมูล {len(df)} สาขา**")
            st.markdown("")

            pass_count=(abs(df["diff_amt"])<0.01).sum()
            diff_count=(abs(df["diff_amt"])>=0.01).sum()
            m1,m2,m3=st.columns(3)
            m1.metric("สาขาทั้งหมด",f"{len(df)} สาขา")
            m2.metric("✅ PASS",f"{pass_count} สาขา")
            m3.metric("❌ DIFF",f"{diff_count} สาขา")
            st.markdown("---")

            # แสดงแต่ละสาขา
            for _,row in df.sort_values("branch_code").iterrows():
                diff=row["diff_amt"]
                actual=row["actual_cash"]
                exp=row["expected"]

                if abs(diff)<0.01:
                    status_html="<span class='diff-zero'>✅ PASS — ยอดตรงกัน</span>"
                    card_border="#6EE7B7"
                elif diff>0:
                    status_html=f"<span class='diff-pos'>+DIFF {diff:,.2f} บาท</span>"
                    card_border="#6EE7B7"
                else:
                    status_html=f"<span class='diff-neg'>-DIFF {abs(diff):,.2f} บาท</span>"
                    card_border="#FECACA"

                lm10=row.get("lineman_box10",0) if "lineman_box10" in row.index else 0
                lm20=row.get("lineman_box20",0) if "lineman_box20" in row.index else 0
                lm_text=""
                if lm10>0 or lm20>0:
                    lm_text=f"<div class='lm-box'>🛵 Line Man: กล่องใส {lm10:,.0f} กล่อง | ถุงกระดาษ {lm20:,.0f} ถุง</div>"

                st.markdown(f"""
                <div style="border:1px solid {card_border};border-radius:12px;padding:1rem 1.25rem;margin-bottom:10px">
                  <div style="font-family:'Prompt',sans-serif;font-size:1.05rem;font-weight:600;color:#78350F;margin-bottom:10px">
                    สาขา {row['branch_code']}
                  </div>
                  <div style="display:flex;gap:2rem;flex-wrap:wrap;align-items:center">
                    <div>
                      <div style="font-size:11px;color:#92400E">ยอดสาขาแจ้ง</div>
                      <div style="font-size:1.2rem;font-weight:600">{actual:,.2f} ฿</div>
                    </div>
                    <div>
                      <div style="font-size:11px;color:#92400E">ยอดระบบคำนวณ</div>
                      <div style="font-size:1.2rem;font-weight:600">{exp:,.2f} ฿</div>
                    </div>
                    <div style="margin-left:auto">
                      {status_html}
                    </div>
                  </div>
                  <div style="font-size:12px;color:#888;margin-top:8px">
                    กล่อง10: {row['box10_used']:,.0f} | ถุง20: {row['box20_used']:,.0f} |
                    ThaiTea: {row['drink_thaitea_used']:,.0f} | Milky: {row['drink_milky_used']:,.0f} |
                    Bright: {row['drink_bright_used']:,.0f} | Honey: {row['drink_honey_used']:,.0f} |
                    ถุงช็อป: {row['shopbag_used']:,.0f}
                  </div>
                  {lm_text}
                </div>
                """,unsafe_allow_html=True)

            # Export
            st.markdown("---")
            export_df=df[["branch_code","actual_cash","expected","diff_amt",
                          "box10_used","box20_used","drink_thaitea_used","drink_milky_used",
                          "drink_bright_used","drink_honey_used","shopbag_used"]].copy()
            export_df.columns=["สาขา","ยอดสาขา","ยอดระบบ","ส่วนต่าง","กล่อง10","ถุง20","ThaiTea","Milky","Bright","Honey","ถุงช็อป"]
            export_df["สถานะ"]=export_df["ส่วนต่าง"].apply(lambda x:"PASS" if abs(x)<0.01 else ("+DIFF" if x>0 else "-DIFF"))
            buf=io.BytesIO()
            with pd.ExcelWriter(buf,engine="openpyxl") as w:
                export_df.to_excel(w,index=False,sheet_name=f"รายงาน_{report_date}")
            st.download_button("📥 Export รายงานวันนี้ Excel",data=buf.getvalue(),
                file_name=f"report_{report_date}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# ══════════════════════════════════════
# CHARTS
# ══════════════════════════════════════
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
            fig.update_layout(barmode="group",height=420,font=dict(family="Sarabun"),
                paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig,use_container_width=True)
        with tab2:
            bd=df.groupby("branch_code")["diff_amt"].sum().reset_index()
            colors=["#991B1B" if v<0 else "#065F46" for v in bd["diff_amt"]]
            fig2=go.Figure(go.Bar(x=bd["branch_code"],y=bd["diff_amt"],marker_color=colors,
                text=bd["diff_amt"].apply(lambda x:f"{x:+,.0f}"),textposition="outside"))
            fig2.update_layout(height=420,font=dict(family="Sarabun"),
                paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig2,use_container_width=True)
        with tab3:
            daily=df.groupby("sale_date")[["actual_cash","expected"]].sum().reset_index()
            fig3=go.Figure()
            fig3.add_trace(go.Scatter(x=daily["sale_date"],y=daily["actual_cash"],name="ยอดสาขาแจ้ง",line=dict(color="#F59E0B",width=2.5)))
            fig3.add_trace(go.Scatter(x=daily["sale_date"],y=daily["expected"],name="ยอดระบบ",line=dict(color="#B45309",width=2.5,dash="dash")))
            fig3.update_layout(height=380,font=dict(family="Sarabun"),
                paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)")
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
