import streamlit as st
import pandas as pd
import io
from datetime import date
import plotly.graph_objects as go
from supabase import create_client, Client

@st.cache_resource
def get_supabase() -> Client:
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    return create_client(url, key)

def init_db():
    sb = get_supabase()
    # สร้าง table ผ่าน Supabase SQL Editor ไม่ได้ — ต้องสร้างผ่าน REST
    # ตรวจสอบว่า table มีอยู่แล้วหรือไม่
    try:
        sb.table("inventory").select("id").limit(1).execute()
    except:
        pass

BRANCH_MAP = {
    "0000":"Emporium","0004":"เดอะมอลล์ท่าพระ","0005":"เดอะมอลล์งามวงศ์วาน",
    "0006":"เดอะมอลล์บางแค","0007":"เดอะมอลล์บางกะปิ","0008":"เซ็นทรัล ลาดพร้าว",
    "0011":"เซ็นทรัล เวสเกต","0012":"ฟิวเจอร์พาร์ครังสิต","0013":"ซีคอน สแควร์ ศรีนครินทร์",
    "0014":"แฟชั่น ไอซ์แลนด์","0015":"เซ็นทรัล พระราม 9","0017":"เซ็นทรัล ปิ่นเกล้า",
    "0018":"เซ็นทรัล หาดใหญ่","0019":"เดอะมอลล์โคราช","0020":"เอเชียทีค",
    "0021":"ตลาดสดธนบุรี","0022":"ซีคอน บางแค","0023":"ตลาดท่าเตียน",
}
BRANCHES = list(BRANCH_MAP.keys())
ITEM_TYPES = [
    "ถุงกระดาษ (ขนมไข่ 10 ชิ้น)","กล่องใส (ขนมไข่ 20 ชิ้น)",
    "แก้ว ชาไทยสลัชชี่","แก้ว ชาไทยเย็น","แก้ว ชาน้ำผึ้งมะนาว",
    "แก้ว ชาน้ำผึ้งมะนาวสลัชชี่","ถุงหูหิ้ว ROON",
]
ITEM_UNIT = {
    "ถุงกระดาษ (ขนมไข่ 10 ชิ้น)":"ถุง","กล่องใส (ขนมไข่ 20 ชิ้น)":"กล่อง",
    "แก้ว ชาไทยสลัชชี่":"แก้ว","แก้ว ชาไทยเย็น":"แก้ว",
    "แก้ว ชาน้ำผึ้งมะนาว":"แก้ว","แก้ว ชาน้ำผึ้งมะนาวสลัชชี่":"แก้ว","ถุงหูหิ้ว ROON":"ใบ",
}
PRODUCTS = [
    ("ถุงกระดาษ 10ชิ้น","box10_used","box10_price",70.0,"ถุงกระดาษ (ขนมไข่ 10 ชิ้น)","ถุง",True),
    ("กล่องใส 20ชิ้น","box20_used","box20_price",130.0,"กล่องใส (ขนมไข่ 20 ชิ้น)","กล่อง",True),
    ("ชาไทยสลัชชี่","drink_thaitea_used","drink_thaitea_price",89.0,"แก้ว ชาไทยสลัชชี่","แก้ว",True),
    ("ชาไทยเย็น","drink_milky_used","drink_milky_price",79.0,"แก้ว ชาไทยเย็น","แก้ว",True),
    ("ชาน้ำผึ้งมะนาว","drink_bright_used","drink_bright_price",79.0,"แก้ว ชาน้ำผึ้งมะนาว","แก้ว",True),
    ("ชาน้ำผึ้งมะนาวสลัชชี่","drink_honey_used","drink_honey_price",89.0,"แก้ว ชาน้ำผึ้งมะนาวสลัชชี่","แก้ว",True),
    ("ถุงหูหิ้ว ROON (ขาย)","shopbag_used","shopbag_price",15.0,"ถุงหูหิ้ว ROON","ใบ",True),
    ("Topping YUZU","yuzu_used","yuzu_price",15.0,None,None,True),
    ("Line Man ถุงกระดาษ","lineman_box10",None,0,"ถุงกระดาษ (ขนมไข่ 10 ชิ้น)","ถุง",False),
    ("Line Man กล่องใส","lineman_box20",None,0,"กล่องใส (ขนมไข่ 20 ชิ้น)","กล่อง",False),
    ("Shopee ถุงกระดาษ","shopee_box10",None,0,"ถุงกระดาษ (ขนมไข่ 10 ชิ้น)","ถุง",False),
    ("Shopee กล่องใส","shopee_box20",None,0,"กล่องใส (ขนมไข่ 20 ชิ้น)","กล่อง",False),
    ("TikTok ถุงกระดาษ","tiktok_box10",None,0,"ถุงกระดาษ (ขนมไข่ 10 ชิ้น)","ถุง",False),
    ("TikTok กล่องใส","tiktok_box20",None,0,"กล่องใส (ขนมไข่ 20 ชิ้น)","กล่อง",False),
    ("Grab ถุงกระดาษ","grab_box10",None,0,"ถุงกระดาษ (ขนมไข่ 10 ชิ้น)","ถุง",False),
    ("Grab กล่องใส","grab_box20",None,0,"กล่องใส (ขนมไข่ 20 ชิ้น)","กล่อง",False),
]

def load_inventory():
    sb = get_supabase()
    res = sb.table("inventory").select("*").order("entry_date", desc=True).execute()
    return pd.DataFrame(res.data) if res.data else pd.DataFrame(columns=["id","entry_date","branch_code","item_type","qty","entry_kind","note"])

def load_sales():
    sb = get_supabase()
    try:
        res = sb.table("sales").select("*").order("sale_date", desc=True).execute()
    except Exception as e:
        st.error(f"load_sales error: {str(e)}")
        return pd.DataFrame()
    if res.data:
        df = pd.DataFrame(res.data)
        num_cols = ["actual_cash","box10_used","box20_used","box10_price","box20_price",
                    "drink_thaitea_used","drink_milky_used","drink_bright_used","drink_honey_used",
                    "drink_thaitea_price","drink_milky_price","drink_bright_price","drink_honey_price",
                    "shopbag_used","shopbag_price","lineman_box10","lineman_box20",
                    "shopee_box10","shopee_box20","tiktok_box10","tiktok_box20","grab_box10","grab_box20"]
        for c in num_cols:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
        return df
    return pd.DataFrame()

def calc_expected(row):
    total = 0.0
    for _,uc,pc,_,_,_,calc in PRODUCTS:
        if calc and pc and uc in row.index and pc in row.index:
            total += float(row[uc]) * float(row[pc])
    return total

def get_stock_balance_by_branch(branch_code):
    inv_df = load_inventory()
    sales_df = load_sales()
    box10_cols = ["box10_used","lineman_box10","shopee_box10","tiktok_box10","grab_box10"]
    box20_cols = ["box20_used","lineman_box20","shopee_box20","tiktok_box20","grab_box20"]
    used_map = {
        "ถุงกระดาษ (ขนมไข่ 10 ชิ้น)": box10_cols,
        "กล่องใส (ขนมไข่ 20 ชิ้น)": box20_cols,
        "แก้ว ชาไทยสลัชชี่": ["drink_thaitea_used"],
        "แก้ว ชาไทยเย็น": ["drink_milky_used"],
        "แก้ว ชาน้ำผึ้งมะนาว": ["drink_bright_used"],
        "แก้ว ชาน้ำผึ้งมะนาวสลัชชี่": ["drink_honey_used"],
        "ถุงหูหิ้ว ROON": ["shopbag_used","shopbag_free"],
    }
    result = {}
    inv_branch = inv_df[inv_df["branch_code"]==branch_code] if len(inv_df)>0 else pd.DataFrame()
    for item in ITEM_TYPES:
        sub = inv_branch[inv_branch["item_type"]==item] if len(inv_branch)>0 else pd.DataFrame()
        incoming = pd.to_numeric(sub[sub["entry_kind"]=="นำเข้า"]["qty"], errors="coerce").sum() if len(sub)>0 else 0
        waste = pd.to_numeric(sub[sub["entry_kind"]=="ของเสีย"]["qty"], errors="coerce").sum() if len(sub)>0 else 0
        used = 0
        if len(sales_df)>0 and item in used_map:
            sf = sales_df[sales_df["branch_code"]==branch_code]
            for col in used_map[item]:
                if col in sf.columns:
                    used += pd.to_numeric(sf[col], errors="coerce").sum()
        result[item] = incoming - waste - used
    return result

def get_stock_balance_total():
    inv_df = load_inventory()
    sales_df = load_sales()
    used_map = {
        "ถุงกระดาษ (ขนมไข่ 10 ชิ้น)": ["box10_used","lineman_box10","shopee_box10","tiktok_box10","grab_box10"],
        "กล่องใส (ขนมไข่ 20 ชิ้น)": ["box20_used","lineman_box20","shopee_box20","tiktok_box20","grab_box20"],
        "แก้ว ชาไทยสลัชชี่": ["drink_thaitea_used"],
        "แก้ว ชาไทยเย็น": ["drink_milky_used"],
        "แก้ว ชาน้ำผึ้งมะนาว": ["drink_bright_used"],
        "แก้ว ชาน้ำผึ้งมะนาวสลัชชี่": ["drink_honey_used"],
        "ถุงหูหิ้ว ROON": ["shopbag_used","shopbag_free"],
    }
    result = {}
    for item in ITEM_TYPES:
        sub = inv_df[inv_df["item_type"]==item] if len(inv_df)>0 else pd.DataFrame()
        incoming = pd.to_numeric(sub[sub["entry_kind"]=="นำเข้า"]["qty"], errors="coerce").sum() if len(sub)>0 else 0
        waste = pd.to_numeric(sub[sub["entry_kind"]=="ของเสีย"]["qty"], errors="coerce").sum() if len(sub)>0 else 0
        used = 0
        if len(sales_df)>0 and item in used_map:
            for col in used_map[item]:
                if col in sales_df.columns:
                    used += pd.to_numeric(sales_df[col], errors="coerce").sum()
        result[item] = incoming - waste - used
    return result

def get_inv_duplicate(entry_date, branch_code, item_type):
    sb = get_supabase()
    res = sb.table("inventory").select("*")\
        .eq("entry_date", str(entry_date))\
        .eq("branch_code", branch_code)\
        .eq("item_type", item_type)\
        .eq("entry_kind", "นำเข้า").execute()
    return res.data

def get_sale_duplicate(sale_date, branch_code):
    sb = get_supabase()
    res = sb.table("sales").select("*")\
        .eq("sale_date", str(sale_date))\
        .eq("branch_code", branch_code).execute()
    return res.data

def safe_val(row, key, default=0.0):
    try:
        v = row.get(key, default) if isinstance(row, dict) else default
        return float(v) if v is not None else default
    except:
        return default

# ── PAGE CONFIG ──
st.set_page_config(page_title="โปรแกรมตรวจเช็คยอดขายกับบรรจุภัณฑ์", page_icon="🥚", layout="wide", initial_sidebar_state="expanded")
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Sarabun:wght@300;400;600;700&family=Prompt:wght@400;600;700&display=swap');
html,body,[class*="css"]{font-family:'Sarabun',sans-serif}
.main-title{font-family:'Prompt',sans-serif;font-size:1.6rem;font-weight:700;color:#B45309;margin-bottom:.1rem}
.sub-title{font-size:.85rem;color:#92400E;margin-bottom:1.2rem}
section[data-testid="stSidebar"]{background:linear-gradient(180deg,#FEF3C7 0%,#FFFBF0 100%);border-right:1px solid #FDE68A}
.stButton>button{background:#B45309!important;color:white!important;border:none!important;border-radius:8px!important;font-family:'Sarabun',sans-serif!important;font-weight:600!important}
.stButton>button:hover{background:#92400E!important}
.section-header{font-family:'Prompt',sans-serif;font-size:1rem;font-weight:600;color:#78350F;border-left:4px solid #F59E0B;padding-left:10px;margin:1rem 0 .8rem}
.diff-big{font-size:2rem;font-weight:700;color:#991B1B}
.pass-big{font-size:2rem;font-weight:700;color:#065F46}
</style>
""", unsafe_allow_html=True)

# ── SIDEBAR ──
with st.sidebar:
    st.markdown("### 🥚 รุนขนมไข่ไส้เนย")
    st.markdown("**สงขลา | 18 สาขา**")
    st.markdown("<span style='font-size:.78rem;color:#92400E'>โดย ดร.อภิวรรณ์ ดำแสงสวัสดิ์</span>", unsafe_allow_html=True)
    st.divider()
    menu = st.radio("เมนู",[
        "📊 แดชบอร์ด","📦 จัดการสต็อกบรรจุภัณฑ์",
        "💰 บันทึกยอดขายรายวัน","📋 รายงานตรวจสอบ","📈 กราฟวิเคราะห์"
    ], label_visibility="collapsed")
    st.divider()
    st.markdown("**สต็อกรวมคงเหลือ**")
    try:
        bal = get_stock_balance_total()
        for k,v in bal.items():
            color = "#065F46" if v>50 else ("#FF6B00" if v>0 else "#991B1B")
            unit = ITEM_UNIT.get(k,"ชิ้น")
            lbl = k[:22]+"..." if len(k)>22 else k
            st.markdown(f"<span style='font-size:.76rem'>{lbl}: <b style='color:{color}'>{v:,.0f} {unit}</b></span>",unsafe_allow_html=True)
    except Exception as e:
        st.caption("ไม่สามารถโหลดสต็อกได้")
    st.divider()
    st.markdown("**🗑️ ล้างข้อมูล**")
    with st.expander("ล้างข้อมูลทั้งหมด"):
        st.warning("⚠️ การลบข้อมูลไม่สามารถกู้คืนได้!")
        del_choice = st.radio("เลือกประเภทข้อมูลที่ต้องการลบ",[
            "ยอดขายทั้งหมด","สต็อกบรรจุภัณฑ์ทั้งหมด","ข้อมูลทั้งหมด (ยอดขาย + สต็อก)",
        ], key="del_choice")
        confirm_text = st.text_input("พิมพ์ 'ยืนยันลบ' เพื่อยืนยัน", key="del_confirm")
        if st.button("🗑️ ลบข้อมูล", key="del_btn"):
            if confirm_text == "ยืนยันลบ":
                sb = get_supabase()
                if del_choice == "ยอดขายทั้งหมด":
                    sb.table("sales").delete().neq("id",0).execute()
                elif del_choice == "สต็อกบรรจุภัณฑ์ทั้งหมด":
                    sb.table("inventory").delete().neq("id",0).execute()
                else:
                    sb.table("sales").delete().neq("id",0).execute()
                    sb.table("inventory").delete().neq("id",0).execute()
                st.success("✅ ลบข้อมูลแล้วค่ะ")
                st.rerun()
            else:
                st.error("กรุณาพิมพ์ 'ยืนยันลบ' ให้ถูกต้องค่ะ")

st.markdown('<div class="main-title">🥚 โปรแกรมตรวจเช็คยอดขายกับบรรจุภัณฑ์</div>',unsafe_allow_html=True)
st.markdown('<div class="sub-title">ร้านรุนขนมไข่ไส้เนย · 18 สาขา · บันทึกถาวรด้วย Supabase</div>',unsafe_allow_html=True)

# ══ DASHBOARD ══
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
        top5["ชื่อสาขา"]=top5["branch_code"].map(BRANCH_MAP)
        top5.columns=["รหัส","ส่วนต่างสะสม","ชื่อสาขา"]
        col1,col2=st.columns(2)
        with col1:
            st.markdown('<div class="section-header">5 สาขาที่มีส่วนต่างสูงสุด</div>',unsafe_allow_html=True)
            st.dataframe(top5[["รหัส","ชื่อสาขา","ส่วนต่างสะสม"]],use_container_width=True,hide_index=True)
        with col2:
            st.markdown('<div class="section-header">สต็อกรวมคงเหลือ</div>',unsafe_allow_html=True)
            bal=get_stock_balance_total()
            short_labels=["ถุง10","กล่อง20","ชาไทยสลัช","ชาไทยเย็น","ชาน้ำผึ้ง","ชาน้ำผึ้งสลัช","ถุงหูหิ้ว"]
            fig=go.Figure(go.Bar(x=short_labels,y=list(bal.values()),
                marker_color=["#F59E0B","#B45309","#0891B2","#0E7490","#06B6D4","#67E8F9","#6B7280"],
                text=[f"{v:,.0f}" for v in bal.values()],textposition="outside"))
            fig.update_layout(height=260,margin=dict(t=20,b=40,l=10,r=10),
                paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)",font=dict(family="Sarabun"))
            st.plotly_chart(fig,use_container_width=True)

# ══ INVENTORY ══
elif menu == "📦 จัดการสต็อกบรรจุภัณฑ์":
    st.markdown('<div class="section-header">บันทึกรายการบรรจุภัณฑ์ — ทุกชนิดในหน้าเดียว</div>',unsafe_allow_html=True)
    tab1,tab2 = st.tabs(["➕ บันทึกรายการ","📋 ประวัติรายการ"])
    with tab1:
        c1,c2,c3 = st.columns(3)
        with c1: entry_kind = st.selectbox("ประเภทรายการ",["นำเข้า","ของเสีย"])
        with c2: entry_date = st.date_input("วันที่",value=date.today())
        with c3:
            branch_inv_code = st.selectbox("สาขา",BRANCHES,
                format_func=lambda x:f"{x} — {BRANCH_MAP.get(x,'')}",key="inv_branch")
        st.markdown("---")
        st.markdown("**กรอกจำนวนบรรจุภัณฑ์แต่ละชนิด**")
        bal = get_stock_balance_by_branch(branch_inv_code)
        qty_inputs = {}
        cols_row1 = st.columns(4)
        cols_row2 = st.columns(3)
        all_cols = list(cols_row1)+list(cols_row2)
        for i,item in enumerate(ITEM_TYPES):
            unit = ITEM_UNIT[item]
            stock_left = bal.get(item,0)
            dup = get_inv_duplicate(entry_date,branch_inv_code,item)
            with all_cols[i]:
                if dup and entry_kind=="นำเข้า":
                    try:
                        prev_qty = float(dup[0].get("qty",0))
                        st.warning("ซ้ำ! มีแล้ว " + str(int(prev_qty)) + " " + unit)
                    except:
                        st.warning("ซ้ำ! มีข้อมูลอยู่แล้วค่ะ")
                short = item.replace("(ขนมไข่ 10 ชิ้น)","10ชิ้น").replace("(ขนมไข่ 20 ชิ้น)","20ชิ้น")
                try:
                    default_val = float(dup[0].get("qty",0)) if dup and entry_kind=="นำเข้า" else 0.0
                except:
                    default_val = 0.0
                qty_inputs[item] = st.number_input(
                    f"{short} ({unit}) เหลือ {stock_left:,.0f}",
                    min_value=0.0,step=1.0,format="%.0f",key=f"inv_{i}",value=default_val)
        note_inv = st.text_input("หมายเหตุ",key="inv_note")
        st.markdown("---")
        if st.button("💾 บันทึกรายการทั้งหมด",use_container_width=True):
            saved=0;updated=0
            sb=get_supabase()
            for item,qty in qty_inputs.items():
                if qty>0:
                    dup=get_inv_duplicate(entry_date,branch_inv_code,item)
                    if dup and entry_kind=="นำเข้า":
                        sb.table("inventory").update({"qty":qty,"note":note_inv}).eq("id",dup[0]["id"]).execute()
                        updated+=1
                    else:
                        sb.table("inventory").insert({"entry_date":str(entry_date),"branch_code":branch_inv_code,
                            "item_type":item,"qty":qty,"entry_kind":entry_kind,"note":note_inv}).execute()
                        saved+=1
            if saved>0: st.success(f"✅ บันทึกใหม่ {saved} รายการ")
            if updated>0: st.warning(f"🔄 อัปเดตซ้ำ {updated} รายการ")
            st.rerun()
    with tab2:
        df=load_inventory()
        if len(df)==0:
            st.info("ยังไม่มีรายการค่ะ")
        else:
            f1,f2=st.columns(2)
            with f1: fb=st.selectbox("กรองสาขา",["ทุกสาขา"]+BRANCHES,
                format_func=lambda x:x if x=="ทุกสาขา" else f"{x} {BRANCH_MAP.get(x,'')}",key="inv_f")
            with f2: fk=st.selectbox("ประเภท",["ทั้งหมด","นำเข้า","ของเสีย"],key="inv_fk")
            show=df.copy()
            if fb!="ทุกสาขา": show=show[show["branch_code"]==fb]
            if fk!="ทั้งหมด": show=show[show["entry_kind"]==fk]
            show=show[["entry_date","branch_code","item_type","entry_kind","qty","note"]].copy()
            show.columns=["วันที่","สาขา","ชนิด","ประเภท","จำนวน","หมายเหตุ"]
            st.dataframe(show,use_container_width=True,hide_index=True)
            buf=io.BytesIO()
            with pd.ExcelWriter(buf,engine="openpyxl") as w:
                show.to_excel(w,index=False,sheet_name="สต็อก")
            st.download_button("📥 Export Excel",data=buf.getvalue(),file_name="inventory.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# ══ DAILY SALES ══
elif menu == "💰 บันทึกยอดขายรายวัน":
    st.markdown('<div class="section-header">บันทึกยอดขายรายวัน</div>',unsafe_allow_html=True)
    tab1,tab2 = st.tabs(["➕ บันทึกรายการ","📋 ประวัติยอดขาย"])
    with tab1:
        c1,c2=st.columns(2)
        with c1: sale_date=st.date_input("วันที่",value=date.today())
        with c2: branch_code=st.selectbox("รหัสสาขา",BRANCHES,
            format_func=lambda x:f"{x} — {BRANCH_MAP.get(x,'')}",key="sales_branch")
        st.markdown(f"**สาขา: {branch_code} — {BRANCH_MAP.get(branch_code,'')}**")
        dup_sale=get_sale_duplicate(sale_date,branch_code)
        if dup_sale:
            st.warning(f"⚠️ บันทึกซ้ำ! สาขา {branch_code} วันที่ {sale_date} มีข้อมูลอยู่แล้วค่ะ — แก้ไขได้เลย")
            prev=dup_sale[0]
        else:
            prev=None
        bal=get_stock_balance_by_branch(branch_code)
        actual_cash=st.number_input("ยอดเงินที่สาขาแจ้ง (บาท)",min_value=0.0,step=10.0,format="%.2f",
            value=safe_val(prev,"actual_cash") if prev else 0.0)
        st.markdown("---")
        st.markdown("#### 📦 ขนมไข่ (คำนวณเงิน)")
        c1,c2,c3,c4=st.columns(4)
        s10=bal.get("ถุงกระดาษ (ขนมไข่ 10 ชิ้น)",0)
        s20=bal.get("กล่องใส (ขนมไข่ 20 ชิ้น)",0)
        with c1:
            box10_used=st.number_input(f"ถุงกระดาษ 10ชิ้น (เหลือ {s10:,.0f} ถุง)",min_value=0.0,step=1.0,format="%.0f",key="u_b10",value=safe_val(prev,"box10_used") if prev else 0.0,disabled=(s10<1))
            if s10<1: st.caption("ไม่มีบรรจุภัณฑ์นี้")
        with c2:
            box10_price=st.number_input("ราคา/ถุง",value=safe_val(prev,"box10_price",70.0) if prev else 70.0,min_value=0.0,step=5.0,key="p_b10")
        with c3:
            box20_used=st.number_input(f"กล่องใส 20ชิ้น (เหลือ {s20:,.0f} กล่อง)",min_value=0.0,step=1.0,format="%.0f",key="u_b20",value=safe_val(prev,"box20_used") if prev else 0.0,disabled=(s20<1))
            if s20<1: st.caption("ไม่มีบรรจุภัณฑ์นี้")
        with c4:
            box20_price=st.number_input("ราคา/กล่อง",value=safe_val(prev,"box20_price",130.0) if prev else 130.0,min_value=0.0,step=5.0,key="p_b20")
        st.markdown("#### 🧋 เครื่องดื่ม (คำนวณเงิน)")
        d1,d2,d3,d4=st.columns(4)
        drink_items=[
            ("ชาไทยสลัชชี่","แก้ว ชาไทยสลัชชี่","drink_thaitea_used","drink_thaitea_price",89.0,"u_tt","p_tt"),
            ("ชาไทยเย็น","แก้ว ชาไทยเย็น","drink_milky_used","drink_milky_price",79.0,"u_mk","p_mk"),
            ("ชาน้ำผึ้งมะนาว","แก้ว ชาน้ำผึ้งมะนาว","drink_bright_used","drink_bright_price",79.0,"u_br","p_br"),
            ("ชาน้ำผึ้งมะนาวสลัชชี่","แก้ว ชาน้ำผึ้งมะนาวสลัชชี่","drink_honey_used","drink_honey_price",89.0,"u_hn","p_hn"),
        ]
        drink_vals={}; drink_prices={}
        for col_obj,(dname,itype,ucol,pcol,dp,uk,pk) in zip([d1,d2,d3,d4],drink_items):
            sv=bal.get(itype,0)
            with col_obj:
                drink_vals[uk]=st.number_input(f"{dname} (เหลือ {sv:,.0f} แก้ว)",min_value=0.0,step=1.0,format="%.0f",key=uk,value=safe_val(prev,ucol) if prev else 0.0,disabled=(sv<1))
                if sv<1: st.caption("ไม่มีบรรจุภัณฑ์นี้")
                drink_prices[pk]=st.number_input("ราคา/แก้ว",value=safe_val(prev,pcol,dp) if prev else dp,min_value=0.0,step=1.0,key=pk)
        st.markdown("#### 🛒 ถุงหูหิ้ว ROON + Topping YUZU")
        sb1,sb2,sb3,sb4=st.columns(4)
        ssb=bal.get("ถุงหูหิ้ว ROON",0)
        with sb1:
            shopbag_used=st.number_input(f"ถุงหูหิ้ว ขาย (เหลือ {ssb:,.0f} ใบ)",min_value=0.0,step=1.0,format="%.0f",key="u_sb",value=safe_val(prev,"shopbag_used") if prev else 0.0,disabled=(ssb<1))
            if ssb<1: st.caption("ไม่มีบรรจุภัณฑ์นี้")
        with sb2:
            shopbag_price=st.number_input("ราคา/ใบ (บาท)",value=safe_val(prev,"shopbag_price",15.0) if prev else 15.0,min_value=0.0,step=1.0,key="p_sb")
        with sb3:
            shopbag_free=st.number_input(f"ถุงหูหิ้ว ให้ฟรี (เหลือ {ssb:,.0f} ใบ)",min_value=0.0,step=1.0,format="%.0f",key="u_sb_free",value=safe_val(prev,"shopbag_free") if prev else 0.0,disabled=(ssb<1))
        with sb4:
            yuzu_used=st.number_input("Topping YUZU (ชิ้น)",min_value=0.0,step=1.0,format="%.0f",key="u_yuzu",value=safe_val(prev,"yuzu_used") if prev else 0.0)
            yuzu_price=st.number_input("ราคา YUZU/ชิ้น",value=safe_val(prev,"yuzu_price",15.0) if prev else 15.0,min_value=0.0,step=1.0,key="p_yuzu")
        st.markdown("#### 🛵 Line Man / Shopee / TikTok / Grab (ตัดสต็อกเท่านั้น)")
        lm1,lm2,lm3,lm4=st.columns(4)
        with lm1:
            lineman_box10=st.number_input(f"Line Man ถุงกระดาษ (เหลือ {s10:,.0f})",min_value=0.0,step=1.0,format="%.0f",key="lm_b10",value=safe_val(prev,"lineman_box10") if prev else 0.0)
            lineman_box20=st.number_input(f"Line Man กล่องใส (เหลือ {s20:,.0f})",min_value=0.0,step=1.0,format="%.0f",key="lm_b20",value=safe_val(prev,"lineman_box20") if prev else 0.0)
        with lm2:
            shopee_box10=st.number_input(f"Shopee ถุงกระดาษ (เหลือ {s10:,.0f})",min_value=0.0,step=1.0,format="%.0f",key="sp_b10",value=safe_val(prev,"shopee_box10") if prev else 0.0)
            shopee_box20=st.number_input(f"Shopee กล่องใส (เหลือ {s20:,.0f})",min_value=0.0,step=1.0,format="%.0f",key="sp_b20",value=safe_val(prev,"shopee_box20") if prev else 0.0)
        with lm3:
            tiktok_box10=st.number_input(f"TikTok ถุงกระดาษ (เหลือ {s10:,.0f})",min_value=0.0,step=1.0,format="%.0f",key="tt_b10",value=safe_val(prev,"tiktok_box10") if prev else 0.0)
            tiktok_box20=st.number_input(f"TikTok กล่องใส (เหลือ {s20:,.0f})",min_value=0.0,step=1.0,format="%.0f",key="tt_b20",value=safe_val(prev,"tiktok_box20") if prev else 0.0)
        with lm4:
            grab_box10=st.number_input(f"Grab ถุงกระดาษ (เหลือ {s10:,.0f})",min_value=0.0,step=1.0,format="%.0f",key="gb_b10",value=safe_val(prev,"grab_box10") if prev else 0.0)
            grab_box20=st.number_input(f"Grab กล่องใส (เหลือ {s20:,.0f})",min_value=0.0,step=1.0,format="%.0f",key="gb_b20",value=safe_val(prev,"grab_box20") if prev else 0.0)
        note=st.text_input("หมายเหตุ",value=prev.get("note","") if prev else "")
        st.markdown("---")
        expected=(box10_used*box10_price+box20_used*box20_price+
                  drink_vals["u_tt"]*drink_prices["p_tt"]+drink_vals["u_mk"]*drink_prices["p_mk"]+
                  drink_vals["u_br"]*drink_prices["p_br"]+drink_vals["u_hn"]*drink_prices["p_hn"]+
                  shopbag_used*shopbag_price+yuzu_used*yuzu_price)
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
            if is_diff: st.error("❌  DIFF")
            else: st.success("✅  PASS")
        st.markdown("")
        if st.button("💾 บันทึกยอดขาย",use_container_width=True):
            sb=get_supabase()
            data={"sale_date":str(sale_date),"branch_code":branch_code,"actual_cash":actual_cash,
                "box10_used":box10_used,"box20_used":box20_used,"box10_price":box10_price,"box20_price":box20_price,
                "drink_thaitea_used":drink_vals["u_tt"],"drink_milky_used":drink_vals["u_mk"],
                "drink_bright_used":drink_vals["u_br"],"drink_honey_used":drink_vals["u_hn"],
                "drink_thaitea_price":drink_prices["p_tt"],"drink_milky_price":drink_prices["p_mk"],
                "drink_bright_price":drink_prices["p_br"],"drink_honey_price":drink_prices["p_hn"],
                "shopbag_used":shopbag_used,"shopbag_price":shopbag_price,
                "shopbag_free":shopbag_free,
                "yuzu_used":yuzu_used,"yuzu_price":yuzu_price,
                "lineman_box10":lineman_box10,"lineman_box20":lineman_box20,
                "shopee_box10":shopee_box10,"shopee_box20":shopee_box20,
                "tiktok_box10":tiktok_box10,"tiktok_box20":tiktok_box20,
                "grab_box10":grab_box10,"grab_box20":grab_box20,"note":note}
            if dup_sale:
                sb.table("sales").update(data).eq("id",dup_sale[0]["id"]).execute()
                st.success(f"🔄 อัปเดตแล้ว! สาขา {branch_code} · {sale_date}")
            else:
                sb.table("sales").insert(data).execute()
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
            df2["สถานะ"]=df2["ส่วนต่าง"].apply(lambda x:"PASS" if abs(x)<0.01 else ("+" if x>0 else "-")+"DIFF")
            df2["ชื่อสาขา"]=df2["branch_code"].map(BRANCH_MAP)
            show=df2[["sale_date","branch_code","ชื่อสาขา","actual_cash","ยอดระบบ","ส่วนต่าง","สถานะ"]].copy()
            show.columns=["วันที่","รหัส","ชื่อสาขา","ยอดสาขา","ยอดระบบ","ส่วนต่าง","สถานะ"]
            st.dataframe(show,use_container_width=True,hide_index=True)

# ══ REPORT ══
elif menu == "📋 รายงานตรวจสอบ":
    st.markdown('<div class="section-header">รายงานตรวจสอบ</div>',unsafe_allow_html=True)
    sales_df=load_sales()
    report_date=st.date_input("เลือกวันที่",value=date.today())
    report_tab1,report_tab2=st.tabs(["ยอดเงินแต่ละสาขา","บรรจุภัณฑ์แต่ละสาขา"])
    if len(sales_df)==0:
        st.warning("ไม่มีข้อมูลในระบบค่ะ")
    else:
        df=sales_df.copy()
        df["sale_date_str"]=df["sale_date"].astype(str).str[:10]
        report_date_str=str(report_date)
        df=df[df["sale_date_str"]==report_date_str]
        with report_tab1:
            st.markdown(f"**วันที่ {report_date.strftime('%d/%m/%Y')}**")
            if len(df)==0:
                st.warning(f"ไม่มีข้อมูล วันที่ {report_date.strftime('%d/%m/%Y')} ค่ะ")
            else:
                df2=df.copy()
                df2["expected"]=df2.apply(calc_expected,axis=1)
                df2["diff_amt"]=df2["actual_cash"]-df2["expected"]
                m1,m2,m3=st.columns(3)
                m1.metric("สาขาที่บันทึก",f"{len(df2)} สาขา")
                m2.metric("✅ PASS",(abs(df2["diff_amt"])<0.01).sum())
                m3.metric("❌ DIFF",(abs(df2["diff_amt"])>=0.01).sum())
                st.markdown("---")
                rows=[]
                for _,row in df2.sort_values("branch_code").iterrows():
                    diff=row["diff_amt"]
                    if abs(diff)<0.01: status="PASS"
                    elif diff>0: status=f"+DIFF {diff:,.2f}"
                    else: status=f"-DIFF {abs(diff):,.2f}"
                    rows.append({"รหัสสาขา":row["branch_code"],"ชื่อสาขา":BRANCH_MAP.get(row["branch_code"],""),
                        "ยอดเงินสาขาแจ้ง":f"{row['actual_cash']:,.2f}","ยอดเงินระบบคำนวณ":f"{row['expected']:,.2f}","ส่วนต่าง":status})
                rpt=pd.DataFrame(rows)
                st.dataframe(rpt,use_container_width=True,hide_index=True)
                buf=io.BytesIO()
                with pd.ExcelWriter(buf,engine="openpyxl") as w:
                    rpt.to_excel(w,index=False,sheet_name=f"ยอดเงิน_{report_date}")
                st.download_button("📥 Export Excel",data=buf.getvalue(),file_name=f"money_report_{report_date}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        with report_tab2:
            st.markdown(f"**วันที่ {report_date.strftime('%d/%m/%Y')}**")
            if len(df)==0:
                st.warning(f"ไม่มีข้อมูล วันที่ {report_date.strftime('%d/%m/%Y')} ค่ะ")
            else:
                bal_total=get_stock_balance_total()
                rows2=[]
                for _,row in df.sort_values("branch_code").iterrows():
                    def sg(k): return float(row.get(k,0) or 0)
                    b10=sg("box10_used")+sg("lineman_box10")+sg("shopee_box10")+sg("tiktok_box10")+sg("grab_box10")
                    b20=sg("box20_used")+sg("lineman_box20")+sg("shopee_box20")+sg("tiktok_box20")+sg("grab_box20")
                    drinks=sg("drink_thaitea_used")+sg("drink_milky_used")+sg("drink_bright_used")+sg("drink_honey_used")
                    sb_val=sg("shopbag_used")
                    rem10=bal_total.get("ถุงกระดาษ (ขนมไข่ 10 ชิ้น)",0)
                    rem20=bal_total.get("กล่องใส (ขนมไข่ 20 ชิ้น)",0)
                    remd=sum([bal_total.get(k,0) for k in ["แก้ว ชาไทยสลัชชี่","แก้ว ชาไทยเย็น","แก้ว ชาน้ำผึ้งมะนาว","แก้ว ชาน้ำผึ้งมะนาวสลัชชี่"]])
                    remsb=bal_total.get("ถุงหูหิ้ว ROON",0)
                    rows2.append({"รหัสสาขา":row["branch_code"],"ชื่อสาขา":BRANCH_MAP.get(row["branch_code"],""),
                        f"ถุงกระดาษรวม(เหลือ{rem10:,.0f})":f"{b10:,.0f}",
                        f"กล่องใสรวม(เหลือ{rem20:,.0f})":f"{b20:,.0f}",
                        f"แก้วเครื่องดื่ม(เหลือ{remd:,.0f})":f"{drinks:,.0f}",
                        f"ถุงหูหิ้ว(เหลือ{remsb:,.0f})":f"{sb_val:,.0f}"})
                rpt2=pd.DataFrame(rows2)
                st.dataframe(rpt2,use_container_width=True,hide_index=True)
                buf2=io.BytesIO()
                with pd.ExcelWriter(buf2,engine="openpyxl") as w:
                    rpt2.to_excel(w,index=False,sheet_name=f"บรรจุภัณฑ์_{report_date}")
                st.download_button("📥 Export Excel",data=buf2.getvalue(),file_name=f"pkg_report_{report_date}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# ══ CHARTS ══
elif menu == "📈 กราฟวิเคราะห์":
    st.markdown('<div class="section-header">กราฟวิเคราะห์</div>',unsafe_allow_html=True)
    sales_df=load_sales()
    if len(sales_df)==0:
        st.info("ยังไม่มีข้อมูลยอดขายค่ะ")
    else:
        chart_date=st.date_input("เลือกวันที่",value=date.today(),key="chart_date")
        df_all=sales_df.copy()
        df_all["sale_date_str"]=df_all["sale_date"].astype(str).str[:10]
        chart_date_str=str(chart_date)
        df_day=df_all[df_all["sale_date_str"]==chart_date_str]
        tab1,tab2,tab3=st.tabs(["ยอดขาย+ส่วนต่างรายสาขา","บรรจุภัณฑ์รายสาขา","แนวโน้มรายวัน"])
        with tab1:
            if len(df_day)==0:
                st.warning(f"ไม่มีข้อมูล วันที่ {chart_date.strftime('%d/%m/%Y')}")
            else:
                df_d=df_day.copy()
                df_d["expected"]=df_d.apply(calc_expected,axis=1)
                df_d["diff_amt"]=df_d["actual_cash"]-df_d["expected"]
                df_d["ชื่อสาขา"]=df_d["branch_code"].map(BRANCH_MAP)
                labels=df_d["branch_code"]+" "+df_d["ชื่อสาขา"].fillna("")
                fig=go.Figure()
                fig.add_trace(go.Bar(name="ยอดสาขาแจ้ง",x=labels,y=df_d["actual_cash"],marker_color="#F59E0B"))
                fig.add_trace(go.Bar(name="ยอดระบบ",x=labels,y=df_d["expected"],marker_color="#B45309"))
                fig.add_trace(go.Bar(name="ส่วนต่าง",x=labels,y=df_d["diff_amt"],
                    marker_color=["#065F46" if v>=0 else "#991B1B" for v in df_d["diff_amt"]]))
                fig.update_layout(barmode="group",height=450,font=dict(family="Sarabun"),
                    xaxis_tickangle=-30,paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)")
                st.plotly_chart(fig,use_container_width=True)
        with tab2:
            if len(df_day)==0:
                st.warning(f"ไม่มีข้อมูล วันที่ {chart_date.strftime('%d/%m/%Y')}")
            else:
                colors_pie=["#F59E0B","#B45309","#0891B2","#0E7490","#06B6D4","#67E8F9","#6B7280"]
                pkg_labels=["ถุงกระดาษรวม","กล่องใสรวม","ชาไทยสลัชชี่","ชาไทยเย็น","ชาน้ำผึ้งมะนาว","ชาน้ำผึ้งมะนาวสลัชชี่","ถุงหูหิ้ว"]
                total_b10=total_b20=total_tt=total_mk=total_br=total_hn=total_sb=0
                pie_cols=st.columns(min(len(df_day),3))
                for ci,(_,row) in enumerate(df_day.iterrows()):
                    def sg(k): return float(row.get(k,0) or 0)
                    b10=sg("box10_used")+sg("lineman_box10")+sg("shopee_box10")+sg("tiktok_box10")+sg("grab_box10")
                    b20=sg("box20_used")+sg("lineman_box20")+sg("shopee_box20")+sg("tiktok_box20")+sg("grab_box20")
                    tt=sg("drink_thaitea_used");mk=sg("drink_milky_used");br=sg("drink_bright_used");hn=sg("drink_honey_used");sb_v=sg("shopbag_used")
                    total_b10+=b10;total_b20+=b20;total_tt+=tt;total_mk+=mk;total_br+=br;total_hn+=hn;total_sb+=sb_v
                    vals=[b10,b20,tt,mk,br,hn,sb_v]
                    if sum(vals)>0:
                        with pie_cols[ci%len(pie_cols)]:
                            bname=BRANCH_MAP.get(row["branch_code"],row["branch_code"])
                            st.markdown(f"**สาขา {bname}**")
                            fig_p=go.Figure(go.Pie(labels=pkg_labels,values=vals,marker_colors=colors_pie,hole=0.3,textinfo="label+value"))
                            fig_p.update_layout(height=280,margin=dict(t=10,b=10,l=5,r=5),font=dict(family="Sarabun",size=10),showlegend=False,paper_bgcolor="rgba(0,0,0,0)")
                            st.plotly_chart(fig_p,use_container_width=True)
                st.markdown("---")
                st.markdown("**รวมทุกสาขา**")
                fig_total=go.Figure(go.Pie(labels=pkg_labels,
                    values=[total_b10,total_b20,total_tt,total_mk,total_br,total_hn,total_sb],
                    marker_colors=colors_pie,hole=0.4,textinfo="label+percent+value"))
                fig_total.update_layout(height=420,font=dict(family="Sarabun"),paper_bgcolor="rgba(0,0,0,0)")
                st.plotly_chart(fig_total,use_container_width=True)
        with tab3:
            daily=df_all.copy()
            daily["expected"]=daily.apply(calc_expected,axis=1)
            daily_g=daily.groupby("sale_date_str")[["actual_cash","expected"]].sum().reset_index()
            fig3=go.Figure()
            fig3.add_trace(go.Scatter(x=daily_g["sale_date_str"],y=daily_g["actual_cash"],name="ยอดสาขาแจ้ง",line=dict(color="#F59E0B",width=2.5)))
            fig3.add_trace(go.Scatter(x=daily_g["sale_date_str"],y=daily_g["expected"],name="ยอดระบบ",line=dict(color="#B45309",width=2.5,dash="dash")))
            fig3.update_layout(height=380,font=dict(family="Sarabun"),paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig3,use_container_width=True)
