"""
master_data.py  –  Seed ข้อมูลตั้งต้น + หน้าจอ Master Data CRUD
"""
import pandas as pd
import streamlit as st
from config import (
    SHEET_BRANCH_GROUPS, SHEET_AREA_MASTER, SHEET_BRANCHES,
    SHEET_ITEM_CATEGORIES, SHEET_ITEMS, SHEET_PRODUCTS,
    SHEET_SALES_CHANNELS, SHEET_USERS, SHEET_ROLES,
)
from modules.excel_db import read_sheet, write_sheet, append_row, update_row, delete_row
from utils.id_generator import next_id


# ══════════════════════════════════════════════════════════════
# SEED ข้อมูลตั้งต้น
# ══════════════════════════════════════════════════════════════
def seed_all():
    _seed_branch_groups()
    _seed_area_master()
    _seed_item_categories()
    _seed_sales_channels()
    _seed_roles()


def _seed_if_empty(sheet_name: str, columns: list, rows: list):
    df = read_sheet(sheet_name)
    if df.empty or list(df.columns) != columns:
        df = pd.DataFrame(rows, columns=columns)
        write_sheet(sheet_name, df)


def _seed_branch_groups():
    data = [
        ("01", "ครัวกลาง", "TRUE"),
        ("02", "Central", "TRUE"),
        ("03", "The Mall", "TRUE"),
        ("04", "Seacon", "TRUE"),
        ("05", "Department Store", "TRUE"),
        ("06", "Event", "TRUE"),
        ("07", "Online", "TRUE"),
        ("08", "Market", "TRUE"),
        ("09", "Delivery", "TRUE"),
        ("10", "Modern Trade", "TRUE"),
    ]
    _seed_if_empty(SHEET_BRANCH_GROUPS,
                   ["branch_group_id", "branch_group_name", "is_active"], data)


def _seed_area_master():
    data = [
        ("1", "กรุงเทพฯ", "TRUE"),
        ("2", "ต่างจังหวัด", "TRUE"),
        ("3", "ออนไลน์", "TRUE"),
    ]
    _seed_if_empty(SHEET_AREA_MASTER,
                   ["area_id", "area_name", "is_active"], data)


def _seed_item_categories():
    data = [
        ("raw_material",    "วัตถุดิบ",           "TRUE"),
        ("packaging",       "บรรจุภัณฑ์",          "TRUE"),
        ("finished_product","สินค้าสำเร็จรูป",     "TRUE"),
        ("drink",           "เครื่องดื่ม",         "TRUE"),
        ("office_supply",   "อุปกรณ์สำนักงาน",    "TRUE"),
        ("asset",           "ทรัพย์สิน",           "TRUE"),
        ("consumable",      "วัสดุสิ้นเปลือง",     "TRUE"),
        ("other",           "อื่น ๆ",             "TRUE"),
    ]
    _seed_if_empty(SHEET_ITEM_CATEGORIES,
                   ["item_category_id", "item_category_name", "is_active"], data)


def _seed_sales_channels():
    data = [
        ("01", "หน้าร้าน",    "direct",  "TRUE"),
        ("02", "Event",       "direct",  "TRUE"),
        ("03", "Shopee",      "online",  "TRUE"),
        ("04", "TikTok",      "online",  "TRUE"),
        ("05", "Grab",        "delivery","TRUE"),
        ("06", "Line Man",    "delivery","TRUE"),
        ("07", "Direct Sales","direct",  "TRUE"),
        ("08", "Delivery",    "delivery","TRUE"),
        ("09", "Modern Trade","mt",      "TRUE"),
    ]
    _seed_if_empty(SHEET_SALES_CHANNELS,
                   ["channel_id", "channel_name", "channel_type", "is_active"], data)


def _seed_roles():
    roles = [
        ("admin",     "Admin",           '{"all":true}',         "TRUE"),
        ("branch",    "พนักงานสาขา",     '{"branch_report":true}', "TRUE"),
        ("production","ฝ่ายผลิต",        '{"production":true}',   "TRUE"),
        ("accounting","ฝ่ายบัญชี",       '{"accounting":true}',   "TRUE"),
        ("finance",   "ฝ่ายการเงิน",     '{"finance":true}',      "TRUE"),
        ("purchase",  "ฝ่ายจัดซื้อ",     '{"purchase":true}',     "TRUE"),
        ("audit",     "ฝ่ายตรวจสอบ",     '{"audit":true}',        "TRUE"),
        ("manager",   "ผู้บริหาร",       '{"view_all":true}',     "TRUE"),
    ]
    _seed_if_empty(SHEET_ROLES,
                   ["role_id", "role_name", "permission_json", "is_active"], roles)


# ══════════════════════════════════════════════════════════════
# หน้าจอ MASTER DATA
# ══════════════════════════════════════════════════════════════
def render_master_data():
    st.title("📋 Master Data")
    tab_branch, tab_item, tab_product, tab_view = st.tabs([
        "🏪 สาขา (Branches)",
        "📦 วัตถุดิบ / สินค้า (Items)",
        "🥚 สินค้าสำเร็จรูป (Products)",
        "🔍 ดูข้อมูลอ้างอิง",
    ])

    with tab_branch:
        _render_branches()
    with tab_item:
        _render_items()
    with tab_product:
        _render_products()
    with tab_view:
        _render_reference_tables()


# ──────────────────────────────────────────────
# BRANCHES
# ──────────────────────────────────────────────
def _render_branches():
    st.subheader("🏪 จัดการสาขา")

    df = read_sheet(SHEET_BRANCHES)
    bg_df = read_sheet(SHEET_BRANCH_GROUPS)
    area_df = read_sheet(SHEET_AREA_MASTER)

    bg_options = dict(zip(bg_df["branch_group_id"], bg_df["branch_group_name"])) if not bg_df.empty else {}
    area_options = dict(zip(area_df["area_id"], area_df["area_name"])) if not area_df.empty else {}

    # Search
    search = st.text_input("🔍 ค้นหาสาขา", key="branch_search")
    if search and not df.empty:
        mask = df.apply(lambda r: search.lower() in " ".join(r.values).lower(), axis=1)
        df_show = df[mask]
    else:
        df_show = df

    if df_show.empty:
        st.info("ยังไม่มีข้อมูลสาขา")
    else:
        # แสดงตาราง
        display = df_show.copy()
        if "branch_group_id" in display.columns:
            display["กลุ่มสาขา"] = display["branch_group_id"].map(bg_options).fillna(display["branch_group_id"])
        if "area_id" in display.columns:
            display["พื้นที่"] = display["area_id"].map(area_options).fillna(display["area_id"])
        st.dataframe(display, use_container_width=True)

    st.divider()

    # ADD / EDIT toggle
    action = st.radio("เลือกการดำเนินการ", ["➕ เพิ่มสาขาใหม่", "✏️ แก้ไข / ลบสาขา"],
                      horizontal=True, key="branch_action")

    if action == "➕ เพิ่มสาขาใหม่":
        _form_add_branch(bg_options, area_options)
    else:
        _form_edit_branch(df, bg_options, area_options)


def _form_add_branch(bg_options, area_options):
    with st.form("form_add_branch"):
        st.markdown("#### เพิ่มสาขาใหม่")
        col1, col2 = st.columns(2)
        with col1:
            branch_name = st.text_input("ชื่อสาขา *")
            bg_label = st.selectbox("กลุ่มสาขา *",
                                    options=list(bg_options.keys()),
                                    format_func=lambda k: f"{k} – {bg_options[k]}")
            open_date = st.date_input("วันที่เปิด")
        with col2:
            area_label = st.selectbox("พื้นที่ *",
                                      options=list(area_options.keys()),
                                      format_func=lambda k: f"{k} – {area_options[k]}")
            status = st.selectbox("สถานะ", ["active", "inactive", "temporary_close"])
            remark = st.text_input("หมายเหตุ")

        submitted = st.form_submit_button("💾 บันทึก", type="primary")
        if submitted:
            if not branch_name.strip():
                st.error("กรุณากรอกชื่อสาขา")
                return
            df = read_sheet(SHEET_BRANCHES)
            new_id = next_id(df, "branch_id", "BR")
            row = {
                "branch_id": new_id,
                "branch_name": branch_name.strip(),
                "branch_group_id": bg_label,
                "area_id": area_label,
                "open_date": str(open_date),
                "status": status,
                "remark": remark,
            }
            append_row(SHEET_BRANCHES, row)
            st.success(f"✅ เพิ่มสาขา '{branch_name}' สำเร็จ (ID: {new_id})")
            st.rerun()


def _form_edit_branch(df, bg_options, area_options):
    if df.empty:
        st.info("ยังไม่มีสาขาให้แก้ไข")
        return

    branch_ids = df["branch_id"].tolist()
    selected_id = st.selectbox("เลือกสาขาที่ต้องการแก้ไข",
                               options=branch_ids,
                               format_func=lambda x: f"{x} – {df[df['branch_id']==x]['branch_name'].values[0]}")
    row = df[df["branch_id"] == selected_id].iloc[0]

    with st.form("form_edit_branch"):
        col1, col2 = st.columns(2)
        with col1:
            branch_name = st.text_input("ชื่อสาขา *", value=row.get("branch_name", ""))
            bg_keys = list(bg_options.keys())
            bg_idx = bg_keys.index(row.get("branch_group_id", bg_keys[0])) if row.get("branch_group_id") in bg_keys else 0
            bg_label = st.selectbox("กลุ่มสาขา *", options=bg_keys,
                                    format_func=lambda k: f"{k} – {bg_options[k]}",
                                    index=bg_idx)
        with col2:
            area_keys = list(area_options.keys())
            area_idx = area_keys.index(row.get("area_id", area_keys[0])) if row.get("area_id") in area_keys else 0
            area_label = st.selectbox("พื้นที่ *", options=area_keys,
                                      format_func=lambda k: f"{k} – {area_options[k]}",
                                      index=area_idx)
            status_opts = ["active", "inactive", "temporary_close"]
            st_idx = status_opts.index(row.get("status", "active")) if row.get("status") in status_opts else 0
            status = st.selectbox("สถานะ", status_opts, index=st_idx)
        remark = st.text_input("หมายเหตุ", value=row.get("remark", ""))

        col_save, col_del = st.columns(2)
        with col_save:
            save = st.form_submit_button("💾 บันทึกการแก้ไข", type="primary")
        with col_del:
            delete = st.form_submit_button("🗑️ ลบสาขานี้")

        if save:
            update_row(SHEET_BRANCHES, "branch_id", selected_id, {
                "branch_name": branch_name,
                "branch_group_id": bg_label,
                "area_id": area_label,
                "status": status,
                "remark": remark,
            })
            st.success("✅ แก้ไขสาขาสำเร็จ")
            st.rerun()
        if delete:
            delete_row(SHEET_BRANCHES, "branch_id", selected_id)
            st.warning(f"🗑️ ลบสาขา {selected_id} แล้ว")
            st.rerun()


# ──────────────────────────────────────────────
# ITEMS
# ──────────────────────────────────────────────
def _render_items():
    st.subheader("📦 จัดการวัตถุดิบ / สินค้า (Items)")

    df = read_sheet(SHEET_ITEMS)
    cat_df = read_sheet(SHEET_ITEM_CATEGORIES)
    cat_options = dict(zip(cat_df["item_category_id"], cat_df["item_category_name"])) if not cat_df.empty else {}

    search = st.text_input("🔍 ค้นหา Item", key="item_search")
    if search and not df.empty:
        mask = df.apply(lambda r: search.lower() in " ".join(r.values).lower(), axis=1)
        df_show = df[mask]
    else:
        df_show = df

    if df_show.empty:
        st.info("ยังไม่มีข้อมูล Item")
    else:
        display = df_show.copy()
        if "item_category_id" in display.columns:
            display["หมวดหมู่"] = display["item_category_id"].map(cat_options).fillna(display["item_category_id"])
        st.dataframe(display, use_container_width=True)

    st.divider()
    action = st.radio("เลือกการดำเนินการ", ["➕ เพิ่ม Item ใหม่", "✏️ แก้ไข / ลบ Item"],
                      horizontal=True, key="item_action")

    if action == "➕ เพิ่ม Item ใหม่":
        _form_add_item(cat_options)
    else:
        _form_edit_item(df, cat_options)


def _form_add_item(cat_options):
    with st.form("form_add_item"):
        st.markdown("#### เพิ่ม Item ใหม่")
        col1, col2 = st.columns(2)
        with col1:
            item_name = st.text_input("ชื่อ Item *")
            cat_key = st.selectbox("หมวดหมู่ *",
                                   options=list(cat_options.keys()),
                                   format_func=lambda k: f"{k} – {cat_options[k]}")
            unit = st.text_input("หน่วย (เช่น กก., ชิ้น, กล่อง)", value="ชิ้น")
        with col2:
            standard_cost = st.number_input("ต้นทุนมาตรฐาน (บาท)", min_value=0.0, step=0.01)
            selling_cost  = st.number_input("ราคาขาย (บาท)",        min_value=0.0, step=0.01)
            min_stock     = st.number_input("Stock ขั้นต่ำ",         min_value=0,   step=1)
            is_active     = st.selectbox("สถานะ", ["TRUE", "FALSE"])

        submitted = st.form_submit_button("💾 บันทึก", type="primary")
        if submitted:
            if not item_name.strip():
                st.error("กรุณากรอกชื่อ Item")
                return
            df = read_sheet(SHEET_ITEMS)
            new_id = next_id(df, "item_id", "ITM")
            row = {
                "item_id": new_id,
                "item_name": item_name.strip(),
                "item_category_id": cat_key,
                "unit": unit,
                "standard_cost": standard_cost,
                "selling_cost": selling_cost,
                "min_stock": min_stock,
                "is_active": is_active,
            }
            append_row(SHEET_ITEMS, row)
            st.success(f"✅ เพิ่ม Item '{item_name}' สำเร็จ (ID: {new_id})")
            st.rerun()


def _form_edit_item(df, cat_options):
    if df.empty:
        st.info("ยังไม่มี Item ให้แก้ไข")
        return

    item_ids = df["item_id"].tolist()
    selected_id = st.selectbox("เลือก Item ที่ต้องการแก้ไข",
                               options=item_ids,
                               format_func=lambda x: f"{x} – {df[df['item_id']==x]['item_name'].values[0]}")
    row = df[df["item_id"] == selected_id].iloc[0]

    with st.form("form_edit_item"):
        col1, col2 = st.columns(2)
        with col1:
            item_name = st.text_input("ชื่อ Item *", value=row.get("item_name", ""))
            cat_keys = list(cat_options.keys())
            cat_idx = cat_keys.index(row.get("item_category_id")) if row.get("item_category_id") in cat_keys else 0
            cat_key = st.selectbox("หมวดหมู่ *", options=cat_keys,
                                   format_func=lambda k: f"{k} – {cat_options[k]}", index=cat_idx)
            unit = st.text_input("หน่วย", value=row.get("unit", ""))
        with col2:
            try:
                sc_val = float(row.get("standard_cost", 0))
                sl_val = float(row.get("selling_cost", 0))
                ms_val = int(float(row.get("min_stock", 0)))
            except Exception:
                sc_val, sl_val, ms_val = 0.0, 0.0, 0
            standard_cost = st.number_input("ต้นทุนมาตรฐาน", min_value=0.0, step=0.01, value=sc_val)
            selling_cost  = st.number_input("ราคาขาย",        min_value=0.0, step=0.01, value=sl_val)
            min_stock     = st.number_input("Stock ขั้นต่ำ",  min_value=0,   step=1,    value=ms_val)
            act_opts = ["TRUE", "FALSE"]
            act_idx = act_opts.index(row.get("is_active", "TRUE")) if row.get("is_active") in act_opts else 0
            is_active = st.selectbox("สถานะ", act_opts, index=act_idx)

        col_save, col_del = st.columns(2)
        with col_save:
            save = st.form_submit_button("💾 บันทึกการแก้ไข", type="primary")
        with col_del:
            delete = st.form_submit_button("🗑️ ลบ Item นี้")

        if save:
            update_row(SHEET_ITEMS, "item_id", selected_id, {
                "item_name": item_name, "item_category_id": cat_key,
                "unit": unit, "standard_cost": standard_cost,
                "selling_cost": selling_cost, "min_stock": min_stock,
                "is_active": is_active,
            })
            st.success("✅ แก้ไข Item สำเร็จ")
            st.rerun()
        if delete:
            delete_row(SHEET_ITEMS, "item_id", selected_id)
            st.warning(f"🗑️ ลบ Item {selected_id} แล้ว")
            st.rerun()


# ──────────────────────────────────────────────
# PRODUCTS
# ──────────────────────────────────────────────
def _render_products():
    st.subheader("🥚 จัดการสินค้าสำเร็จรูป (Products)")

    df = read_sheet(SHEET_PRODUCTS)

    search = st.text_input("🔍 ค้นหา Product", key="product_search")
    if search and not df.empty:
        mask = df.apply(lambda r: search.lower() in " ".join(r.values).lower(), axis=1)
        df_show = df[mask]
    else:
        df_show = df

    if df_show.empty:
        st.info("ยังไม่มีข้อมูล Product")
    else:
        st.dataframe(df_show, use_container_width=True)

    st.divider()
    action = st.radio("เลือกการดำเนินการ", ["➕ เพิ่ม Product ใหม่", "✏️ แก้ไข / ลบ Product"],
                      horizontal=True, key="product_action")

    items_df = read_sheet(SHEET_ITEMS)
    item_options = {}
    if not items_df.empty and "item_id" in items_df.columns:
        item_options = dict(zip(items_df["item_id"], items_df["item_name"]))

    if action == "➕ เพิ่ม Product ใหม่":
        _form_add_product(item_options)
    else:
        _form_edit_product(df, item_options)


def _form_add_product(item_options):
    with st.form("form_add_product"):
        st.markdown("#### เพิ่ม Product ใหม่")
        col1, col2 = st.columns(2)
        with col1:
            product_name = st.text_input("ชื่อสินค้า *")
            product_type = st.selectbox("ประเภท", ["ขนมไข่", "เครื่องดื่ม", "ของฝาก", "อื่น ๆ"])
            size = st.text_input("ขนาด (เช่น S, M, L, 6ชิ้น)")
        with col2:
            price = st.number_input("ราคาขาย (บาท)", min_value=0.0, step=1.0)
            pkg_options = [""] + list(item_options.keys())
            pkg_key = st.selectbox("บรรจุภัณฑ์ (packaging_item_id)",
                                   options=pkg_options,
                                   format_func=lambda k: item_options.get(k, "– ไม่ระบุ –") if k else "– ไม่ระบุ –")
            is_active = st.selectbox("สถานะ", ["TRUE", "FALSE"])

        submitted = st.form_submit_button("💾 บันทึก", type="primary")
        if submitted:
            if not product_name.strip():
                st.error("กรุณากรอกชื่อสินค้า")
                return
            df = read_sheet(SHEET_PRODUCTS)
            new_id = next_id(df, "product_id", "PRD")
            row = {
                "product_id": new_id,
                "product_name": product_name.strip(),
                "product_type": product_type,
                "size": size,
                "price": price,
                "packaging_item_id": pkg_key,
                "is_active": is_active,
            }
            append_row(SHEET_PRODUCTS, row)
            st.success(f"✅ เพิ่ม Product '{product_name}' สำเร็จ (ID: {new_id})")
            st.rerun()


def _form_edit_product(df, item_options):
    if df.empty:
        st.info("ยังไม่มี Product ให้แก้ไข")
        return

    prod_ids = df["product_id"].tolist()
    selected_id = st.selectbox("เลือก Product ที่ต้องการแก้ไข",
                               options=prod_ids,
                               format_func=lambda x: f"{x} – {df[df['product_id']==x]['product_name'].values[0]}")
    row = df[df["product_id"] == selected_id].iloc[0]

    with st.form("form_edit_product"):
        col1, col2 = st.columns(2)
        with col1:
            product_name = st.text_input("ชื่อสินค้า *", value=row.get("product_name", ""))
            type_opts = ["ขนมไข่", "เครื่องดื่ม", "ของฝาก", "อื่น ๆ"]
            type_idx = type_opts.index(row.get("product_type")) if row.get("product_type") in type_opts else 0
            product_type = st.selectbox("ประเภท", type_opts, index=type_idx)
            size = st.text_input("ขนาด", value=row.get("size", ""))
        with col2:
            try:
                price_val = float(row.get("price", 0))
            except Exception:
                price_val = 0.0
            price = st.number_input("ราคาขาย", min_value=0.0, step=1.0, value=price_val)
            pkg_options = [""] + list(item_options.keys())
            cur_pkg = row.get("packaging_item_id", "")
            pkg_idx = pkg_options.index(cur_pkg) if cur_pkg in pkg_options else 0
            pkg_key = st.selectbox("บรรจุภัณฑ์",
                                   options=pkg_options,
                                   format_func=lambda k: item_options.get(k, "– ไม่ระบุ –") if k else "– ไม่ระบุ –",
                                   index=pkg_idx)
            act_opts = ["TRUE", "FALSE"]
            act_idx = act_opts.index(row.get("is_active", "TRUE")) if row.get("is_active") in act_opts else 0
            is_active = st.selectbox("สถานะ", act_opts, index=act_idx)

        col_save, col_del = st.columns(2)
        with col_save:
            save = st.form_submit_button("💾 บันทึกการแก้ไข", type="primary")
        with col_del:
            delete = st.form_submit_button("🗑️ ลบ Product นี้")

        if save:
            update_row(SHEET_PRODUCTS, "product_id", selected_id, {
                "product_name": product_name, "product_type": product_type,
                "size": size, "price": price,
                "packaging_item_id": pkg_key, "is_active": is_active,
            })
            st.success("✅ แก้ไข Product สำเร็จ")
            st.rerun()
        if delete:
            delete_row(SHEET_PRODUCTS, "product_id", selected_id)
            st.warning(f"🗑️ ลบ Product {selected_id} แล้ว")
            st.rerun()


# ──────────────────────────────────────────────
# ตารางอ้างอิง
# ──────────────────────────────────────────────
def _render_reference_tables():
    st.subheader("🔍 ตารางข้อมูลอ้างอิง")
    for sheet, label in [
        (SHEET_BRANCH_GROUPS,   "กลุ่มสาขา (branch_groups)"),
        (SHEET_AREA_MASTER,     "พื้นที่ (area_master)"),
        (SHEET_ITEM_CATEGORIES, "หมวดหมู่สินค้า (item_categories)"),
        (SHEET_SALES_CHANNELS,  "ช่องทางขาย (sales_channels)"),
        (SHEET_ROLES,           "สิทธิ์การใช้งาน (roles)"),
    ]:
        with st.expander(label):
            df = read_sheet(sheet)
            st.dataframe(df if not df.empty else pd.DataFrame({"ข้อมูล": ["ยังไม่มีข้อมูล"]}),
                         use_container_width=True)
