-- ═══════════════════════════════════════════════════════════════
-- ROON KHANOMKHAI — รอบที่ 1-3 : ตารางใหม่
-- (รอบ 1: login/คูปอง/บันทึกรายการขาย | รอบ 2: บันทึกสต๊อก | รอบ 3: Audit ตรวจนับ)
-- วิธีใช้: เปิด Supabase → SQL Editor → วางทั้งหมด → กด Run (รันครั้งเดียว)
-- ปลอดภัย: ใช้ IF NOT EXISTS จึงไม่ลบ/ทับข้อมูลตารางเดิม (รันซ้ำได้)
-- ═══════════════════════════════════════════════════════════════

-- 1) รหัสผ่าน Login ของแต่ละสาขา (USER = รหัสสาขา)
create table if not exists branch_login (
    branch_id text primary key,
    pw_hash   text,
    is_active text
);

-- 2) ตารางคูปองแม่ (สำนักงานใหญ่เติมข้อมูลในรอบที่ 3)
--    สาขาจะบันทึกเงินคูปองได้ก็ต่อเมื่อเลขคูปองมีอยู่จริงในตารางนี้ และยัง active
create table if not exists coupons (
    coupon_no      text primary key,
    amount         text,
    status         text,   -- active / used
    used_branch_id text,
    used_sale_id   text,
    used_at        text,
    issued_at      text
);

-- 3) บันทึกรายการขาย (หัวบิล)
create table if not exists branch_sales (
    sale_id               text primary key,
    sale_date             text,
    branch_id             text,
    cash_amount           text,
    transfer_amount       text,
    coupon_amount         text,
    total_amount          text,
    eggs_used             text,   -- ตีแป้ง: จำนวนไข่ที่ใช้ไป (ฟอง)
    leftover_box_qty      text,   -- ขนมไข่คงเหลือ: จำนวนกล่อง (กล่องละ 20 ชิ้น)
    leftover_loose_pieces text,   -- ขนมไข่คงเหลือ: ไม่ใส่บรรจุภัณฑ์ (ชิ้น)
    leftover_total_pieces text,   -- รวมคงเหลือ (ชิ้น) = กล่อง×20 + เศษ
    box_unit_price        text,   -- ราคาขายต่อกล่อง (อ่านจากตาราง products)
    leftover_value        text,   -- มูลค่าที่ยังขายได้ (บาท) = กล่อง×ราคากล่อง + เศษ×(ราคากล่อง÷20)
    remark                text,
    status                text,
    created_at            text,
    updated_at            text
);
-- เผื่อกรณีเคยสร้างตาราง branch_sales ไว้ก่อนแล้ว — เพิ่มคอลัมน์ใหม่
alter table branch_sales add column if not exists eggs_used             text;
alter table branch_sales add column if not exists flour_finished_big_used   text;
alter table branch_sales add column if not exists flour_finished_small_used text;
alter table branch_sales add column if not exists mix_big_used              text;
alter table branch_sales add column if not exists mix_small_used            text;
alter table branch_sales add column if not exists batter_mismatch_reason    text;
alter table branch_sales add column if not exists leftover_box_qty      text;
alter table branch_sales add column if not exists leftover_loose_pieces text;
alter table branch_sales add column if not exists leftover_total_pieces text;
alter table branch_sales add column if not exists box_unit_price        text;
alter table branch_sales add column if not exists leftover_value        text;

-- 4) คูปองที่ใช้ในแต่ละบิล
create table if not exists branch_sales_coupons (
    id        text primary key,
    sale_id   text,
    branch_id text,
    coupon_no text,
    amount    text
);

-- 5) สลิปการโอนที่แนบในแต่ละบิล (เก็บรูปเป็น base64)
create table if not exists branch_sales_slips (
    id          text primary key,
    sale_id     text,
    branch_id   text,
    filename    text,
    image_b64   text,
    uploaded_at text
);

-- 6) จำนวนบรรจุภัณฑ์ที่ขายได้ (หน้าร้าน + Grab / LineMan / อื่นๆ) — จำนวนชนิด ไม่ใช่เงิน
create table if not exists branch_sales_delivery (
    id                     text primary key,
    sale_id                text,
    branch_id              text,
    channel                text,   -- หน้าร้าน / Grab / LineMan / อื่นๆ
    box_qty                text,   -- ขนมไข่ชนิดกล่อง
    bag_qty                text,   -- ชนิดถุง (ถุงกระดาษขาว)
    yellow_premium_bag_qty text,   -- ถุงหูหิ้วกระดาษพิมพ์ลาย
    drip_box_qty           text,   -- กล่องดริป
    water_cup_qty          text,   -- แก้วน้ำ (หน้าร้าน + Delivery)
    ice_cream_cup_qty      text,   -- แก้วไอศครีม (เฉพาะ Delivery)
    ice_cream_ring_qty     text    -- วงแหวนรองถ้วยไอศครีม (เฉพาะ Delivery)
);
-- เผื่อกรณีเคยสร้างตารางนี้ไว้ก่อนแล้ว — เพิ่มคอลัมน์ใหม่
alter table branch_sales_delivery add column if not exists drip_box_qty       text;
alter table branch_sales_delivery add column if not exists water_cup_qty       text;
alter table branch_sales_delivery add column if not exists ice_cream_cup_qty   text;
alter table branch_sales_delivery add column if not exists ice_cream_ring_qty  text;

-- 7) บันทึกสต๊อก (รอบ 2) — ยอดบรรจุภัณฑ์คงเหลือรายวันของสาขา (12 รายการ)
create table if not exists branch_stock_daily (
    stock_id              text primary key,
    stock_date            text,
    branch_id             text,
    paper_bag_qty         text,   -- ถุงกระดาษ (ถุง)
    plastic_box_qty       text,   -- กล่องพลาสติก (รวมกล่องใส่ขนมเหลือ) (กล่อง)
    band_qty              text,   -- สายคาด (เส้น)
    skewer_pack_qty       text,   -- ไม้เสียบ (แพ็ค)
    hot_bag_pack_qty      text,   -- ถุงร้อน (แพ็ค)
    printed_carry_bag_qty text,   -- ถุงหูหิ้วกระดาษพิมพ์ลาย (ใบ)
    carry_bag_7x15_qty    text,   -- ถุงหูหิ้ว 7"x15" (แพ็ค)
    carry_bag_8x16_qty    text,   -- ถุงหูหิ้วใหญ่ 8"x16" (แพ็ค)
    water_cup_qty         text,   -- แก้วน้ำ (ใบ)
    cup_lid_qty           text,   -- ฝาแก้วน้ำ (ฝา)
    ice_cream_cup_qty     text,   -- แก้วไอศครีม (ใบ)
    ice_cream_ring_qty    text,   -- วงแหวนรองถ้วยไอศครีม (แผ่น)
    recorded_by           text,
    remark                text,
    created_at            text,
    updated_at            text
);
-- วัตถุดิบคงเหลือ (รอบ 2 เพิ่ม) — เพิ่มคอลัมน์ให้ตาราง branch_stock_daily
alter table branch_stock_daily add column if not exists egg_remaining        text;
alter table branch_stock_daily add column if not exists flour_finished_big   text;
alter table branch_stock_daily add column if not exists flour_finished_small text;
alter table branch_stock_daily add column if not exists mix_big              text;
alter table branch_stock_daily add column if not exists mix_small            text;
alter table branch_stock_daily add column if not exists butter_unopened_qty  text;
alter table branch_stock_daily add column if not exists butter_used_image    text;
alter table branch_stock_daily add column if not exists butter_used_image_2  text;
alter table branch_stock_daily add column if not exists butter_used_image_3  text;
-- บรรจุภัณฑ์เพิ่ม: ถ้วยดิป
alter table branch_stock_daily add column if not exists dip_cup_qty          text;
-- บรรจุภัณฑ์เพิ่ม (รอบ 16/5/2569): วงแหวนรองแก้วไอศกรีม
alter table branch_stock_daily add column if not exists ice_cream_cup_ring_qty text;

-- 8) ตรวจนับสต๊อกโดยฝ่าย Audit (รอบ 3) — ยอดจริงที่ Audit นับ (12 รายการ)
create table if not exists audit_stock_balance (
    audit_id              text primary key,
    audit_date            text,   -- วันที่เข้าตรวจสอบ
    compare_date          text,   -- วันที่ของยอดคงเหลือที่นับ (= วันที่ตรวจ − 1)
    branch_id             text,
    paper_bag_qty         text,
    plastic_box_qty       text,
    band_qty              text,
    skewer_pack_qty       text,
    hot_bag_pack_qty      text,
    printed_carry_bag_qty text,
    carry_bag_7x15_qty    text,
    carry_bag_8x16_qty    text,
    water_cup_qty         text,
    cup_lid_qty           text,
    ice_cream_cup_qty     text,
    ice_cream_ring_qty    text,
    auditor               text,
    remark                text,
    created_at            text,
    updated_at            text
);
-- เผื่อกรณีเคยสร้างตาราง audit_stock_balance ไว้ก่อนแล้ว — เพิ่มคอลัมน์ใหม่
alter table audit_stock_balance add column if not exists audit_time   text;   -- เวลาที่ตรวจ
alter table audit_stock_balance add column if not exists auditor_id   text;   -- รหัสผู้ตรวจสอบ (employees)
alter table audit_stock_balance add column if not exists auditor_name text;   -- ชื่อผู้ตรวจสอบ
alter table audit_stock_balance add column if not exists dip_cup_qty  text;   -- ถ้วยดิป (ถ้วย)
alter table audit_stock_balance add column if not exists ice_cream_cup_ring_qty text;  -- วงแหวนรองแก้วไอศกรีม

-- 8.1) ฝ่ายจัดซื้อ: เพิ่มคอลัมน์ 'ประเภทการซื้อ' ให้ตาราง items (เมนูบันทึกชื่อวัตถุดิบ/บรรจุภัณฑ์)
alter table items add column if not exists purchase_category text;

-- 8.2) HR: เพิ่มคอลัมน์ ชื่อเล่น/สัญชาติ/บัตร ให้ตาราง employees (รอบ 15/5/2569)
alter table employees add column if not exists nickname    text;
alter table employees add column if not exists nationality text;
alter table employees add column if not exists national_id text;
alter table employees add column if not exists passport_no text;
alter table employees add column if not exists mou_no        text;
alter table employees add column if not exists resign_reason text;   -- เหตุผลที่ลาออก (รอบ 16/5/2569)

-- 8.4) HR: เพิ่มคอลัมน์บันทึกเงินเดือนสาขา (เงินเดือน + รายได้ 1/2/3 + รวม)
alter table payroll_records add column if not exists base_salary  text;
alter table payroll_records add column if not exists income1      text;
alter table payroll_records add column if not exists income2      text;
alter table payroll_records add column if not exists income3      text;
alter table payroll_records add column if not exists total_income text;

-- 8.3) การตลาด: ตรวจยอดขาย POS เทียบกับสาขา (Reconcile) — รอบ 15/5/2569
create table if not exists marketing_pos_reconcile (
    reconcile_id        text primary key,
    sales_date          text,
    branch_id           text,
    channel_id          text,
    channel_name        text,
    pos_num_items       text,   -- จำนวนรายการจาก POS (พิมพ์เอง)
    pos_total           text,   -- ยอดขายรวมของวันจาก POS (พิมพ์เอง)
    branch_cash         text,
    branch_transfer     text,
    branch_coupon       text,
    branch_total        text,   -- เงินสด+เงินโอน+คูปอง (จาก branch_sales)
    pkg_expected_total  text,   -- ยอดขายประมาณจากบรรจุภัณฑ์ที่ใช้ไป
    diff_amount         text,   -- branch_total - pos_total
    diff_flag           text,   -- 'DIFF +' / 'DIFF -' / 'OK'
    diff_reason         text,   -- เหตุผลของการ DIFF (แก้ไขภายหลังได้)
    diff_solution       text,   -- การแก้ปัญหา (แก้ไขภายหลังได้)
    created_by          text,
    created_at          text,
    updated_at          text
);
alter table marketing_pos_reconcile disable row level security;

-- 9) ฝ่ายจัดซื้อ: วัตถุดิบรายวัน (long format: 1 แถว/วัตถุดิบ/วัน)
create table if not exists material_daily (
    id             text primary key,
    entry_date     text,
    material_key   text,
    material_label text,
    opening_qty    text,   -- ยกมา
    purchased_qty  text,   -- ซื้อเข้า
    used_qty       text,   -- ใช้ไปวันนี้ (กรอกตรง)
    remaining_qty  text,   -- คงเหลือ = ยกมา + ซื้อเข้า − ใช้ไป
    unit_cost      text,   -- ต้นทุนต่อหน่วย (snapshot)
    used_cost      text,   -- ต้นทุนที่ใช้ = ใช้ไป × ต้นทุนต่อหน่วย
    created_at     text,
    updated_at     text
);

-- 10) ตารางราคาต้นทุนวัตถุดิบ (ตั้ง/แก้ได้)
create table if not exists material_cost (
    material_key   text primary key,
    material_label text,
    unit           text,
    unit_cost      text
);

-- 11) ฝ่ายจัดซื้อ/สต๊อก/ผลิต — ตารางหลัก (แก้ error: ไม่มีคอลัมน์ reference_id ฯลฯ)
--     สร้างถ้ายังไม่มี + เพิ่มคอลัมน์ที่ขาด (ปลอดภัย รันซ้ำได้)
create table if not exists stock_movements (
    stock_movement_id text primary key,
    movement_date  text, item_id text, branch_id text, movement_type text,
    qty_in text, qty_out text, unit_cost text, total_value text,
    reference_type text, reference_id text, remark text
);
alter table stock_movements add column if not exists movement_date  text;
alter table stock_movements add column if not exists item_id        text;
alter table stock_movements add column if not exists branch_id      text;
alter table stock_movements add column if not exists movement_type  text;
alter table stock_movements add column if not exists qty_in         text;
alter table stock_movements add column if not exists qty_out        text;
alter table stock_movements add column if not exists unit_cost      text;
alter table stock_movements add column if not exists total_value    text;
alter table stock_movements add column if not exists reference_type text;
alter table stock_movements add column if not exists reference_id   text;
alter table stock_movements add column if not exists remark         text;

create table if not exists purchase_orders (
    purchase_id text primary key,
    purchase_date text, supplier_name text, invoice_no text,
    purchase_category text, total_amount text, vat_amount text, grand_total text,
    created_by text, remark text
);
alter table purchase_orders add column if not exists purchase_date     text;
alter table purchase_orders add column if not exists supplier_name     text;
alter table purchase_orders add column if not exists invoice_no        text;
alter table purchase_orders add column if not exists purchase_category text;
alter table purchase_orders add column if not exists total_amount      text;
alter table purchase_orders add column if not exists vat_amount        text;
alter table purchase_orders add column if not exists grand_total       text;
alter table purchase_orders add column if not exists created_by        text;
alter table purchase_orders add column if not exists remark            text;

create table if not exists purchase_order_items (
    purchase_item_id text primary key,
    purchase_id text, item_id text, qty text,
    unit_price_inc_vat text, total_value text
);
alter table purchase_order_items add column if not exists purchase_id        text;
alter table purchase_order_items add column if not exists item_id            text;
alter table purchase_order_items add column if not exists qty                text;
alter table purchase_order_items add column if not exists unit_price_inc_vat text;
alter table purchase_order_items add column if not exists total_value        text;

create table if not exists stock_in_to_branch (
    stock_in_id text primary key,
    stock_in_date text, branch_id text, item_id text,
    qty_in text, unit text, unit_cost text, total_cost text,
    recorded_by text, remark text
);
alter table stock_in_to_branch add column if not exists stock_in_date text;
alter table stock_in_to_branch add column if not exists branch_id     text;
alter table stock_in_to_branch add column if not exists item_id       text;
alter table stock_in_to_branch add column if not exists qty_in        text;
alter table stock_in_to_branch add column if not exists unit          text;
alter table stock_in_to_branch add column if not exists unit_cost     text;
alter table stock_in_to_branch add column if not exists total_cost    text;
alter table stock_in_to_branch add column if not exists recorded_by   text;
alter table stock_in_to_branch add column if not exists remark        text;

create table if not exists production_batches (
    batch_id text primary key,
    production_date text,
    finished_flour_big_bag text, finished_flour_small_bag text,
    ingredient_mix_big_bag text, ingredient_mix_small_bag text,
    produced_by text, remark text
);
alter table production_batches add column if not exists production_date          text;
alter table production_batches add column if not exists finished_flour_big_bag   text;
alter table production_batches add column if not exists finished_flour_small_bag text;
alter table production_batches add column if not exists ingredient_mix_big_bag   text;
alter table production_batches add column if not exists ingredient_mix_small_bag text;
alter table production_batches add column if not exists produced_by              text;
alter table production_batches add column if not exists remark                   text;

create table if not exists production_material_used (
    production_used_id text primary key,
    batch_id text, item_id text, qty_used text,
    unit text, unit_cost text, total_cost text
);
alter table production_material_used add column if not exists batch_id   text;
alter table production_material_used add column if not exists item_id    text;
alter table production_material_used add column if not exists qty_used   text;
alter table production_material_used add column if not exists unit       text;
alter table production_material_used add column if not exists unit_cost  text;
alter table production_material_used add column if not exists total_cost text;

-- items: เผื่อคอลัมน์ที่ฝ่ายจัดซื้อใช้ยังไม่ครบ
alter table items add column if not exists standard_cost text;
alter table items add column if not exists min_stock     text;
alter table items add column if not exists is_active     text;

-- ===== ยอดขายฝ่ายการตลาด (marketing daily sales) — แก้ error PGRST204 branch_id =====
create table if not exists marketing_daily_sales (
    marketing_sales_id text primary key,
    sales_date text, branch_id text, channel_id text,
    created_by text, total_sales text, remark text
);
alter table marketing_daily_sales add column if not exists sales_date  text;
alter table marketing_daily_sales add column if not exists branch_id   text;
alter table marketing_daily_sales add column if not exists channel_id  text;
alter table marketing_daily_sales add column if not exists created_by  text;
alter table marketing_daily_sales add column if not exists total_sales text;
alter table marketing_daily_sales add column if not exists remark      text;

create table if not exists marketing_daily_sales_items (
    marketing_sales_item_id text primary key,
    marketing_sales_id text, product_id text,
    qty_sold text, unit_price text, total_amount text
);
alter table marketing_daily_sales_items add column if not exists marketing_sales_id text;
alter table marketing_daily_sales_items add column if not exists product_id   text;
alter table marketing_daily_sales_items add column if not exists qty_sold     text;
alter table marketing_daily_sales_items add column if not exists unit_price   text;
alter table marketing_daily_sales_items add column if not exists total_amount text;

create table if not exists sales_reconcile (
    reconcile_id text primary key,
    sales_date text, branch_id text,
    branch_report_id text, accounting_sales_id text, marketing_sales_id text,
    branch_total_sales text, accounting_total_sales text, marketing_total_sales text,
    diff_branch_accounting text, diff_branch_marketing text, diff_accounting_marketing text,
    status text, remark text
);
alter table sales_reconcile add column if not exists branch_id  text;
alter table sales_reconcile add column if not exists sales_date text;

alter table stock_movements         disable row level security;
alter table purchase_orders         disable row level security;
alter table purchase_order_items    disable row level security;
alter table stock_in_to_branch      disable row level security;
alter table production_batches      disable row level security;
alter table production_material_used disable row level security;
alter table marketing_daily_sales       disable row level security;
alter table marketing_daily_sales_items disable row level security;
alter table sales_reconcile             disable row level security;

-- รีเฟรช schema cache ของ PostgREST (แก้ error PGRST204 ที่มองไม่เห็นคอลัมน์ใหม่)
notify pgrst, 'reload schema';

-- ปิด Row Level Security (ให้ระบบเข้าถึงได้เหมือนตารางอื่น ๆ)
alter table branch_login          disable row level security;
alter table coupons               disable row level security;
alter table branch_sales          disable row level security;
alter table branch_sales_coupons  disable row level security;
alter table branch_sales_slips    disable row level security;
alter table branch_sales_delivery disable row level security;
alter table branch_stock_daily    disable row level security;
alter table audit_stock_balance   disable row level security;
alter table material_daily        disable row level security;
alter table material_cost         disable row level security;

-- หมายเหตุ: รหัสผ่านของสาขา ระบบจะ seed ให้อัตโนมัติในการเปิดแอปครั้งแรก
--          (ดึงจากไฟล์ modules/branch_auth.py) — ไม่ต้อง insert ที่นี่

-- ============================================================
-- SAFETY: เพิ่มคอลัมน์ Primary Key ที่ create-if-not-exists อาจข้ามไป
-- (กัน error PGRST204 กับตารางเก่าที่มีอยู่แล้วแต่ขาดคอลัมน์ PK)
-- ============================================================
alter table stock_movements          add column if not exists stock_movement_id       text;
alter table purchase_orders          add column if not exists purchase_id              text;
alter table purchase_order_items     add column if not exists purchase_item_id         text;
alter table stock_in_to_branch       add column if not exists stock_in_id              text;
alter table production_batches       add column if not exists batch_id                 text;
alter table production_material_used add column if not exists production_used_id        text;
alter table marketing_daily_sales       add column if not exists marketing_sales_id      text;
alter table marketing_daily_sales_items add column if not exists marketing_sales_item_id text;
alter table sales_reconcile          add column if not exists reconcile_id             text;
notify pgrst, 'reload schema';
-- ============================================================
-- roon_asset_tables.sql
-- ตารางสำหรับเมนู 'ซ่อมบำรุงทรัพย์สิน' (ฝ่ายจัดซื้อ)
--   assets        = ข้อมูลทรัพย์สิน (ผูกกับ item ที่ประเภทการซื้อ = ทรัพย์สิน)
--   asset_repairs = ประวัติการส่งซ่อม
-- ปลอดภัย: create-if-not-exists + add column if not exists (รันซ้ำได้)
--          เปิด RLS ไว้ (แอปใช้ service_role key จึงทำงานได้ปกติ)
-- ============================================================

create table if not exists assets (
    asset_id text primary key,
    item_id text, item_name text, purchase_date text, brand text,
    spec text, seller text, seller_phone text, serial text, created_at text
);
alter table assets add column if not exists asset_id      text;
alter table assets add column if not exists item_id       text;
alter table assets add column if not exists item_name     text;
alter table assets add column if not exists purchase_date text;
alter table assets add column if not exists brand         text;
alter table assets add column if not exists spec          text;
alter table assets add column if not exists seller        text;
alter table assets add column if not exists seller_phone  text;
alter table assets add column if not exists serial        text;
alter table assets add column if not exists created_at    text;

create table if not exists asset_repairs (
    repair_id text primary key,
    item_id text, send_date text, symptom text,
    repair_shop text, repair_shop_phone text,
    repairer_name text, repairer_phone text,
    how_repaired text, return_date text, status text,
    created_at text, updated_at text
);
alter table asset_repairs add column if not exists repair_id         text;
alter table asset_repairs add column if not exists item_id           text;
alter table asset_repairs add column if not exists send_date         text;
alter table asset_repairs add column if not exists symptom           text;
alter table asset_repairs add column if not exists repair_shop       text;
alter table asset_repairs add column if not exists repair_shop_phone text;
alter table asset_repairs add column if not exists repairer_name     text;
alter table asset_repairs add column if not exists repairer_phone    text;
alter table asset_repairs add column if not exists how_repaired      text;
alter table asset_repairs add column if not exists return_date       text;
alter table asset_repairs add column if not exists status            text;
alter table asset_repairs add column if not exists created_at        text;
alter table asset_repairs add column if not exists updated_at        text;

-- items: เผื่อยังไม่มีคอลัมน์ประเภทการซื้อ (ใช้แยกว่าเป็น 'ทรัพย์สิน')
alter table items add column if not exists purchase_category text;

-- เปิด RLS (ปลอดภัย — service_role ข้าม RLS อยู่แล้ว)
alter table assets        enable row level security;
alter table asset_repairs enable row level security;

notify pgrst, 'reload schema';
-- ============================================================
-- roon_bank_tables.sql
-- แก้ error PGRST204: Could not find the 'bank_account_id' column of 'bank_accounts'
-- สร้างตารางบัญชีธนาคาร + รายการเดินบัญชี (ยอดเงินประจำวัน)
-- ปลอดภัย: create-if-not-exists + add column if not exists (รันซ้ำได้) • เปิด RLS
-- ============================================================

create table if not exists bank_accounts (
    bank_account_id text primary key,
    bank_name text, bank_branch text, account_no text,
    account_name text, current_balance text, is_active text
);
alter table bank_accounts add column if not exists bank_account_id text;
alter table bank_accounts add column if not exists bank_name       text;
alter table bank_accounts add column if not exists bank_branch     text;
alter table bank_accounts add column if not exists account_no      text;
alter table bank_accounts add column if not exists account_name    text;
alter table bank_accounts add column if not exists current_balance text;
alter table bank_accounts add column if not exists is_active       text;

create table if not exists bank_transactions (
    transaction_id text primary key,
    transaction_date text, bank_account_id text,
    deposit_amount text, deposit_detail text,
    withdraw_amount text, withdraw_detail text,
    balance_after text, remark text
);
alter table bank_transactions add column if not exists transaction_id   text;
alter table bank_transactions add column if not exists transaction_date text;
alter table bank_transactions add column if not exists bank_account_id  text;
alter table bank_transactions add column if not exists deposit_amount   text;
alter table bank_transactions add column if not exists deposit_detail   text;
alter table bank_transactions add column if not exists withdraw_amount  text;
alter table bank_transactions add column if not exists withdraw_detail  text;
alter table bank_transactions add column if not exists balance_after    text;
alter table bank_transactions add column if not exists remark           text;

-- branches: เก็บเลขที่บัญชีธนาคารที่ใช้รับเงินของสาขา (แทนช่องหมายเหตุเดิม)
alter table branches add column if not exists bank_account_no text;

alter table bank_accounts     enable row level security;
alter table bank_transactions enable row level security;

notify pgrst, 'reload schema';
-- ============================================================
-- roon_sale_audit_tables.sql
-- ตารางสำหรับแอป Sale Audit
--   sale_bank_income  = ยอดเงินขายเข้าธนาคารรายวัน/สาขา (เมนู ①)
--   sale_audit_config = ตั้งค่า (รหัสผ่านเมนู 1.1)
--   + เพิ่มคอลัมน์ 'บรรจุภัณฑ์เสียหาย' และรูปภาพ ใน audit_stock_balance
-- ปลอดภัย: รันซ้ำได้ • เปิด RLS (แอปใช้ service_role key)
-- ============================================================

create table if not exists sale_bank_income (
    income_id text primary key,
    sale_date text, branch_id text, branch_group_id text,
    bank_account_no text, amount text, entered_by text,
    created_at text, updated_at text
);
alter table sale_bank_income add column if not exists income_id       text;
alter table sale_bank_income add column if not exists sale_date       text;
alter table sale_bank_income add column if not exists branch_id       text;
alter table sale_bank_income add column if not exists branch_group_id text;
alter table sale_bank_income add column if not exists bank_account_no text;
alter table sale_bank_income add column if not exists amount          text;
alter table sale_bank_income add column if not exists entered_by      text;
alter table sale_bank_income add column if not exists created_at      text;
alter table sale_bank_income add column if not exists updated_at      text;

create table if not exists sale_audit_config (
    config_key text primary key,
    config_value text
);
alter table sale_audit_config add column if not exists config_value text;
-- รหัสผ่านเริ่มต้นเมนู 1.1 (เปลี่ยนได้ภายหลัง)
insert into sale_audit_config (config_key, config_value)
  values ('mall_password', 'roon-mall')
  on conflict (config_key) do nothing;

-- บรรจุภัณฑ์เสียหาย/แตกหัก + รูปภาพ (กรอกที่แอปฝ่ายตรวจสอบนับ)
alter table audit_stock_balance add column if not exists dmg_plastic_box_qty       text;
alter table audit_stock_balance add column if not exists dmg_paper_bag_qty         text;
alter table audit_stock_balance add column if not exists dmg_printed_carry_bag_qty text;
alter table audit_stock_balance add column if not exists dmg_water_cup_qty         text;
alter table audit_stock_balance add column if not exists dmg_ice_cream_cup_qty     text;
alter table audit_stock_balance add column if not exists damage_photo              text;

alter table sale_bank_income  enable row level security;
alter table sale_audit_config enable row level security;

notify pgrst, 'reload schema';
-- ============================================================
-- roon_sale_audit_resolution.sql
-- ตารางบันทึก 'ชี้แจงการแก้ปัญหา DIFF' โดยฝ่าย Audit (Sale Audit เมนู ④)
--   แก้ไขได้ · ลบไม่ได้ · แนบรูปได้ 5 รูป
-- ปลอดภัย: create-if-not-exists + add column if not exists (รันซ้ำได้) • เปิด RLS
-- ============================================================

create table if not exists sale_audit_resolution (
    resolution_id text primary key,
    sale_date text, branch_id text,
    called_who text, call_time text, call_date text, how_fixed text,
    photo1 text, photo2 text, photo3 text, photo4 text, photo5 text,
    created_by text, created_at text, updated_at text
);
alter table sale_audit_resolution add column if not exists resolution_id text;
alter table sale_audit_resolution add column if not exists sale_date  text;
alter table sale_audit_resolution add column if not exists branch_id  text;
alter table sale_audit_resolution add column if not exists called_who text;
alter table sale_audit_resolution add column if not exists call_time  text;
alter table sale_audit_resolution add column if not exists call_date  text;
alter table sale_audit_resolution add column if not exists how_fixed  text;
alter table sale_audit_resolution add column if not exists photo1     text;
alter table sale_audit_resolution add column if not exists photo2     text;
alter table sale_audit_resolution add column if not exists photo3     text;
alter table sale_audit_resolution add column if not exists photo4     text;
alter table sale_audit_resolution add column if not exists photo5     text;
alter table sale_audit_resolution add column if not exists created_by text;
alter table sale_audit_resolution add column if not exists created_at text;
alter table sale_audit_resolution add column if not exists updated_at text;

alter table sale_audit_resolution enable row level security;

notify pgrst, 'reload schema';

-- ============================================================
-- ROUND 3 (19/8/2026) — คอลัมน์ใหม่ทั้งหมด (idempotent)
-- ============================================================
alter table coupons add column if not exists expire_date text;

alter table branch_sales_delivery add column if not exists clear_box_qty text;
alter table branch_sales_delivery add column if not exists damage_photo  text;
alter table branch_sales_delivery add column if not exists remark        text;

alter table branch_sales add column if not exists egg_damage_qty        text;
alter table branch_sales add column if not exists egg_damage_photo      text;
alter table branch_sales add column if not exists flour_damage_qty      text;
alter table branch_sales add column if not exists flour_damage_photo    text;
alter table branch_sales add column if not exists leftover_damage_qty   text;
alter table branch_sales add column if not exists leftover_damage_photo text;
alter table branch_sales add column if not exists drink_damage_qty      text;
alter table branch_sales add column if not exists drink_damage_photo    text;

notify pgrst, 'reload schema';

-- ============================================================
-- ROUND 2.x (19/8/2026) — product_types + product_packaging (BOM)
-- ============================================================
create table if not exists product_types (
    product_type_id text primary key, product_type_name text, is_active text);
alter table product_types add column if not exists product_type_name text;
alter table product_types add column if not exists is_active text;
alter table product_types enable row level security;
insert into product_types (product_type_id, product_type_name, is_active)
values ('ขนมไข่','ขนมไข่','TRUE'),('เครื่องดื่ม','เครื่องดื่ม','TRUE'),
       ('ของฝาก','ของฝาก','TRUE'),('อื่น ๆ','อื่น ๆ','TRUE')
on conflict (product_type_id) do nothing;

create table if not exists product_packaging (
    bom_id text primary key, product_id text, packaging_field text, qty text);
alter table product_packaging add column if not exists product_id text;
alter table product_packaging add column if not exists packaging_field text;
alter table product_packaging add column if not exists qty text;
alter table product_packaging enable row level security;

notify pgrst, 'reload schema';

-- ============================================================
-- ROUND 20/8/2026 — coupons.approver + sale_audit_correction + branch_front_products
-- ============================================================
alter table coupons add column if not exists approver text;

create table if not exists sale_audit_correction (
    correction_id text primary key, sale_date text, branch_id text,
    bank_account_id text, bank_account_no text, amount text, reason text,
    slip_photo text, entered_by text, created_at text);
alter table sale_audit_correction enable row level security;

create table if not exists branch_front_products (
    id text primary key, sale_id text, branch_id text, product_id text, qty text);
alter table branch_front_products enable row level security;

notify pgrst, 'reload schema';
