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
