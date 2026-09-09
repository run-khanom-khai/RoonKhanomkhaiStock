-- ============================================================
-- roon_fix_all.sql  —  แก้ error ทั้งหมดในครั้งเดียว (สั้น ปลอดภัย รันซ้ำได้)
-- วิธีใช้: Supabase → SQL Editor → New query → วางทั้งหมด → Run
-- ทุกคำสั่งเป็น "if not exists" จึงรันซ้ำได้ ไม่ทำข้อมูลเดิมหาย
-- ============================================================

-- ── บัญชีธนาคาร (แก้ error bank_account_id) ─────────────────
create table if not exists bank_accounts (bank_account_id text primary key);
alter table bank_accounts add column if not exists bank_account_id text;
alter table bank_accounts add column if not exists bank_name       text;
alter table bank_accounts add column if not exists bank_branch     text;
alter table bank_accounts add column if not exists account_no      text;
alter table bank_accounts add column if not exists account_name    text;
alter table bank_accounts add column if not exists current_balance text;
alter table bank_accounts add column if not exists is_active       text;
alter table bank_accounts enable row level security;

-- ── ประเภทสินค้า (แก้ error product_types) ──────────────────
create table if not exists product_types (product_type_id text primary key);
alter table product_types add column if not exists product_type_name text;
alter table product_types add column if not exists is_active text;
alter table product_types enable row level security;
insert into product_types (product_type_id, product_type_name, is_active)
values ('ขนมไข่','ขนมไข่','TRUE'),('เครื่องดื่ม','เครื่องดื่ม','TRUE'),
       ('ของฝาก','ของฝาก','TRUE'),('อื่น ๆ','อื่น ๆ','TRUE')
on conflict (product_type_id) do nothing;

-- ── สูตรบรรจุภัณฑ์ต่อสินค้า (BOM) ───────────────────────────
create table if not exists product_packaging (bom_id text primary key);
alter table product_packaging add column if not exists product_id text;
alter table product_packaging add column if not exists packaging_field text;
alter table product_packaging add column if not exists qty text;
alter table product_packaging enable row level security;

-- ── คูปอง: วันหมดอายุ + ผู้อนุมัติ ──────────────────────────
alter table coupons add column if not exists expire_date text;
alter table coupons add column if not exists approver    text;

-- ── เมนู 1.3 เงินคืนจากความผิดพลาดของสาขา ───────────────────
create table if not exists sale_audit_correction (correction_id text primary key);
alter table sale_audit_correction add column if not exists sale_date text;
alter table sale_audit_correction add column if not exists branch_id text;
alter table sale_audit_correction add column if not exists bank_account_id text;
alter table sale_audit_correction add column if not exists bank_account_no text;
alter table sale_audit_correction add column if not exists amount text;
alter table sale_audit_correction add column if not exists reason text;
alter table sale_audit_correction add column if not exists slip_photo text;
alter table sale_audit_correction add column if not exists entered_by text;
alter table sale_audit_correction add column if not exists created_at text;
alter table sale_audit_correction enable row level security;

-- ── ขายหน้าร้านตามประเภทสินค้า ──────────────────────────────
create table if not exists branch_front_products (id text primary key);
alter table branch_front_products add column if not exists sale_id text;
alter table branch_front_products add column if not exists branch_id text;
alter table branch_front_products add column if not exists product_id text;
alter table branch_front_products add column if not exists qty text;
alter table branch_front_products enable row level security;

-- ── บรรจุภัณฑ์ที่ขายได้ (คอลัมน์ใหม่) ───────────────────────
alter table branch_sales_delivery add column if not exists clear_box_qty text;
alter table branch_sales_delivery add column if not exists damage_photo  text;
alter table branch_sales_delivery add column if not exists remark        text;

-- ── ความเสียหาย + รูป (ไข่/แป้ง/ขนมไข่คงเหลือ/เครื่องดื่ม) ───
alter table branch_sales add column if not exists egg_damage_qty        text;
alter table branch_sales add column if not exists egg_damage_photo      text;
alter table branch_sales add column if not exists flour_damage_qty      text;
alter table branch_sales add column if not exists flour_damage_photo    text;
alter table branch_sales add column if not exists leftover_damage_qty   text;
alter table branch_sales add column if not exists leftover_damage_photo text;
alter table branch_sales add column if not exists drink_damage_qty      text;
alter table branch_sales add column if not exists drink_damage_photo    text;

-- ── รีเฟรช schema cache (สำคัญมาก! แก้ error PGRST204/205) ───
notify pgrst, 'reload schema';
