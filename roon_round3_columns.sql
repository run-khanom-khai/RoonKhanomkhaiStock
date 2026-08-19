-- ============================================================
-- roon_round3_columns.sql  —  คอลัมน์ใหม่ทั้งหมดของ "รอบ 3" (19/8/2026)
-- ปลอดภัย: add column if not exists (รันซ้ำได้) • ไม่ลบข้อมูลเดิม
-- รันไฟล์นี้ใน Supabase → SQL Editor ครั้งเดียว
-- ============================================================

-- ── 1) คูปอง / Promotion : วันหมดอายุ (Expire) ──────────────
alter table coupons add column if not exists expire_date text;

-- ── 2) บรรจุภัณฑ์ที่ขายได้ (branch_sales_delivery) ──────────
--     เพิ่มชนิด 'กล่องใส 15 ชิ้น' + รูปของชำรุด + หมายเหตุ
alter table branch_sales_delivery add column if not exists clear_box_qty text;
alter table branch_sales_delivery add column if not exists damage_photo  text;
alter table branch_sales_delivery add column if not exists remark        text;

-- ── 3) ตีแป้ง / ขนมไข่คงเหลือ / เครื่องดื่ม : ความเสียหาย + รูปภาพ ──
alter table branch_sales add column if not exists egg_damage_qty        text;
alter table branch_sales add column if not exists egg_damage_photo      text;
alter table branch_sales add column if not exists flour_damage_qty      text;
alter table branch_sales add column if not exists flour_damage_photo    text;
alter table branch_sales add column if not exists leftover_damage_qty   text;
alter table branch_sales add column if not exists leftover_damage_photo text;
alter table branch_sales add column if not exists drink_damage_qty      text;
alter table branch_sales add column if not exists drink_damage_photo    text;

-- ── 4) สร้างตาราง coupons เผื่อยังไม่มี (idempotent) ────────
create table if not exists coupons (
    coupon_no text primary key,
    amount text, status text, expire_date text,
    used_branch_id text, used_sale_id text, used_at text, issued_at text
);
alter table coupons enable row level security;

notify pgrst, 'reload schema';
