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
