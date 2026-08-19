-- ============================================================
-- roon_master_bom.sql — ตารางใหม่ ข้อ 2.2 / 2.3 (จัดการกลุ่มสาขา ใช้ตารางเดิม)
--   product_types      : ประเภทสินค้า (แก้ไข/เพิ่มเองได้)
--   product_packaging  : สูตรบรรจุภัณฑ์ต่อสินค้า (BOM)
-- ปลอดภัย: create-if-not-exists + add column if not exists (รันซ้ำได้) • เปิด RLS
-- ============================================================

-- ── ประเภทสินค้า (2.3) ──────────────────────────────────────
create table if not exists product_types (
    product_type_id   text primary key,
    product_type_name text,
    is_active         text
);
alter table product_types add column if not exists product_type_name text;
alter table product_types add column if not exists is_active text;
alter table product_types enable row level security;

-- seed 4 ประเภทเริ่มต้น (ถ้ายังไม่มี)
insert into product_types (product_type_id, product_type_name, is_active)
values ('ขนมไข่','ขนมไข่','TRUE'),
       ('เครื่องดื่ม','เครื่องดื่ม','TRUE'),
       ('ของฝาก','ของฝาก','TRUE'),
       ('อื่น ๆ','อื่น ๆ','TRUE')
on conflict (product_type_id) do nothing;

-- ── สูตรบรรจุภัณฑ์ต่อสินค้า / BOM (2.2) ─────────────────────
--   packaging_field = คอลัมน์บรรจุภัณฑ์ในตารางตรวจนับสต๊อก (เช่น plastic_box_qty)
--   qty             = จำนวนบรรจุภัณฑ์ต่อการขายสินค้า 1 หน่วย
create table if not exists product_packaging (
    bom_id          text primary key,
    product_id      text,
    packaging_field text,
    qty             text
);
alter table product_packaging add column if not exists product_id text;
alter table product_packaging add column if not exists packaging_field text;
alter table product_packaging add column if not exists qty text;
alter table product_packaging enable row level security;

notify pgrst, 'reload schema';
