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
