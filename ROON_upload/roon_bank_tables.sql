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
