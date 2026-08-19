-- ============================================================
-- roon_round4_columns.sql — ตาราง/คอลัมน์ใหม่ "รอบ 20/8/2026"
-- ปลอดภัย: create-if-not-exists + add column if not exists (รันซ้ำได้) • เปิด RLS
-- ============================================================

-- ── A) คูปอง: เพิ่มผู้อนุมัติ ──────────────────────────────
alter table coupons add column if not exists approver text;

-- ── B) เมนู 1.3 เงินคืนจากความผิดพลาดของสาขา ────────────────
create table if not exists sale_audit_correction (
    correction_id text primary key,
    sale_date text, branch_id text,
    bank_account_id text, bank_account_no text,
    amount text, reason text, slip_photo text,
    entered_by text, created_at text
);
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

-- ── C) ขายหน้าร้านตามประเภทสินค้า (แตกบรรจุภัณฑ์ตาม BOM) ─────
create table if not exists branch_front_products (
    id text primary key,
    sale_id text, branch_id text, product_id text, qty text
);
alter table branch_front_products add column if not exists sale_id text;
alter table branch_front_products add column if not exists branch_id text;
alter table branch_front_products add column if not exists product_id text;
alter table branch_front_products add column if not exists qty text;
alter table branch_front_products enable row level security;

notify pgrst, 'reload schema';
