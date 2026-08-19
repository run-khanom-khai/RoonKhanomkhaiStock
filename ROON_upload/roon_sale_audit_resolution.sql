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
