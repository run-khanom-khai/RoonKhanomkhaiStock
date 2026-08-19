-- ═══════════════════════════════════════════════════════════════
-- ROON KHANOMKHAI — แก้ความปลอดภัย Supabase (RLS + Function hardening)
-- ปรับปรุง 14/8/2026
--
-- ‼️ ต้องทำ "หลังจาก" สลับให้ทุกแอปใช้ service_role key ใน Streamlit Secrets แล้วเท่านั้น
--    (service_role ข้าม RLS อัตโนมัติ → เปิด RLS แล้วแอปยังทำงานได้เต็มที่
--     ส่วน anon/publishable key ที่รั่ว จะเข้าถึงข้อมูลไม่ได้อีกต่อไป)
--
-- วิธีใช้: Supabase → SQL Editor → วางทั้งหมด → Run (รันซ้ำได้ ปลอดภัย)
-- ═══════════════════════════════════════════════════════════════

-- ───────────────────────────────────────────────────────────────
-- 1) เปิด Row Level Security (RLS) ให้ "ทุกตาราง" ใน schema public
--    ไม่มี policy = ปฏิเสธ anon/authenticated ทั้งหมด (deny-all)
--    service_role (ที่แอปใช้) ข้าม RLS จึงยังอ่าน/เขียนได้ตามปกติ
-- ───────────────────────────────────────────────────────────────
do $$
declare
    r record;
begin
    for r in
        select tablename
        from pg_tables
        where schemaname = 'public'
    loop
        execute format('alter table public.%I enable row level security;', r.tablename);
    end loop;
end $$;

-- ───────────────────────────────────────────────────────────────
-- 2) ปิดสิทธิ์ตรง ๆ ของ anon / authenticated (กันชั้นที่สอง)
--    PostgREST ใช้ role เหล่านี้ผ่าน publishable/anon key
--    เมื่อ revoke แล้ว แม้ในอนาคตมีคน enable policy ผิดพลาด ก็ยังไม่รั่ว
--    (service_role เป็นเจ้าของสิทธิ์ระดับ superuser-bypass จึงไม่กระทบ)
-- ───────────────────────────────────────────────────────────────
do $$
declare
    r record;
begin
    for r in
        select tablename
        from pg_tables
        where schemaname = 'public'
    loop
        execute format('revoke all on table public.%I from anon, authenticated;', r.tablename);
    end loop;
end $$;

-- ───────────────────────────────────────────────────────────────
-- 3) แก้คำเตือนฟังก์ชัน truncate_table:
--    - กำหนด search_path = '' (กัน search_path hijack)
--    - schema-qualify ทุกชื่อ
--    - จำกัดสิทธิ์เรียกใช้เฉพาะ service_role (แอปฝั่งเซิร์ฟเวอร์)
-- ───────────────────────────────────────────────────────────────
create or replace function public.truncate_table(tbl text)
returns void
language plpgsql
security definer
set search_path = ''
as $$
begin
    execute format('truncate table public.%I;', tbl);
end;
$$;

revoke all on function public.truncate_table(text) from public, anon, authenticated;
grant execute on function public.truncate_table(text) to service_role;

-- ───────────────────────────────────────────────────────────────
-- 4) ตรวจผล (ไม่บังคับ) — ดูว่าทุกตารางเปิด RLS แล้ว
--    ควรได้ rls_enabled = true ทุกแถว
-- ───────────────────────────────────────────────────────────────
-- select tablename, rowsecurity as rls_enabled
-- from pg_tables where schemaname = 'public' order by tablename;
