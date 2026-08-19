-- ============================================================
-- roon_branch_groups_update.sql
-- ปรับ "กลุ่มสาขา" (branch_groups) ให้เป็นชื่อมาตรฐาน 9 กลุ่ม + เพิ่ม 'Shopping Mall'
-- และปรับ branch_group_id ในตาราง branches ให้สะกดตรงกัน
-- ปลอดภัย: รันซ้ำได้ • ไม่แตะ RLS
-- ============================================================

create table if not exists branch_groups (
    branch_group_id text primary key,
    branch_group_name text,
    is_active text
);
alter table branch_groups add column if not exists branch_group_name text;
alter table branch_groups add column if not exists is_active         text;

-- ตั้งกลุ่มสาขาใหม่ทั้งหมด (id = ชื่อ) 9 กลุ่ม
delete from branch_groups;
insert into branch_groups (branch_group_id, branch_group_name, is_active) values
  ('Central',         'Central',         'TRUE'),
  ('The Mall',        'The Mall',        'TRUE'),
  ('Seacon',          'Seacon',          'TRUE'),
  ('Market',          'Market',          'TRUE'),
  ('Delivery',        'Delivery',        'TRUE'),
  ('Online',          'Online',          'TRUE'),
  ('Event',           'Event',           'TRUE'),
  ('DepartmentStore', 'DepartmentStore', 'TRUE'),
  ('Shopping Mall',   'Shopping Mall',   'TRUE');   -- ← กลุ่มใหม่ (ยังไม่มีสาขา รอกำหนดเอง)

-- ปรับ branch_group_id ในตาราง branches ให้สะกดตรงกับกลุ่มมาตรฐาน
update branches set branch_group_id = 'DepartmentStore'
  where branch_group_id in ('Department Store', 'Department_Store', 'department store');
update branches set branch_group_id = 'Seacon'
  where branch_group_id in ('SeaCon', 'SEACON', 'seacon', 'Sea Con');
update branches set branch_group_id = 'The Mall'
  where branch_group_id in ('The_Mall', 'THE MALL', 'the mall');
-- (Central / Market / Delivery / Online / Event สะกดตรงอยู่แล้ว ไม่ต้องแก้)

notify pgrst, 'reload schema';
