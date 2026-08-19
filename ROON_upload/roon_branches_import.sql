-- ═══════════════════════════════════════════════════════════════
-- ROON — Import รายชื่อสาขา 20 สาขา (ที่เปิด) ลงตาราง branches
-- ปรับปรุง 14/8/2026 | จากไฟล์ต้นฉบับ ROON_รายชื่อและรหัสแต่ละสาขา
-- วิธีใช้: Supabase → SQL Editor → วางทั้งหมด → Run (ลบ 20 รหัสนี้แล้วใส่ใหม่ = รันซ้ำได้)
-- คอลัมน์: branch_id, branch_name, branch_group_id, area_id, open_date, status, remark
-- ═══════════════════════════════════════════════════════════════

-- เผื่อกรณียังไม่มีตาราง branches — สร้างให้ (ถ้ามีแล้วจะข้าม)
create table if not exists branches (
    branch_id       text primary key,
    branch_name     text,
    branch_group_id text,
    area_id         text,
    open_date       text,
    status          text,
    remark          text
);

delete from branches where branch_id in ('BR004', 'BR005', 'BR006', 'BR007', 'BR008', 'BR011', 'BR012', 'BR013', 'BR014', 'BR015', 'BR017', 'BR018', 'BR019', 'BR021', 'BR022', 'BR024', 'BR025', 'BR026', 'BR027', 'BR028');

insert into branches (branch_id, branch_name, branch_group_id, area_id, open_date, status, remark) values
  ('BR004', 'เดอะมอลล์ ท่าพระ', 'The Mall', 'กรุงเทพ', '', 'active', ''),
  ('BR005', 'เดอะมอลล์ งามวงศ์วาน', 'The Mall', 'กรุงเทพ', '', 'active', ''),
  ('BR006', 'เดอะมอลล์ บางแค', 'The Mall', 'กรุงเทพ', '', 'active', ''),
  ('BR007', 'เดอะมอลล์ บางกะปิ', 'The Mall', 'กรุงเทพ', '', 'active', ''),
  ('BR008', 'Central Lad', 'Central', 'กรุงเทพ', '', 'active', ''),
  ('BR011', 'Central เวสเกต', 'Central', 'กรุงเทพ', '', 'active', ''),
  ('BR012', 'Future Park รังสิต', 'Department Store', 'กรุงเทพ', '', 'active', ''),
  ('BR013', 'ซีคอน สแควร์ ศรีนครินทร์', 'SeaCon', 'กรุงเทพ', '', 'active', ''),
  ('BR014', 'Fashion Island', 'Department Store', 'กรุงเทพ', '', 'active', ''),
  ('BR015', 'Central พระราม 9', 'Central', 'กรุงเทพ', '', 'active', ''),
  ('BR017', 'Central ปิ่นเกล้า', 'Central', 'กรุงเทพ', '', 'active', ''),
  ('BR018', 'Central หาดใหญ่', 'Central', 'สงขลา', '', 'active', ''),
  ('BR019', 'เดอะมอลล์ โคราช', 'The Mall', 'โคราช', '', 'active', ''),
  ('BR021', 'ตลาดสดธนบุรี', 'Market', 'กรุงเทพ', '', 'active', ''),
  ('BR022', 'ซีคอน บางแค', 'SeaCon', 'กรุงเทพ', '', 'active', ''),
  ('BR024', 'Central บางนา', 'Central', 'กรุงเทพ', '', 'active', ''),
  ('BR025', 'Central แจ้งวัฒนะ', 'Central', 'กรุงเทพ', '', 'active', ''),
  ('BR026', 'The Market', 'Market', 'กรุงเทพ', '', 'active', ''),
  ('BR027', 'Central นอร์ทวิลล์', 'Central', 'กรุงเทพ', '', 'active', ''),
  ('BR028', 'The Mall รามคำแหง 1981', 'The Mall', 'กรุงเทพ', '', 'active', '');
