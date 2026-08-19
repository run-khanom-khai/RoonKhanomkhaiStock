-- ═══════════════════════════════════════════════════════════════
-- รีเซ็ตรหัสผ่านเข้าระบบของสาขา (branch_login) — ปรับปรุง 14/8/2026
-- run จากไฟล์ต้นฉบับ ROON_รายชื่อและรหัสแต่ละสาขา (เฉพาะสาขาเปิด 20 สาขา)
-- วิธีใช้: Supabase → SQL Editor → วางทั้งหมด → Run (ลบรหัสเก่าทั้งหมดแล้วใส่ใหม่)
-- ═══════════════════════════════════════════════════════════════
delete from branch_login;

insert into branch_login (branch_id, pw_hash, is_active) values ('BR004', 'e65c6a1d71ed155ddd47d93743a7fce95d317a74f6e4f8392d341d1d959381aa', 'TRUE');  -- เดอะมอลล์ ท่าพระ (รหัส 767118)
insert into branch_login (branch_id, pw_hash, is_active) values ('BR005', '7b71a33c0c9057f0a5e2121f3c9cb522ad257f731c483e84f65ad2c3e5c25464', 'TRUE');  -- เดอะมอลล์ งามวงศ์วาน (รหัส 356686)
insert into branch_login (branch_id, pw_hash, is_active) values ('BR006', '0def927d6c3a1e7f42722b0c6f7e5360ab6f42e8195c6758f72e54d27be11fb1', 'TRUE');  -- เดอะมอลล์ บางแค (รหัส 810575)
insert into branch_login (branch_id, pw_hash, is_active) values ('BR007', 'b79a4a316fd2c25207668a9bbfd6e3e810623e5da1d9a64ac74362f3b86e2744', 'TRUE');  -- เดอะมอลล์ บางกะปิ (รหัส 631693)
insert into branch_login (branch_id, pw_hash, is_active) values ('BR008', 'a3a14daac121c9280b68381d86472ff809696a23fec7c2f1c79480fd15a2ec9b', 'TRUE');  -- Central Lad (รหัส 548223)
insert into branch_login (branch_id, pw_hash, is_active) values ('BR011', 'd25843f8c48527a1df347210f281e103b46a49a2e5dff91363526d1a774ac235', 'TRUE');  -- Central เวสเกต (รหัส 771231)
insert into branch_login (branch_id, pw_hash, is_active) values ('BR012', 'cb7583a2fa00a142c0201a959d3deb335cddb3fe25b750d6e8abed69abec512d', 'TRUE');  -- Future Park รังสิต (รหัส 035533)
insert into branch_login (branch_id, pw_hash, is_active) values ('BR013', '9589bdb71f206834c2f861c4baae9e6396287beaa78afb13f76ade6555fad068', 'TRUE');  -- ซีคอน สแควร์ ศรีนครินทร์ (รหัส 379062)
insert into branch_login (branch_id, pw_hash, is_active) values ('BR014', '552ee15fc07f073bec1a911152babb521b9f8d2abdb216ad737ffeb6ad24aa48', 'TRUE');  -- Fashion Island (รหัส 883692)
insert into branch_login (branch_id, pw_hash, is_active) values ('BR015', 'f0e55a311f560770106654d0baee41df8c74bedb62fd07bf132e4f1d6a40b541', 'TRUE');  -- Central พระราม 9 (รหัส 531445)
insert into branch_login (branch_id, pw_hash, is_active) values ('BR017', 'b57bcdfa63dafc7fc5724412a3d2f1054a63dc7eff843e70336ae9776957373c', 'TRUE');  -- Central ปิ่นเกล้า (รหัส 837981)
insert into branch_login (branch_id, pw_hash, is_active) values ('BR018', 'cd48bf80c86d1fa71094b75837fedc8d581e771ac35eb13685c2f197ab5fb634', 'TRUE');  -- Central หาดใหญ่ (รหัส 905657)
insert into branch_login (branch_id, pw_hash, is_active) values ('BR019', '643aa01402138e797c7ce6de239f505e7aba62712b9a79c543316ea83c207171', 'TRUE');  -- เดอะมอลล์ โคราช (รหัส 672158)
insert into branch_login (branch_id, pw_hash, is_active) values ('BR021', '80a981e780c158cde196dd187f8782601bc55205ed7522c99e05ea5e4e37790a', 'TRUE');  -- ตลาดสดธนบุรี (รหัส 601690)
insert into branch_login (branch_id, pw_hash, is_active) values ('BR022', '2bc7521e7e301d32de51d2fe52c59b7f7be4213aa27a9acf2601ffb7155df167', 'TRUE');  -- ซีคอน บางแค (รหัส 061568)
insert into branch_login (branch_id, pw_hash, is_active) values ('BR024', 'e37736b5a73b7da26dacf89c8d75d528631a45887c98c99b78457f0b0d5b20f5', 'TRUE');  -- Central บางนา (รหัส 839037)
insert into branch_login (branch_id, pw_hash, is_active) values ('BR025', '31cecd1bb3a7546411923a2b52ed25de3f87d4081e1878739fd4d2c8c41eff36', 'TRUE');  -- Central แจ้งวัฒนะ (รหัส 612868)
insert into branch_login (branch_id, pw_hash, is_active) values ('BR026', 'e81a7650c03d1441277789656b53b18d255ae82f6d9d0aed63aaa989ca6b8542', 'TRUE');  -- The Market (รหัส 453840)
insert into branch_login (branch_id, pw_hash, is_active) values ('BR027', '8f5c9d398d92b8ffae334ef88682a6a2edee23a7cb1a9727d1244eace2cef3a9', 'TRUE');  -- Central นอร์ทวิลล์ (รหัส 730059)
insert into branch_login (branch_id, pw_hash, is_active) values ('BR028', '3abfb0295764538513427649af15603b369ef89bced2f4c5e6d7d8337b46c8f3', 'TRUE');  -- The Mall รามคำแหง 1981 (รหัส 542187)
