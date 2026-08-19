-- ============================================================
-- roon_fix_production_used.sql
-- แก้ error: null value in column "record_id" ... violates not-null constraint
-- สาเหตุ: ตาราง production_material_used มีคอลัมน์เก่า record_id (NOT NULL)
--         ที่แอปเวอร์ชันใหม่ไม่ได้ใช้ (แอปใช้ production_used_id เป็น key)
--
-- ตารางนี้เป็น "ตารางลูก" ของการผลิต — เก็บรายการวัตถุดิบที่ใช้ต่อ Batch
-- แอปจะลบ/สร้างใหม่จาก Batch ได้เสมอ จึงสร้างตารางใหม่ให้ตรงกับแอปได้อย่างปลอดภัย
-- (ยังบันทึกไม่เคยสำเร็จเพราะติด error อยู่ จึงไม่มีข้อมูลจริงหาย)
--
-- ปลอดภัย: เปิด RLS ไว้ (แอปเชื่อมด้วย service_role key จึงทำงานได้ปกติ)
-- ============================================================

drop table if exists production_material_used cascade;

create table production_material_used (
    production_used_id text primary key,
    batch_id   text,
    item_id    text,
    qty_used   text,
    unit       text,
    unit_cost  text,
    total_cost text
);

-- เปิด Row Level Security (คงความปลอดภัยเหมือนตารางอื่น)
alter table production_material_used enable row level security;

-- รีเฟรช schema cache ของ PostgREST
notify pgrst, 'reload schema';
