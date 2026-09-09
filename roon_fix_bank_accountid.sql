-- ============================================================
-- roon_fix_bank_accountid.sql
-- แก้ error: null value in column "account_id" ... not-null constraint (23502)
-- สาเหตุ: ตาราง bank_accounts มีคอลัมน์เก่า account_id (NOT NULL) ที่แอปใหม่ไม่ได้ใช้
-- วิธีแก้: ให้ account_id เติมค่าอัตโนมัติ (default) เวลาบันทึกแถวใหม่
-- ปลอดภัย รันซ้ำได้
-- ============================================================

-- ให้ account_id เติมค่า id อัตโนมัติเมื่อไม่ได้ระบุ (กัน not-null error)
alter table bank_accounts alter column account_id set default gen_random_uuid()::text;

-- เผื่อคอลัมน์ id/ตัวอื่นที่อาจ NOT NULL ในตารางนี้ ก็ให้มี default ด้วย
-- (ถ้าไม่มีคอลัมน์เหล่านี้ บรรทัดจะไม่ทำอะไร — ไม่ error)
do $$
begin
  begin
    execute 'alter table bank_accounts alter column id set default gen_random_uuid()::text';
  exception when undefined_column then null;
  end;
end $$;

notify pgrst, 'reload schema';
