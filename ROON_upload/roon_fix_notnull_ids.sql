-- ============================================================
-- roon_fix_notnull_ids.sql
-- แก้ error: null value in column "movement_id"/"record_id"/... violates not-null constraint (code 23502)
--
-- สาเหตุ: ตารางถูกสร้างด้วยโครงสร้างเก่าที่มีคอลัมน์ id บังคับ NOT NULL
--         (เช่น movement_id, record_id) แต่แอปเวอร์ชันใหม่ใช้ชื่อ id คนละตัว
--         (stock_movement_id, production_used_id, ...) เลยไม่ได้ใส่ค่าคอลัมน์เก่า → null → error
--
-- วิธีแก้ (ไม่ลบข้อมูล): หาคอลัมน์ที่ "บังคับ NOT NULL แต่ไม่มีค่า default"
--         ในตารางธุรกรรมทั้งหมด แล้วตั้ง default ให้เติม uuid อัตโนมัติ
--         → ตอน insert ถ้าแอปไม่ได้ใส่ ระบบจะเติมค่าเองให้ ไม่ติด null อีก
--
-- ปลอดภัย: ไม่ลบ/ไม่แก้ข้อมูลเดิม, ไม่แตะ RLS, รันซ้ำได้
-- ============================================================

do $$
declare
    r record;
begin
    for r in
        select c.table_name, c.column_name
        from information_schema.columns c
        where c.table_schema = 'public'
          and c.table_name in (
              'stock_movements',
              'purchase_orders',
              'purchase_order_items',
              'stock_in_to_branch',
              'production_batches',
              'production_material_used',
              'marketing_daily_sales',
              'marketing_daily_sales_items',
              'sales_reconcile',
              'marketing_pos_reconcile',
              'branch_sales',
              'branch_sales_coupons',
              'branch_sales_slips',
              'branch_sales_delivery',
              'branch_stock_daily',
              'audit_stock_balance',
              'material_daily',
              'material_cost',
              'payroll_periods',
              'payroll_records',
              'employees',
              'petty_cash_requests'
          )
          and c.is_nullable = 'NO'
          and c.column_default is null
    loop
        begin
            execute format(
                'alter table public.%I alter column %I set default gen_random_uuid()::text',
                r.table_name, r.column_name
            );
            raise notice 'set default -> %.%', r.table_name, r.column_name;
        exception when others then
            -- คอลัมน์ที่ตั้ง default ไม่ได้ (เช่นชนิดไม่ใช่ text) ให้ข้ามไป ไม่ทำให้ทั้งสคริปต์ล้ม
            raise notice 'skip %.% : %', r.table_name, r.column_name, sqlerrm;
        end;
    end loop;
end $$;

-- รีเฟรช schema cache ของ PostgREST
notify pgrst, 'reload schema';
