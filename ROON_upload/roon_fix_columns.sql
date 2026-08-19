-- ============================================================
-- roon_fix_columns.sql
-- แก้ error PGRST204 "Could not find the '...' column" ให้ครบทุกตาราง
-- (เพิ่มคอลัมน์ที่ขาด รวมคอลัมน์ Primary Key ที่ create table if not exists ข้ามไป)
-- ปลอดภัย: ใช้ add column if not exists ทั้งหมด (รันซ้ำได้) และ **ไม่แตะ RLS**
-- ============================================================

-- 1) stock_movements
alter table stock_movements add column if not exists stock_movement_id text;
alter table stock_movements add column if not exists movement_date     text;
alter table stock_movements add column if not exists item_id           text;
alter table stock_movements add column if not exists branch_id         text;
alter table stock_movements add column if not exists movement_type     text;
alter table stock_movements add column if not exists qty_in            text;
alter table stock_movements add column if not exists qty_out           text;
alter table stock_movements add column if not exists unit_cost         text;
alter table stock_movements add column if not exists total_value       text;
alter table stock_movements add column if not exists reference_type    text;
alter table stock_movements add column if not exists reference_id      text;
alter table stock_movements add column if not exists remark            text;

-- 2) purchase_orders
alter table purchase_orders add column if not exists purchase_id       text;
alter table purchase_orders add column if not exists purchase_date     text;
alter table purchase_orders add column if not exists supplier_name     text;
alter table purchase_orders add column if not exists invoice_no        text;
alter table purchase_orders add column if not exists purchase_category text;
alter table purchase_orders add column if not exists total_amount      text;
alter table purchase_orders add column if not exists vat_amount        text;
alter table purchase_orders add column if not exists grand_total       text;
alter table purchase_orders add column if not exists created_by        text;
alter table purchase_orders add column if not exists remark            text;

-- 3) purchase_order_items
alter table purchase_order_items add column if not exists purchase_item_id   text;
alter table purchase_order_items add column if not exists purchase_id        text;
alter table purchase_order_items add column if not exists item_id            text;
alter table purchase_order_items add column if not exists qty                text;
alter table purchase_order_items add column if not exists unit_price_inc_vat text;
alter table purchase_order_items add column if not exists total_value        text;

-- 4) stock_in_to_branch
alter table stock_in_to_branch add column if not exists stock_in_id   text;
alter table stock_in_to_branch add column if not exists stock_in_date text;
alter table stock_in_to_branch add column if not exists branch_id     text;
alter table stock_in_to_branch add column if not exists item_id       text;
alter table stock_in_to_branch add column if not exists qty_in        text;
alter table stock_in_to_branch add column if not exists unit          text;
alter table stock_in_to_branch add column if not exists unit_cost     text;
alter table stock_in_to_branch add column if not exists total_cost    text;
alter table stock_in_to_branch add column if not exists recorded_by   text;
alter table stock_in_to_branch add column if not exists remark        text;

-- 5) production_batches
alter table production_batches add column if not exists batch_id                 text;
alter table production_batches add column if not exists production_date          text;
alter table production_batches add column if not exists finished_flour_big_bag   text;
alter table production_batches add column if not exists finished_flour_small_bag text;
alter table production_batches add column if not exists ingredient_mix_big_bag   text;
alter table production_batches add column if not exists ingredient_mix_small_bag text;
alter table production_batches add column if not exists produced_by              text;
alter table production_batches add column if not exists remark                   text;

-- 6) production_material_used  ← ตัวที่ทำให้เกิด error รอบนี้
alter table production_material_used add column if not exists production_used_id text;
alter table production_material_used add column if not exists batch_id           text;
alter table production_material_used add column if not exists item_id            text;
alter table production_material_used add column if not exists qty_used           text;
alter table production_material_used add column if not exists unit               text;
alter table production_material_used add column if not exists unit_cost          text;
alter table production_material_used add column if not exists total_cost         text;

-- 7) marketing_daily_sales
alter table marketing_daily_sales add column if not exists marketing_sales_id text;
alter table marketing_daily_sales add column if not exists sales_date         text;
alter table marketing_daily_sales add column if not exists branch_id          text;
alter table marketing_daily_sales add column if not exists channel_id         text;
alter table marketing_daily_sales add column if not exists created_by         text;
alter table marketing_daily_sales add column if not exists total_sales        text;
alter table marketing_daily_sales add column if not exists remark             text;

-- 8) marketing_daily_sales_items
alter table marketing_daily_sales_items add column if not exists marketing_sales_item_id text;
alter table marketing_daily_sales_items add column if not exists marketing_sales_id      text;
alter table marketing_daily_sales_items add column if not exists product_id              text;
alter table marketing_daily_sales_items add column if not exists qty_sold                text;
alter table marketing_daily_sales_items add column if not exists unit_price              text;
alter table marketing_daily_sales_items add column if not exists total_amount            text;

-- 9) sales_reconcile
alter table sales_reconcile add column if not exists reconcile_id              text;
alter table sales_reconcile add column if not exists sales_date               text;
alter table sales_reconcile add column if not exists branch_id                text;
alter table sales_reconcile add column if not exists branch_report_id         text;
alter table sales_reconcile add column if not exists accounting_sales_id      text;
alter table sales_reconcile add column if not exists marketing_sales_id       text;
alter table sales_reconcile add column if not exists branch_total_sales       text;
alter table sales_reconcile add column if not exists accounting_total_sales   text;
alter table sales_reconcile add column if not exists marketing_total_sales    text;
alter table sales_reconcile add column if not exists diff_branch_accounting   text;
alter table sales_reconcile add column if not exists diff_branch_marketing    text;
alter table sales_reconcile add column if not exists diff_accounting_marketing text;
alter table sales_reconcile add column if not exists status                   text;
alter table sales_reconcile add column if not exists remark                   text;

-- 10) marketing_pos_reconcile
alter table marketing_pos_reconcile add column if not exists reconcile_id       text;
alter table marketing_pos_reconcile add column if not exists sales_date         text;
alter table marketing_pos_reconcile add column if not exists branch_id          text;
alter table marketing_pos_reconcile add column if not exists channel_id         text;
alter table marketing_pos_reconcile add column if not exists channel_name       text;
alter table marketing_pos_reconcile add column if not exists pos_num_items      text;
alter table marketing_pos_reconcile add column if not exists pos_total          text;
alter table marketing_pos_reconcile add column if not exists branch_cash        text;
alter table marketing_pos_reconcile add column if not exists branch_transfer    text;
alter table marketing_pos_reconcile add column if not exists branch_coupon      text;
alter table marketing_pos_reconcile add column if not exists branch_total       text;
alter table marketing_pos_reconcile add column if not exists pkg_expected_total text;
alter table marketing_pos_reconcile add column if not exists diff_amount        text;
alter table marketing_pos_reconcile add column if not exists diff_flag          text;
alter table marketing_pos_reconcile add column if not exists diff_reason        text;
alter table marketing_pos_reconcile add column if not exists diff_solution      text;
alter table marketing_pos_reconcile add column if not exists created_by         text;
alter table marketing_pos_reconcile add column if not exists created_at         text;
alter table marketing_pos_reconcile add column if not exists updated_at         text;

-- 11) items (เผื่อคอลัมน์ที่ฝ่ายจัดซื้อ/ผลิตใช้)
alter table items add column if not exists standard_cost text;
alter table items add column if not exists selling_cost  text;
alter table items add column if not exists min_stock     text;
alter table items add column if not exists is_active     text;

-- ============================================================
-- รีเฟรช schema cache ของ PostgREST (แก้ error PGRST204)
-- ============================================================
notify pgrst, 'reload schema';
