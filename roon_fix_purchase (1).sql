drop table if exists purchase_orders cascade;
create table purchase_orders (
    purchase_id text primary key,
    purchase_date text,
    supplier_name text,
    invoice_no text,
    purchase_category text,
    total_amount text,
    vat_amount text,
    grand_total text,
    created_by text,
    remark text
);

drop table if exists purchase_order_items cascade;
create table purchase_order_items (
    purchase_item_id text primary key,
    purchase_id text,
    item_id text,
    qty text,
    unit_price_inc_vat text,
    total_value text
);

drop table if exists stock_in_to_branch cascade;
create table stock_in_to_branch (
    stock_in_id text primary key,
    stock_in_date text,
    branch_id text,
    item_id text,
    qty_in text,
    unit text,
    unit_cost text,
    total_cost text,
    recorded_by text,
    remark text
);

drop table if exists stock_movements cascade;
create table stock_movements (
    movement_id text primary key,
    movement_date text,
    branch_id text,
    item_id text,
    movement_type text,
    qty_in text,
    qty_out text,
    unit_cost text,
    ref_id text,
    note text,
    created_at text
);

alter table purchase_orders disable row level security;
alter table purchase_order_items disable row level security;
alter table stock_in_to_branch disable row level security;
alter table stock_movements disable row level security;

-- เพิ่ม column yellow_bag ใน branch_front_sales_packaging
alter table branch_front_sales_packaging 
add column if not exists yellow_bag_qty text,
add column if not exists yellow_bag_price text;
