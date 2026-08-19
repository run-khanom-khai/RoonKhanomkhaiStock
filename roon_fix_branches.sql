drop table if exists branches cascade;
create table branches (
    branch_id text primary key,
    branch_name text,
    branch_group_id text,
    area_id text,
    open_date text,
    status text,
    remark text
);
alter table branches disable row level security;
