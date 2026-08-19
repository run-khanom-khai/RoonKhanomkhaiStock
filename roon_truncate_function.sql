-- วางใน SQL Editor ใน Supabase แล้วกด Run
create or replace function truncate_table(tbl text)
returns void language plpgsql as $$
begin
  execute format('truncate table %I', tbl);
end;
$$;
