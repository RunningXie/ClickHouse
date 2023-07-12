select name, disk_name, path from system.parts where database='{test_database}' and table='{test_table}' and active order by active;

