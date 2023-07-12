insert into {test_database}.{test_table} select 'test_cubefs', now(), 'test1000', rand64() from system.numbers limit 1000;
