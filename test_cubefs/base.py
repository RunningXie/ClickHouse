import pytest
import yaml
import os
import time
import uuid
import logging
import csv
from kazoo.client import KazooClient
from datetime import datetime
from clickhouse_driver import Client

TTL_TIMEOUT_SECURETY_INTERVAL_SECONDS=8
SLEEP_DEFAULT_INTERVAL_SECONDS=5

TABLE_DEFAULT_TTL_SECONDS=30

class Timer():
	def init(self, time):
		self.time = time + TTL_TIMEOUT_SECURETY_INTERVAL_SECONDS
		self.passed_time = 0

	def sleep(self, seconds=SLEEP_DEFAULT_INTERVAL_SECONDS):
		self.passed_time += seconds
		print("\033[32m**sleep for %s sconds**\033[0m"%seconds)
		time.sleep(seconds)

	def sleep_until_ttl_timeout(self):
		if self.passed_time >= self.time:
			sleep_time = TTL_TIMEOUT_SECURETY_INTERVAL_SECONDS
		elif (self.time - self.passed_time) > (TTL_TIMEOUT_SECURETY_INTERVAL_SECONDS * 2):
			sleep_time = self.time - self.passed_time
		else:
			sleep_time = TTL_TIMEOUT_SECURETY_INTERVAL_SECONDS * 2
		self.sleep(sleep_time)

class Base():
	def getClient(self, host, port, user, password,  client_name='python test', db='default'):
		settings = {'input_format_null_as_default': True}
		return Client(host=host, user=user, password=password, port=port, database=db, client_name=client_name, settings=settings)

	def connect(self):
		self.client1 = self.getNewClient(self.replica1_ip)
		self.client2 = self.getNewClient(self.replica2_ip)

	def getNewClient(self, host):
		return self.getClient(host, self.port, self.user, self.password, self.test_table_name)

	def client1(self):
		return self.client1

	def client2(self):
		return self.client2

	def get_replica1_host(self):
		return self.replica1_ip;

	def get_replica2_host(self):
		return self.replica2_ip;

	def timer(self):
		return self.timer

	def disconnect(self):
		self.client1.disconnect()
		self.client2.disconnect()

	def init(self, test_name='test_for_cubefs', ttl_timeout=TABLE_DEFAULT_TTL_SECONDS, database='default'):
		stream = open('config.yaml', 'r')
		data = yaml.load(stream, Loader=yaml.FullLoader)
		print (data)
		stream.close()

		self.client = data['clickhouse']['client_path']
		self.replica1_ip = data['cluster']['replica1_ip']
		self.replica2_ip = data['cluster']['replica2_ip']
		self.port = data['cluster']['port']
		self.user = data['cluster']['user']
		self.password = data['cluster']['password']
		self.table_uuid = uuid.uuid4()
		self.test_db =database
		self.test_table_name = test_name
		self.sql_path_prefix = data['tests']['sql_path_prefix']
		self.ttl_timeout = ttl_timeout
		self.zk_cluster = data['cluster']['zookeeper']
		self.zk_client = KazooClient(hosts=self.zk_cluster, timeout=100)
		self.zk_client.start()
		self.connect()
		self.timer = Timer()
		self.timer.init(self.ttl_timeout)
#		self.drop_table_if_exist(self.client1)
#		self.drop_table_if_exist(self.client2)

	def uninit(self):
		self.disconnect()

	def log(self, msg):
		cur_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
		print("%s %s"%(cur_time, msg))

	def executeQuery(self, client, query):
		host, port = client.connection.hosts[0]
		query_id=uuid.uuid1()
		self.log("host[%s:%s] query[%s] execute: { %s }" % (host, port, query_id, query))
		res = client.execute(query=query, query_id=str(query_id))
		self.log("host[%s:%s] query[%s] results: { %s }" % (host, port, query_id, res))
		return res

	def __executeSQLFile(self, client, file_name, rent_dict={}):
		# common param to rent
		file_path = os.path.join('%s/%s' % (self.sql_path_prefix, file_name))
		return self.executeSQLFile(client, file_path, rent_dict)

	def executeSQLFile(self, client, file_name, rent_dict={}):
		# common param to rent
		rent_dict["{test_database}"] = self.test_db
		rent_dict["{test_table}"] = self.test_table_name

		if os.path.exists(file_name):
			sql_file = open(file_name, 'r')
			content = sql_file.read().strip()
			for k, v in rent_dict.items():
				content = content.replace(k, v)
			sql_file.close()
			return self.executeQuery(client, content)
		else:
			print ('SQL file {%s} not exists.' % file_name)

	def __create_table(self, client, table_ttl_second):
		rent_dict = dict()
		rent_dict["{table_uuid}"] = str(self.table_uuid)
		rent_dict["{table_ttl_second}"] = str(table_ttl_second)
		return self.__executeSQLFile(client, "table.sql", rent_dict)

	def create_table(self, client):
		try:
			self.__create_table(client, self.ttl_timeout)
		except:
			print("****** create table exception, drop table then create it ******")
			self.drop_table_if_exist(client)
			time.sleep(TTL_TIMEOUT_SECURETY_INTERVAL_SECONDS)
			self.__create_table(client, self.ttl_timeout)
		else:
			time.sleep(TTL_TIMEOUT_SECURETY_INTERVAL_SECONDS)

	def truncate_table(self, client):
		return self.__executeSQLFile(client, "truncate.sql")

	def drop_table_if_exist(self, client):
		self.__drop_table_if_exist(client)

	def __drop_table_if_exist(self, client):
		res = self.__executeSQLFile(client, "drop.sql")
		time.sleep(TTL_TIMEOUT_SECURETY_INTERVAL_SECONDS * 2)
		return res

	def insert_one_row_for_loop(self, client, times):
		for i in range(int(times)):
			self.__executeSQLFile(client, "insert.sql")

	def insert_one_thousand_row_for_loop(self, client, times):
		for i in range(int(times)):
			self.__executeSQLFile(client, "batch_insert.sql")




	def optimize_table(self, client):
		self.__executeSQLFile(client, "optimize.sql")

	def truncate_table(self, client):
		return self.__executeSQLFile(client, "truncate.sql")

	def delete_table(self, client, pred_expr):
		rent_dict = dict()
		rent_dict["{pred_expr}"] = pred_expr
		return self.__executeSQLFile(client, "delete.sql", rent_dict)

	def attach_table(self, client):
		return self.__executeSQLFile(client, "attach_table.sql")

	def detach_table(self, client):
		return self.__executeSQLFile(client, "detach_table.sql")


	def alter_part(self, client, part_id, action, sleep=True):
		rent_dict = dict()
		rent_dict["{part_id}"] = part_id
		rent_dict["{action}"] = action
		res = self.__executeSQLFile(client, "alter_part.sql", rent_dict)
		if sleep :
			self.timer.sleep()
		return res

	def detach_part(self, client, part_id):
		return self.alter_part(client, part_id, "detach")

	def attach_part(self, client, part_id):
		return self.alter_part(client, part_id, "attach")

	def drop_part(self, client, part_id):
		return self.alter_part(client, part_id, "drop")


	def update_partition(self, client, partition_id, pred_expr, update_expr):
		return self.alter_in_partition(client, "update", partition_id, pred_expr, update_expr)

	def delete_partition(self, client, partition_id, pred_expr):
		return self.alter_in_partition(client, "delete", partition_id, pred_expr)

	def alter_column(self, client, action, column_name, default_expr="", sleep=True):
		rent_dict = dict()
		rent_dict["{column_name}"] = column_name
		rent_dict["{default_expr}"] = default_expr
		rent_dict["{action}"] = action
		res = self.__executeSQLFile(client, "alter_column.sql", rent_dict)
		if sleep :
			self.timer.sleep()
		return res

	def add_column(self, client, column_name, type, default_expr):
		return self.alter_column(client, "add", column_name, "%s DEFAULT %s"%(type, default_expr))

	def drop_column(self, client, column_name):
		return self.alter_column(client, "drop", column_name)

	def modify_column(self, client, column_name, type, default_expr):
		return self.alter_column(client, "modify", column_name, "%s DEFAULT %s"%(type, default_expr))

	def alter_in_partition(self, client, action, partition_id, pred_expr, update_expr='', sleep=True):
		rent_dict = dict()
		rent_dict["{action}"] = action
		rent_dict["{partition_id}"] = partition_id
		rent_dict["{update_expr}"] = update_expr
		rent_dict["{pred_expr}"] = pred_expr
		res = self.__executeSQLFile(client, "alter_in_partition.sql", rent_dict)
		if sleep :
			self.timer.sleep()
		return res

	def alter_partition(self, client, partition_id, value, sleep=True):
		rent_dict = dict()
		rent_dict["{partition_id}"] = partition_id
		rent_dict["{action}"] = value
		res = self.__executeSQLFile(client, "alter_partition.sql", rent_dict)
		if sleep :
			self.timer.sleep()
		return res

	def detach_partition(self, client, partition_id):
		return self.alter_partition(client, partition_id, "detach")

	def attach_partition(self, client, partition_id):
		return self.alter_partition(client, partition_id, "attach")

	def drop_partition(self, client, partition_id):
		return self.alter_partition(client, partition_id, "drop")

	def get_small_size_partition_id(self, client):
		data = self.__executeSQLFile(client, "get_small_size_partition_id.sql")
		assert len(data) == 1
		row = data[0]
		return row[0]

	def get_small_size_part_id(self, client):
		data = self.__executeSQLFile(client, "get_small_size_part_id.sql")
		assert len(data) == 1
		row = data[0]
		return row[0]

	def get_unfinished_mutations(self, client):
		data = self.__executeSQLFile(client, "count_unfinished_mutations.sql")
		assert len(data) == 1
		row = data[0]
		return row[0]

	def wait_for_all_mutations_finished(self, client):
		retry_times = 0
		while retry_times < 10:
			count = self.get_unfinished_mutations(client)
			if count == 0:
				break
			else:
				self.timer.sleep()
				retry_times += 1

	def __do_system_action(self, client, action):
		rent_dict = dict()
		rent_dict["{action}"] = action
		return self.__executeSQLFile(client, "system.sql", rent_dict)

	def start_move(self, client):
		self.__do_system_action(client, "START MOVES")
		self.timer.sleep_until_ttl_timeout()

	def stop_move(self, client):
		self.__do_system_action(client, "STOP MOVES")

	def start_fetch(self, client):
		self.__do_system_action(client, "START FETCHES")
		self.timer.sleep()

	def stop_fetch(self, client):
		self.__do_system_action(client, "STOP FETCHES")

	def start_merge(self, client):
		self.__do_system_action(client, "START MERGES")
		self.optimize_table(client)
		self.timer.sleep()

	def stop_merge(self, client):
		self.__do_system_action(client, "STOP MERGES")

	def start_send(self, client):
		self.__do_system_action(client, "START REPLICATED SENDS")

	def stop_send(self, client):
		self.__do_system_action(client, "STOP REPLICATED SENDS")

	def get_record_count(self, client, pred_conditions='1=1'):
		rent_dict = dict()
		rent_dict["{pred_conditions}"] = pred_conditions
		data = self.__executeSQLFile(client, "count.sql", rent_dict)
		assert len(data) == 1
		row = data[0]
		return row[0]

	def is_table_exist(self, client):
		rent_dict = dict()
		rent_dict["{table_uuid}"] = str(self.table_uuid)
		return 1 == self.__executeSQLFile(client, "table_exists.sql", rent_dict)

	def get_part_count(self, client):
		data = self.__executeSQLFile(client, "part_count.sql")
		assert len(data) == 1
		row = data[0]
		return row[0]

	def get_column_count(self, client):
		data = self.__executeSQLFile(client, "get_column_count.sql")
		assert len(data) == 1
		row = data[0]
		return row[0]

	def get_part_info(self, client):
		return self.__executeSQLFile(client, "part.sql")

	def assert_table_exist(self, client, expect):
		exist = self.is_table_exist(client)
		if expect != exist:
			pytest.fail("table exists actual[" + str(exist) +"], expect[" + str(expect) +"]")

	def assert_record_count(self, client, expect_record_count):
		rows = self.get_record_count(client)
		if rows != expect_record_count:
			pytest.fail("table record count actual[" + str(rows) +"], expect[" + str(expect_record_count) +"]")
			input('press any key to exit')

	def assert_part_count(self, client, expect_part_count):
		part_count = self.get_part_count(client)
		if part_count != expect_part_count:
			pytest.fail("table part count actual[" + str(part_count) +"], expect[" + str(expect_part_count) +"]")

	def assert_part_info(self, part_info, expect_disk_name):
		part_name = part_info[0]
		disk_name = part_info[1]
		path = part_info[2]
		if disk_name != expect_disk_name:
			pytest.fail("part disk_name actual[" + disk_name +"], expect[" + expect_disk_name +"]")

	def assert_table_state(self, client, expect_record_count, expect_part_count, expect_disk_name):
		data = self.get_part_info(client)
		for row in data:
			self.assert_part_info(row, expect_disk_name)
		self.assert_part_count(client, expect_part_count)
		self.assert_record_count(client, expect_record_count)


	def forbidden_all_actions(self, client):
		self.stop_fetch(client)
		self.stop_move(client)
		self.stop_merge(client)
		self.stop_send(client)

	def restore_all_actions(self, client):
		self.start_fetch(client)
		self.start_move(client)
		self.start_merge(client)
		self.start_send(client)

	def restart_replica(self, client):
		self.__do_system_action(client, "RESTART REPLICA")
		self.timer.sleep(60)


	def restart_server(self, client):
		try:
			self.executeQuery(client, "SYSTEM KILL;")
		except:
			self.timer.sleep(60)
		self.timer.sleep(120)
		self.disconnect()
		self.connect()

	def stop_replication_queue(self, client):
		self.__do_system_action(client, "STOP REPLICATION QUEUES")

	def start_replication_queue(self, client):
		self.__do_system_action(client, "START REPLICATION QUEUES")

	def check_table(self, client):
		self.__executeSQLFile(client, "check_table.sql")
		self.timer.sleep()

	def get_replica_path(self, client):
		data = self.__executeSQLFile(client, "get_replica_path.sql")
		assert len(data) == 1
		row = data[0]
		return row[0]

	def delete_zk_part(self, client):
		replica_path = self.get_replica_path(client)
		part_path = replica_path + "/parts"
		nodes = self.zk_client.get_children(part_path)
		for node in nodes:
			node_path = part_path + "/" + node
			self.zk_client.delete(node_path, recursive=True)
			self.log("delete zk path : %s" % node_path)
		self.timer.sleep(20)
