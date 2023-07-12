from concurrent.futures import ThreadPoolExecutor
from csv import DictReader
from datetime import datetime
from ctypes import *
import re
from threading import Thread
import threading
import pytest
import yaml
import os
import time
import uuid
import base

from clickhouse_driver import Client


class Test_cubefs_for_check_data(base.Base):
	def setup_method(self):
		super().init(self.__class__.__name__, database='mgbench')
		super().executeQuery(super().client1(), "CREATE DATABASE if not exists mgbench;")
		super().executeQuery(super().client2(), "CREATE DATABASE if not exists mgbench;")
		super().executeSQLFile(super().client1(), "./stress/sql/mgbench1.sql")
		super().executeSQLFile(super().client2(), "./stress/sql/mgbench1.sql")
		super().executeSQLFile(super().client1(), "./stress/sql/mgbench2.sql")
		super().executeSQLFile(super().client2(), "./stress/sql/mgbench2.sql")
		super().executeSQLFile(super().client1(), "./stress/sql/mgbench3.sql")
		super().executeSQLFile(super().client2(), "./stress/sql/mgbench3.sql")

		# client1 = super().getNewClient(super().get_replica1_host())
		# client2 = super().getNewClient(super().get_replica1_host())
		# client3 = super().getNewClient(super().get_replica1_host())
		# insert_threads1 = threading.Thread(target=self.insert_data_from_csv, args=[client1, "./stress/data/mgbench1.csv", "logs1"])
		# insert_threads2 = threading.Thread(target=self.insert_data_from_csv, args=[client2, "./stress/data/mgbench2.csv", "logs2"])
		# insert_threads3 = threading.Thread(target=self.insert_data_from_csv, args=[client3, "./stress/data/mgbench3.csv", "logs3"])

		# insert_threads1.start()
		# insert_threads2.start()
		# insert_threads3.start()

		# insert_threads1.join()
		# insert_threads2.join()
		# insert_threads3.join()


	def teardown_method(self):
		#input('press any key to continue')

		# super().executeQuery(super().client1(), "DROP TABLE if exists mgbench.logs1 no delay;")
		# super().executeQuery(super().client2(), "DROP TABLE if exists mgbench.logs1 no delay;")
		# super().executeQuery(super().client1(), "DROP TABLE if exists mgbench.logs2 no delay;")
		# super().executeQuery(super().client2(), "DROP TABLE if exists mgbench.logs2 no delay;")
		# super().executeQuery(super().client1(), "DROP TABLE if exists mgbench.logs3 no delay;")
		# super().executeQuery(super().client2(), "DROP TABLE if exists mgbench.logs3 no delay;")
		# super().executeQuery(super().client1(), "DROP DATABASE if exists mgbench;")
		# super().executeQuery(super().client2(), "DROP DATABASE if exists mgbench;")

		super().uninit()

	def insert_data_from_csv(self, client:Client, file_name, table_name):
		super().log("insert data from csv: %s"%file_name)
		rv = client.execute("desc %s.%s;"%('mgbench', table_name))
		fields_name_types = {}
		for data in rv:
			filed_name = data[0]
			field_type = data[1]
			fields_name_types[filed_name] = field_type

		def getRealType(field_type):
			matchObj = re.match(r'LowCardinality\((.*)\)', field_type)
			if matchObj:
				field_type = matchObj.group(1)
				return getRealType(field_type)

			matchObj = re.match(r'Nullable\((.*)\)', field_type)
			if matchObj:
				field_type = matchObj.group(1)
				return getRealType(field_type)
			return field_type

		def convert_field(field_name, value):
			if value is None or value == '':
				return None
			field_type = getRealType(fields_name_types[field_name])
			if (field_type.startswith('Float')):
				return float(value)
			elif (field_type == 'DateTime'):
				return datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
			elif (field_type.startswith('DateTime64')):
				return datetime.strptime(value, '%Y-%m-%d %H:%M:%S.%f')
			elif (field_type.startswith('UInt64')):
				return int(value)
			elif (field_type.startswith('Int64')):
				return int(value)
			elif (field_type.startswith('UInt')):
				return int(value)
			elif (field_type.startswith('Int')):
				return int(value)

			elif (field_type.startswith('FixedString')):
				return str(value)
			else:
				return value

		def iter_csv(filename):
			with open(filename, 'r') as f:
				reader = DictReader(f, delimiter=',')
				for line in reader:
					yield {k: convert_field(k, v) for k, v in line.items()}

		client.execute("INSERT INTO %s.%s VALUES"%(self.test_db, table_name), iter_csv(file_name), types_check=True)


	def schedule_task(self, file_name):
		with self.lock_con:
			self.tasks.append(file_name)
			self.lock_con.notify()

	def run_task(self, file_name):
		expect_file_name = file_name.replace('.sql', '.txt').replace('sql', 'expect')
		print(expect_file_name)
		start = time.time()
		result = super().executeSQLFile(super().client1(), file_name)
		end = time.time()
		print(f"耗时{end - start}秒")
		with open(expect_file_name, 'crw') as f:
			f.write(result)

	def test_case1(self):
		cases = []
		for i in range(0, 1):
			cases.append("./stress/sql/case_1_1.sql")
			cases.append("./stress/sql/case_1_2.sql")
			cases.append("./stress/sql/case_1_3.sql")
			cases.append("./stress/sql/case_1_4.sql")
			cases.append("./stress/sql/case_1_5.sql")
			cases.append("./stress/sql/case_1_6.sql")

			cases.append("./stress/sql/case_2_1.sql")
			cases.append("./stress/sql/case_2_2.sql")
			cases.append("./stress/sql/case_2_3.sql")
			cases.append("./stress/sql/case_2_4.sql")
			cases.append("./stress/sql/case_2_5.sql")
			cases.append("./stress/sql/case_2_6.sql")

			cases.append("./stress/sql/case_3_1.sql")
			cases.append("./stress/sql/case_3_4.sql")
			cases.append("./stress/sql/case_3_5.sql")
			cases.append("./stress/sql/case_3_6.sql")

		with ThreadPoolExecutor(max_workers=4) as pool:
			pool.map(self.run_task, cases)



		pool.shutdown()

if __name__=="__main__":
#	pytest.main(['-v', '-n8' , '--dist=loadscope', 'test_cubefs_for_bugfix.py'])
	pytest.main(['-s' , 'test_cubefs_for_stress.py'])
