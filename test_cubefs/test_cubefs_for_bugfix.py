import pytest
import yaml
import os
import time
import uuid
import base

from clickhouse_driver import Client


class Test_cubefs_for_check_data(base.Base):
	def setup_method(self):
		super().init(self.__class__.__name__)

	def teardown_method(self):
		#input('press any key to continue')
		super().drop_table_if_exist(super().client1())
		super().drop_table_if_exist(super().client2())
		super().uninit()

	def __test_case1(self):
		super().create_table(super().client1())
		super().create_table(super().client2())

		super().insert_one_row_for_loop(super().client1(), 10)
		super().insert_one_thousand_row_for_loop(super().client1(), 10)
		super().start_merge(super().client1())
		super().start_merge(super().client2())
		super().start_move(super().client1())
		super().start_move(super().client2())
		super().assert_table_state(super().client1(), 10010, 1, "cfs")
		super().assert_table_state(super().client2(), 10010, 1, "cfs")

		super().stop_replication_queue(super().client1())
		super().delete_zk_part(super().client1())
		super().restart_server(super().client1())

		super().assert_table_state(super().client1(), 10010, 1, "cfs")
		super().assert_table_state(super().client2(), 10010, 1, "cfs")
	

		super().drop_table_if_exist(super().client1())
		super().assert_table_exist(super().client1(), False)
		super().assert_table_state(super().client2(), 10010, 1, "cfs")

		super().drop_table_if_exist(super().client2())
		super().assert_table_exist(super().client1(), False)
		super().assert_table_exist(super().client2(), False)

	def __assert_parts(self, client, value):
		replica_path = super().get_replica_path(client)
		part_path = replica_path + "/parts"
		nodes = self.zk_client.get_children(part_path)
		for node in nodes:
			super().log("part path: %s"% node)
		assert len(nodes) == value
	
	def test_case2(self):
		super().create_table(super().client1())
		super().create_table(super().client2())

		super().forbidden_all_actions(super().client2())
		super().stop_replication_queue(super().client2())
		super().insert_one_row_for_loop(super().client1(), 2)
		super().insert_one_thousand_row_for_loop(super().client1(), 2)
		super().start_move(super().client1())
		super().start_merge(super().client1())
		super().assert_table_state(super().client1(), 2002, 1, "cfs")
		self.__assert_parts(super().client2(), 0)
		super().forbidden_all_actions(super().client1())

		super().restart_server(super().client2())
		super().restore_all_actions(super().client1())
		super().restore_all_actions(super().client2())

		super().assert_table_state(super().client1(), 2002, 1, "cfs")
		super().assert_table_state(super().client2(), 2002, 1, "cfs")
	

		super().drop_table_if_exist(super().client1())
		super().assert_table_exist(super().client1(), False)
		super().assert_table_state(super().client2(), 2002, 1, "cfs")

		super().drop_table_if_exist(super().client2())
		super().assert_table_exist(super().client1(), False)
		super().assert_table_exist(super().client2(), False)




if __name__=="__main__":
#	pytest.main(['-v', '-n8' , '--dist=loadscope', 'test_cubefs_for_bugfix.py'])
	pytest.main(['-s' , 'test_cubefs_for_bugfix.py'])
