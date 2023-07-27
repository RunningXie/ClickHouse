import pytest
import yaml
import os
import time
import uuid
import base

from clickhouse_driver import Client


class Test_cubefs_for_drop(base.Base):
	def setup_method(self):
		super().init(self.__class__.__name__)

	def teardown_method(self):
		#input('press any key to continue')
		super().drop_table_if_exist(super().client1())
		super().drop_table_if_exist(super().client2())
		super().uninit()

	def test_case1(self):
		super().create_table(super().client1())
		super().create_table(super().client2())

		super().insert_one_row_for_loop(super().client1(), 1)
		super().insert_one_thousand_row_for_loop(super().client1(), 1)
		super().assert_table_state(super().client1(), 1001, 2, "localdisk")
		super().assert_table_state(super().client2(), 1001, 2, "localdisk")

		super().start_move(super().client1())
		super().start_move(super().client2())
		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "cfs")

		super().drop_table_if_exist(super().client1())
		super().assert_table_exist(super().client1(), False)
		super().assert_table_state(super().client2(), 1001, 2, "cfs")

		super().create_table(super().client1())
		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "cfs")

		super().drop_table_if_exist(super().client2())
		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_exist(super().client2(), False)

		super().drop_table_if_exist(super().client1())
		super().assert_table_exist(super().client1(), False)
		super().assert_table_exist(super().client2(), False)

		super().create_table(super().client1())
		super().assert_table_state(super().client1(), 0, 0, "localdisk")
		super().assert_table_exist(super().client2(), False)


class Test_cubefs_for_truncate(base.Base):
	def setup_method(self):
		super().init(self.__class__.__name__)

	def teardown_method(self):
		#input('press any key to exit')
		super().drop_table_if_exist(super().client1())
		super().drop_table_if_exist(super().client2())
		super().uninit()

	def test_case2(self):
		super().create_table(super().client1())
		super().create_table(super().client2())

		super().insert_one_row_for_loop(super().client1(), 1)
		super().insert_one_thousand_row_for_loop(super().client1(), 1)
		super().assert_table_state(super().client1(), 1001, 2, "localdisk")
		super().assert_table_state(super().client2(), 1001, 2, "localdisk")

		super().start_move(super().client1())
		super().start_move(super().client2())
		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "cfs")

		super().truncate_table(super().client1())
		super().assert_table_state(super().client1(), 0, 0, "cfs")
		super().assert_table_state(super().client2(), 0, 0, "cfs")

		super().drop_table_if_exist(super().client2())
		super().assert_table_state(super().client1(), 0, 0, "cfs")
		super().assert_table_exist(super().client2(), False)

		super().create_table(super().client2())
		super().assert_table_state(super().client1(), 0, 0, "cfs")
		super().assert_table_state(super().client2(), 0, 0, "cfs")

		super().drop_table_if_exist(super().client1())
		super().assert_table_exist(super().client1(), False)
		super().assert_table_state(super().client2(), 0, 0, "cfs")

class Test_cubefs_for_delete(base.Base):
	def setup_method(self):
		super().init(self.__class__.__name__)
		super().create_table(super().client1())
		super().create_table(super().client2())


	def teardown_method(self):
		# input('press any key to exit')
		super().drop_table_if_exist(super().client1())
		super().drop_table_if_exist(super().client2())
		super().uninit()

	def test_case3(self):
		super().stop_move(super().client1())
		super().stop_move(super().client2())

		super().insert_one_row_for_loop(super().client1(), 1)
		super().insert_one_thousand_row_for_loop(super().client1(), 1)
		super().assert_table_state(super().client1(), 1001, 2, "localdisk")
		super().assert_table_state(super().client2(), 1001, 2, "localdisk")

		super().start_move(super().client1())
		super().start_move(super().client2())
		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "cfs")

		super().delete_table(super().client1(), "appName='test_cubefs'")
		super().wait_for_all_mutations_finished(super().client1())
		super().wait_for_all_mutations_finished(super().client2())

		super().assert_table_state(super().client1(), 0, 2, "cfs")
		super().assert_table_state(super().client2(), 0, 2, "cfs")

		super().drop_table_if_exist(super().client1())
		super().assert_table_exist(super().client1(), False)
		super().assert_table_state(super().client2(), 0, 2, "cfs")

		super().create_table(super().client1())
		super().assert_table_state(super().client1(), 0, 2, "cfs")
		super().assert_table_state(super().client2(), 0, 2, "cfs")


		super().drop_table_if_exist(super().client1())
		super().drop_table_if_exist(super().client2())
		super().assert_table_exist(super().client1(), False)
		super().assert_table_exist(super().client2(), False)

		super().create_table(super().client1())
		super().assert_table_state(super().client1(), 0, 0, "cfs")
		super().assert_table_exist(super().client2(), False)

class Test_cubefs_for_drop_partition(base.Base):
	def setup_method(self):
		super().init(self.__class__.__name__)
		super().drop_table_if_exist(super().client1())
		super().drop_table_if_exist(super().client2())

	def teardown_method(self):
		#input('press any key to exit')
		super().drop_table_if_exist(super().client1())
		super().drop_table_if_exist(super().client2())
		super().uninit()

	def test_case4(self):
		super().create_table(super().client1())
		super().create_table(super().client2())

		super().insert_one_row_for_loop(super().client1(), 1)
		super().insert_one_thousand_row_for_loop(super().client1(), 1)
		super().assert_table_state(super().client1(), 1001, 2, "localdisk")
		super().assert_table_state(super().client2(), 1001, 2, "localdisk")

		super().start_move(super().client1())
		super().start_move(super().client2())
		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "cfs")

		partition_id = self.get_small_size_partition_id(super().client1())
		super().drop_partition(super().client1(), partition_id)
		super().wait_for_all_mutations_finished(super().client1())
		super().wait_for_all_mutations_finished(super().client2())
		super().assert_table_state(super().client1(), 0, 0, "cfs")
		super().assert_table_state(super().client2(), 0, 0, "cfs")

		super().drop_table_if_exist(super().client1())
		super().assert_table_exist(super().client1(), False)
		super().assert_table_state(super().client2(), 0, 0, "cfs")

		super().create_table(super().client1())
		super().assert_table_state(super().client1(), 0, 0, "cfs")
		super().assert_table_state(super().client2(), 0, 0, "cfs")

		super().drop_table_if_exist(super().client1())
		super().drop_table_if_exist(super().client2())
		super().assert_table_exist(super().client1(), False)
		super().assert_table_exist(super().client2(), False)

		super().create_table(super().client1())
		super().assert_table_state(super().client1(), 0, 0, "cfs")
		super().assert_table_exist(super().client2(), False)


class Test_cubefs_for_drop_part(base.Base):
	def setup_method(self):
		super().init(self.__class__.__name__)
		super().create_table(super().client1())
		super().create_table(super().client2())

	def teardown_method(self):
		#input('press any key to exit')
		super().drop_table_if_exist(super().client1())
		super().drop_table_if_exist(super().client2())
		super().uninit()

	def test_case5(self):
		super().stop_move(super().client1())
		super().stop_move(super().client2())
		super().insert_one_row_for_loop(super().client1(), 1)
		super().insert_one_thousand_row_for_loop(super().client1(), 1)
		super().assert_table_state(super().client1(), 1001, 2, "localdisk")
		super().assert_table_state(super().client2(), 1001, 2, "localdisk")

		super().start_move(super().client1())
		super().start_move(super().client2())
		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "cfs")

		part_id = self.get_small_size_part_id(super().client1())
		super().drop_part(super().client1(), part_id)
		super().wait_for_all_mutations_finished(super().client1())
		super().wait_for_all_mutations_finished(super().client2())

		super().assert_table_state(super().client1(), 1000, 1, "cfs")
		super().assert_table_state(super().client2(), 1000, 1, "cfs")

		super().drop_table_if_exist(super().client1())
		super().assert_table_exist(super().client1(), False)
		super().assert_table_state(super().client2(), 1000, 1, "cfs")


		super().create_table(super().client1())
		super().assert_table_state(super().client1(), 1000, 1, "cfs")
		super().assert_table_state(super().client2(), 1000, 1, "cfs")

		super().drop_table_if_exist(super().client1())
		super().drop_table_if_exist(super().client2())
		super().assert_table_exist(super().client1(), False)
		super().assert_table_exist(super().client2(), False)

		super().create_table(super().client1())
		super().assert_table_state(super().client1(), 0, 0, "cfs")
		super().assert_table_exist(super().client2(), False)

if __name__=="__main__":
	pytest.main(['-v', '-n8' , '--dist=loadscope', 'test_cubefs_for_delete.py'])
	# pytest.main(['-s' , 'test_cubefs_for_delete.py'])
