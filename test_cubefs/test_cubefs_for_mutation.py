import pytest
import yaml
import os
import time
import uuid
import base

from clickhouse_driver import Client

class Test_cubefs_for_detach_table(base.Base):
	def setup_method(self):
		super().init(self.__class__.__name__)
		super().create_table(super().client1())
		super().create_table(super().client2())


	def teardown_method(self):
		#input('press any key to continue')
		super().drop_table_if_exist(super().client1())
		super().drop_table_if_exist(super().client2())
		super().uninit()

	def test_case1(self):

		super().insert_one_row_for_loop(super().client1(), 1)
		super().insert_one_thousand_row_for_loop(super().client1(), 1)

		super().start_move(super().client1())
		super().start_move(super().client2())

		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "cfs")
		super().detach_table(super().client1())
		super().assert_table_exist(super().client1(), False)
		super().assert_table_state(super().client2(), 1001, 2, "cfs")

		super().attach_table(super().client1())
		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "cfs")

		super().detach_table(super().client2())
		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_exist(super().client2(), False)

		super().detach_table(super().client1())
		super().assert_table_exist(super().client1(), False)
		super().assert_table_exist(super().client2(), False)

		# for delete
		super().attach_table(super().client1())
		super().attach_table(super().client2())



class Test_cubefs_for_detach_part(base.Base):
	def setup_method(self):
		super().init(self.__class__.__name__)
		super().create_table(super().client1())
		super().create_table(super().client2())
		super().insert_one_row_for_loop(super().client1(), 1)
		super().insert_one_thousand_row_for_loop(super().client1(), 1)

		super().start_move(super().client1())
		super().start_move(super().client2())

	def teardown_method(self):
		#input('press any key to continue')
		super().drop_table_if_exist(super().client1())
		super().drop_table_if_exist(super().client2())
		super().uninit()

	def test_case2(self):
		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "cfs")
		part_id = super().get_small_size_part_id(super().client1())
		super().detach_part(super().client1(), part_id)
		super().assert_table_state(super().client1(), 1000, 1, "cfs")
		super().assert_table_state(super().client2(), 1000, 1, "cfs")

		super().attach_part(super().client1(), part_id)
		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "cfs")

		part_id = super().get_small_size_part_id(super().client1())
		super().detach_part(super().client1(), part_id)
		super().assert_table_state(super().client1(), 1000, 1, "cfs")
		super().assert_table_state(super().client2(), 1000, 1, "cfs")


class Test_cubefs_for_detach_partition(base.Base):
	def setup_method(self):
		super().init(self.__class__.__name__)
		super().create_table(super().client1())
		super().create_table(super().client2())
		super().insert_one_row_for_loop(super().client1(), 1)
		super().insert_one_thousand_row_for_loop(super().client1(), 1)

		super().start_move(super().client1())
		super().start_move(super().client2())

	def teardown_method(self):
		#input('press any key to continue')
		super().drop_table_if_exist(super().client1())
		super().drop_table_if_exist(super().client2())
		super().uninit()

	def test_case3(self):
		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "cfs")
		partition_id = super().get_small_size_partition_id(super().client1())
		super().detach_partition(super().client1(), partition_id)
		super().assert_table_state(super().client1(), 0, 0, "cfs")
		super().assert_table_state(super().client2(), 0, 0, "cfs")

		super().attach_partition(super().client1(), partition_id)
		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "cfs")

		partition_id = super().get_small_size_partition_id(super().client1())
		super().detach_partition(super().client1(), partition_id)
		super().assert_table_state(super().client1(), 0, 0, "cfs")
		super().assert_table_state(super().client2(), 0, 0, "cfs")


class Test_cubefs_for_update_partiton(base.Base):
	def setup_method(self):
		super().init(self.__class__.__name__)
		super().create_table(super().client1())
		super().create_table(super().client2())
		super().insert_one_row_for_loop(super().client1(), 1)
		super().insert_one_thousand_row_for_loop(super().client1(), 1)

		super().start_move(super().client1())
		super().start_move(super().client2())

	def teardown_method(self):
		#input('press any key to continue')
		super().drop_table_if_exist(super().client1())
		super().drop_table_if_exist(super().client2())
		super().uninit()

	def test_case4(self):
		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "cfs")
		partition_id = super().get_small_size_partition_id(super().client1())
		super().update_partition(super().client1(), partition_id, "name='test1'", "value=2")
		assert super().get_record_count(super().client1(), "value=2") == 1
		assert super().get_record_count(super().client2(), "value=2") == 1
		assert super().get_record_count(super().client1(), "name='test1000'") == 1000
		assert super().get_record_count(super().client2(), "name='test1000'") == 1000
		assert super().get_record_count(super().client1(), "name!='test1000'") == 1
		assert super().get_record_count(super().client2(), "name!='test1000'") == 1

		super().delete_partition(super().client1(), partition_id, "value=2")
		assert super().get_record_count(super().client1(), "value=2") == 0
		assert super().get_record_count(super().client2(), "value=2") == 0
		assert super().get_record_count(super().client1(), "name='test1'") == 0
		assert super().get_record_count(super().client2(), "name='test'") == 0
		assert super().get_record_count(super().client1(), "name!='test1'") == 1000
		assert super().get_record_count(super().client2(), "name!='test1'") == 1000



class Test_cubefs_for_add_column(base.Base):
	def setup_method(self):
		super().init(self.__class__.__name__)
		super().create_table(super().client1())
		super().create_table(super().client2())
		super().insert_one_row_for_loop(super().client1(), 1)
		super().insert_one_thousand_row_for_loop(super().client1(), 1)

		super().start_move(super().client1())
		super().start_move(super().client2())

	def teardown_method(self):
		#input('press any key to continue')
		super().drop_table_if_exist(super().client1())
		super().drop_table_if_exist(super().client2())
		super().uninit()

	def test_case5(self):
		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "cfs")
		partition_id = super().get_small_size_partition_id(super().client1())

		super().add_column(super().client1(), "new_column", "UInt32", 1)
		assert super().get_column_count(super().client1()) == 5
		assert super().get_column_count(super().client2()) == 5
		assert super().get_record_count(super().client1(), "new_column=1") == 1001
		assert super().get_record_count(super().client2(), "new_column=1") == 1001

		super().modify_column(super().client1(), "new_column", "UInt64", 2)
		assert super().get_column_count(super().client1()) == 5
		assert super().get_column_count(super().client2()) == 5
		assert super().get_record_count(super().client1(), "new_column=1") == 0
		assert super().get_record_count(super().client2(), "new_column=1") == 0
		assert super().get_record_count(super().client1(), "new_column=2") == 1001
		assert super().get_record_count(super().client2(), "new_column=2") == 1001

		super().drop_column(super().client1(), "new_column")
		assert super().get_column_count(super().client1()) == 4
		assert super().get_column_count(super().client2()) == 4
		assert super().get_record_count(super().client1(), "value=2") == 0
		assert super().get_record_count(super().client2(), "value=2") == 0
		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "cfs")


if __name__=="__main__":
	pytest.main(['-v', '-n8' , '--dist=loadscope', 'test_cubefs_for_mutation.py'])
#	pytest.main(['-s' , 'test_cubefs_for_mutation.py'])
