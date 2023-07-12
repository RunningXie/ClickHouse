import pytest
import yaml
import os
import time
import uuid
import base

from clickhouse_driver import Client


class Test_cubefs_for_merge_piece_0(base.Base):
	def setup_method(self):
		super().init(self.__class__.__name__)
		super().drop_table_if_exist(super().client1())
		super().drop_table_if_exist(super().client2())
		super().create_table(super().client1())
		super().create_table(super().client2())
		super().forbidden_all_actions(super().client1());
		super().forbidden_all_actions(super().client2());

		super().start_send(super().client1())
		super().start_send(super().client2())

		super().insert_one_row_for_loop(super().client1(), 1)
		super().insert_one_thousand_row_for_loop(super().client1(), 1)
		super().assert_table_state(super().client1(), 1001, 2, "localdisk")
		super().assert_table_state(super().client2(), 0, 0, "localdisk")

	def teardown_method(self):
		#input('press any key to exit')
		super().drop_table_if_exist(super().client1())
		super().drop_table_if_exist(super().client2())
		super().uninit()

	def test_case1(self):
		super().start_fetch(super().client2())
		super().assert_table_state(super().client1(), 1001, 2, "localdisk")
		super().assert_table_state(super().client2(), 1001, 2, "localdisk")

		super().stop_fetch(super().client2())
		super().start_merge(super().client1())
		super().assert_table_state(super().client1(), 1001, 1, "localdisk")
		super().assert_table_state(super().client2(), 1001, 2, "localdisk")

		super().start_merge(super().client2())
		super().assert_table_state(super().client1(), 1001, 1, "localdisk")
		super().assert_table_state(super().client2(), 1001, 1, "localdisk")

		super().start_move(super().client1())
		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "localdisk")

		super().start_move(super().client2())
		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")


	def test_case2(self):
		super().start_fetch(super().client2())
		super().assert_table_state(super().client1(), 1001, 2, "localdisk")
		super().assert_table_state(super().client2(), 1001, 2, "localdisk")

		super().stop_fetch(super().client2())
		super().start_merge(super().client2())

		super().assert_record_count(super().client1(), 1001)
		super().assert_record_count(super().client2(), 1001)
		super().assert_table_state(super().client1(), 1001, 2, "localdisk")
		super().assert_table_state(super().client2(), 1001, 1, "localdisk")

		super().start_merge(super().client1())
		super().assert_table_state(super().client1(), 1001, 1, "localdisk")
		super().assert_table_state(super().client2(), 1001, 1, "localdisk")

		super().start_move(super().client1())
		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "localdisk")

		super().start_move(super().client2())
		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")


	def test_case3(self):
		super().start_fetch(super().client1())
		super().start_fetch(super().client2())
		super().assert_table_state(super().client1(), 1001, 2, "localdisk")
		super().assert_table_state(super().client2(), 1001, 2, "localdisk")

		super().start_merge(super().client1())
		super().assert_table_state(super().client1(), 1001, 1, "localdisk")
		super().assert_table_state(super().client2(), 1001, 2, "localdisk")


		super().start_merge(super().client2())
		super().assert_table_state(super().client1(), 1001, 1, "localdisk")
		super().assert_table_state(super().client2(), 1001, 1, "localdisk")

		super().start_move(super().client2())
		super().assert_table_state(super().client1(), 1001, 1, "localdisk")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

		super().start_move(super().client1())
		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")


	def test_case4(self):
		super().start_fetch(super().client1())
		super().start_fetch(super().client2())
		super().assert_table_state(super().client1(), 1001, 2, "localdisk")
		super().assert_table_state(super().client2(), 1001, 2, "localdisk")

		super().start_merge(super().client2())
		super().assert_table_state(super().client1(), 1001, 2, "localdisk")
		super().assert_table_state(super().client2(), 1001, 1, "localdisk")


		super().start_merge(super().client1())
		super().assert_table_state(super().client1(), 1001, 1, "localdisk")
		super().assert_table_state(super().client2(), 1001, 1, "localdisk")

		super().start_move(super().client2())
		super().assert_table_state(super().client1(), 1001, 1, "localdisk")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

		super().start_move(super().client1())
		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")



	def test_case5(self):
		super().start_fetch(super().client1())
		super().start_fetch(super().client2())
		super().assert_table_state(super().client1(), 1001, 2, "localdisk")
		super().assert_table_state(super().client2(), 1001, 2, "localdisk")

		super().start_merge(super().client2())
		super().assert_table_state(super().client1(), 1001, 2, "localdisk")
		super().assert_table_state(super().client2(), 1001, 1, "localdisk")

		super().start_move(super().client2())
		super().assert_table_state(super().client1(), 1001, 2, "localdisk")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

		super().start_merge(super().client1())
		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")


		super().start_move(super().client1())
		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")


class Test_cubefs_for_merge_piece_1(base.Base):
	def setup_method(self):
		super().init(self.__class__.__name__)
		super().drop_table_if_exist(super().client1())
		super().drop_table_if_exist(super().client2())
		super().create_table(super().client1())
		super().create_table(super().client2())
		super().forbidden_all_actions(super().client1());
		super().forbidden_all_actions(super().client2());

		super().start_send(super().client1())
		super().start_send(super().client2())

		super().insert_one_row_for_loop(super().client1(), 1)
		super().insert_one_thousand_row_for_loop(super().client1(), 1)
		super().assert_table_state(super().client1(), 1001, 2, "localdisk")
		super().assert_table_state(super().client2(), 0, 0, "localdisk")

	def teardown_method(self):
		#input('press any key to exit')
		super().drop_table_if_exist(super().client1())
		super().drop_table_if_exist(super().client2())
		super().uninit()

	def test_case6(self):
		super().start_merge(super().client1())
		super().assert_table_state(super().client1(), 1001, 1, "localdisk")
		super().assert_table_state(super().client2(), 0, 0, "localdisk")

		super().start_move(super().client1())
		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 0, 0, "localdisk")


		super().start_fetch(super().client1())
		super().start_fetch(super().client2())
		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

		super().start_merge(super().client2())
		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

		super().start_move(super().client2())
		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

	def test_case7(self):
		super().start_merge(super().client1())
		super().assert_table_state(super().client1(), 1001, 1, "localdisk")
		super().assert_table_state(super().client2(), 0, 0, "localdisk")

		super().start_fetch(super().client1())
		super().start_fetch(super().client2())
		super().assert_table_state(super().client1(), 1001, 1, "localdisk")
		super().assert_table_state(super().client2(), 1001, 1, "localdisk")

		super().start_move(super().client1())
		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "localdisk")

		super().start_merge(super().client2())
		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "localdisk")

		super().start_move(super().client2())
		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

	def test_case8(self):
		super().start_merge(super().client1())
		super().assert_table_state(super().client1(), 1001, 1, "localdisk")
		super().assert_table_state(super().client2(), 0, 0, "localdisk")

		super().start_fetch(super().client1())
		super().start_fetch(super().client2())
		super().assert_table_state(super().client1(), 1001, 1, "localdisk")
		super().assert_table_state(super().client2(), 1001, 1, "localdisk")

		super().start_merge(super().client2())
		super().assert_table_state(super().client1(), 1001, 1, "localdisk")
		super().assert_table_state(super().client2(), 1001, 1, "localdisk")

		super().start_move(super().client1())
		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "localdisk")

		super().start_move(super().client2())
		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")


	def test_case9(self):
		super().start_merge(super().client1())
		super().assert_table_state(super().client1(), 1001, 1, "localdisk")
		super().assert_table_state(super().client2(), 0, 0, "localdisk")

		super().start_fetch(super().client1())
		super().start_fetch(super().client2())
		super().assert_table_state(super().client1(), 1001, 1, "localdisk")
		super().assert_table_state(super().client2(), 1001, 1, "localdisk")

		super().start_merge(super().client2())
		super().assert_table_state(super().client1(), 1001, 1, "localdisk")
		super().assert_table_state(super().client2(), 1001, 1, "localdisk")

		super().start_move(super().client2())
		super().assert_table_state(super().client1(), 1001, 1, "localdisk")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

		super().start_move(super().client1())
		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")


	def test_case10(self):
		super().start_fetch(super().client1())
		super().start_fetch(super().client2())
		super().assert_table_state(super().client1(), 1001, 2, "localdisk")
		super().assert_table_state(super().client2(), 1001, 2, "localdisk")

		super().start_move(super().client1())
		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "localdisk")


		super().start_merge(super().client1())
		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "localdisk")



		super().start_merge(super().client2())
		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

		super().start_move(super().client2())
		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

class Test_cubefs_for_merge_piece_2(base.Base):
	def setup_method(self):
		super().init(self.__class__.__name__)
		super().drop_table_if_exist(super().client1())
		super().drop_table_if_exist(super().client2())
		super().create_table(super().client1())
		super().create_table(super().client2())
		super().forbidden_all_actions(super().client1());
		super().forbidden_all_actions(super().client2());

		super().start_send(super().client1())
		super().start_send(super().client2())

		super().insert_one_row_for_loop(super().client1(), 1)
		super().insert_one_thousand_row_for_loop(super().client1(), 1)
		super().assert_table_state(super().client1(), 1001, 2, "localdisk")
		super().assert_table_state(super().client2(), 0, 0, "localdisk")

	def teardown_method(self):
		#input('press any key to exit')
		super().drop_table_if_exist(super().client1())
		super().drop_table_if_exist(super().client2())
		super().uninit()

	def test_case11(self):
		super().start_fetch(super().client1())
		super().start_fetch(super().client2())
		super().assert_table_state(super().client1(), 1001, 2, "localdisk")
		super().assert_table_state(super().client2(), 1001, 2, "localdisk")

		super().start_move(super().client1())
		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "localdisk")

		super().start_merge(super().client1())
		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "localdisk")

		super().start_move(super().client2())
		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "localdisk")

		super().start_merge(super().client2())
		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

	def test_case12(self):
		super().start_fetch(super().client1())
		super().start_fetch(super().client2())
		super().assert_table_state(super().client1(), 1001, 2, "localdisk")
		super().assert_table_state(super().client2(), 1001, 2, "localdisk")

		super().start_move(super().client1())
		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "localdisk")

		super().start_move(super().client2())
		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "cfs")

		super().start_merge(super().client1())
		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "cfs")

		super().start_merge(super().client2())
		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

	def test_case13(self):
		super().start_fetch(super().client1())
		super().start_fetch(super().client2())
		super().assert_table_state(super().client1(), 1001, 2, "localdisk")
		super().assert_table_state(super().client2(), 1001, 2, "localdisk")

		super().start_move(super().client1())
		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "localdisk")

		super().start_move(super().client2())
		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "cfs")

		super().start_merge(super().client2())
		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

		super().start_merge(super().client1())
		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

	def test_case14(self):
		super().start_move(super().client1())
		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 0, 0, "localdisk")

		super().start_merge(super().client1())
		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 0, 0, "localdisk")

		super().start_fetch(super().client1())
		super().start_fetch(super().client2())
		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

		super().start_merge(super().client2())
		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

		super().start_move(super().client2())
		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

	def test_case15(self):
		super().start_move(super().client1())
		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 0, 0, "localdisk")

		super().start_merge(super().client1())
		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 0, 0, "localdisk")

		super().start_fetch(super().client1())
		super().start_fetch(super().client2())
		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

		super().start_move(super().client2())
		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

		super().start_merge(super().client2())
		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")


class Test_cubefs_for_merge_piece_3(base.Base):
	def setup_method(self):
		super().init(self.__class__.__name__)
		super().drop_table_if_exist(super().client1())
		super().drop_table_if_exist(super().client2())
		super().create_table(super().client1())
		super().create_table(super().client2())
		super().forbidden_all_actions(super().client1());
		super().forbidden_all_actions(super().client2());

		super().start_send(super().client1())
		super().start_send(super().client2())

		super().insert_one_row_for_loop(super().client1(), 1)
		super().insert_one_thousand_row_for_loop(super().client1(), 1)
		super().assert_table_state(super().client1(), 1001, 2, "localdisk")
		super().assert_table_state(super().client2(), 0, 0, "localdisk")

	def teardown_method(self):
		#input('press any key to exit')
		super().drop_table_if_exist(super().client1())
		super().drop_table_if_exist(super().client2())
		super().uninit()

	def test_case16(self):
		super().start_move(super().client1())
		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 0, 0, "localdisk")

		super().start_fetch(super().client1())
		super().start_fetch(super().client2())
		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "cfs")

		super().start_merge(super().client1())
		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "cfs")

		super().start_move(super().client2())
		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "cfs")

		super().start_merge(super().client2())
		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

	def test_case17(self):
		super().start_move(super().client1())
		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 0, 0, "localdisk")

		super().start_fetch(super().client1())
		super().start_fetch(super().client2())
		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "cfs")

		super().start_merge(super().client1())
		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "cfs")

		super().start_move(super().client2())
		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "cfs")

		super().start_merge(super().client2())
		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")


	def test_case18(self):
		super().start_move(super().client1())
		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 0, 0, "localdisk")

		super().start_fetch(super().client1())
		super().start_fetch(super().client2())
		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "cfs")

		super().start_move(super().client2())
		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "cfs")

		super().start_merge(super().client1())
		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "cfs")

		super().start_merge(super().client2())
		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

	def test_case19(self):
		super().start_move(super().client1())
		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 0, 0, "localdisk")

		super().start_fetch(super().client1())
		super().start_fetch(super().client2())
		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "cfs")

		super().start_move(super().client2())
		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "cfs")


		super().start_merge(super().client2())
		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

		super().start_merge(super().client1())
		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

	def test_case20(self):
		super().start_move(super().client1())
		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 0, 0, "localdisk")

		super().start_fetch(super().client1())
		super().start_fetch(super().client2())
		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "cfs")

		super().start_merge(super().client2())
		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

		super().start_move(super().client2())
		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

		super().start_merge(super().client1())
		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")


class Test_cubefs_for_merge_special_1(base.Base):
	def setup_method(self):
		super().init(self.__class__.__name__)
		super().drop_table_if_exist(super().client1())
		super().drop_table_if_exist(super().client2())
		super().create_table(super().client1())
		super().create_table(super().client2())
		super().forbidden_all_actions(super().client1());
		super().forbidden_all_actions(super().client2());

		super().start_send(super().client1())
		super().start_send(super().client2())

		super().insert_one_row_for_loop(super().client1(), 1)
		super().insert_one_thousand_row_for_loop(super().client1(), 1)
		super().assert_table_state(super().client1(), 1001, 2, "localdisk")
		super().assert_table_state(super().client2(), 0, 0, "localdisk")

	def teardown_method(self):
		#input('press any key to exit')
		super().drop_table_if_exist(super().client1())
		super().drop_table_if_exist(super().client2())
		super().uninit()

	def merge_parts_on_all_replicas(self):
		super().stop_fetch(super().client1())
		super().stop_fetch(super().client2())
		super().start_merge(super().client1())
		super().start_merge(super().client2())
		retry_times = 0
		while True:
			if ((2 != super().get_part_count(super().client1())) or (2 != super().get_part_count(super().client2()))) :
				break
			elif (retry_times > 3):
				pytest.fail("table merge failed")
			else:
				retry_times += 1

		if (2 != super().get_part_count(super().client1())):
			merge_client = super().client1()
			no_merge_client = super().client2()
		else:
			merge_client = super().client2()
			no_merge_client = super().client1()
		return merge_client, no_merge_client 

	def test_case21(self):
		super().start_move(super().client1())
		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 0, 0, "localdisk")

		super().start_fetch(super().client1())
		super().start_fetch(super().client2())
		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "cfs")

		super().start_merge(super().client2())
		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

		super().start_move(super().client2())
		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")


		super().start_merge(super().client1())
		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

	def test_case_special_x(self):
		super().start_fetch(super().client2())
		super().assert_table_state(super().client1(), 1001, 2, "localdisk")
		super().assert_table_state(super().client2(), 1001, 2, "localdisk")
		super().stop_fetch(super().client2())

		merge_client, no_merge_client = self.merge_parts_on_all_replicas()
		super().start_move(no_merge_client)
		super().assert_table_state(no_merge_client, 1001, 1, "cfs")
		super().assert_table_state(merge_client, 1001, 1, "localdisk")

		super().start_move(merge_client)
		super().start_fetch(no_merge_client)
		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

	def test_case_special_1_1(self):
		super().start_fetch(super().client2())
		super().assert_table_state(super().client1(), 1001, 2, "localdisk")
		super().assert_table_state(super().client2(), 1001, 2, "localdisk")

		super().stop_fetch(super().client2())
		super().timer().sleep_until_ttl_timeout()
		super().start_merge(super().client1())
		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "localdisk")

		super().start_merge(super().client2())
		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "localdisk")

		super().start_fetch(super().client2())
		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

		super().start_move(super().client1())
		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

		super().start_move(super().client1())
		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

	def test_case_special_1_2(self):
		super().start_fetch(super().client2())
		super().assert_table_state(super().client1(), 1001, 2, "localdisk")
		super().assert_table_state(super().client2(), 1001, 2, "localdisk")

		merge_client, no_merge_client = self.merge_parts_on_all_replicas()

		super().assert_record_count(merge_client, 1001)
		super().assert_record_count(no_merge_client, 1001)
		super().assert_table_state(merge_client, 1001, 1, "localdisk")
		super().assert_table_state(no_merge_client, 1001, 1, "localdisk")

		super().start_fetch(no_merge_client)
		super().assert_table_state(merge_client, 1001, 1, "localdisk")
		super().assert_table_state(no_merge_client, 1001, 1, "localdisk")

		super().start_move(merge_client)
		super().assert_table_state(merge_client, 1001, 1, "cfs")
		super().assert_table_state(no_merge_client, 1001, 1, "localdisk")

		super().start_move(no_merge_client)
		super().assert_table_state(merge_client, 1001, 1, "cfs")
		super().assert_table_state(no_merge_client, 1001, 1, "cfs")


class Test_cubefs_for_merge2_piece_0(base.Base):
	def setup_method(self):
		super().init(self.__class__.__name__)
		super().drop_table_if_exist(super().client1())
		super().drop_table_if_exist(super().client2())
		super().create_table(super().client1())
		super().forbidden_all_actions(super().client1())
		super().insert_one_row_for_loop(super().client1(), 1)
		super().insert_one_thousand_row_for_loop(super().client1(), 1)
		super().assert_table_state(super().client1(), 1001, 2, "localdisk")
		super().create_table(super().client2())
		super().forbidden_all_actions(super().client2())
		super().assert_table_state(super().client2(), 0, 0, "localdisk")
		super().start_send(super().client1())
		super().start_send(super().client2())

	def teardown_method(self):
		#input('press any key to exit')
		super().drop_table_if_exist(super().client1())
		super().drop_table_if_exist(super().client2())
		super().uninit()


	def test_case1(self):
		super().start_fetch(super().client2())

		super().assert_table_state(super().client1(), 1001, 2, "localdisk")
		super().assert_table_state(super().client2(), 1001, 2, "localdisk")

		super().start_merge(super().client2())

		super().assert_table_state(super().client1(), 1001, 2, "localdisk")
		super().assert_table_state(super().client2(), 1001, 1, "localdisk")

		super().start_move(super().client2())

		super().assert_table_state(super().client1(), 1001, 2, "localdisk")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

		super().start_merge(super().client1())

		super().assert_table_state(super().client1(), 1001, 2, "localdisk")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

		super().start_fetch(super().client1())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

		super().start_move(super().client1())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")


	def test_case2(self):
		super().start_fetch(super().client2())

		super().assert_table_state(super().client1(), 1001, 2, "localdisk")
		super().assert_table_state(super().client2(), 1001, 2, "localdisk")

		super().start_merge(super().client2())

		super().assert_table_state(super().client1(), 1001, 2, "localdisk")
		super().assert_table_state(super().client2(), 1001, 1, "localdisk")

		super().start_merge(super().client1())

		super().assert_table_state(super().client1(), 1001, 1, "localdisk")
		super().assert_table_state(super().client2(), 1001, 1, "localdisk")

		super().start_move(super().client1())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "localdisk")

		super().start_move(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

	def test_case3(self):
		super().start_fetch(super().client2())

		super().assert_table_state(super().client1(), 1001, 2, "localdisk")
		super().assert_table_state(super().client2(), 1001, 2, "localdisk")

		super().start_merge(super().client2())

		super().assert_table_state(super().client1(), 1001, 2, "localdisk")
		super().assert_table_state(super().client2(), 1001, 1, "localdisk")

		super().start_merge(super().client1())

		super().assert_table_state(super().client1(), 1001, 1, "localdisk")
		super().assert_table_state(super().client2(), 1001, 1, "localdisk")

		super().start_move(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "localdisk")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

		super().start_move(super().client1())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")


	def test_case4(self):
		super().start_fetch(super().client2())

		super().assert_table_state(super().client1(), 1001, 2, "localdisk")
		super().assert_table_state(super().client2(), 1001, 2, "localdisk")

		super().start_merge(super().client1())

		super().assert_table_state(super().client1(), 1001, 1, "localdisk")
		super().assert_table_state(super().client2(), 1001, 2, "localdisk")

		super().start_move(super().client1())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "localdisk")

		super().stop_fetch(super().client2())
		super().start_merge(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "localdisk")

		super().start_fetch(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

		super().start_move(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")


	def test_case_special_4_1(self):
		super().start_fetch(super().client2())

		super().assert_table_state(super().client1(), 1001, 2, "localdisk")
		super().assert_table_state(super().client2(), 1001, 2, "localdisk")

		super().start_merge(super().client1())

		super().assert_table_state(super().client1(), 1001, 1, "localdisk")
		super().assert_table_state(super().client2(), 1001, 2, "localdisk")

		super().start_move(super().client1())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "localdisk")

		super().start_merge(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

		super().start_move(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

	def test_case5(self):
		super().start_fetch(super().client2())

		super().assert_table_state(super().client1(), 1001, 2, "localdisk")
		super().assert_table_state(super().client2(), 1001, 2, "localdisk")

		super().start_merge(super().client1())

		super().assert_table_state(super().client1(), 1001, 1, "localdisk")
		super().assert_table_state(super().client2(), 1001, 2, "localdisk")

		super().start_merge(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "localdisk")
		super().assert_table_state(super().client2(), 1001, 1, "localdisk")

		super().start_move(super().client1())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "localdisk")


		super().start_fetch(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "localdisk")

		super().start_move(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

class Test_cubefs_for_merge2_piece_1(base.Base):
	def setup_method(self):
		super().init(self.__class__.__name__)
		super().drop_table_if_exist(super().client1())
		super().drop_table_if_exist(super().client2())
		super().create_table(super().client1())
		super().forbidden_all_actions(super().client1())
		super().insert_one_row_for_loop(super().client1(), 1)
		super().insert_one_thousand_row_for_loop(super().client1(), 1)
		super().assert_table_state(super().client1(), 1001, 2, "localdisk")
		super().create_table(super().client2())
		super().forbidden_all_actions(super().client2())
		super().assert_table_state(super().client2(), 0, 0, "localdisk")
		super().start_send(super().client1())
		super().start_send(super().client2())

	def teardown_method(self):
		#input('press any key to exit')
		super().drop_table_if_exist(super().client1())
		super().drop_table_if_exist(super().client2())
		super().uninit()

	def test_case6(self):
		super().start_fetch(super().client2())

		super().assert_table_state(super().client1(), 1001, 2, "localdisk")
		super().assert_table_state(super().client2(), 1001, 2, "localdisk")

		super().start_merge(super().client1())

		super().assert_table_state(super().client1(), 1001, 1, "localdisk")
		super().assert_table_state(super().client2(), 1001, 2, "localdisk")

		super().start_merge(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "localdisk")
		super().assert_table_state(super().client2(), 1001, 1, "localdisk")

		super().start_move(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "localdisk")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

		super().start_move(super().client1())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")


	def test_case7(self):

		super().start_merge(super().client1())

		super().assert_table_state(super().client1(), 1001, 1, "localdisk")
		super().assert_table_state(super().client2(), 0, 0, "localdisk")

		super().start_move(super().client1())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 0, 0, "localdisk")

		super().start_fetch(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")


		super().start_merge(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

		super().start_move(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

	def test_case8(self):

		super().start_merge(super().client1())

		super().assert_table_state(super().client1(), 1001, 1, "localdisk")
		super().assert_table_state(super().client2(), 0, 0, "localdisk")

		super().start_fetch(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "localdisk")
		super().assert_table_state(super().client2(), 1001, 1, "localdisk")

		super().start_move(super().client1())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "localdisk")


		super().start_merge(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "localdisk")

		super().start_move(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")


	def test_case9(self):

		super().start_merge(super().client1())

		super().assert_table_state(super().client1(), 1001, 1, "localdisk")
		super().assert_table_state(super().client2(), 0, 0, "localdisk")

		super().start_fetch(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "localdisk")
		super().assert_table_state(super().client2(), 1001, 1, "localdisk")

		super().start_merge(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "localdisk")
		super().assert_table_state(super().client2(), 1001, 1, "localdisk")

		super().start_move(super().client1())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "localdisk")

		super().start_move(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

	def test_case10(self):
		super().start_merge(super().client1())

		super().assert_table_state(super().client1(), 1001, 1, "localdisk")
		super().assert_table_state(super().client2(), 0, 0, "localdisk")

		super().start_fetch(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "localdisk")
		super().assert_table_state(super().client2(), 1001, 1, "localdisk")

		super().start_merge(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "localdisk")
		super().assert_table_state(super().client2(), 1001, 1, "localdisk")

		super().start_move(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "localdisk")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")


		super().start_move(super().client1())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

class Test_cubefs_for_merge2_piece_3(base.Base):
	def setup_method(self):
		super().init(self.__class__.__name__)
		super().drop_table_if_exist(super().client1())
		super().drop_table_if_exist(super().client2())
		super().create_table(super().client1())
		super().forbidden_all_actions(super().client1())
		super().insert_one_row_for_loop(super().client1(), 1)
		super().insert_one_thousand_row_for_loop(super().client1(), 1)
		super().assert_table_state(super().client1(), 1001, 2, "localdisk")
		super().create_table(super().client2())
		super().forbidden_all_actions(super().client2())
		super().assert_table_state(super().client2(), 0, 0, "localdisk")
		super().start_send(super().client1())
		super().start_send(super().client2())

	def teardown_method(self):
		#input('press any key to exit')
		super().drop_table_if_exist(super().client1())
		super().drop_table_if_exist(super().client2())
		super().uninit()

	def test_case16(self):
		super().start_fetch(super().client2())

		super().assert_table_state(super().client1(), 1001, 2, "localdisk")
		super().assert_table_state(super().client2(), 1001, 2, "localdisk")

		super().start_merge(super().client2())

		super().assert_table_state(super().client1(), 1001, 2, "localdisk")
		super().assert_table_state(super().client2(), 1001, 1, "localdisk")

		super().start_move(super().client2())

		super().assert_table_state(super().client1(), 1001, 2, "localdisk")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

		super().start_merge(super().client1())
		super().start_move(super().client2())

		super().assert_table_state(super().client1(), 1001, 2, "localdisk")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

		super().start_fetch(super().client1())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

		super().start_move(super().client1())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

	def test_case17(self):
		super().start_fetch(super().client1())
		super().start_fetch(super().client2())

		super().assert_table_state(super().client1(), 1001, 2, "localdisk")
		super().assert_table_state(super().client2(), 1001, 2, "localdisk")

		super().start_merge(super().client2())

		super().assert_table_state(super().client1(), 1001, 2, "localdisk")
		super().assert_table_state(super().client2(), 1001, 1, "localdisk")

		super().start_move(super().client1())

		super().assert_table_state(super().client1(), 1001, 2, "localdisk")
		super().assert_table_state(super().client2(), 1001, 1, "localdisk")

		super().start_merge(super().client1())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "localdisk")

		super().start_move(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

	def test_case18(self):
		super().start_fetch(super().client1())
		super().start_fetch(super().client2())

		super().assert_table_state(super().client1(), 1001, 2, "localdisk")
		super().assert_table_state(super().client2(), 1001, 2, "localdisk")

		super().start_move(super().client1())

		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "localdisk")

		super().start_merge(super().client2())

		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

		super().start_move(super().client2())

		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

		super().start_merge(super().client1())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

	def test_case19(self):
		super().start_fetch(super().client1())
		super().start_fetch(super().client2())

		super().assert_table_state(super().client1(), 1001, 2, "localdisk")
		super().assert_table_state(super().client2(), 1001, 2, "localdisk")

		super().start_move(super().client1())

		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "localdisk")

		super().start_merge(super().client2())

		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

		super().start_merge(super().client1())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

		super().start_move(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

	def test_case20(self):
		super().start_fetch(super().client1())
		super().start_fetch(super().client2())

		super().assert_table_state(super().client1(), 1001, 2, "localdisk")
		super().assert_table_state(super().client2(), 1001, 2, "localdisk")

		super().start_move(super().client1())

		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "localdisk")

		super().start_move(super().client2())

		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "cfs")

		super().start_merge(super().client2())

		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

		super().start_merge(super().client1())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

class Test_cubefs_for_merge2_piece_4(base.Base):
	def setup_method(self):
		super().init(self.__class__.__name__)
		super().drop_table_if_exist(super().client1())
		super().drop_table_if_exist(super().client2())
		super().create_table(super().client1())
		super().forbidden_all_actions(super().client1())
		super().insert_one_row_for_loop(super().client1(), 1)
		super().insert_one_thousand_row_for_loop(super().client1(), 1)
		super().assert_table_state(super().client1(), 1001, 2, "localdisk")
		super().create_table(super().client2())
		super().forbidden_all_actions(super().client2())
		super().assert_table_state(super().client2(), 0, 0, "localdisk")
		super().start_send(super().client1())
		super().start_send(super().client2())

	def teardown_method(self):
		#input('press any key to exit')
		super().drop_table_if_exist(super().client1())
		super().drop_table_if_exist(super().client2())
		super().uninit()

	def test_case21(self):
		super().start_fetch(super().client1())
		super().start_fetch(super().client2())

		super().assert_table_state(super().client1(), 1001, 2, "localdisk")
		super().assert_table_state(super().client2(), 1001, 2, "localdisk")

		super().start_move(super().client1())

		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "localdisk")

		super().start_move(super().client2())

		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "cfs")

		super().start_merge(super().client1())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "cfs")

		super().start_merge(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")


	def test_case22(self):
		super().start_move(super().client1())

		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 0, 0, "localdisk")

		super().start_fetch(super().client1())
		super().start_fetch(super().client2())

		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "cfs")

		super().start_move(super().client2())

		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "cfs")

		super().start_merge(super().client1())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "cfs")

		super().start_merge(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

	def test_case23(self):
		super().start_move(super().client1())
		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 0, 0, "localdisk")

		super().start_fetch(super().client1())
		super().start_fetch(super().client2())

		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "cfs")

		super().start_merge(super().client1())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "cfs")

		super().start_move(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "cfs")

		super().start_merge(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

	def test_case24(self):
		super().start_move(super().client1())

		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 0, 0, "localdisk")

		super().start_merge(super().client1())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 0, 0, "localdisk")

		super().start_fetch(super().client1())
		super().start_fetch(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

		super().start_move(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

		super().start_merge(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")


	def test_case25(self):
		super().start_move(super().client1())

		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 0, 0, "localdisk")

		super().start_merge(super().client1())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 0, 0, "localdisk")

		super().start_fetch(super().client1())
		super().start_fetch(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

		super().start_merge(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

		super().start_move(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")


class Test_cubefs_for_merge2_piece_5(base.Base):
	def setup_method(self):
		super().init(self.__class__.__name__)
		super().drop_table_if_exist(super().client1())
		super().drop_table_if_exist(super().client2())
		super().create_table(super().client1())
		super().forbidden_all_actions(super().client1())
		super().insert_one_row_for_loop(super().client1(), 1)
		super().insert_one_thousand_row_for_loop(super().client1(), 1)
		super().assert_table_state(super().client1(), 1001, 2, "localdisk")
		super().create_table(super().client2())
		super().forbidden_all_actions(super().client2())
		super().assert_table_state(super().client2(), 0, 0, "localdisk")
		super().start_send(super().client1())
		super().start_send(super().client2())

	def teardown_method(self):
		#input('press any key to exit')
		super().drop_table_if_exist(super().client1())
		super().drop_table_if_exist(super().client2())
		super().uninit()

	def test_case26(self):
		super().start_move(super().client1())

		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 0, 0, "localdisk")

		super().start_fetch(super().client1())
		super().start_fetch(super().client2())

		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "cfs")

		super().start_merge(super().client1())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "cfs")

		super().start_merge(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

		super().start_move(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")


	def test_case27(self):
		super().start_move(super().client1())

		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 0, 0, "localdisk")

		super().start_fetch(super().client1())
		super().start_fetch(super().client2())

		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "cfs")

		super().start_move(super().client2())

		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "cfs")

		super().start_merge(super().client1())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "cfs")

		super().start_merge(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

	def test_case28(self):
		super().start_fetch(super().client1())
		super().start_fetch(super().client2())

		super().assert_table_state(super().client1(), 1001, 2, "localdisk")
		super().assert_table_state(super().client2(), 1001, 2, "localdisk")

		super().start_move(super().client1())

		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "localdisk")

		super().start_move(super().client2())

		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "cfs")

		super().start_merge(super().client1())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "cfs")

		super().start_merge(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")


	def test_case29(self):
		super().start_fetch(super().client1())
		super().start_fetch(super().client2())

		super().assert_table_state(super().client1(), 1001, 2, "localdisk")
		super().assert_table_state(super().client2(), 1001, 2, "localdisk")

		super().start_move(super().client1())

		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "localdisk")


		super().start_merge(super().client1())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "localdisk")

		super().start_move(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "localdisk")

		super().start_merge(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")


class Test_cubefs_for_merge2_special_1(base.Base):
	def setup_method(self):
		super().init(self.__class__.__name__)
		super().drop_table_if_exist(super().client1())
		super().drop_table_if_exist(super().client2())
		super().create_table(super().client1())
		super().forbidden_all_actions(super().client1())
		super().insert_one_row_for_loop(super().client1(), 1)
		super().insert_one_thousand_row_for_loop(super().client1(), 1)
		super().assert_table_state(super().client1(), 1001, 2, "localdisk")

		super().start_merge(super().client1())
		super().assert_table_state(super().client1(), 1001, 1, "localdisk")

		super().create_table(super().client2())
		super().forbidden_all_actions(super().client2())
		super().assert_table_state(super().client2(), 0, 0, "localdisk")
		super().start_send(super().client1())
		super().start_send(super().client2())

	def teardown_method(self):
		#input('press any key to exit')
		super().drop_table_if_exist(super().client1())
		super().drop_table_if_exist(super().client2())
		super().uninit()


	def test_case11(self):
		super().start_fetch(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "localdisk")
		super().assert_table_state(super().client2(), 1001, 1, "localdisk")

		super().start_merge(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "localdisk")
		super().assert_table_state(super().client2(), 1001, 1, "localdisk")

		super().start_move(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "localdisk")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

		super().start_move(super().client1())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")


	def test_case12(self):
		super().start_fetch(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "localdisk")
		super().assert_table_state(super().client2(), 1001, 1, "localdisk")

		super().start_merge(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "localdisk")
		super().assert_table_state(super().client2(), 1001, 1, "localdisk")

		super().start_move(super().client1())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "localdisk")

		super().start_move(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")


	def test_case13(self):
		super().start_fetch(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "localdisk")
		super().assert_table_state(super().client2(), 1001, 1, "localdisk")

		super().start_move(super().client1())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "localdisk")

		super().start_merge(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "localdisk")

		super().start_move(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")



	def test_case14(self):
		super().start_move(super().client1())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 0, 0, "localdisk")

		super().start_fetch(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

		super().start_merge(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

		super().start_move(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")


class Test_cubefs_for_merge2_special_2(base.Base):
	def setup_method(self):
		super().init(self.__class__.__name__)
		super().drop_table_if_exist(super().client1())
		super().drop_table_if_exist(super().client2())
		super().create_table(super().client1())
		super().forbidden_all_actions(super().client1())
		super().insert_one_row_for_loop(super().client1(), 1)
		super().insert_one_thousand_row_for_loop(super().client1(), 1)
		super().assert_table_state(super().client1(), 1001, 2, "localdisk")

		super().start_merge(super().client1())

		super().assert_table_state(super().client1(), 1001, 1, "localdisk")

		super().start_move(super().client1())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")

		super().create_table(super().client2())
		super().forbidden_all_actions(super().client2())
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

		super().start_send(super().client1())
		super().start_send(super().client2())

	def teardown_method(self):
		#input('press any key to exit')
		super().drop_table_if_exist(super().client1())
		super().drop_table_if_exist(super().client2())
		super().uninit()

	def test_case15(self):

		super().start_fetch(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

		super().start_merge(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

		super().start_move(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")


class Test_cubefs_for_merge2_special_3(base.Base):
	def setup_method(self):
		super().init(self.__class__.__name__)
		super().drop_table_if_exist(super().client1())
		super().drop_table_if_exist(super().client2())
		super().create_table(super().client1())
		super().forbidden_all_actions(super().client1())
		super().insert_one_row_for_loop(super().client1(), 1)
		super().insert_one_thousand_row_for_loop(super().client1(), 1)
		super().assert_table_state(super().client1(), 1001, 2, "localdisk")

		super().start_move(super().client1())
		super().assert_table_state(super().client1(), 1001, 2, "cfs")

		super().create_table(super().client2())
		super().forbidden_all_actions(super().client2())
		super().assert_table_state(super().client2(), 1001, 2, "cfs")
		super().start_send(super().client1())
		super().start_send(super().client2())

	def teardown_method(self):
		#input('press any key to exit')
		super().drop_table_if_exist(super().client1())
		super().drop_table_if_exist(super().client2())
		super().uninit()

	def test_case30(self):
		super().start_fetch(super().client1())
		super().start_fetch(super().client2())

		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "cfs")

		super().start_merge(super().client1())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "cfs")

		super().start_move(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "cfs")

		super().start_merge(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

	def test_case31(self):
		super().start_fetch(super().client1())
		super().start_fetch(super().client2())

		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "cfs")

		super().start_merge(super().client1())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "cfs")

		super().start_merge(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

		super().start_move(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

	def test_case32(self):
		super().start_merge(super().client1())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "cfs")

		super().start_fetch(super().client1())
		super().start_fetch(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "cfs")

		super().start_move(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "cfs")

		super().start_merge(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")



	def test_case33(self):
		super().start_merge(super().client1())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "cfs")

		super().start_fetch(super().client1())
		super().start_fetch(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "cfs")

		super().start_merge(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

		super().start_move(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

class Test_cubefs_for_merge2_special_4(base.Base):
	def setup_method(self):
		super().init(self.__class__.__name__)
		super().drop_table_if_exist(super().client1())
		super().drop_table_if_exist(super().client2())
		super().create_table(super().client1())
		super().forbidden_all_actions(super().client1())
		super().insert_one_row_for_loop(super().client1(), 1)
		super().insert_one_thousand_row_for_loop(super().client1(), 1)
		super().assert_table_state(super().client1(), 1001, 2, "localdisk")

		super().start_move(super().client1())
		super().assert_table_state(super().client1(), 1001, 2, "cfs")

		super().start_merge(super().client1())
		super().assert_table_state(super().client1(), 1001, 1, "cfs")

		super().create_table(super().client2())
		super().forbidden_all_actions(super().client2())
		super().assert_table_state(super().client2(), 1001, 1, "cfs")
		super().start_send(super().client1())
		super().start_send(super().client2())

	def teardown_method(self):
		#input('press any key to exit')
		super().restore_all_actions(super().client1())
		super().restore_all_actions(super().client2())
		super().drop_table_if_exist(super().client1())
		super().drop_table_if_exist(super().client2())
		super().uninit()

	def test_case34(self):
		super().start_fetch(super().client1())
		super().start_fetch(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

		super().start_merge(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

		super().start_move(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

	def test_case35(self):
		super().start_fetch(super().client1())
		super().start_fetch(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

		super().start_move(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")

		super().start_merge(super().client2())

		super().assert_table_state(super().client1(), 1001, 1, "cfs")
		super().assert_table_state(super().client2(), 1001, 1, "cfs")


class Test_cubefs_for_no_merge(base.Base):
	def setup_method(self):
		super().init(self.__class__.__name__)
		super().create_table(super().client1())
		super().create_table(super().client2())
		super().forbidden_all_actions(super().client1())
		super().forbidden_all_actions(super().client2())
		super().insert_one_row_for_loop(super().client1(), 1)
		super().insert_one_thousand_row_for_loop(super().client1(), 1)

	def teardown_method(self):
		#input('press any key to exit')
		super().drop_table_if_exist(super().client1())
		super().drop_table_if_exist(super().client2())
		super().uninit()

	def test_case1(self):
		super().assert_table_state(super().client1(), 1001, 2, "localdisk")
		super().assert_table_state(super().client2(), 0, 0, "localdisk")

		super().start_send(super().client1())
		super().start_fetch(super().client2())

		super().assert_table_state(super().client1(), 1001, 2, "localdisk")
		super().assert_table_state(super().client2(), 1001, 2, "localdisk")

		super().start_move(super().client1())
		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "localdisk")

		super().start_move(super().client2())
		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "cfs")

	def test_case2(self):
		super().assert_table_state(super().client1(), 1001, 2, "localdisk")
		super().assert_table_state(super().client2(), 0, 0, "localdisk")

		super().start_move(super().client1())
		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 0, 0, "localdisk")

		super().start_send(super().client1())
		super().start_fetch(super().client2())
		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "cfs")

		super().start_move(super().client2())
		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "cfs")

class Test_cubefs_for_no_merge2(base.Base):
	def setup_method(self):
		super().init(self.__class__.__name__)
		super().drop_table_if_exist(super().client1())
		super().drop_table_if_exist(super().client2())
		super().create_table(super().client1())
		super().forbidden_all_actions(super().client1())
		super().insert_one_row_for_loop(super().client1(), 1)
		super().insert_one_thousand_row_for_loop(super().client1(), 1)

		super().assert_table_state(super().client1(), 1001, 2, "localdisk")
		super().assert_table_exist(super().client2(), False)

		super().create_table(super().client2())
		super().forbidden_all_actions(super().client2())

	def teardown_method(self):
		#input('press any key to exit')
		super().drop_table_if_exist(super().client1())
		super().drop_table_if_exist(super().client2())
		super().uninit()

	def test_case3(self):
		super().start_send(super().client1())
		super().assert_table_state(super().client1(), 1001, 2, "localdisk")
		super().assert_table_state(super().client2(), 0, 0, "localdisk")

		super().start_fetch(super().client2())
		super().assert_table_state(super().client1(), 1001, 2, "localdisk")
		super().assert_table_state(super().client2(), 1001, 2, "localdisk")


		super().start_move(super().client1())
		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "localdisk")

		super().start_move(super().client2())

		super().assert_table_state(super().client1(), 1001, 2, "cfs")
		super().assert_table_state(super().client2(), 1001, 2, "cfs")
		super().drop_table_if_exist(super().client1())
		super().drop_table_if_exist(super().client2())

if __name__=="__main__":
	pytest.main(['-v', '-n32' , '--dist=loadscope', 'test_cubefs_for_merge.py'])
