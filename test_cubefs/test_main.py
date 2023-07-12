import pytest
import yaml
import os
import time
import uuid
import base

from clickhouse_driver import Client

if __name__=="__main__":
	pytest.main(['-v', '-n64' , '--dist=loadscope', 'test_cubefs_for_merge.py', 'test_cubefs_for_mutation.py', 'test_cubefs_for_delete.py'])

