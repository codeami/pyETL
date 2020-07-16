# todo Fix extra elements in excluded_schema_tables_set
# todo Check if strip transform is expected by default on Varchar type
# todo Add a feature to run certain <strip> transform by default on certain <Varchar> type of column
import os
import sys
import pytest
from pandas_helper import *
from _pytest.fixtures import pytestconfig

if sys.platform == 'linux':
    sys.path.insert(1, os.path.dirname(os.getcwd()))

from pandas_helper import *

# Todo Fix for local pycharm runner - DONE Ashish

db = os.path.basename(__file__).replace('test_', '').replace('.py', '')
abs_script_name = os.path.abspath(__file__).replace('.py', '')
all_data = read_data(abs_script_name)
test_data = filter_dict(all_data, 'test')
schema_tables_dict = all_data['schema_tables_dict']
excluded_tables_dict = all_data['excluded']
# class1_schema_tables_dict = all_data['class1_schema_tables_dict']
test_content_config = test_data['test_content_config']
connector1 = test_content_config['connector1']
connector2 = test_content_config['connector2']
query1 = test_content_config['query1']
query2 = test_content_config['query2']


@pytest.fixture()
def c1(pytestconfig):
    return connector1 if pytestconfig.getoption("--conn1") is None else pytestconfig.getoption("--conn1")


@pytest.fixture()
def c2(pytestconfig):
    return connector2 if pytestconfig.getoption("--conn2") is None else pytestconfig.getoption("--conn2")


@pytest.fixture()
def q1(pytestconfig):
    return query2 if pytestconfig.getoption("--query1") is None else pytestconfig.getoption("--query1")


@pytest.fixture()
def q2(pytestconfig):
    return query2 if pytestconfig.getoption("--query1") is None else pytestconfig.getoption("--query2")


# params = []
# for schema1, tables_list in class1_schema_tables_dict.items():
#     schema2 = db + "_" + schema1
#     if 'data_lake' in connector1.lower():
#         schema1 = db + "_" + schema1
#     params.append((c1, c2, schema1, schema2, tables_list))
#

os.environ['use_cache'] = test_content_config['use_cache'] if 'use_cache' in test_content_config else ''
os.environ['size_check'] = test_content_config['size_check'] if 'size_check' in test_content_config else ''


# @pytest.mark.parametrize(('c1', 'c2', 'sch1', 'sch2', 'tables'), params, indirect=["c1", "c2"])
# def test_rowcount(c1, c2, sch1, sch2, tables):
#     assert db_rowcount_diff(c1, c2, sch1, sch2, tables) <= 3.0, "FAILURE MESSAGE: Diff is more than threshold"


excluded_schema_tables_set = {k + x for k, v in excluded_tables_dict.items() for x in v}
params2 = []
for schema, tables_list in schema_tables_dict.items():
    for key, test_data_set in test_data.items():
        for table in tables_list:
            if schema + table not in excluded_schema_tables_set:
                primary_key = test_data_set['p_key']
                transforms = dict(test_data_set['transforms']) if 'transforms' in test_data_set else ''

                params2.append(
                    (db + '__' + schema + '__' + table.lower() + '__', primary_key, c1, c2, transforms))


@pytest.mark.parametrize(('db_schema_table', 'primary_key', 'c1', 'c2', 'transforms'), params2,
                         indirect=["c1", "c2"])
def test_content(db_schema_table, c1, c2, primary_key, transforms):
    assert table_content_diff(db_schema_table, c1, c2, primary_key,
                              transforms) <= 1.0, "FAILURE MESSAGE: Diff is more than threshold"

#
# def test_columns_content_diff(q1, q2, c1, c2, primary_key='', transforms=''):
#     assert columns_content_diff(q1, q2, c1, c2) <= 1.0, "FAILURE MESSAGE: Diff is more than threshold"
