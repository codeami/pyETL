# todo Fix extra elements in excluded_schema_tables_set
# todo Check if strip transform is expected by default on Varchar type
# todo Add a feature to run certain <strip> transform by default on certain <Varchar> type of column
import os
import sys
import pytest
from _pytest.fixtures import pytestconfig

if sys.platform == 'linux':
    sys.path.insert(1, os.path.dirname(os.getcwd()))

from pandas_helper import *

# Todo Fix for local pycharm runner

script_name = os.path.basename(__file__).replace('test_', '').replace('.py', '')
abs_script_name = os.path.abspath(__file__).replace('.py', '')
schema_tables_dict = read_data(abs_script_name)['schema_tables_dict']
test_data = filter_dict(read_data(abs_script_name), 'test')
class1_schema_tables_dict = read_data(abs_script_name)['class1_schema_tables_dict']
connector1 = test_data['test_content_config']['connector1']
connector2 = test_data['test_content_config']['connector2']
schema_prefix = script_name


@pytest.fixture()
def c1(pytestconfig):
    return connector1 if pytestconfig.getoption("--conn1") is None else pytestconfig.getoption("--conn1")


@pytest.fixture()
def c2(pytestconfig):
    return connector2 if pytestconfig.getoption("--conn2") is None else pytestconfig.getoption("--conn2")


params = []
for schema1, tables_list in class1_schema_tables_dict.items():
    schema2 = schema_prefix + "_" + schema1
    if 'data_lake' in connector1.lower():
        schema1 = schema_prefix + "_" + schema1
    params.append((c1, c2, schema1, schema2, tables_list))


@pytest.mark.parametrize(('c1', 'c2', 'sch1', 'sch2', 'all_tables'), params, indirect=["c1", "c2"])
def test_rowcount(c1, c2, sch1, sch2, all_tables):
    assert db_rowcount_diff(c1, c2, sch1, sch2, all_tables) <= 3.0, "FAILURE MESSAGE: Diff is more than threshold"


schema_tables_list = []
for schema, tables_list in schema_tables_dict.items():
    if type(tables_list) == list:
        for table in tables_list:
            schema_tables_list.append(schema + table)

    else:
        schema_tables_list.append(schema + str(tables_list))

excluded_tables_dict = read_data(abs_script_name)['excluded']
excluded_schema_tables_set = {k + x for k, v in excluded_tables_dict.items() for x in v}

params2 = []
for schema, tables_list in schema_tables_dict.items():
    for key, test_data_set in test_data.items():
        for table in tables_list:
            if schema + table not in excluded_schema_tables_set:
                # @pytest.mark.parametrize('table', table)
                query1 = test_data_set['query1'].replace('table', table.upper()).replace('schema', schema)
                query2 = test_data_set['query2'].replace('table', table.lower()).replace('schema', schema)
                connector1 = test_data_set['connector1']
                connector2 = test_data_set['connector2']
                primary_key = test_data_set['p_key']
                os.environ['query2'] = query2

                transforms = dict(test_data_set['transforms']) if 'transforms' in test_data_set else ''
                os.environ['use_cache'] = test_data_set['use_cache'] if 'use_cache' in test_data_set else ''
                params2.append((schema + '_' + table, query1, query2, primary_key, connector1, connector2, transforms))


@pytest.mark.parametrize(('schema_table', 'query1', 'query2', 'primary_key', 'connector1', 'connector2', 'transforms'), params2)
def test_content(schema_table, query1, query2, primary_key, connector1, connector2, transforms):
    assert table_content_diff(schema_table, query1, query2, primary_key, connector1, connector2, transforms) <= 1.0, "FAILURE MESSAGE: Diff is more than threshold"
