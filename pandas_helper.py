import os
import shutil
import sys
import time
import uuid
import re
from datetime import datetime, timedelta
from tkinter import messagebox # GUI

from IPython.utils.io import Tee
from contextlib import closing

from ruamel.yaml import YAML
import pytest

import pyodbc
import cx_Oracle
import psycopg2
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine

import pandas as pd
import numpy as np


# Extract helpers - Query databases- Oracle, AWS (Aurora), Snowflake, MSSQL
def extract(conn_key, query, label=''):
    connection = get_connection(conn_key)
    log('EXTRACTING query ' + str(label) + ': -\n' + query + ' - from' + str(label) + ': ' + conn_key)
    start = time.time()
    if 'snowflake' in str(connection).lower():
        df = connection.execute(query).fetch_pandas_all()
    if 'pyodbc' in str(connection).lower():
        df = pd.read_sql(query, connection)
    if 'oracle' in str(connection).lower() or 'auroradb' in str(connection).lower():
        try:
            df = pd.read_sql_query(query, connection)
        except ValueError:
            log('ERROR: read_sql_query')

            connection.outputtypehandler = OutputHandler
            df = pd.read_sql_query(query, connection)
            log('ERROR: read_sql_query')

    time_taken = timedelta(seconds=int(time.time() - start))
    os.environ['time_taken'] = str(time_taken)
    log('EXTRACTING time taken to extract query: ' + str(time_taken))
    log('SIZE Extracted from Rows-Columns counts: ' + str(df.shape))
    return df


os.environ['NLS_DATE_FORMAT'] = 'YYYY-MM-DD HH24:MI:SS'


def DateTimeConverter(value):
    if value.startswith('9999'):
        return None
    return datetime.now().strptime(value, '%d-%b-%y %H-%M-%S')


def OutputHandler(cursor, name, defaulttype, length, precision, scale):
    if defaulttype == cx_Oracle.DATETIME:
        return cursor.var(cx_Oracle.STRING, arraysize=cursor.arraysize,
                          outconverter=DateTimeConverter)


# data_chunks = pandas.read_sql_query(
#     "SELECT * FROM table_name",
#     con=pyodbc_connection,
#     chunksize=100000
# )

def size_of_table_in_MB(testname, schema2, table):
    check_oracle_size = "select meg from ( select  owner,  segment_name,  bytes/1024/1024 meg from  dba_segments \
    where  segment_type = 'TABLE') where segment_name = '" + table + "'"

    data_lake_conn = 'data_lake_prod'
    check_size2 = "select pg_relation_size('" + schema2 + "." + table + "')"
    table_size_in_mb = dict(extract(data_lake_conn, check_size2))['pg_relation_size'][0] // 1024 // 1024
    log('INFO: Table size is: ' + str(table_size_in_mb) + ' MB.')
    log('INFO: Approximate Extraction time is: ' + str(table_size_in_mb // 11.78 // 2) + ' Minutes.')
    # 247 mins - 2909 MB ~ 11.77 MB per minute
    if table_size_in_mb > 3000:
        log('SKIPPING THIS TABLE TEST due to Large result size. PLease partition your results'
            '------------------------------------------------')
        save_temp_output_log(testname, '_large_table_error_size_' + str(table_size_in_mb) + "_MB")

    return table_size_in_mb


def extract_or_load(conn, query, idx, cached_df):
    create_folder('cache')
    cached_df = 'cache\\' + cached_df
    # todo add check cache size > 0
    if bool(os.environ['use_cache']) and os.path.isfile(cached_df) and os.stat(cached_df).st_size > 0:
        c_age = str(timedelta(seconds=int(time.time() - os.path.getctime(cached_df))))
        m_age = str(timedelta(seconds=int(time.time() - os.path.getmtime(cached_df))))
        # CACHED_QUERY, CACHE_MODIFIED_AGE, EXTRACT_DURATION_SOURCE, EXTRACT_DURATION_TARGET, TEST_RUN_DURATION
        os.environ['cache_age'] = m_age
        log("LOADING query" + str(idx) + " results table from cached file: " + cached_df + ' Created: ' + c_age +
            ' H:MM:SS ago. Modified: ' + m_age + ' H:MM:SS ago.')
        start = time.time()
        df = pd.read_pickle(cached_df)
        os.environ['time_taken' + str(idx)] = str(timedelta(seconds=int(time.time() - start)))
        log("SIZE of cached query" + str(idx) + " results table is: " + str(df.shape) + " (rows, columns)")
    elif bool(os.environ['use_cache']):  # Save Cache
        os.environ['cache_age'] = ''
        df = extract(conn, query, str(idx))
        os.environ['time_taken' + str(idx)] = os.environ['time_taken']
        # log_columns_dtypes(df)
        try:
            df.to_pickle(cached_df)
            log("SAVING query" + str(idx) + " result to cache file: " + cached_df)
        except TypeError:
            log("WARN: UNABLE to Save query" + str(idx) + " result to cache file: " + cached_df)
            col_types = set()
            memoryview_cols = set()
            for col in df.columns:
                if str(type(df[col][0])) == "<class 'memoryview'>":
                    memoryview_cols.add(col)
                    df[col] = df[col].fillna(0)
                    df[col] = df[col].apply(bytes)
                    side = 'Source' if idx == 1 else 'Target'
                    log("WARN: Found memoryview column - cast to bytes. " + side + ": " + str(memoryview_cols))
            # print(col_types)
            df.to_pickle(cached_df)
            log("SAVING query" + str(idx) + " result to cache file: " + cached_df)
            # df = 0
            # df = dataframe_memoryview_to_bytes(df)
            # df.to_pickle(cached_df)
    else:
        os.environ['cache_age'] = ''
        df = extract(conn, query, str(idx))
        os.environ['time_taken' + str(idx)] = os.environ['time_taken']

        # todo TypeError: can't pickle memoryview objects

    return df


def dataframe_memoryview_to_bytes(df):
    for col in df.columns:
        if df[col].dtype == object:
            # if type(dataframe[col][0]) == memoryview:
            #     log('Found a memoryview type column converting it to bytes')

            log(df[col][0])
            log(str(df[col][0]))
            log(str(type(df[col][0])))
            # log(str(df[col][0].PyMemoryView_Check))
            # df[col] = df[col].apply(str(encoding='utf8'))
    return df


# Load Results DB Helpers
def send_result_to_db(result):
    conn = get_connection('snowflake')
    schema = 'SOURCE_DB,SOURCE_SCHEMA,TARGET_DB,TARGET_SCHEMA,ONEDATATARGETZONE,SOURCE_TABLE,CONTENT_DIFF,' \
             'CONTENT_DIFF_PCT,SOURCE_ONLY,TARGET_ONLY,PRIMARY_KEY,ROWCOUNT_SOURCE,ROWCOUNT_TARGET,' \
             'COLCOUNT_SOURCE,COLCOUNT_TARGET,ROWCOUNT_DIFF,ROWCOUNT_DIFF_PCT,LASTRUNDATE,SOURCESYSTEMNAME,REPLICATIONTYPE,RUNID,' \
             'CACHED_QUERY,CACHE_AGE,EXTRACT_DURATION_SOURCE,EXTRACT_DURATION_TARGET,TEST_RUN_DURATION'

    conn.execute(
        'INSERT INTO DATA_LAKE_DEV.ONEDATA_SYSTEM_STATUS.TBL_DATA_CONTENT_VALIDATION_AR_TESTING2 (' + schema +
        ') VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
        result)
    log('SENT result to db: ' + str(dict(zip(list(schema.split(',')), result))))


# Compare helpers
def db_rowcount_diff(conn1, conn2, schema1, schema2, tables_list):
    log(
        "------------------------------------------ START OF ROWCOUNT TEST -----------------------------------------------")
    env_type = conn1.upper().split('_')[1]
    source = '_'.join(conn1.split('_')[:-1])
    database2 = ''
    if 'data_lake' in conn2.lower():
        targetzone = 'SOURCE_TO_DL'
        target = 'US_DATA_LAKE'

    if 'snowflake' in conn2.lower():
        database2 = 'data_lake.'
        target = 'SNOWFLAKE_DATA_LAKE'
        if 'data_lake' in conn1.lower():
            targetzone = 'DL_TO_SNOWFLAKE'
        else:
            targetzone = 'SOURCE_TO_SNOWFLAKE'

    # result = [source, schema1, target, schema2, table, datetime.now(), source, env_type, targetzone]

    query1 = ""
    query2 = ""
    for table in tables_list:
        query1 = query1 + "SELECT '" + table + "' AS SOURCE_TABLE, COUNT(*) AS SOURCE_COUNT FROM " + schema1 + "." + table + "\nUNION ALL\n"
        query2 = query2 + "SELECT '" + table + "' AS SOURCE_TABLE, COUNT(*) AS TARGET_COUNT FROM " + database2 + schema2 + "." + table + "\nUNION ALL\n"
    query1 = query1[:-len("\nUNION ALL\n")]
    query2 = query2[:-len("\nUNION ALL\n")]
    df1 = extract(conn1, query1, '1')
    df2 = extract(conn2, query2, '2')
    # Todo Refactor
    df1.columns = ['SOURCE_TABLE', 'SOURCE_COUNT']
    df2.columns = ['SOURCE_TABLE', 'TARGET_COUNT']
    df1['TARGET_COUNT'] = df2['TARGET_COUNT']
    df1['DIFFERENCE'] = df1['SOURCE_COUNT'] - df1['TARGET_COUNT']
    df1['PCT_DIFF'] = round((df1['SOURCE_COUNT'] - df1['TARGET_COUNT']) / df1['SOURCE_COUNT'], 3) * 100
    df1['LASTRUNDATE'] = datetime.now()
    df1['SOURCESYSTEMNAME'] = source
    df1['ONEDATATARGETZONE'] = targetzone
    df1['REPLICATIONTYPE'] = env_type
    df1.insert(0, 'TARGET_SCHEMA', schema2)
    df1.insert(0, 'TARGET_DB', target)
    df1.insert(0, 'SOURCE_SCHEMA', schema1)
    df1.insert(0, 'SOURCE_DB', source)
    engine = get_connection('snowflake_sqlalchemy_connector')
    conn = engine.connect()
    df1.to_sql('TBL_ROWCOUNT_VALIDATION_AR_PYTEST'.lower(), con=engine, if_exists='append', index=False)
    conn.close()
    engine.dispose()
    max_diff_pct = df1['PCT_DIFF'].max()
    log(
        "------------------------------------------ END OF ROWCOUNT TEST -----------------------------------------------")
    return max_diff_pct


def table_content_diff(schema_tables, conn1='', conn2='', p_key='', transforms=''):
    log("\n------------------------------------------ START OF TABLE CONTENT DIFF "
        "-----------------------------------------------")
    start = time.time()
    env_type = conn1.upper().split('_')[1]
    source = '_'.join(conn1.upper().split('_')[:-1])
    if schema_tables != '':
        table = schema_tables.split('__')[2]
        db = schema_tables.split('__')[0]
        schema1 = schema_tables.split('__')[1]
        schema2 = (db + '_' + schema1).lower()
    # schema2 = query2.upper().split('FROM ')[1].split(' ')[0].split('.')[0]
    # schema1 = query1.upper().split('FROM ')[1].split(' ')[0].split('.')[0]
    database2 = ""
    if 'data_lake' in conn2.lower():
        targetzone = 'SOURCE_TO_DL'
        target = 'US_DATA_LAKE' if 'US' in conn2.upper() else 'UK_DATA_LAKE'
    elif 'snowflake' in conn2.lower():
        database2 = 'data_lake.'
        target = 'SNOWFLAKE_DATA_LAKE'
        if 'data_lake' in conn1.lower():
            targetzone = 'DL_TO_SNOWFLAKE'
            schema1 = schema2
        else:
            targetzone = 'SOURCE_TO_SNOWFLAKE'
    elif 'edw' in conn2.lower():
        messagebox.showerror("Target EDW is not supported for select * queries"
                             "Terminating execution. Please partition your query")
        return -1
    schema2 = schema2.replace('.dbo', '_dbo')
    query1 = "select * from " + schema1 + ".\"" + table + "\""  # if len(query1) == 0 else query1
    query2 = "select * from " + database2 + schema2 + ".\"" + table + "\""  # if len(query2) == 0 else query2
    if 'snowflake' in conn2.lower():
        query2 = query2.replace("\"", "")

    result = [source, schema1, target, schema2, targetzone, table, datetime.now(), source, env_type, uuid.uuid4().hex]

    # Naming Cache files with unique names- to reuse Query Results
    cached_df1 = conn1.upper() + '__' + query1.upper().split('FROM ')[1].split(' ')[0].replace('"', '') + '.pkl'
    cached_df2 = conn2.upper() + '__' + query2.upper().split('FROM ')[1].split(' ')[0].replace('"', '') + '.pkl'
    testname = conn1.upper() + '__' + conn2.upper() + '__' + query2.upper().split('FROM ')[1].split(' ')[0].replace('"',
                                                                                                                    '')

    # size_of_table_in_MB(testname, schema2, table)
    df1 = extract_or_load(conn1, query1, 1, cached_df1)
    df2 = extract_or_load(conn2, query2, 2, cached_df2)

    if type(df1) == int or type(df2) == int:
        log('SKIPPING THIS TABLE TEST due to memoryview error------------------------------------------------')
        save_temp_output_log(testname, '_memoryview error type(df1) eq int')
        return -1

    df1, df2 = upcase_column_names(df1, df2)

    if 'edw' in query2.lower():
        df1, df2 = concat_column_names(df1, df2)

    df1, df2 = delete_etl_status_columns(df1, df2)

    df1, df2 = cast_date_columns_to_timestamp(df1, df2)

    if type(transforms) == dict:
        if depth(transforms) == 4:
            df1, df2 = run_multiple_custom_transforms(df1, df2, transforms, table)
        else:
            df1, df2 = run_custom_transforms(df1, df2, transforms)

    # todo MONTHLYLOAD - check prev, current , and next month
    # todo after month close 20th - goes for a week - then validate entire data-sets
    # todo at other times - validate just 1-2 weeks
    # todo AS_OF_DT
    if not len(df1.columns) == len(df2.columns):
        log('INFO: COLUMNS do not match: Source: ' + str(df1.shape[1]) + ' vs Target: ' + str(df2.shape[1]))
        missing_in_target = str(set(list(df1.columns)) - set(list(df2.columns)))
        log('INFO: COLUMNS Missing: ' + missing_in_target)
        sys.exit("Columns likely missing in Target - please add Table-column-dropTransform in test config yaml file - "
                 + str(missing_in_target))
        # df1, df2 = remove_not_matching_columns_from_source_and_target(df1, df2)
        # Todo add stronger check for col names - PART DONE

    if not (df1.dtypes.values == df2.dtypes.values).all():
        df2, df1 = cast_column_data_types(df2, df1)

    final_result = dataframe_difference(df2, df1, p_key, testname, result)
    diff_pct = final_result[7]
    log('RESULT Dataframe difference Percentage: ' + str(diff_pct) + '%')
    final_result_dev = [os.environ['use_cache'], os.environ['cache_age'], os.environ['time_taken1'],
                        os.environ['time_taken2'], timedelta(seconds=int(time.time() - start))]
    send_result_to_db(final_result + final_result_dev)
    log(
        '------------------------------------------END OF TABLE CONTENT DIFF -----------------------------------------------')
    save_temp_output_log(testname, '_content_diff_pct-' + str(diff_pct))
    return diff_pct


def columns_content_diff(query1, query2, conn1, conn2, p_key='', transforms=''):
    os.environ['USE_CACHE'] = ''
    log(
        "\n------------------------------------------ START OF QUERIES CONTENT DIFF -----------------------------------------------")
    start = time.time()
    env_type = conn1.upper().rsplit('_', 1)[1]
    env_type2 = conn2.upper().rsplit('_', 1)[1]
    source = conn1
    schema1 = ''
    table = ''
    schema2 = schema1
    targetzone = 'SOURCE_TO_EDW'
    target = 'EDW_DEV'

    result = [source, schema1, target, schema2, targetzone, table, datetime.now(), source, env_type, uuid.uuid4().hex]

    # Naming Cache files with unique names- to reuse Query Results
    query1 = re.sub('\n', '', re.sub(' +', ' ', query1.strip()))
    query2 = re.sub('\n', '', re.sub(' +', ' ', query2.strip()))
    schema_table1 = query1.upper().split('FROM ')[1].split(' ')[0]
    schema_table2 = query2.upper().split('FROM ')[1].split(' ')[0]
    columns1 = query1.upper().split('FROM ')[0].replace('SELECT', '').replace('DISTINCT', '').replace('CONCAT',
                                                                                                      '').replace(
        'TRIM', '').replace("'|',", '').replace("(", '').replace(")", '').replace(",", ' ')
    columns1 = re.sub('\n', ' ', re.sub(' +', ' ', columns1.strip()))

    columns2 = query2.upper().split('FROM ')[0].replace('SELECT', '').replace('DISTINCT', '').replace('CONCAT',
                                                                                                      '').replace(
        'TRIM', '').replace("'|',", '').replace("(", '').replace(")", '').replace(",", ' ')
    columns2 = re.sub('\n', ' ', re.sub(' +', ' ', columns2.strip()))
    q1_lower = query1.lower()
    q2_lower = query2.lower()
    if 'concat' not in q1_lower and 'concat' not in q2_lower and 'join' not in q1_lower and 'join' not in q2_lower:
        if len(columns1.split(' ')) != len(columns2.split(' ')):
            messagebox.showerror('COLUMNS between source and target do NOT MATCH',
                                 '\nSource Columns: ' + columns1 + "\n Target Columns:  " + columns2 + "\n Please fix your queries")
            pytest.fail(
                "COLUMNS between source and target do NOT MATCH. Source Columns: " + columns1 + "\n Target Columns:  " + columns2 + "\n Please fix your queries")

    cached_df1 = conn1.upper() + '-' + schema_table1 + '.pkl'
    cached_df2 = conn2.upper() + '-' + schema_table2 + '.pkl'
    if 'edw' in conn2.lower() or 'edw' in conn1.lower():
        testname = 'EDW' + schema_table1 + '-' + schema_table2
    else:
        testname = conn1.upper() + '-' + conn2.upper() + '-' + schema_table1 + '-' + schema_table2
    # size_of_table_in_MB(testname, schema2, table)
    df1 = extract_or_load(conn1, query1, 1, cached_df1)
    df2 = extract_or_load(conn2, query2, 2, cached_df2)

    if type(df1) == int or type(df2) == int:
        log('SKIPPING THIS TABLE TEST due to memoryview error------------------------------------------------')
        save_temp_output_log(testname, '_memoryview error')
        return -1

    df1, df2 = upcase_column_names(df1, df2)

    if 'edw' in conn2.lower():
        df1, df2 = concat_column_names(df1, df2)

    # df1, df2 = cast_date_columns_to_timestamp(df1, df2)

    if type(transforms) == dict:
        if depth(transforms) == 4:
            df1, df2 = run_multiple_custom_transforms(df1, df2, transforms, table)
        else:
            df1, df2 = run_custom_transforms(df1, df2, transforms)

    # if not (df1.dtypes.values == df2.dtypes.values).all():
    # df2, df1 = cast_column_data_types(df2, df1)

    final_result = dataframe_difference(df2, df1, p_key, testname, result)
    diff_pct = final_result[7]
    log('RESULT Dataframe difference Percentage: ' + str(diff_pct) + '%')
    # CACHED_QUERY, CACHE_AGE, EXTRACT_DURATION_SOURCE, EXTRACT_DURATION_TARGET, TEST_RUN_DURATION
    final_result_dev = [os.environ['use_cache'], os.environ['cache_age'], os.environ['time_taken1'],
                        os.environ['time_taken2'], timedelta(seconds=int(time.time() - start))]
    send_result_to_db(final_result + final_result_dev)
    # send_dev_result_to_db(final_result_dev)
    log(
        '------------------------------------------END OF QUERIES CONTENT DIFF -----------------------------------------------')
    save_temp_output_log(testname, '_content_diff_pct-' + str(diff_pct))
    return diff_pct


def dataframe_difference(df1, df2, p_key='', testname='', result=[]):
    """Find rows which are different between two DataFrames."""
    start = time.time()
    comparison_df = df2.merge(df1, sort=p_key, indicator=True, how='outer')
    # todo catch exception raise ValueError(msg)
    # ValueError: You are trying to merge on datetime64[ns] and object columns. If you wish to proceed you should use pd.concat
    diff_df = comparison_df[comparison_df['_merge'] != 'both']
    log('RESULT time taken to Compare via Outer Join: ' +
        str(timedelta(seconds=int(time.time() - start))) + ' H:MM:SS')
    log('RESULT Diff-Merge calulated. Total Size: ' + str(diff_df.shape) + ' (rows, columns)')
    diff_rows = diff_df.shape[0]

    d1_count = -1
    d2_count = -1
    if diff_rows > 0:
        d1 = comparison_df[comparison_df['_merge'] == 'left_only']
        log('RESULT Diff-Merge calulated. Left Only Size:  ' + str(d1.shape))
        d1_count = d1.shape[0]
        d2 = comparison_df[comparison_df['_merge'] == 'right_only']
        log('RESULT Diff-Merge calulated. Right Only Size: ' + str(d2.shape))
        d2_count = d2.shape[0]
        # Sort and save diff to csv
        if p_key in diff_df:
            log('INFO Default primary key found in table. Primary Key column: ' + str(p_key))
            diff_df = diff_df.sort_values(p_key)
        else:
            # calculate primary key based on column with most unique values
            log('INFO Default primary key not found: ' + str(p_key))
            col_unique_count = {}
            for col in d1:
                col_unique_count[col] = d1[col].nunique()
            for col in d2:
                col_unique_count[col] = col_unique_count[col] + d2[col].nunique()
            p_key = max(col_unique_count, key=col_unique_count.get)
            log('CALCULATED primary key based on column with most unique values:  ' + str(p_key))

        primary_keys_intersection = list(set(d1[p_key]) & set(d2[p_key]))

        log('RESULT Diff-Merge Left VS Right PRIMARY KEY intersection length: ' + str(len(primary_keys_intersection)))

        # Drop _merge_result column
        # d1.drop('_merge', axis=1, inplace=True)
        # d2.drop('_merge', axis=1, inplace=True)

        # Find common rows with inner join
        # d1_common_d2 = d1_intersection.merge(d2_intersection, how='inner', indicator=False)

        file_name_suffix = testname + '_merge_diff_' + str(diff_rows) + '_rows-' + str(d1.shape[0]) + '_left_only-' \
                           + str(d2.shape[0]) + '_right_only-' + str(len(primary_keys_intersection)) + '_mismatch'
        fn = file_name_suffix + '-' + datetime.now().strftime("%d-%m-%Y %H-%M-%S") + '.csv'

        # todo move folder exist checks to setup
        create_folder('output')
        diff2_f = 'output\\' + fn
        if '/' in os.environ['PYTEST_CURRENT_TEST']:
            s_folder = os.environ['PYTEST_CURRENT_TEST'].split('/')[0]
        else:
            s_folder = '..'
        create_folder(s_folder + '\\output')
        diff_f = s_folder + '\\output\\' + fn
        # Save diff to csv, calculate trim-diff and save to xlsx
        if len(primary_keys_intersection) > 0:
            # Move Compare-Result column to the front
            cols = list(diff_df.columns)
            cols.insert(0, cols.pop(cols.index('_merge')))
            diff_df = diff_df.loc[:, cols]  # use ix to reorder
            # todo decide on logic of what diff too big to save
            if 100000 < diff_df.shape[0]:
                # if diff_df.shape[0] > d1.shape[0] * 1.5:
                log('SAVING RESULT Diff-Merge has more than 100K rows.' +
                    'LIMIT diff csv creation to 100K rows.')
                # else:
                diff_df.head(100000).to_csv(diff_f)
                # replace with fs.copy
                diff_df.head(100000).to_csv(diff2_f)
                log('SAVING RESULT Diff-Merge     . CSV filename: ' + diff_f)
                log('SAVING RESULT Diff-Merge     . CSV filename: ' + diff2_f)
            else:
                diff_df.to_csv(diff_f)
                diff_df.to_csv(diff2_f)
                # diff_df.to_excel(diff2_f + '.xlsx', float_format="%.20f")
                log('SAVING RESULT Diff-Merge     . CSV filename: ' + diff_f)
                log('SAVING RESULT Diff-Merge     . CSV filename: ' + diff2_f)
            if len(primary_keys_intersection) < diff_rows / 2:
                d1_intersection = d1[d1[p_key].isin(primary_keys_intersection)]
                d2_intersection = d2[d2[p_key].isin(primary_keys_intersection)]
                # d1_common_d2 = d1_intersection.merge(d2_intersection, how='inner', indicator=False)
                log('RESULT Diff-Merge   Left-Only and Right-Only Intersection subset filtered.')
                d_concat = pd.concat([d1_intersection, d2_intersection], ignore_index=True)
                trim_diff(d1_intersection, d2_intersection, d_concat, p_key, file_name_suffix, testname)

            if len(primary_keys_intersection) >= diff_rows / 2:
                trim_diff(d1, d2, diff_df, p_key, file_name_suffix, testname)
        else:
            log('SKIPPING Content Diff calculation since p_key intersection is 0')
            diff_df.to_csv(diff_f)
            diff_df.to_csv(diff2_f)
            log('SAVING RESULT of extra/missing rows   . CSV filename: ' + diff2_f)
            log('SAVING RESULT of extra/missing rows   . CSV filename: ' + diff2_f)

    res_diff_pct = round(diff_rows / df1.shape[0] * 100, 3) if df1.shape[0] != 0 else 0
    rowcount_diff = d2_count - d1_count
    rowcount_diff_pct = round(rowcount_diff / df1.shape[0] * 100, 3) if df1.shape[0] != 0 else 0

    final_result = result[:6] + [diff_rows, res_diff_pct, d1_count, d2_count, p_key, df1.shape[0], df2.shape[0],
                                 df1.shape[1], df2.shape[1], rowcount_diff, rowcount_diff_pct] + result[6:]

    return final_result


def trim_diff(d1, d2, diff_subset, p_key, file_name_suffix, testname):
    # Fill NA values with Mode()
    # The mode of a set of values is the value that appears most often. It can be multiple values.
    # d1 = d1.fillna(d1.mode().iloc[0])
    # d2 = d2.fillna(d1.mode().iloc[0])

    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 2000)
    # u = d1.select_dtypes(include=['datetime'])
    # d1[u.columns] = u.fillna(pd.to_datetime('today'))
    # u = d2.select_dtypes(include=['datetime'])
    # d2[u.columns] = u.fillna(pd.to_datetime('today'))

    # Drop Columns where ALL values are NA
    # d1 = d1.dropna(axis=1, how='all')
    # d2 = d2.dropna(axis=1, how='all')
    d1 = d1.sort_values(by=[p_key]).reset_index(drop=True)
    d2 = d2.sort_values(by=[p_key]).reset_index(drop=True)
    if d1.shape[0] == d2.shape[0]:
        # pd.set_option('display.max_columns', None)
        # pd.set_option('display.width', 2000)

        # Calculate each value is diff or not (boolean array)  using where and notna:
        # where - Replace values where the condition is False.
        # notna - Detect existing (non-missing) values. Return a boolean same-sized object indicating if the values
        # are not NA. i.e  Non-missing values get mapped to True.
        diff_bool = d1.values == d2.values
        # diff_bool2 = d1.where(d1.values == d2.values, other='FAIL').notna()
        # diff_bool2 = d1.where(d1.values == d2.values).notnull
        # todo - obsolete- Generate diff result with every 3rd row containing diff (bool result) of previous 2 rows
        # diff_bool_mixed_with_data = pd.concat([d1, d2, diff_bool]).sort_index(kind='merge')
        # log('SAVING RESULT Diff-Merge-_result_row_added. Total Size: ' + str(diff_bool_mixed_with_data.shape))
        #
        # # Save diff with extra result row
        # res_f = '..\\output\\' + datetime.now().strftime("%d-%m-%Y %H-%M-%S") + '-' + file_name_suffix + '_result.csv'
        # diff_bool_mixed_with_data.to_csv(res_f)
        # log('SAVING RESULT Diff-Merge-_result_row_added. CSV filename: ' + res_f)

        # cols_diff_bool2 = diff_bool2.all(axis=0)
        # cols_diff2 = cols_diff_bool2[lambda x: x == False]

        # Trim columns which have no diff
        cols_diff_bool = diff_bool.all(axis=0)
        # cols_diff_bool_list = cols_diff_bool.tolist()
        values = np.array(cols_diff_bool.tolist())
        ii = np.where(values == True)[0]
        d1_trim = d1.drop(d1.columns[ii.tolist()[:-1]], axis=1)
        d2_trim = d2.drop(d2.columns[ii.tolist()[:-1]], axis=1)
        # cols_diff = cols_diff_bool[lambda x: x == False]
        d1_trim[p_key] = d1[p_key]
        d2_trim[p_key] = d2[p_key]
        # d1.drop(d1.columns[[0, 4, 2]], axis=1, inplace=True)

        # log('RESULT TRANSFORM List of columns dropped - have no diff : ' + str(cols_diff) )
        log('RESULT TRANSFORM List of columns with possible diff : ' + str(list(d1_trim.columns)))
        # todo log list of columns with diff

        # d1_trim = pd.DataFrame()
        # d2_trim = pd.DataFrame()
        # d1_trim[p_key] = d1[p_key]
        # d2_trim[p_key] = d2[p_key]
        # for col, value in cols_diff.items():
        #     if d1[col].nunique() > 0 or d2[col].nunique() > 0:
        #         log('\t Found Column with diff ' + col + '-- Unique Count source: ' + str(d1[col].nunique()) +
        #             '-- Unique Count target: ' + str(d2[col].nunique()))
        #         d1_trim[col] = d1[col]
        #         d2_trim[col] = d2[col]
        #     else:
        #         log('\t Dropping Result Column with 0 unique values' + col + '-- Unique Count1: ' + str(
        #             d1[col].nunique()) +
        #             '-- Unique Count2: ' + str(d2[col].nunique()))

        diff_trimmed_data = pd.concat([d1_trim, d2_trim], sort=p_key).sort_index(kind='merge')
        # todo log list of na columns dropped by dropna
        log('RESULT Trim Merge shape: ' + str(diff_trimmed_data.shape))
        print('RESULT Trim Merge head: -\n', diff_trimmed_data.head(8))
        diff_trimmed_data = diff_trimmed_data.dropna(axis=1, how='all')
        log('RESULT Trim Merge shape after drop na: ' + str(diff_trimmed_data.shape))
        # Move Compare-Result column to the front
        cols = list(diff_trimmed_data.columns)
        cols.insert(0, cols.pop(cols.index('_merge')))
        cols.insert(1, cols.pop(cols.index(p_key)))
        diff_trimmed_data = diff_trimmed_data.loc[:, cols]  # use ix to reorder

    else:
        diff_trimmed_data = pd.concat([d1, d2], sort=False).sort_index(kind='merge')

    if '/' in os.environ['PYTEST_CURRENT_TEST']:
        s_folder = os.environ['PYTEST_CURRENT_TEST'].split('/')[0]
    else:
        s_folder = '..'
    fn = testname + '-' + datetime.now().strftime("%d-%m-%Y %H-%M-%S") + '_diff_highlight_trimmed.xlsx'
    trim_f = s_folder + '\\output\\' + fn
    trim_f2 = 'output\\' + fn

    if 100000 < diff_trimmed_data.shape[0]:
        # Max sheet size is: 1048576, 16384
        log('SAVING RESULT Diff-Merge has more than 100K rows.' +
            ' LIMIT diff csv creation to 100K rows.')
        save_diff_to_xlsx_with_formatting(diff_trimmed_data.head(100000), trim_f)
        # replace with fs.copy
        save_diff_to_xlsx_with_formatting(diff_trimmed_data.head(100000), trim_f2)
    else:
        save_diff_to_xlsx_with_formatting(diff_trimmed_data, trim_f)
        # replace with fs.copy
        save_diff_to_xlsx_with_formatting(diff_trimmed_data, trim_f2)
    # # Calculate each value is diff or not (boolean array)
    # diff_bool_trim = d1_trim.where(d1_trim.values == d2_trim.values).notna()
    #
    # # Generate diff result with every 3rd row containing diff (bool result) of previous 2 rows
    # diff_bool_mixed_with_trimmed_data = pd.concat([d1_trim, d2_trim, diff_bool_trim], sort=False).sort_index(
    # kind='merge') log('GENERATE RESULT Diff-Merge-_trimmed_result_row_added. Total Size: ' + str(
    # diff_bool_mixed_with_trimmed_data.shape))
    #
    # # Save diff with extra result row
    # trim_res_f = '..\\output\\' + datetime.now().strftime(
    #     "%d-%m-%Y %H-%M-%S") + '-' + file_name_suffix + '_result_trimmed.csv'
    # diff_bool_mixed_with_trimmed_data.to_csv(trim_res_f)
    # log('SAVING RESULT Diff-Merge-_trimmed_result_row_added. CSV filename: ' + trim_res_f)


def save_diff_to_xlsx_with_formatting(diff_trimmed_data, trim_f):
    log('SAVING RESULT Diff-Merge-XLSX. Total Size: ' + str(diff_trimmed_data.shape))

    # Create a Pandas Excel writer using XlsxWriter as the engine.
    writer = pd.ExcelWriter(trim_f, engine='xlsxwriter')

    # Convert the dataframe to an XlsxWriter Excel object.
    diff_trimmed_data.to_excel(writer, sheet_name='Sheet1')

    # Get the xlsxwriter workbook and worksheet objects.
    workbook = writer.book
    worksheet = writer.sheets['Sheet1']

    # Calculate cell range
    nrows = diff_trimmed_data.shape[0]
    ncols = diff_trimmed_data.shape[1]
    if ncols // 26 > 0:
        cols_char_prefix = chr(64 + (ncols // 26))
    else:
        cols_char_prefix = ''
    cols_char = cols_char_prefix + chr(65 + (ncols % 26))
    range = '$C$2' + ':$' + str(cols_char) + '$' + str(nrows)

    # Apply a conditional format to the cell range.
    white = workbook.add_format({'bg_color': '#FFFFFF'})
    red = workbook.add_format({'bg_color': '#FFC7CE'})
    worksheet.conditional_format(range, {'type': 'cell',
                                         'criteria': 'equal to',
                                         'value': '=INDIRECT(ADDRESS(ROW()+1,COLUMN()))',
                                         'format': white})
    worksheet.conditional_format(range, {'type': 'cell',
                                         'criteria': 'not equal to',
                                         'value': '=INDIRECT(ADDRESS(ROW()-1,COLUMN()))',
                                         'format': red})

    worksheet.freeze_panes(1, 0)
    # length_list = [len(x) for x in diff_trimmed_data.columns]
    # for i, width in enumerate(length_list):
    #     worksheet.set_column(i+1, i, width)
    # Close the Pandas Excel writer and output the Excel file.
    writer.save()


# Transforms custom - user defined
def run_custom_transforms(df1, df2, transforms):
    for origin, dict in transforms.items():
        if origin.lower() == 'source':
            log('TRANSFORM: Running Custom Transforms on source')
            df1 = run_custom_transform(df1, dict)
        else:
            df2 = run_custom_transform(df2, dict)
            log('TRANSFORM Target.')
    return df1, df2


def run_multiple_custom_transforms(df1, df2, transforms, table):
    for origin, dict in transforms.items():
        if origin.lower() == 'source':
            if table.upper() in dict:
                log('TRANSFORM: Running Custom Transforms on source')
                df1 = run_multiple_custom_transform(df1, dict[table.upper()])
        else:
            if table.upper() in dict:
                log('TRANSFORM: Running Custom Transforms on target')
                df2 = run_multiple_custom_transform(df2, dict[table.upper()])
    return df1, df2


def run_custom_transform(df, dict):
    for col, actions_dict in dict.items():
        for key, action in actions_dict.items():
            if 'transform' in str(key).lower():
                transform_each_item_in_column(df, col, action)
            if 'find_replace' in str(key).lower():
                transform_each_item_in_column_with_params(df, col, 'replace', action[0], action[1])
            if 'drop_columns' in str(key).lower():
                drop_column(df, col)
    return df


def run_multiple_custom_transform(df, cols_dict):
    for col, actions_dict in cols_dict.items():
        for key, action in actions_dict.items():
            if 'transform' in str(key).lower():
                transform_each_item_in_column(df, col, action)
            if 'find_replace' in str(key).lower():
                transform_each_item_in_column_with_params(df, col, 'replace', action[0], action[1])
            if 'drop_columns' in str(key).lower():
                drop_column(df, col)
            if 'truncate' in str(key).lower():
                truncate_each_item_in_column(df, col, action)
            if 'cast' in str(key).lower():
                df[col] = getattr(pd, action.lower())(df[col], errors='coerce')
    return df


def transform_each_item_in_column(df, col, transform):
    if col in df.columns:
        log("TRANSFORM Column: " + col + ' values. Transformation type: ' + transform)
        df[col] = df[col].apply(lambda v: getattr(v, transform)() if isinstance(v, str) else v)
    return df


# def trunc(values, decs=0):
#     return np.trunc(values*10**decs)/(10**decs)


def trunc_length(values, length=0):
    dec_length = length - len(str(round(values))) - 1
    return np.trunc(values*10**dec_length)/(10**dec_length)


def round_length(values, length=0):
    dec_length = length - len(str(round(values))) - 1
    return np.round(values, dec_length)


def truncate_each_item_in_column(df, col, length):
    if col in df.columns:
        log("TRANSFORM Column: " + col + ' values. Transformation type:  Truncate to ' + str(length))
        if df[col].dtype == float:
            df[col] = df[col].apply(lambda v: round_length(v, length))
        else:
            df[col] = df[col].apply(lambda v: str(v)[0:length] if isinstance(v, str) else v)
        # if isinstance(v, str) else v)
        # df[col] = df[col].apply(lambda v: print(v[0:length]))
    return df


def transform_each_item_in_column_with_params(df, col, transform, p1, p2):
    if col in df.columns:
        log(
            "TRANSFORM Column: " + col + ' values. Transformation type: ' + transform + ' "' + p1 + '" with "' + p2 + '"')
        df[col] = df[col].apply(lambda v: getattr(v, transform)(p1, p2) if isinstance(v, str) else v)


# Transforms - Use Case Specific
def upcase_column_names(df1, df2):
    log('TRANSFORM: Uppercasing column names on both sides')
    df1.columns = map(lambda x: x.upper(), df1.columns)  # uppercase column names to make 1 and 2 match
    df2.columns = map(lambda x: x.upper(), df2.columns)  # uppercase column names to make 1 and 2 match
    return df1, df2


def delete_etl_status_columns(df1, df2):
    log('TRANSFORM: Dropping Columns prefixed with "TECH_/CONF_" on both sides')
    df1.drop(df1.filter(regex='TECH_').columns, axis=1, inplace=True)
    df2.drop(df2.filter(regex='TECH_').columns, axis=1, inplace=True)
    df1.drop(df1.filter(regex='CONF_').columns, axis=1, inplace=True)
    df2.drop(df2.filter(regex='CONF_').columns, axis=1, inplace=True)
    df2.drop(df2.filter(regex='ETL_LAST_UPDATED_').columns, axis=1, inplace=True)
    # missing_in_target = list(set(list(df1.columns)) - set(list(df2.columns)))
    # if len(missing_in_target) > 0:
    #     log('INFO: Found missing columns: ' + str(missing_in_target))
    #     sys.exit(
    #         "Columns likely missing in Target - please add Table-column-dropTransform in test config yaml file - " + str(
    #             missing_in_target))
    return df1, df2


def drop_column(df1, name):
    log('TRANSFORM Dropping column named: ' + name)
    df1.drop(name, axis=1, inplace=True)
    return df1


# Concat-Columns names from query 1 and query 2 for EDW comparision
def concat_column_names(df1, df2):
    concat_col_names = []
    # df2_cols = list(df2.columns)
    df2_cols = df2.columns
    for idx, col in enumerate(df1.columns):
        concat_col_names.append(col + '-|-' + df2_cols[idx]) if col != df2_cols[idx] else concat_col_names.append(col)
    df1.columns = concat_col_names
    df2.columns = concat_col_names
    log('Pandas new column names: ' + str(concat_col_names))
    return df1, df2


def cast_column_data_types(df1, df2):
    # Transforms -update Data Types to make 1 and 2 match
    log("TRANSFORM: Casting columns on either side to match the other side")
    for x in df2.columns:
        if df1[x].dtype != df2[x].dtype:
            if df1[x].dtype.name == 'object':
                print('\tCasting column1:', x, 'type from:', df1[x].dtype.name, 'to:', df2[x].dtype.name)
                df1[x] = df1[x].astype(df2[x].dtype.name) if df2[x].dtype.name != 'datetime64[ns]' else pd.to_datetime(
                    df1[x], errors='coerce')
            else:
                print('\tCasting column2:', x, 'type from:', df2[x].dtype.name, 'to:', df1[x].dtype.name)
                df2[x] = df2[x].astype(df1[x].dtype.name) if df1[x].dtype.name != 'datetime64[ns]' else pd.to_datetime(
                    df2[x], errors='coerce')
        # if 'date' in x.lower():
        #     df1['LOSSDATEFROM'] = pd.to_datetime(df1['LOSSDATEFROM'], errors='coerce')
        #     df2['LOSSDATEFROM'] = pd.to_datetime(df2['LOSSDATEFROM'], errors='coerce')

    return df1, df2

    # if dt1.equals(dt2):
    #     log('INFO Pandas columns data types - table 1 vs 2 - are equal')
    # else:
    #     log('Pandas tables 1 vs 2 -data types diff: ' + str(dt1.eq(dt2)))
    #     sys.exit("Data types are different - please fix")


# Columns names containing string 'date' or 'time' in them- are cast to timestamp
def cast_date_columns_to_timestamp(df1, df2):
    cols = []
    for col in df2.columns:
        if 'date' in col.lower():
            cols.append(col)
            df1[col] = pd.to_datetime(df1[col], errors='coerce')
            df2[col] = pd.to_datetime(df2[col], errors='coerce')
    log("TRANSFORM: Casting Date columns on either side to datetime: " + str(cols))
    return df1, df2


# Result helpers
def save_temp_output_log(testname, suffix, src_dir='.', dst_dir='..\\output'):
    src_dir = 'outputfile.log'
    filename = testname + '-' + datetime.now().strftime("%d-%m-%Y %H-%M-%S") + suffix + '.txt'
    if '/' in os.environ['PYTEST_CURRENT_TEST']:
        s_folder = os.environ['PYTEST_CURRENT_TEST'].split('/')[0]
    else:
        s_folder = '..'
    fn = testname + '-' + datetime.now().strftime("%d-%m-%Y %H-%M-%S") + '_diff_highlight_trimmed.xlsx'
    dst_dir = s_folder + '\\output\\' + filename
    dst2_dir = 'output\\' + filename

    # dst_dir = '..\\output\\' + filename
    # dst2_dir = 'output\\' + filename
    create_folder('..\\output\\')  # todo move to pytest setup
    create_folder('output\\')  # todo move to pytest setup
    shutil.copy(src_dir, dst_dir)
    shutil.copy(src_dir, dst2_dir)
    log('SAVING this log to: ' + str(dst_dir))
    log('SAVING this log to: ' + str(dst2_dir))
    # os.remove("outputfile.log")
    open('outputfile.log', 'w').close()
    os.system('cls' if os.name == 'nt' else 'clear')


# Obsolete - Missing columns indicate a bug - use explicit transform to drop a column
def remove_not_matching_columns_from_source_and_target(df1, df2):
    missing_in_target = list(set(list(df1.columns)) - set(list(df2.columns)))
    df1.drop(missing_in_target, axis=1, inplace=True)
    # l1 = df1.columns
    # l2 = list(df2.columns)
    # non_match = set(l2) - set(list(l1))
    log('TRANSFORM: Dropping non-matching column: ' + str(missing_in_target))
    # common_columns = list(sorted(set(l1) & set(l2), key=l1.index))
    # df1 = df1[common_columns]
    # df2 = df2[common_columns]
    # log('TRANSFORM: Dropping non-matching on both sides. Common Columns: ' + str(common_columns))
    return df1, df2


# Db Connectors
def get_connection(x):
    if os.path.isfile('../databases.yml'):
        path = '../databases.yml'
    else:
        path = 'databases.yml'
    my_dict = dict(YAML().load(open(path, 'r')))
    all_connections = {k.lower(): v for k, v in my_dict.items()}
    # todo generalize this case
    if 'snowflake_sqlalchemy_connector' in x.lower():
        conn_str = all_connections['snowflake_dev']
        return snowflake_sqlalchemy_connection(conn_str)

    conn_str = all_connections[x.lower()]
    adapter = conn_str['adapter'].lower()
    if 'sqlserver' in adapter:
        return mssql_connection(conn_str)
    if 'oracle' in adapter:
        return oracle_connection(conn_str)
    if 'snowflake' in adapter:
        return snowflake_connection(conn_str)
    if 'postgres' in adapter:
        return postgres_connection(conn_str)

    # return {
    #     'oracle_prod': oracle_connection(all_connections['oracle_prod']),
    #     'edge_r3_prod': oracle_connection(all_connections['edge_r3_prod']),
    #     'edge_r2_prod': oracle_connection(all_connections['edge_r2_prod']),
    #     'snowflake': snowflake_connection(all_connections['snowflake']),
    #     # 'snowflake_edw_dev':    snowflake_connection(all_connections['snowflake_edw_dev']),
    #     # 'snowflake_dev':        snowflake_connection(all_connections['snowflake_dev']),
    #     # 'snowflake_prod':       snowflake_connection(all_connections['snowflake_prod']),
    #     'data_lake_dev': postgres_connection(all_connections['data_lake_dev']),
    #     'data_lake_prod': postgres_connection(all_connections['data_lake_prod'])
    # }[x.lower()]


def postgres_connection(conn_str):
    return psycopg2.connect(
        host=conn_str['host'],
        port=conn_str['port'], database=conn_str['database'], user=conn_str['username'], password=conn_str['password'],
        options='-c extra_float_digits=3')


def oracle_connection(conn_str):
    dsn_tns = cx_Oracle.makedsn(conn_str['host'], conn_str['port'], service_name=conn_str['service_name'])
    return cx_Oracle.connect(user=conn_str['username'], password=conn_str['password'], dsn=dsn_tns)


def mssql_connection(conn_str):
    pwd = conn_str['password'].replace('Â', '')
    conn_string = 'DRIVER={SQL Server};SERVER=' + conn_str['host'] + ';DATABASE=' + conn_str[
        'database'] + ';UID=' + conn_str['username'] + ';PWD=' + pwd
    return pyodbc.connect(conn_string)


def snowflake_connection(conn_str):
    # snowflake EDW
    return snowflake.connector.connect(
        user=conn_str['username'],
        password=conn_str['password'],
        account=conn_str['account'],
        port=conn_str['port'],
        warehouse=conn_str['warehouse']).cursor()


def snowflake_sqlalchemy_connection(conn_str):
    return create_engine(URL(
        account=conn_str['account'],
        host=conn_str['host'],
        user=conn_str['username'],
        password=conn_str['password'],
        port=conn_str['port'],
        database='DATA_LAKE_DEV',
        schema='ONEDATA_SYSTEM_STATUS',
        warehouse=conn_str['warehouse']))


# Logging helpers
def log_columns_dtypes(df):
    for col in df.columns:
        log('\t Column: ' + col + ', Type: ' + str(df[col].dtype))


def timestamp(delta_hours=0):
    return (datetime.now() - timedelta(hours=delta_hours)).strftime("%d/%m/%Y %H:%M:%S")


# Todo check pytest compatability
def log(msg):
    with closing(Tee("outputfile.log", "a", channel="stdout")) as outputstream:
        if sys.platform == 'win32':
            print(timestamp(), msg, flush=True)
        else:
            print(timestamp(), msg)
        # raise Exception('The file "outputfile.log" is closed anyway.')
    if sys.platform == 'win32':
        sys.stdout.flush()


# OS/ Python helpers
def create_folder(path):
    if not os.path.exists(path):
        os.makedirs(path)


def read_data(script_name):
    if os.stat(script_name + '.yml').st_size > 0:
        return dict(YAML().load(open(script_name + '.yml', 'r')))
    else:
        return {}


def append_data(script_name, new_data_dict):
    existing_data = dict(YAML().load(open(script_name + '.yml', 'r'))) if os.stat(
        script_name + '.yml').st_size > 0 else {}
    existing_data.update(new_data_dict)
    # file = YAML().load(open(script_name + '_1.yml', 'w')
    with open(script_name + '.yml', 'w') as file:
        YAML().dump(existing_data, file)


def filter_dict(d, name):
    return dict((k, v) for k, v in d.items() if k.lower().startswith(name))


def filter_dicts(d, names):
    res = {}
    for name in names:
        for k, v in d.items():
            if k.lower().startswith(name):
                res[k] = v
    return res


def depth(d):
    if isinstance(d, dict):
        return 1 + (max(map(depth, d.values())) if d else 0)
    return 0


def print_full(x):
    pd.set_option('display.max_rows', len(x))
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 2000)
    pd.set_option('display.float_format', '{:20,.2f}'.format)
    pd.set_option('display.max_colwidth', 2000)
    print(x)
    pd.reset_option('display.max_rows')
    pd.reset_option('display.max_columns')
    pd.reset_option('display.width')
    pd.reset_option('display.float_format')
    pd.reset_option('display.max_colwidth')


# WIP helpers
def finder(df, row):
    for col in df:
        df = df.loc[(df[col] == row[col]) | (df[col].isnull() & pd.isnull(row[col]))]
    return df


def columns_content_diff_old(query1, query2, p_key, conn1='', conn2=''):
    # EXTRACTING Tables 1 and 2
    df2 = extract(conn2, query2, '2')
    df1 = extract(conn1, query1, '1')

    df1, df2 = upcase_column_names(df1, df2)

    if not (df1.columns == df2.columns).all():
        df1, df2 = concat_column_names(df1, df2)

    if not (df1.dtypes.values == df2.dtypes.values).all():
        df2, df1 = cast_column_data_types(df2, df1)

    dfd = dataframe_difference(df2, df1, p_key)
    log('RESULT Dataframe difference percentage: ' + str(dfd) + '%')
    return dfd

# todo Obsolete- to delete
# def counts_diff(query1, query2):
#     # EXTRACTING Tables 1 and 2
#     print('\n')
#     print(timestamp(), 'Pandas extracting query1:', query1)
#     df1 = pd.read_sql_query(query1)
#     print(timestamp(), 'Pandas table1 rows-columns counts \n', df1.head)
#     print(timestamp(), 'Pandas extracting query2:', query2)
#     df2 = snowflake_edw_connection().execute(query2).fetch_pandas_all()
#     print(timestamp(), 'Pandas table2 rows-columns counts \n', df2.head)
#
#     if not (df1.columns == df2.columns).all():
#         df2, df1 = concat_column_names(df2, df1)
#
#     for x in df1.columns:
#         c1 = df1[x][0]
#         c2 = df2[x][0]
#         diff_percent = (c2 - c1) / c1 * 100
#     log('RESULT Percetage diff calculated: ' + str(round(diff_percent, 3)) + '%')
#     return diff_percent.round(3)
