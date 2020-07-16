import sys
import pytest


def pytest_addoption(parser):
    parser.addoption(
        "-E",
        action="store",
        metavar="NAME",
        help="only run tests matching the environment NAME.",
    )
    parser.addoption("--conn1", action="store", help="input conn1")
    parser.addoption("--conn2", action="store", help="input conn2")
    parser.addoption("--query1", action="store", help="input query1")
    parser.addoption("--query2", action="store", help="input query2")


def pytest_configure(config):
    # register an additional marker
    config.addinivalue_line(
        "markers", "env(name): mark test to run only on named environment"
    )


def pytest_runtest_setup(item):
    envnames = [mark.args[0] for mark in item.iter_markers(name="env")]
    if envnames:
        if item.config.getoption("-E") not in envnames:
            pytest.skip("test requires env in {!r}".format(envnames))

#
# def pytest_generate_tests(metafunc):
#     # This is called for every test. Only get/set command line arguments
#     # if the argument is specified in the list of test "fixturenames".
#     option_value1 = metafunc.config.option.conn1
#     option_value2 = metafunc.config.option.conn2
#     if 'conn1' in metafunc.fixturenames and option_value1 is not None:
#         metafunc.parametrize("conn1", [option_value1], "conn2", [option_value2])
    # option_value = metafunc.config.option.conn1
    # if 'conn2' in metafunc.fixturenames and option_value is not None:
    #     metafunc.parametrize("conn2", [option_value])
# def pytest_runtest_setup(item):
#     for marker in item.iter_markers(name="my_marker"):
#         print(marker)
#         sys.stdout.flush()