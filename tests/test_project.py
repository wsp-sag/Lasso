import os
import glob

import pytest
from pandas import DataFrame

from lasso import Project

"""
Run tests from bash/shell
Run just the tests labeled project using `pytest -m project`
To run with print statments, use `pytest -s -m project`
"""

EX_DIR = os.path.join(os.getcwd(),'examples','cube')
BASE_ROADWAY_DIR = os.path.join(os.getcwd(),'examples','stpaul')

## create list of example logfiles to use as input
logfile_list=glob.glob(os.path.join(EX_DIR,"st_paul_test.log"))
@pytest.mark.parametrize("logfilename", logfile_list)


#@pytest.mark.sijia
def test_logfile_read(request,logfilename):
    '''
    Tests that the logfile can be read in and
    produces a DataFrame.
    '''
    print("\n--Starting:",request.node.name)

    print("Reading: {}".format(logfilename))
    lf = Project.read_logfile(logfilename)
    assert(type(lf)==DataFrame)

@pytest.mark.parametrize("logfilename", logfile_list)
@pytest.mark.sijia
def test_highway_change(request,logfilename):
    '''
    Tests that the logfile can be read in and
    produces a DataFrame.
    '''
    print("\n--Starting:",request.node.name)

    print("Reading: {}".format(logfilename))
    lf = Project.read_logfile(logfilename)
    assert(type(lf)==DataFrame)

    test_project = Project.create_project( logfilename,
                                            BASE_ROADWAY_DIR,
                                            None,
                                            None,
                                            None,
                                            None,
                                            None,
                                            None)

    assert(type(test_project.roadway_changes)==DataFrame)
    assert(type(test_project.card_data)==Dict[str, Dict[str, Any]])
