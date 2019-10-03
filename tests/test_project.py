import os
import glob
import re
from typing import Any, Dict, Optional

import pytest
from pandas import DataFrame

from lasso import Project

"""
Run tests from bash/shell
Run just the tests labeled project using `pytest -m project`
To run with print statments, use `pytest -s -m project`
"""

EX_DIR = os.path.join(os.getcwd(),'examples','cube')
#BASE_ROADWAY_DIR = os.path.join(os.getcwd(),'examples','stpaul')
BASE_ROADWAY_DIR = os.path.join("Z:/Data/Users/Sijia/Met_Council/Network Standard")

## create list of example logfiles to use as input
logfile_list=glob.glob(os.path.join(EX_DIR,"*.log"))
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

    test_project = Project.create_project( roadway_log_file=logfilename,
                                           base_roadway_dir=BASE_ROADWAY_DIR)

    assert(type(test_project.roadway_changes)==DataFrame)
    #assert(type(test_project.card_data)==Dict[str, Dict[str, Any]])
    assert(type(test_project.card_data)==dict)

    test_project.write_project_card(os.path.join(os.getcwd(),"tests",logfilename.replace(".", "\\").split("\\")[-2]+".yml"))

@pytest.mark.parametrize("logfilename", logfile_list)
#@pytest.mark.sijia
def test_highway_change_project_card_valid(request,logfilename):
    print("\n--Starting:",request.node.name)

    print("Reading: {}".format(logfilename))
    lf = Project.read_logfile(logfilename)
    test_project = Project.create_project( roadway_log_file=logfilename,
                                           base_roadway_dir=BASE_ROADWAY_DIR)

    from network_wrangler import ProjectCard
    valid = ProjectCard.validate_project_card_schema(os.path.join(os.getcwd(),"tests",logfilename,".yml"))

    assert(valid == True)

lineFile_list = glob.glob(os.path.join(EX_DIR,"*.LIN"))
@pytest.mark.parametrize("linefile", lineFile_list)
#@pytest.mark.sijia
def test_transit_linefile(request,linefile):
    print("\n--Starting:",request.node.name)

    print("Reading: {}".format(linefilename))
    tn = Wrangler.TranstiNetwrok()
    thisdir = os.path.dirname(os.path.realpath(__file__))

    tn.mergeDir(thisdir)
