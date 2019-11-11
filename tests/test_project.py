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
BASE_ROADWAY_DIR = os.path.join(os.getcwd(),'examples','stpaul')
#BASE_ROADWAY_DIR = os.path.join("Z:/Data/Users/Sijia/Met_Council/Network Standard")

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
#@pytest.mark.sijia
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
@pytest.mark.parametrize("linefilename", lineFile_list)
#@pytest.mark.sijia
def test_read_transit_linefile(request,linefilename):
    print("\n--Starting:",request.node.name)

    from lasso.TransitNetwork import TransitNetworkLasso

    print("Reading: {}".format(linefilename))
    tn = TransitNetworkLasso("CHAMP", 1.0)
    print(tn.isEmpty())
    thisdir = os.path.dirname(os.path.realpath(__file__))

    tn.mergeDir(EX_DIR)
    print(tn.isEmpty())

    #print(tn.lines[1].getFreq("AM", "CHAMP"))
    print(tn.lines[5].getFreq())
    print(tn.lines[5].name)
    print(tn.lines[5].n[0].num)
    print(tn.lines[5].getModeType("CHAMP"))

EX_DIR = os.path.join(os.getcwd(),'examples','cube', 'single_transit_route_attribute_change')
#BASE_ROADWAY_DIR = os.path.join(os.getcwd(),'examples','stpaul')
BASE_TRANSIT_DIR = os.path.join(os.getcwd(),'examples','stpaul')

#lineFile_list = glob.glob(os.path.join(EX_DIR,"*.LIN"))
@pytest.mark.parametrize("logfilename", logfile_list)
#@pytest.mark.parametrize("linefilename", lineFile_list)
@pytest.mark.sijia
def test_read_transit_linefile(request,logfilename):
    print("\n--Starting:",request.node.name)

    test_project = Project.create_project(base_transit_dir = BASE_TRANSIT_DIR,
                                          build_transit_dir = EX_DIR,
                                          roadway_log_file=logfilename,
                                          base_roadway_dir=BASE_ROADWAY_DIR)
    test_project.write_project_card("Z:/Data/Users/Sijia/Met_Council/github/client_met_council_wrangler_utilities/tests/transit_test.yml")
