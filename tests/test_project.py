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
CUBE_DIR = os.path.join(os.getcwd(), "examples", "cube")
ROADWAY_DIR = os.path.join(os.getcwd(), "examples", "stpaul")
BUILD_TRANSIT_DIR = os.path.join(CUBE_DIR, "single_transit_route_attribute_change")
SCRATCH_DIR =
 os.path.join(
    os.getcwd(),
    "tests",
    "scratch")
## create list of example logfiles to use as input
logfile_list = [os.path.join(CUBE_DIR , "st_paul_test.log")]


@pytest.mark.parametrize("logfilename", logfile_list)
@pytest.mark.travis
def test_logfile_read(request, logfilename):
    """
    Tests that the logfile can be read in and
    produces a DataFrame.
    """
    print("\n--Starting:", request.node.name)

    print("Reading: {}".format(logfilename))
    lf = Project.read_logfile(logfilename)
    assert type(lf) == DataFrame


@pytest.mark.parametrize("logfilename", logfile_list)
@pytest.mark.travis
def test_highway_project_card(request, logfilename):
    """
    Tests that the logfile can be read in and
    produces a DataFrame.
    """
    print("\n--Starting:", request.node.name)

    print("Reading: {}".format(logfilename))
    lf = Project.read_logfile(logfilename)
    assert type(lf) == DataFrame

    test_project = Project.create_project(
        roadway_log_file=logfilename, base_roadway_dir=ROADWAY_DIR
    )

    assert type(test_project.roadway_changes) == DataFrame
    # assert(type(test_project.card_data)==Dict[str, Dict[str, Any]])
    assert type(test_project.card_data) == dict

    test_project.write_project_card(
        os.path.join(
            SCRATCH_DIR,
            "t_"+os.path.splitext(os.path.basename(logfilename))[0] + ".yml",
        )
    )



@pytest.mark.parametrize("logfilename", logfile_list)
@pytest.mark.skip("Need to update project card schema")
def test_highway_change_project_card_valid(request, logfilename):
    print("\n--Starting:", request.node.name)

    print("Reading: {}".format(logfilename))
    lf = Project.read_logfile(logfilename)
    test_project = Project.create_project(
        roadway_log_file=logfilename, base_roadway_dir=ROADWAY_DIR
    )

    test_project.write_project_card(
        os.path.join(
            SCRATCH_DIR,
            "t_"+os.path.splitext(os.path.basename(logfilename))[0] + ".yml",
        )
    )

    from network_wrangler import ProjectCard

    valid = ProjectCard.validate_project_card_schema(
        os.path.join(
            SCRATCH_DIR,
            "t_"+os.path.splitext(os.path.basename(logfilename))[0] + ".yml",
        )
    )

    assert valid == True
