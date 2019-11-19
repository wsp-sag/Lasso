import os
import glob
import re

import pytest

from lasso import process_line_file
from lasso import Project

"""
Run tests from bash/shell
Run just the tests labeled project using `pytest -m project`
To run with print statments, use `pytest -s -m project`
"""

EX_DIR = os.path.join(os.getcwd(), "examples", "cube")
# BASE_TRANSIT_DIR = os.path.join(os.getcwd(),'examples','stpaul')
BASE_TRANSIT_DIR = "Z:/Data/Users/Sijia/Met_Council/github/client_met_council_wrangler_utilities/examples/stpaul/"
lineFile_list = glob.glob(
    os.path.join(EX_DIR, "single_transit_route_attribute_change", "*.LIN")
)
logfile_list = glob.glob(os.path.join(EX_DIR, "st_paul_test.log"))
BASE_ROADWAY_DIR = os.path.join(os.getcwd(), "examples", "stpaul")


@pytest.mark.parametrize("linefilename", lineFile_list)
def test_read_transit_linefile(request, linefilename):
    print("\n--Starting:", request.node.name)

    from lasso.TransitNetwork import TransitNetworkLasso

    print("Reading: {}".format(linefilename))
    tn = TransitNetworkLasso("CHAMP", 1.0)
    print(tn.isEmpty())
    thisdir = os.path.dirname(os.path.realpath(__file__))

    tn.mergeDir(EX_DIR)
    print(tn.isEmpty())

    # print(tn.lines[1].getFreq("AM", "CHAMP"))
    print(tn.lines[5].getFreq())
    print(tn.lines[5].name)
    print(tn.lines[5].n[0].num)
    print(tn.lines[5].getModeType("CHAMP"))


@pytest.mark.parametrize("logfilename", logfile_list)
def test_read_transit_linefile(request, logfilename):
    print("\n--Starting:", request.node.name)

    test_project = Project.create_project(
        base_transit_dir=BASE_TRANSIT_DIR,
        build_transit_dir=EX_DIR,
        roadway_log_file=logfilename,
        base_roadway_dir=BASE_ROADWAY_DIR,
    )
    test_project.write_project_card(
        "Z:/Data/Users/Sijia/Met_Council/github/client_met_council_wrangler_utilities/tests/transit_test.yml"
    )


@pytest.mark.parametrize("logfilename", logfile_list)
def test_write_transit_standard(request, logfilename):
    print("\n--Starting:", request.node.name)

    test_project = Project.create_project(
        base_transit_dir=BASE_TRANSIT_DIR,
        build_transit_dir=EX_DIR,
        roadway_log_file=logfilename,
        base_roadway_dir=BASE_ROADWAY_DIR,
    )

    test_project.base_transit_network.write_cube_transit(
        "Z:/Data/Users/Sijia/Met_Council/github/client_met_council_wrangler_utilities/tests/transit_test.lin"
    )
    test_project.write_project_card(
        "Z:/Data/Users/Sijia/Met_Council/github/client_met_council_wrangler_utilities/tests/transit_test.yml"
    )
