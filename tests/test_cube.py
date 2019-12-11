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


CUBE_DIR = os.path.join(os.getcwd(), "examples", "cube")
BASE_TRANSIT_DIR = os.path.join(os.getcwd(), "examples", "stpaul")
BASE_ROADWAY_DIR = os.path.join(os.getcwd(), "examples", "stpaul")
SCRATCH_DIR = os.path.join(os.getcwd(), "tests", "scratch")

logfile_list = glob.glob(os.path.join(CUBE_DIR, "st_paul_test.log"))


linefile_list = glob.glob(os.path.join(CUBE_DIR, "*.LIN"))


@pytest.mark.travis
@pytest.mark.parametrize("linefilename", linefile_list)
def test_read_transit_linefile(request, linefilename):
    print("\n--Starting:", request.node.name)

    from lasso.TransitNetwork import TransitNetworkLasso

    print("Reading: {}".format(linefilename))
    tn = TransitNetworkLasso("CHAMP", 1.0)
    assert tn.isEmpty() == True

    tn.mergeDir(CUBE_DIR)
    assert tn.isEmpty() == False

    print("Frequencies: ", tn.lines[5].getFreq())
    print("Name: ", tn.lines[5].name)
    print("First node number: ", tn.lines[5].n[0].num)
    print("Mode Type: ", tn.lines[5].getModeType("CHAMP"))
    ## todo write an assert that actually tests something


@pytest.mark.travis
def test_create_cube_transit_network_from_dir(request):
    print("\n--Starting:", request.node.name)
    from lasso import CubeTransit

    cube_transit_net = CubeTransit.create_cubetransit(cube_transit_dir=CUBE_DIR)
    ## todo write an assert that actually tests something


@pytest.mark.travis
def test_create_cube_transit_network_from_file(request):
    print("\n--Starting:", request.node.name)
    from lasso import CubeTransit

    cube_transit_net = CubeTransit.create_cubetransit(
        cube_transit_file=os.path.join(
            CUBE_DIR, "single_transit_route_attribute_change", "transit.LIN"
        )
    )
    ## todo write an assert that actually tests something


@pytest.mark.parametrize("logfilename", logfile_list)
@pytest.mark.travis
def test_write_roadway_project_card_from_logfile(request, logfilename):
    print("\n--Starting:", request.node.name)

    test_project = Project.create_project(
        roadway_log_file=logfilename, base_roadway_dir=BASE_ROADWAY_DIR,
    )
    test_project.write_project_card(os.path.join(SCRATCH_DIR, "t_roadway_pc_test.yml"))
    ## todo write an assert that actually tests something


@pytest.mark.travis
def test_write_transit_project_card(request):
    print("\n--Starting:", request.node.name)

    test_project = Project.create_project(
        base_transit_file=os.path.join(CUBE_DIR, "transit.LIN"),
        build_transit_dir=os.path.join(
            CUBE_DIR, "single_transit_route_attribute_change"
        ),
    )
    test_project.write_project_card(os.path.join(SCRATCH_DIR, "t_transit_test.yml"))
    ## todo write an assert that actually tests something


@pytest.mark.travis
@pytest.mark.skip("Required files are in Z:/Sijia")
def test_write_cube_transit_standard(request):
    print("\n--Starting:", request.node.name)

    from lasso import CubeTransit

    cube_transit_net = CubeTransit.create_cubetransit(
        cube_transit_file=os.path.join(
            CUBE_DIR, "single_transit_route_attribute_change", "transit.LIN"
        )
    )

    cube_transit_net.write_cube_transit(os.path.join(SCRATCH_DIR, "t_transit_test.lin"))
