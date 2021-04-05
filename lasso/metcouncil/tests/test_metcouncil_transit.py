#! pytest -s
import os
import glob

import pytest

from lasso.metcouncil.metcouncil_transit import MetCouncilTransit

"""
Run tests from bash/shell
usage:
    pytest -s lasso/metcouncil/tests/test_metcouncil_transit.py
"""


CUBE_DIR = os.path.join(os.getcwd(), "examples", "cube")
BASE_TRANSIT_DIR = os.path.join(os.getcwd(), "examples", "stpaul")
BASE_ROADWAY_DIR = os.path.join(os.getcwd(), "examples", "stpaul")
SCRATCH_DIR = os.path.join(os.getcwd(), "tests", "scratch")

logfile_list = glob.glob(os.path.join(CUBE_DIR, "st_paul_test.log"))


@pytest.mark.transit
@pytest.mark.cube
@pytest.mark.metcouncil
def test_read_transit_linefile(request):
    print("\n--Starting:", request.node.name)

    linefilename = os.path.join(CUBE_DIR, "transit.LIN")
    print("Reading: {}".format(linefilename))
    tn = MetCouncilTransit.from_source(linefilename)
    print("Read {} LINES:\n{}".format(len(tn.routes), "\n - ".join(tn.routes)))
    ex_line_name = tn.routes[1]
    print("Line: {}".format(ex_line_name))
    print(
        "Properties: ",
        tn.route_properties_df[tn.route_properties_df.NAME == ex_line_name],
    )
    print("Nodes: ", tn.shapes_df[tn.shapes_df.NAME == ex_line_name])
    ## todo write an assert that actually tests something
