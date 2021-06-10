import os
import glob

import pytest

from lasso.cube.cube_project import CubeProject
from lasso.cube.cube_model_transit import CubeTransit

"""
Run tests from bash/shell
usage:
    pytest -s lasso/cube/tests/test_cube_project.py
"""
BASE_ROADWAY_DIR = os.path.join(os.getcwd(), "examples", "stpaul")
BASE_TRANSIT_DIR = os.path.join(os.getcwd(), "examples", "stpaul")
CUBE_DIR = os.path.join(BASE_TRANSIT_DIR, "cube")
SCRATCH_DIR = os.path.join(os.getcwd(), "tests", "scratch")

##################################
# 1. Transit Project Coding      #
# - shape diffing                #
# - property diffing             #
# - project card writing         #
# - lin diffs to card            #
##################################

route_edits = [
    [
        "Adding stops at node 3 and 4",
        "N=1,2,-3,-4,-5,-6,7,8,9,10",
        "N=1,2,3,4,-5,-6,7,8,9,10",
        ["1", "2", "-3", "-4", "-5", "-6"],
        ["1", "2", "3", "4", "-5", "-6"],
    ],
    [
        "Extend from 4 to 7",
        "N=1,2,3,4",
        "N=1,2,3,4,5,6,7",
        ["3", "4"],
        ["3", "4", "5", "6", "7"],
    ],
    [
        "Reroute between 3 and 4",
        "N=1,2,3,4",
        "1,2,3,31,35,37,4",
        ["2", "3", "4"],
        ["2", "3", "31", "35", "37", "4"],
    ],
    [
        "Shorten from 7 to 5",
        "N=1,2,3,4,5,6,7",
        "N=1,2,3,4,5",
        ["4", "5", "6", "7"],
        ["4", "5"],
    ],
    [
        "Shorten from from 1 to 4",
        "N=1,2,3,4,5,6,7",
        "4,5,6,7",
        ["1", "2", "3", "4", "5"],
        ["4", "5"],
    ],
    [
        "Edit within a loop",
        "N=1,2,3,4,15,16,4,5,6",
        "N=1,2,3,4,25,26,27,4,5,6",
        ["3", "4", "15", "16", "4", "5"],
        ["3", "4", "25", "26", "27", "4", "5"],
    ],
    [
        "Edit after a loop",
        "N=1,2,3,4,15,16,4,5,6",
        "N=1,2,3,4,15,16,4,55,66",
        ["16", "4", "5", "6"],
        ["16", "4", "55", "66"],
    ],
    [
        "Edit before a loop",
        "N=1,2,3,4,15,16,4,5,6",
        "N=1,22,33,4,15,16,4,5,6",
        ["1", "2", "3", "4", "15"],
        ["1", "22", "33", "4", "15"],
    ],
]


@pytest.mark.menow
@pytest.mark.travis
@pytest.mark.transit
@pytest.mark.parametrize(
    "name,base_r,edit_r,expected_existing,expected_set", route_edits
)
def test_compare_route_shapes(
    request, name, base_r, edit_r, expected_existing, expected_set
):
    print("\n--Starting:", request.node.name)
    print("\n--------Edit:", name)
    base_t = f"""
    ;;<<PT>><<LINE>>;;
    LINE NAME="1234",
     LONGNAME="Adding stops at node 3 and 4",
     HEADWAY[1]=60, MODE=7, {base_r}
    """
    edit_t = f"""
    ;;<<PT>><<LINE>>;;
    LINE NAME="1234",
     LONGNAME="Adding stops at node 3 and 4",
     HEADWAY[1]=60, MODE=7, {edit_r}
    """

    base_tnet = CubeTransit.from_source(base_t)
    edit_tnet = CubeTransit.from_source(edit_t)

    from lasso.cube.cube_model_transit import evaluate_route_shape_changes

    change_list = evaluate_route_shape_changes(base_tnet, edit_tnet, n_buffer_vals=2,)
    change_dict = change_list[0]

    print(f"\nexisting: {base_r}\nedit: {edit_r}\nchange_dict: {change_dict}")
    assert (change_dict["existing"], change_dict["set"]) == (
        expected_existing,
        expected_set,
    )


@pytest.mark.transit
@pytest.mark.travis
def test_write_transit_project_card(request):
    print("\n--Starting:", request.node.name)
    test_lin_base = """
    ;;<<PT>><<LINE>>;;
    LINE NAME="0_452-111_452_pk1",
    MODE=5,
    HEADWAY[1]=10,
    NODES=
     39249,
     -39240,
     54648

     LINE NAME="0_134-111_134_pk1",
      LONGNAME="Ltd Stop - Highland - Cleveland - Cretin - Mpls",
      HEADWAY[1]=20,
      MODE=5,
      ONEWAY=T,
      OPERATOR=3,
     NODES=
      39249,
      -39240,
      54648,
      43503,
      -55786,
      -55785,
      55782,
      -55781,
      -55779

     LINE NAME="0_134-111_134_pk0",
      LONGNAME="Ltd Stop - Highland - Cleveland - Cretin - Mpls",
      HEADWAY[1]=90,
      MODE=5,
      ONEWAY=T,
      OPERATOR=3,
     NODES=
      83733,
      -9533,
      20208,
      84250,
      92566,
      129190
    """
    test_lin_build = """
    ;;<<PT>><<LINE>>;;
    LINE NAME="0_452-111_452_pk1",
    MODE=5,
    HEADWAY[1]=15,
    NODES=
     39249,
     -39240,
     54648

     LINE NAME="0_134-111_134_pk1",
      LONGNAME="Ltd Stop - Highland - Cleveland - Cretin - Mpls",
      HEADWAY[1]=20,
      MODE=6,
      ONEWAY=T,
      OPERATOR=3,
     NODES=
      39249,
      -39240,
      54648,
      43503,

     LINE NAME="0_134-111_134_pk0",
      LONGNAME="Ltd Stop - Highland - Cleveland - Cretin - Mpls",
      HEADWAY[1]=120,
      MODE=5,
      ONEWAY=T,
      OPERATOR=3,
     NODES=
      84250,
      92566,
      129190
    """
    test_project = CubeProject.create_project(
        base_transit_source=test_lin_base,
        build_transit_source=test_lin_build,
        project_name="test suite small changes",
    )
    print("")
    test_project.write_project_card(os.path.join(SCRATCH_DIR, "t_transit_test.yml"))
    ## todo write an assert that actually tests something


@pytest.mark.transit
@pytest.mark.travis
def test_write_transit_project_card_2(request):
    print("\n--Starting:", request.node.name)
    test_lin_base = """
    ;;<<PT>><<LINE>>;;
    LINE NAME="0_452-111_452_pk1",
    MODE=5,
    HEADWAY[1]=10,
    NODES=
     39249,
     -39240,
     54648

    LINE NAME="0_134-111_134_pk1",
     LONGNAME="Ltd Stop - Highland - Cleveland - Cretin - Mpls",
     HEADWAY[1]=20,
     MODE=5,
     ONEWAY=T,
     OPERATOR=3,
    NODES=
     39249,
     -39240,
     54648,

    """
    test_lin_build = """
    ;;<<PT>><<LINE>>;;
    LINE NAME="0_452-111_452_pk1",
    MODE=5,
    HEADWAY[1]=15,
    HEADWAY[2]=10,
    NODES=
     39249,
     -39240,
     54648

     LINE NAME="0_134-111_134_pk0",
      LONGNAME="Ltd Stop - Highland - Cleveland - Cretin - Mpls",
      HEADWAY[1]=120,
      MODE=5,
      ONEWAY=T,
      OPERATOR=3,
     NODES=
      84250,
      92566,
      129190
    """
    test_project = CubeProject.create_project(
        base_transit_source=test_lin_base,
        build_transit_source=test_lin_build,
        project_name="test suite small changes",
    )
    print("")
    test_project.write_project_card(os.path.join(SCRATCH_DIR, "t_transit_test.yml"))
    ## todo write an assert that actually tests something


@pytest.mark.transit
@pytest.mark.travis
def test_write_transit_project_card_diffing_lin(request):
    print("\n--Starting:", request.node.name)

    test_project = CubeProject.create_project(
        base_transit_source=os.path.join(CUBE_DIR, "transit.LIN"),
        build_transit_source=os.path.join(
            CUBE_DIR, "single_transit_route_attribute_change"
        ),
    )
    test_project.write_project_card(os.path.join(SCRATCH_DIR, "t_transit_test.yml"))
    ## todo write an assert that actually tests something


@pytest.mark.transit
@pytest.mark.travis
def test_write_transit_project_card_route_shape(request):
    print("\n--Starting:", request.node.name)

    test_project = CubeProject.create_project(
        base_transit_source=os.path.join(CUBE_DIR, "transit.LIN"),
        build_transit_source=os.path.join(CUBE_DIR, "transit_route_shape_change"),
    )
    test_project.write_project_card(
        os.path.join(SCRATCH_DIR, "t_transit_shape_test.yml")
    )
    ## todo write an assert that actually tests something


##########################################
# 2. Roadway Project Coding              #
# -  write project card from logfile     #
# -  write ML project card from logfile  #
##########################################

logfile_list = glob.glob(os.path.join(CUBE_DIR, "st_paul_test.log"))


@pytest.mark.parametrize("logfilename", logfile_list)
@pytest.mark.travis
def test_write_roadway_project_card_from_logfile(request, logfilename):
    print("\n--Starting:", request.node.name)

    test_project = CubeProject.create_project(
        roadway_log_file=logfilename,
        base_roadway_dir=BASE_ROADWAY_DIR,
        shape_foreign_key="shape_id",
    )
    test_project.write_project_card(os.path.join(SCRATCH_DIR, "t_roadway_pc_test.yml"))
    ## todo write an assert that actually tests something


@pytest.mark.travis
@pytest.mark.skip(
    reason="Not currently able to automagically create a managed lane project card"
)
def test_write_ml_roadway_project_card_from_logfile(request):
    print("\n--Starting:", request.node.name)

    test_project = CubeProject.create_project(
        roadway_log_file=os.path.join(CUBE_DIR, "ML_log.log"),
        base_roadway_dir=BASE_ROADWAY_DIR,
    )
    test_project.write_project_card(os.path.join(SCRATCH_DIR, "t_roadway_pc_test.yml"))
    ## todo write an assert that actually tests something
