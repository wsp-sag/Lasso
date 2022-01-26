import os
import glob
from unittest import TestCase

import pandas as pd
import pytest

from lasso.cube.cube_project import CubeProject
from lasso.cube.cube_model_transit import CubeTransit

from lasso.utils import ordered

"""
Contains tests for creating Cube Project cards:

1. Transit Projects:
      - shape diffing
      - property diffing
      - project card writing
      - lin diffs to card

2. Roadway Projects
    -  read logfile
    - 
    -  write project card from logfile
    -  write ML project card from logfile

usage:
    pytest -s lasso/tests/cube/test_cube_project.py
"""

BASE_ROADWAY_DIR = os.path.join(os.getcwd(), "examples", "stpaul")
BASE_TRANSIT_DIR = os.path.join(os.getcwd(), "examples", "stpaul")
CUBE_DIR = os.path.join(os.getcwd(), "examples", "cube")
SCRATCH_DIR = os.path.join(os.getcwd(), "tests", "scratch")

LOGFILE_LIST = glob.glob(os.path.join(CUBE_DIR, "*.log"))
NUM_LOGFILES = 2
LOGFILE_LIST = LOGFILE_LIST[0 : min(NUM_LOGFILES, len(LOGFILE_LIST))]

print(f"LOGFILE LIST: {','.join(LOGFILE_LIST)}")

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


@pytest.mark.ci
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

    changes = evaluate_route_shape_changes(
        base_tnet,
        edit_tnet,
        n_buffer_vals=2,
    )
    change_dict = list(changes.values())[0]

    print(f"\nexisting: {base_r}\nedit: {edit_r}\nchange_dict: {change_dict}")
    tc1 = TestCase()
    tc1.maxDiff = None
    tc2 = TestCase()
    tc2.maxDiff = None
    tc1.assertEqual(change_dict["existing"], expected_existing)
    tc2.assertEqual(change_dict["set"], expected_set)


lin_changes = [
    [
        "Update Black Headway; Update Pink mode and routing; Shorten White Route and increase headway",
        # base
        """
        ;;<<PT>><<LINE>>;;
        LINE NAME="Black",
        MODE=5,
        HEADWAY[1]=10,
        NODES=
        39249,
        -39240,
        54648

        LINE NAME="Pink",
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

        LINE NAME="White",
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
        """,
        # build
        """
        ;;<<PT>><<LINE>>;;
        LINE NAME="BLACK",
        MODE=5,
        HEADWAY[1]=15,
        NODES=
        39249,
        -39240,
        54648

        LINE NAME="0_134-111_134_pk1",
        LONGNAME="Pink",
        HEADWAY[1]=20,
        MODE=6,
        ONEWAY=T,
        OPERATOR=3,
        NODES=
        39249,
        -39240,
        54648,
        43503

        LINE NAME="White",
        LONGNAME="Ltd Stop - Highland - Cleveland - Cretin - Mpls",
        HEADWAY[1]=120,
        MODE=5,
        ONEWAY=T,
        OPERATOR=3,
        NODES=
        84250,
        92566,
        129190
        """,
        # expected result
        {},
    ],
    [
        "Delete Green route; Add Teal route; Add red midday service; Update Red AM service route and headway.",
        # base
        """
        ;;<<PT>><<LINE>>;;
        LINE NAME="RED",
        MODE=5,
        HEADWAY[1]=10,
        NODES=
        39249,
        -39240,
        54648

        LINE NAME="GREEN",
        LONGNAME="Very long green name",
        HEADWAY[1]=20,
        MODE=5,
        ONEWAY=T,
        OPERATOR=3,
        NODES=
        39249,
        -39240,
        54648
        """,
        # build
        """
        ;;<<PT>><<LINE>>;;
        LINE NAME="RED",
        MODE=5,
        HEADWAY[1]=15,
        HEADWAY[2]=10,
        NODES=
        39249,
        -39240,
        54648

        LINE NAME="TEAL",
        LONGNAME="Very long teal name",
        HEADWAY[1]=120,
        MODE=5,
        ONEWAY=T,
        OPERATOR=3,
        NODES=
        84250,
        92566,
        129190
        """,
        # expected
        {
            "project": "TBD",
            "tags": "",
            "dependencies": "",
            "changes": [
                {
                    "category": "Delete Transit Service",
                    "facility": {
                        "route_id": "GREEN",
                        "start_time": "6:00",
                        "end_time": "9:00",
                    },
                },
                {
                    "category": "New Transit Service",
                    "facility": {
                        "route_id": "TEAL",
                        "start_time": "6:00",
                        "end_time": "9:00",
                    },
                    "properties": [
                        {"property": "headway_secs", "set": 7200},
                        {"property": "OPERATOR", "set": 3},
                        {"property": "ONEWAY", "set": "T"},
                        {"property": "MODE", "set": 5},
                        {"property": "route_long_name", "set": "Very long teal name"},
                        {"property": "routing", "set": ["84250", "92566", "129190"]},
                    ],
                },
                {
                    "category": "New Transit Service",
                    "facility": {
                        "route_id": "RED",
                        "start_time": "9:00",
                        "end_time": "16:00",
                    },
                    "properties": [
                        {"property": "MODE", "set": 5},
                        {"property": "headway_secs", "set": 600},
                        {"property": "routing", "set": ["39249", "-39240", "54648"]},
                    ],
                },
                {
                    "category": "Update Transit Service",
                    "facility": {
                        "route_id": "RED",
                        "start_time": "6:00",
                        "end_time": "9:00",
                    },
                    "properties": [
                        {"property": "headway_secs", "set": 900},
                        {
                            "property": "routing",
                            "existing": ["-39240", "54648"],
                            "set": ["-39240", "54648"],
                        },
                    ],
                },
            ],
        },
    ],
]


@pytest.mark.transit
@pytest.mark.ci
@pytest.mark.parametrize("test_name,base_lin,build_lin,expected_result", lin_changes)
def test_diffing_transit_routes(
    test_name, base_lin, build_lin, expected_result, request
):
    print("\n--Starting:", request.node.name)
    print("\n-----------", test_name)
    unique_id = hash((base_lin, build_lin))

    test_project = CubeProject.create_project(
        base_transit_source=base_lin,
        build_transit_source=build_lin,
        project_name=test_name,
    )

    print("test_project.card_data: ", test_project.card_data)

    test_project.write_project_card(
        os.path.join(SCRATCH_DIR, f"t_transit_test_{unique_id}.yml")
    )

    if expected_result:
        expected_result["project"] = test_name
        assert ordered(expected_result) == ordered(test_project.card_data)


@pytest.mark.menow
@pytest.mark.transit
@pytest.mark.ci
def test_write_transit_project_card_diffing_lin(request):
    print("\n--Starting:", request.node.name)

    test_project = CubeProject.create_project(
        base_transit_source=os.path.join(CUBE_DIR, "transit.LIN"),
        build_transit_source=os.path.join(
            CUBE_DIR, "single_transit_route_attribute_change", "transit.LIN"
        ),
    )
    test_project.write_project_card(os.path.join(SCRATCH_DIR, "t_transit_test.yml"))
    ## todo write an assert that actually tests something


@pytest.mark.transit
@pytest.mark.ci
def test_write_transit_project_card_route_shape(request):
    print("\n--Starting:", request.node.name)

    test_project = CubeProject.create_project(
        base_transit_source=os.path.join(CUBE_DIR, "transit.LIN"),
        build_transit_source=os.path.join(
            CUBE_DIR, "transit_route_shape_change", "transit.LIN"
        ),
    )
    test_project.write_project_card(
        os.path.join(SCRATCH_DIR, "t_transit_shape_test.yml")
    )
    ## todo write an assert that actually tests something


@pytest.mark.transit
@pytest.mark.ci
def test_update_route_prop_change_dict(request):
    print("\n--Starting:", request.node.name)
    from lasso import CubeProject

    df = pd.DataFrame(
        {
            ("name", "self"): "Line to be updated",
            ("headway_secs", "self"): 300,
            ("headway_secs", "other"): 30,
            ("start_time", "self"): "6:00",
            ("end_time", "self"): "9:00",
        }
    )
    expected_update_change_dict = {
        "property": "headway_secs",
        "existing": 300,
        "set": 30,
    }

    test_update_change_dict = CubeProject.update_route_prop_change_dict(df.iloc[0])

    assert test_update_change_dict == expected_update_change_dict


@pytest.mark.transit
def test_delete_route_change_dict(request):
    print("\n--Starting:", request.node.name)
    from lasso import project

    df = pd.DataFrame(
        {
            "name": "Line to be deleted",
            "direction_id": 0,
            "start_time_H": "6:00",
            "end_time_HHMM": "9:00",
        }
    )
    test_delete_change_dict = CubeProject.delete_route_change_dict(df.iloc[0])
    expected_delete_change_dict = {
        "category": "Delete Transit Service",
        "direction_id": 0,
        "start_time": "6:00",
        "end_time": "9:00",
    }
    assert test_delete_change_dict == expected_delete_change_dict


@pytest.mark.transit
def test_new_transit_route_change_dict(request):
    print("\n--Starting:", request.node.name)
    from lasso import project

    route_props_df = pd.DataFrame(
        {
            "name": "New and blue line",
            "agency_id": 22,
            "direction_id": 0,
            "start_time": "7:00",
            "end_time": "10:00",
            "headway_sec": 300,
            "operator": 6,
        }
    )

    shapes_df = pd.DataFrame(
        {
            "NAME": ["New and blue line"] * 6,
            "N": [1, 2, 3, 4, 5, 6],
            "stop": [True, True, True, True, False, True],
        }
    )

    test_new_line_change_dict = CubeProject.new_transit_route_change_dict(
        route_props_df.iloc[0],
        ["headway_sec", "operator"],
        shapes_df,
    )
    expected_new_line_change_dict = {
        "category": "New Transit Service",
        "facility": {
            "route_id": "New and blue line",
            "direction_id": 0,
            "start_time": "7:00",
            "end_time": "10:00",
        },
        "properties": [
            {"property": "headway_sec", "set": 300},
            {"property": "operator", "set": 6},
            {"property": "routing", "set": [1, 3, 4, -5, 6]},
        ],
    }
    assert test_new_line_change_dict == expected_new_line_change_dict


##########################################
# 2. Roadway Project Coding              #
# -  read logfile                        #
# -  write project card from logfile     #
# -  write ML project card from logfile  #
##########################################


@pytest.mark.parametrize("logfilename", LOGFILE_LIST)
@pytest.mark.ci
def test_logfile_read(request, logfilename):
    """
    Tests that the logfile can be read in and
    produces a DataFrame.
    """
    print("\n--Starting:", request.node.name)

    print("Reading: {}".format(logfilename))
    lf = CubeProject.read_logfile(logfilename)
    assert type(lf) == pd.DataFrame


@pytest.mark.parametrize("logfilename", LOGFILE_LIST)
@pytest.mark.ci
def test_write_roadway_project_card_from_logfile(request, logfilename):
    print("\n--Starting:", request.node.name)

    test_project = CubeProject.create_project(
        roadway_log_file=logfilename,
        base_roadway_dir=BASE_ROADWAY_DIR,
        shape_foreign_key="shape_id",
    )
    test_project.write_project_card(os.path.join(SCRATCH_DIR, "t_roadway_pc_test.yml"))
    ## todo write an assert that actually tests something


@pytest.mark.ci
@pytest.mark.parametrize("logfilename", LOGFILE_LIST)
def test_project_card_create_with_parameters_kw(request, logfilename):
    print("\n--Starting:", request.node.name)
    print("Reading: {}".format(logfilename))

    test_roadway_project = CubeProject.create_project(
        base_roadway_dir=BASE_ROADWAY_DIR,
        roadway_log_file=os.path.join(CUBE_DIR, logfilename),
        parameters={"lasso_base_dir": os.getcwd()},
        shape_foreign_key="shape_id",
    )

    test_roadway_project.write_project_card()


@pytest.mark.ci
def test_project_card_concatenate(request):
    """
    Tests that you can add multiple log files together.
    """
    print("\n--Starting:", request.node.name)
    whole_logfile = os.path.join(CUBE_DIR, "st_paul_test.log")

    split_logfile_list = [
        os.path.join(CUBE_DIR, "st_paul_test-A.log"),
        os.path.join(CUBE_DIR, "st_paul_test-B.log"),
    ]

    print("Reading Whole Logfile: {}".format(whole_logfile))
    whole_logfile_project = CubeProject.create_project(
        roadway_log_file=whole_logfile,
        base_roadway_dir=BASE_ROADWAY_DIR,
        shape_foreign_key="shape_id",
    )
    print(
        "\nWHOLE  Card Dict:\n  {}".format(whole_logfile_project.card_data["changes"])
    )

    print("Reading Split Logfiles: {}".format(split_logfile_list))
    # lf = Project.read_logfile(split_logfile_list)
    split_logfile_project = CubeProject.create_project(
        roadway_log_file=split_logfile_list,
        base_roadway_dir=BASE_ROADWAY_DIR,
        shape_foreign_key="shape_id",
    )

    print(
        "\nSPLIT  Card Dict:\n  {}".format(split_logfile_project.card_data["changes"])
    )

    assert (
        whole_logfile_project.card_data["changes"]
        == split_logfile_project.card_data["changes"]
    )


@pytest.mark.ci
def test_shp_changes(request):
    """
    Tests that the shp can be read in as a set changes with which to
    create a valid project card.
    """
    print("\n--Starting:", request.node.name)

    test_project = CubeProject.create_project(
        roadway_shp_file=os.path.join(CUBE_DIR, "example_shapefile_roadway_change.shp"),
        base_roadway_dir=BASE_ROADWAY_DIR,
        shape_foreign_key="shape_id",
    )
    assert type(test_project.roadway_changes) == pd.DataFrame
    assert type(test_project.card_data) == dict
    print(test_project)

    test_project.write_project_card(
        os.path.join(SCRATCH_DIR, "t_" + "example_shapefile_roadway_change" + ".yml")
    )


@pytest.mark.ci
def test_csv_changes(request):
    """
    Tests that the csv can be read in as a set changes with which to
    create a valid project card.
    """
    print("\n--Starting:", request.node.name)

    test_project = CubeProject.create_project(
        roadway_csv_file=os.path.join(CUBE_DIR, "example_csv_roadway_change.csv"),
        base_roadway_dir=BASE_ROADWAY_DIR,
        shape_foreign_key="shape_id",
    )
    assert type(test_project.roadway_changes) == pd.DataFrame
    assert type(test_project.card_data) == dict
    print(test_project)

    test_project.write_project_card(
        os.path.join(SCRATCH_DIR, "t_" + "example_csv_roadway_change" + ".yml")
    )


@pytest.mark.parametrize("logfilename", LOGFILE_LIST)
@pytest.mark.skip("Need to update project card schema")
def test_highway_change_project_card_valid(request, logfilename):
    print("\n--Starting:", request.node.name)

    print("Reading: {}".format(logfilename))
    test_project = CubeProject.create_project(
        roadway_log_file=logfilename,
        base_roadway_dir=BASE_ROADWAY_DIR,
        shape_foreign_key="shape_id",
    )

    test_project.write_project_card(
        os.path.join(
            SCRATCH_DIR,
            "t_" + os.path.splitext(os.path.basename(logfilename))[0] + ".yml",
        )
    )

    from network_wrangler import ProjectCard

    valid = ProjectCard.validate_project_card_schema(
        os.path.join(
            SCRATCH_DIR,
            "t_" + os.path.splitext(os.path.basename(logfilename))[0] + ".yml",
        )
    )

    assert valid


@pytest.mark.ci
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
