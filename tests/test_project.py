import os

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
SCRATCH_DIR = os.path.join(os.getcwd(), "tests", "scratch")

## create list of example logfiles to use as input
logfile_list = [os.path.join(CUBE_DIR, "st_paul_test.log")]


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
        roadway_log_file=logfilename,
        base_roadway_dir=ROADWAY_DIR,
        shape_foreign_key="shape_id",
    )

    assert type(test_project.roadway_changes) == DataFrame
    # assert(type(test_project.card_data)==Dict[str, Dict[str, Any]])
    assert type(test_project.card_data) == dict

    test_project.write_project_card(
        os.path.join(
            SCRATCH_DIR,
            "t_" + os.path.splitext(os.path.basename(logfilename))[0] + ".yml",
        )
    )


@pytest.mark.travis
@pytest.mark.parametrize("logfilename", [logfile_list[0]])
def test_project_card_create_with_parameters_kw(request, logfilename):
    print("\n--Starting:", request.node.name)
    print("Reading: {}".format(logfilename))

    test_roadway_project = Project.create_project(
        base_roadway_dir=ROADWAY_DIR,
        roadway_log_file=os.path.join(CUBE_DIR, logfilename),
        parameters={"lasso_base_dir": os.getcwd()},
        shape_foreign_key="shape_id",
    )

    test_roadway_project.write_project_card()


@pytest.mark.travis
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
    whole_logfile_project = Project.create_project(
        roadway_log_file=whole_logfile,
        base_roadway_dir=ROADWAY_DIR,
        shape_foreign_key="shape_id",
    )
    print(
        "\nWHOLE  Card Dict:\n  {}".format(whole_logfile_project.card_data["changes"])
    )

    print("Reading Split Logfiles: {}".format(split_logfile_list))
    # lf = Project.read_logfile(split_logfile_list)
    split_logfile_project = Project.create_project(
        roadway_log_file=split_logfile_list,
        base_roadway_dir=ROADWAY_DIR,
        shape_foreign_key="shape_id",
    )

    print(
        "\nSPLIT  Card Dict:\n  {}".format(split_logfile_project.card_data["changes"])
    )

    assert (
        whole_logfile_project.card_data["changes"]
        == split_logfile_project.card_data["changes"]
    )


@pytest.mark.travis
def test_shp_changes(request):
    """
    Tests that the shp can be read in as a set changes with which to
    create a valid project card.
    """
    print("\n--Starting:", request.node.name)

    test_project = Project.create_project(
        roadway_shp_file=os.path.join(CUBE_DIR, "example_shapefile_roadway_change.shp"),
        base_roadway_dir=ROADWAY_DIR,
        shape_foreign_key="shape_id",
    )
    assert type(test_project.roadway_changes) == DataFrame
    assert type(test_project.card_data) == dict
    print(test_project)

    test_project.write_project_card(
        os.path.join(SCRATCH_DIR, "t_" + "example_shapefile_roadway_change" + ".yml")
    )


@pytest.mark.travis
def test_csv_changes(request):
    """
    Tests that the csv can be read in as a set changes with which to
    create a valid project card.
    """
    print("\n--Starting:", request.node.name)

    test_project = Project.create_project(
        roadway_csv_file=os.path.join(CUBE_DIR, "example_csv_roadway_change.csv"),
        base_roadway_dir=ROADWAY_DIR,
        shape_foreign_key="shape_id",
    )
    assert type(test_project.roadway_changes) == DataFrame
    assert type(test_project.card_data) == dict
    print(test_project)

    test_project.write_project_card(
        os.path.join(SCRATCH_DIR, "t_" + "example_csv_roadway_change" + ".yml")
    )


@pytest.mark.parametrize("logfilename", logfile_list)
@pytest.mark.skip("Need to update project card schema")
def test_highway_change_project_card_valid(request, logfilename):
    print("\n--Starting:", request.node.name)

    print("Reading: {}".format(logfilename))
    test_project = Project.create_project(
        roadway_log_file=logfilename,
        base_roadway_dir=ROADWAY_DIR,
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


@pytest.mark.transit
@pytest.mark.travis
def test_update_route_prop_change_dict(request):
    print("\n--Starting:", request.node.name)
    from lasso import project

    df = DataFrame(
        {
            ("name", "self"): "Line to be updated",
            ("headway_secs", "self"): 300,
            ("headway_secs", "other"): 30,
            ("start_time_HHMM", "self"): "6:00",
            ("end_time_HHMM", "self"): "9:00",
        }
    )
    expected_update_change_dict = {
        "property": "headway_secs",
        "existing": 300,
        "set": 30,
    }

    test_update_change_dict = project.update_route_prop_change_dict(df.iloc[0])

    assert test_update_change_dict == expected_update_change_dict


@pytest.mark.transit
def test_delete_route_change_dict(request):
    print("\n--Starting:", request.node.name)
    from lasso import project

    df = DataFrame(
        {
            "name": "Line to be deleted",
            "direction_id": 0,
            "start_time_HHMM": "6:00",
            "end_time_HHMM": "9:00",
        }
    )
    test_delete_change_dict = project.delete_route_change_dict(df.iloc[0])
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

    route_props_df = DataFrame(
        {
            "name": "New and blue line",
            "agency_id": 22,
            "direction_id": 0,
            "start_time_HHMM": "7:00",
            "end_time_HHMM": "10:00",
            "headway_sec": 300,
            "operator": 6,
        }
    )

    shapes_df = DataFrame(
        {
            "NAME": ["New and blue line"] * 6,
            "N": [1, 2, 3, 4, 5, 6],
            "stop": [True, True, True, True, False, True],
        }
    )

    test_new_line_change_dict = project.new_transit_route_change_dict(
        route_props_df.iloc[0], ["headway_sec", "operator"], shapes_df,
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
