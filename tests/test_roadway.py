import re
import os

import pytest

from lasso import Parameters, ModelRoadwayNetwork

"""
Run tests from bash/shell
Run just the tests labeled project using `pytest -m roadway`
To run with print statments, use `pytest -s -m roadway`
"""

STPAUL_DIR = os.path.join(os.getcwd(), "examples", "stpaul")

STPAUL_SHAPE_FILE = os.path.join(STPAUL_DIR, "shape.geojson")
STPAUL_LINK_FILE = os.path.join(STPAUL_DIR, "link.json")
STPAUL_NODE_FILE = os.path.join(STPAUL_DIR, "node.geojson")


@pytest.mark.roadway
def test_parameter_read(request):
    """
    Tests that parameters are read
    """
    print("\n--Starting:", request.node.name)

    params = Parameters()
    print(params.__dict__)


@pytest.mark.roadway
def test_network_calculate_variables(request):
    """
    Tests that parameters are read
    """
    print("\n--Starting:", request.node.name)

    net = ModelRoadwayNetwork.read(
        link_file=STPAUL_LINK_FILE,
        node_file=STPAUL_NODE_FILE,
        shape_file=STPAUL_SHAPE_FILE,
        fast=True,
    )
    net.calculate_county(
        net.parameters.DEFAULT_COUNTY_SHAPE, net.parameters.DEFAULT_COUNTY_VARIABLE_SHP
    )
    print(net.links_df["county"].value_counts())

    net.calculate_mpo(net.parameters.DEFAULT_MPO_COUNTIES)
    print(net.links_df["mpo"].value_counts())


@pytest.mark.roadway
def test_network_split_variables_by_time(request):
    """
    Tests that parameters are read
    """
    print("\n--Starting:", request.node.name)

    net = ModelRoadwayNetwork.read(
        link_file=STPAUL_LINK_FILE,
        node_file=STPAUL_NODE_FILE,
        shape_file=STPAUL_SHAPE_FILE,
        fast=True,
    )

    net.split_properties_by_time_period_and_category(
        {
            "transit_priority": {
                "v": "transit_priority",
                "time_periods": Parameters.DEFAULT_TIME_PERIOD_TO_TIME,
                #'categories': Parameters.DEFAULT_CATEGORIES
            },
            "traveltime_assert": {
                "v": "traveltime_assert",
                "time_periods": Parameters.DEFAULT_TIME_PERIOD_TO_TIME,
            },
            "lanes": {
                "v": "lanes",
                "time_periods": Parameters.DEFAULT_TIME_PERIOD_TO_TIME,
            },
        }
    )
    assert "transit_priority_AM" in net.links_df.columns
    print(net.links_df.info())


@pytest.mark.roadway
def test_calculate_area_type(request):
    """
    Tests that parameters are read
    """
    print("\n--Starting:", request.node.name)

    net = ModelRoadwayNetwork.read(
        link_file=STPAUL_LINK_FILE,
        node_file=STPAUL_NODE_FILE,
        shape_file=STPAUL_SHAPE_FILE,
        fast=True,
    )

    net.calculate_area_type()
    assert "area_type" in net.links_df.columns
    print(net.links_df.area_type.value_counts())


@pytest.mark.roadway
@pytest.mark.menow
def test_calculate_assignment_group_rdclass(request):
    """
    Tests that parameters are read
    """
    print("\n--Starting:", request.node.name)

    net = ModelRoadwayNetwork.read(
        link_file=STPAUL_LINK_FILE,
        node_file=STPAUL_NODE_FILE,
        shape_file=STPAUL_SHAPE_FILE,
        fast=True,
    )

    net.calculate_assignment_group()
    net.calculate_roadway_class()
    assert "assignment_group" in net.links_df.columns
    assert "roadway_class" in net.links_df.columns
    print(net.links_df[net.links_df.drive_access == 1].assignment_group.value_counts())
    print(net.links_df[net.links_df.drive_access == 1].roadway_class.value_counts())


@pytest.mark.roadway
def test_calculate_count(request):
    """
    Tests that parameters are read
    """
    print("\n--Starting:", request.node.name)

    net = ModelRoadwayNetwork.read(
        link_file=STPAUL_LINK_FILE,
        node_file=STPAUL_NODE_FILE,
        shape_file=STPAUL_SHAPE_FILE,
        fast=True,
    )

    net.add_counts()
    assert "AADT" in net.links_df.columns
    print(net.links_df[net.links_df.drive_access == 1].AADT.value_counts())


@pytest.mark.roadway
def test_roadway_standard_to_dbf_for_cube(request):
    """
    Tests that parameters are read
    """
    print("\n--Starting:", request.node.name)

    net = ModelRoadwayNetwork.read(
        link_file=STPAUL_LINK_FILE,
        node_file=STPAUL_NODE_FILE,
        shape_file=STPAUL_SHAPE_FILE,
        fast=True,
    )

    links_dbf_df, nodes_dbf_df = net.roadway_standard_to_dbf_for_cube()
    print(links_dbf_df.info())
    print(nodes_dbf_df.info())


@pytest.mark.roadway
def test_write_cube_roadway(request):
    """
    Tests that parameters are read
    """
    print("\n--Starting:", request.node.name)

    net = ModelRoadwayNetwork.read(
        link_file=STPAUL_LINK_FILE,
        node_file=STPAUL_NODE_FILE,
        shape_file=STPAUL_SHAPE_FILE,
        fast=True,
    )

    net.write_roadway_as_shp()
