import os

import pytest

from lasso.metcouncil import MetCouncilRoadwayNetwork

"""
Run tests from bash/shell
Run just the tests labeled project using `pytest -m metcouncil`
To run with print statments, use `pytest -s -m metcouncil`
"""

STPAUL_DIR = os.path.join(os.getcwd(), "examples", "stpaul")

STPAUL_SHAPE_FILE = os.path.join(STPAUL_DIR, "shape.geojson")
STPAUL_LINK_FILE = os.path.join(STPAUL_DIR, "link.json")
STPAUL_NODE_FILE = os.path.join(STPAUL_DIR, "node.geojson")


@pytest.fixture
def stpaul_model_net():
    return MetCouncilRoadwayNetwork.read(
        link_filename=STPAUL_LINK_FILE,
        node_filename=STPAUL_NODE_FILE,
        shape_filename=STPAUL_SHAPE_FILE,
        fast=True,
    )


@pytest.mark.metcouncil
@pytest.mark.params
def test_read_metcouncil_net_with_params(request, stpaul_model_net):
    if request:
        print("\n--Starting:", request.node.name)
    stpaul_model_net


@pytest.mark.metcouncil
@pytest.mark.ci
@pytest.mark.sijia
def test_calculate_lanes(request, stpaul_model_net):
    """
    Tests that lanes are computed.

    TODO: see why join for model_limk id leaves 56,226 links empty?
    """
    if request:
        print("\n--Starting:", request.node.name)

    net = stpaul_model_net

    if "lanes" in net.links_df.columns:
        net.links_df.drop(["lanes"], axis=1)

    net.links_df = net.calculate_number_of_lanes(net.links_df)

    assert "lanes" in net.links_df.columns
    print("Number of Lanes Frequency for all links")
    print(net.links_df.lanes.value_counts())
    ## todo write an assert that actually tests something


@pytest.mark.metcouncil
@pytest.mark.ci
def test_assign_group_roadway_class(request, stpaul_model_net):
    """
    Tests that assign group and roadway class are computed
    """
    if request:
        print("\n--Starting:", request.node.name)

    net = stpaul_model_net

    net.links_df = net.calculate_assign_group_and_roadway_class(net.links_df)
    assert "assign_group" in net.links_df.columns
    assert "roadway_class" in net.links_df.columns
    print("Assign Group Frequency for all links")
    print(net.links_df.assign_group.value_counts())
    print("Roadway Class Frequency for all links")
    print(net.links_df.roadway_class.value_counts())
    ## todo write an assert that actually tests something


@pytest.mark.metcouncil
@pytest.mark.ci
def test_calculate_area_type(request, stpaul_model_net):
    """
    Tests that parameters are read
    """
    if request:
        print("\n--Starting:", request.node.name)

    net = stpaul_model_net
    net.links_df = net.calculate_area_type(net.links_df)
    assert "area_type" in net.links_df.columns

    print("Area Type  Frequency")
    print(net.links_df.area_type.value_counts())

    ## todo write an assert that actually tests something


@pytest.mark.metcouncil
@pytest.mark.ci
def test_calculate_county_mpo(request, stpaul_model_net):
    """
    Tests that parameters are read
    """
    if request:
        print("\n--Starting:", request.node.name)

    net = stpaul_model_net

    net.links_df = net.calculate_county_mpo(net.links_df)

    assert "county" in net.links_df.columns
    assert "mpo" in net.links_df.columns
    print(net.links_df[["mpo", "county"]].value_counts())
    ## todo write an assert that actually tests something


@pytest.mark.metcouncil
@pytest.mark.ci
def test_roadway_standard_to_met_council_network(request, stpaul_model_net):
    """
    Tests that parameters are read
    """
    if request:
        print("\n--Starting:", request.node.name)

    net = stpaul_model_net

    net.roadway_standard_to_met_council_network()
    ## todo write an assert that actually tests something


if __name__ == "__main__":
    # test_read_metcouncil_net_with_params(None)
    # test_calculate_lanes()
    # test_calculate_area_type(None)
    pass
