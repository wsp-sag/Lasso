import re
import os

import pytest

from lasso import Parameters, ModelRoadwayNetwork, metcouncil
from lasso.metcouncil import mc_parameters

"""
Run tests from bash/shell
Run just the tests labeled project using `pytest -m metcouncil`
To run with print statments, use `pytest -s -m metcouncil`
"""

STPAUL_DIR = os.path.join(os.getcwd(), "examples", "stpaul")

STPAUL_SHAPE_FILE = os.path.join(STPAUL_DIR, "shape.geojson")
STPAUL_LINK_FILE = os.path.join(STPAUL_DIR, "link.json")
STPAUL_NODE_FILE = os.path.join(STPAUL_DIR, "node.geojson")


def _read_stpaul_model_net():
    print("Used Parameters in _read_stpaul_model_net(): {}".format(mc_parameters))
    net = ModelRoadwayNetwork.read(
        link_filename=STPAUL_LINK_FILE,
        node_filename=STPAUL_NODE_FILE,
        shape_filename=STPAUL_SHAPE_FILE,
        fast=True,
        parameters = mc_parameters,
        shape_foreign_key = mc_parameters.file_ps.shape_foreign_key,
    )

    print("net.shape_foreign_key: ",net.shape_foreign_key)
    return net

@pytest.mark.metcouncil
@pytest.mark.params
def test_read_metcouncil_net_with_params(request):
    print("\n--Starting:", request.node.name)
    _read_stpaul_model_net()


@pytest.mark.metcouncil
@pytest.mark.travis
def test_calculate_lanes(request):
    """
    Tests that lanes are computed
    """
    print("\n--Starting:", request.node.name)

    net = _read_stpaul_model_net()

    if "lanes" in net.links_df.columns:
        net.links_df.drop(["lanes"], axis=1)

    l_net = metcouncil.calculate_number_of_lanes(
        roadway_net=net,
    )
    assert "lanes" in l_net.links_df.columns
    print("Number of Lanes Frequency for all links")
    print(l_net.links_df.lanes.value_counts())
    ## todo write an assert that actually tests something


@pytest.mark.metcouncil
@pytest.mark.travis
def test_assign_group_roadway_class(request):
    """
    Tests that assign group and roadway class are computed
    """
    print("\n--Starting:", request.node.name)

    net = _read_stpaul_model_net()

    l_net = metcouncil.calculate_assign_group_and_roadway_class(
        roadway_net=net,
    )
    assert "assign_group" in l_net.links_df.columns
    assert "roadway_class" in l_net.links_df.columns
    print("Assign Group Frequency for all links")
    print(l_net.links_df.assign_group.value_counts())
    print("Roadway Class Frequency for all links")
    print(l_net.links_df.roadway_class.value_counts())
    ## todo write an assert that actually tests something

@pytest.mark.metcouncil
@pytest.mark.travis
def test_calculate_area_type(request):
    """
    Tests that parameters are read
    """
    print("\n--Starting:", request.node.name)
    from metcouncil import calculate_area_type

    net = _read_stpaul_model_net()
    net = calculate_area_type(model_road_net)
    assert "area_type" in net.links_df.columns

    print("Area Type  Frequency")
    print(net.links_df.area_type.value_counts())

    ## todo write an assert that actually tests something

@pytest.mark.metcouncil
@pytest.mark.travis
def test_calculate_county_mpo(request):
    """
    Tests that parameters are read
    """
    print("\n--Starting:", request.node.name)

    from metcouncil import calculate_county_mpo
    net = _read_stpaul_model_net()

    net = calculate_county_mpo(net)

    assert "county" in net.links_df.columns
    assert "mpo" in net.links_df.columns
    print(net.links_df.area_type.value_counts())
    ## todo write an assert that actually tests something

@pytest.mark.metcouncil
@pytest.mark.travis
def test_roadway_standard_to_met_council_network(request):
    """
    Tests that parameters are read
    """
    print("\n--Starting:", request.node.name)

    from metcouncil import roadway_standard_to_met_council_network

    net = _read_stpaul_model_net()

    metcouncil_net = roadway_standard_to_met_council_network(net)
    ## todo write an assert that actually tests something