import os

import pytest

from lasso import ModelRoadwayNetwork
from lasso.metcouncil.metcouncil_roadway import MetCouncilRoadwayNetwork
from lasso import Parameters

"""
Run tests from bash/shell
Run just the tests labeled project using `pytest -m roadway`
To run with print statments, use `pytest -s -m roadway`
"""

STPAUL_DIR = os.path.join(os.getcwd(), "examples", "stpaul")

STPAUL_SHAPE_FILE = os.path.join(STPAUL_DIR, "shape.geojson")
STPAUL_LINK_FILE = os.path.join(STPAUL_DIR, "link.json")
STPAUL_NODE_FILE = os.path.join(STPAUL_DIR, "node.geojson")


def _read_stpaul_net():
    net = MetCouncilRoadwayNetwork.read(
        link_filename=STPAUL_LINK_FILE,
        node_filename=STPAUL_NODE_FILE,
        shape_filename=STPAUL_SHAPE_FILE,
        fast=True,
        shape_foreign_key="shape_id",
    )
    return net


@pytest.mark.travis
def test_parameter_read(request):
    """
    Tests that parameters are read
    """
    print("\n--Starting:", request.node.name)

    params = Parameters()
    print(params.__dict__)
    ## todo write an assert that actually tests something


@pytest.mark.travis
def test_network_split_variables_by_time(request):
    """
    Tests that parameters are read
    """
    print("\n--Starting:", request.node.name)

    net = _read_stpaul_net()

    net.links_df = net.split_properties_by_time_period_and_category(net.links_df)
    assert "trn_priority_AM" in net.links_df.columns
    print(net.links_df.info())
    ## todo write an assert that actually tests something


@pytest.mark.travis
def test_calculate_count(request):
    """
    Tests that parameters are read
    """
    print("\n--Starting:", request.node.name)

    net = _read_stpaul_net()

    net.links_df = net.add_counts(net.links_df)
    assert "count_daily" in net.links_df.columns
    print(net.links_df[net.links_df.drive_access == 1].count_daily.value_counts())
    ## todo write an assert that actually tests something


@pytest.mark.travis
def test_write_cube_roadway_as_ff(request):
    """
    Tests that parameters are read
    """
    print("\n--Starting:", request.node.name)

    net = _read_stpaul_net()
    net.roadway_standard_to_met_council_network()
    net.write_roadway_as_fixedwidth(net.model_links_df, net.nodes_df)
    ## todo write an assert that actually tests something


@pytest.mark.travis
def test_write_roadway_as_shape(request):
    """"""
    print("\n--Starting:", request.node.name)

    net = _read_stpaul_net()
    net.roadway_standard_to_met_council_network()
    net.write_roadway_as_shp(net.model_links_df, net.nodes_df)
    ## todo write an assert that actually tests something
