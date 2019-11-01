
import re
import os

import pytest

from lasso import Parameters, ModelRoadwayNetwork

"""
Run tests from bash/shell
Run just the tests labeled project using `pytest -m roadway`
To run with print statments, use `pytest -s -m roadway`
"""

STPAUL_DIR = os.path.join(os.getcwd(), 'examples', 'stpaul')

STPAUL_SHAPE_FILE = os.path.join(STPAUL_DIR, "shape.geojson")
STPAUL_LINK_FILE = os.path.join(STPAUL_DIR, "link.json")
STPAUL_NODE_FILE = os.path.join(STPAUL_DIR, "node.geojson")


@pytest.mark.roadway
def test_parameter_read(request):
    '''
    Tests that parameters are read
    '''
    print("\n--Starting:",request.node.name)

    params = Parameters()
    print(params.__dict__)


@pytest.mark.roadway
def test_network_calculate_variables(request):
    '''
    Tests that parameters are read
    '''
    print("\n--Starting:",request.node.name)

    net = ModelRoadwayNetwork.read(
        link_file=STPAUL_LINK_FILE,
        node_file=STPAUL_NODE_FILE,
        shape_file=STPAUL_SHAPE_FILE,
        fast=True,
    )
    net.calculate_county(
        net.parameters.DEFAULT_COUNTY_SHAPE,
        net.parameters.DEFAULT_COUNTY_VARIABLE_SHP,
    )
    print(net.links_df['county'].value_counts())

    net.calculate_mpo(net.parameters.DEFAULT_MPO_COUNTIES)
    print(net.links_df['mpo'].value_counts())

@pytest.mark.roadway
@pytest.mark.menow
def test_network_split_variables_by_time(request):
    '''
    Tests that parameters are read
    '''
    print("\n--Starting:",request.node.name)

    net = ModelRoadwayNetwork.read(
        link_file=STPAUL_LINK_FILE,
        node_file=STPAUL_NODE_FILE,
        shape_file=STPAUL_SHAPE_FILE,
        fast=True,
    )
    net.split_properties_by_time_period_and_category(
        {
        'transit_priority' :
            {
                'v':'T_PRIORITY',
                'time_periods':Parameters.DEFAULT_TIME_PERIOD_TO_TIME,
                #'categories': Parameters.DEFAULT_CATEGORIES
            }
        }
    )
    assert('transit_priority_AM' in net.links_df.columns)
