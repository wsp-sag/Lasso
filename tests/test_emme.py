import os

import pytest
import pickle
import copy

from lasso import Parameters, transit
from lasso import emme
from network_wrangler import TransitNetwork
from lasso import StandardTransit

test_dir = r"Z:\Data\Users\Sijia\MTC\github\Lasso\examples\mtc"
union_city_links_df = pickle.load(open(os.path.join(test_dir, "union_city_links.pickle"), 'rb'))
union_city_nodes_df = pickle.load(open(os.path.join(test_dir, "union_city_nodes.pickle"), 'rb'))

params = Parameters()

transit_dir = test_dir

transit_net = TransitNetwork.read(feed_path = transit_dir)

# transit netowrk for union city
agency_df = transit_net.feed.agency.loc[transit_net.feed.agency['agency_id'] == 'UCT']
routes_df = transit_net.feed.routes.loc[transit_net.feed.routes['agency_id'] == 'UCT']
trips_df = transit_net.feed.trips[transit_net.feed.trips['route_id'].isin(routes_df['route_id'])]
freqs_df = transit_net.feed.frequencies[transit_net.feed.frequencies['trip_id'].isin(trips_df['trip_id'])]
shapes_df = transit_net.feed.shapes[transit_net.feed.shapes['shape_id'].isin(trips_df['shape_id'])]
stop_times_df = transit_net.feed.stop_times[transit_net.feed.stop_times['trip_id'].isin(trips_df['trip_id'])]
stops_df = transit_net.feed.stops[transit_net.feed.stops['stop_id'].isin(stop_times_df['stop_id'])]

union_city_transit_net =  copy.deepcopy(transit_net)
union_city_transit_net.feed.agency = agency_df
union_city_transit_net.feed.routes = routes_df
union_city_transit_net.feed.trips  = trips_df
union_city_transit_net.feed.frequencies = freqs_df
union_city_transit_net.feed.shapes = shapes_df
union_city_transit_net.feed.stop_times = stop_times_df
union_city_transit_net.feed.stops = stops_df

model_transit_net = StandardTransit.fromTransitNetwork(union_city_transit_net, parameters = params)


@pytest.mark.roadway
@pytest.mark.travis
def test_write_emme_roadway(request):
    """
    Tests that write out emme roadway
    """
    print("\n--Starting:", request.node.name)
    params = Parameters()

    emme.create_emme_network(
        links_df=union_city_links_df,
        nodes_df=union_city_nodes_df,
        name="test lasso roadway",
        path="D:/MTC/emme",
        parameters=params
    )

@pytest.mark.transit
@pytest.mark.travis
def test_write_emme_transit(request):
    """
    Tests that write out emme transit
    """
    print("\n--Starting:", request.node.name)
    params = Parameters()

    emme.create_emme_network(
        links_df=union_city_links_df,
        nodes_df=union_city_nodes_df,
        transit_network=model_transit_net,
        include_transit=True,
        name="test lasso transit",
        path="D:/MTC/emme",
        parameters=params
    )

@pytest.mark.network
@pytest.mark.travis
def test_write_emme_all_five_networks(request):
    """
    Tests that write out emme roadway
    """
    print("\n--Starting:", request.node.name)
    params = Parameters()

    emme.create_emme_network(
        links_df=union_city_links_df,
        nodes_df=union_city_nodes_df,
        transit_network=model_transit_net,
        name="test lasso",
        path="Z:/Data/Users/Sijia/MTC/github/Lasso/tests/scratch",
        write_taz_drive_network = False,
        write_maz_drive_network = False,
        write_maz_active_modes_network = True,
        write_tap_transit_network = False,
        parameters=params
    )