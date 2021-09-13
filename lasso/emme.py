"""Setup Emme project, database (Emmebank) and import network data."""

import os as _os
from collections import defaultdict as _defaultdict
from copy import deepcopy as _copy

from geopandas.geodataframe import GeoDataFrame
from osgeo import ogr as _ogr

import inro.emme.database.emmebank as _eb
import inro.emme.desktop.app as _app
import inro.emme.network as _network

import pickle
import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString
from typing import Optional, Union
import numpy as np
import math

from .roadway import ModelRoadwayNetwork
from .parameters import Parameters
from .logger import WranglerLogger
from .mtc import _is_express_bus, _special_vehicle_type

from lasso import StandardTransit

_join = _os.path.join
_dir = _os.path.dirname


def _norm(path):
    return _os.path.normcase(_os.path.normpath(path))


def create_emme_network(
    roadway_network: Optional[ModelRoadwayNetwork] = None,
    transit_network: Optional[StandardTransit] = None,
    include_transit: Optional[bool] =False,
    links_df: Optional[GeoDataFrame]=None,
    nodes_df: Optional[GeoDataFrame]=None,
    name: Optional[str]="",
    path: Optional[str]="",
    parameters: Union[Parameters, dict] = {},
):
    """
    method that calls emme to write out EMME network from Lasso network
    """

    _NAME = name

    out_dir = path
    
    model_tables = {}

    if roadway_network:
        links_df = roadway_network.links_mtc_df.copy()
        nodes_df = roadway_network.nodes_mtc_df.copy()

    elif (len(links_df)>0) & (len(nodes_df)>0):
        links_df = links_df.copy()
        nodes_df = nodes_df.copy()

    else:
        msg = "Missing roadway network to write to emme, please specify either model_net or links_df and nodes_df."
        WranglerLogger.error(msg)
        raise ValueError(msg)

    if include_transit:
        if not transit_network:
            msg = "Missing transit network to write to emme, please specify transit_network."
            WranglerLogger.error(msg)
            raise ValueError(msg)

    links_df = links_df.to_crs(epsg = 4326)
    nodes_df = nodes_df.to_crs(epsg = 4326)

    nodes_df["X"] = nodes_df["geometry"].apply(lambda g: g.x)
    nodes_df["Y"] = nodes_df["geometry"].apply(lambda g: g.y)

    # create reverse shapes
    links_df = pd.merge(
        links_df,
        nodes_df[["N", "X", "Y"]].rename(columns = {"N" : "A", "X": "A_X", "Y" : "A_Y"}),
        how = "left",
        on = "A"
    )

    links_df = pd.merge(
        links_df,
        nodes_df[["N", "X", "Y"]].rename(columns = {"N" : "B", "X": "B_X", "Y" : "B_Y"}),
        how = "left",
        on = "B"
    )

    links_df["geometry"] = links_df.apply(
        lambda g: LineString(list(g["geometry"].coords)[::-1]) if list(g["geometry"].coords)[0][0] == g["B_X"] else g["geometry"],
        axis = 1
    )

    # geometry to wkt geometry
    links_df["geometry_wkt"] = links_df["geometry"].apply(lambda x: x.wkt)

    if include_transit:
        # gtfs trips
        trips_df = route_properties_gtfs_to_emme(
            transit_network=transit_network,
            parameters=parameters
        )

        itinerary_df=pd.DataFrame()
        for index, row in trips_df.iterrows():
            trip_itinerary_df = shape_gtfs_to_emme(
                transit_network=transit_network,
                trip_row=row
            )
            itinerary_df = itinerary_df.append(trip_itinerary_df, sort =False, ignore_index=True)
        #print(itinerary_df)
        model_tables["line_table"] = trips_df.to_dict('records')
        print(trips_df[:2])
        model_tables['itinerary_table']=itinerary_df.to_dict('records')

    model_tables["centroid_table"] = nodes_df[nodes_df.N.isin(parameters.taz_N_list + parameters.maz_N_list)].to_dict('records')
    model_tables["node_table"] = nodes_df[~(nodes_df.N.isin(parameters.taz_N_list + parameters.maz_N_list))].to_dict('records')
    model_tables["connector_table"] = links_df[(links_df.A.isin(parameters.taz_N_list + parameters.maz_N_list)) | (links_df.B.isin(parameters.taz_N_list + parameters.maz_N_list))].to_dict('records')
    model_tables["link_table"] = links_df[~(links_df.A.isin(parameters.taz_N_list + parameters.maz_N_list)) & ~(links_df.B.isin(parameters.taz_N_list + parameters.maz_N_list))].to_dict('records')

    model_tables["vehicle_table"] = [
        {
            "id": 1,
            "mode": "b",
            "total_capacity": 70,
            "seated_capacity": 35,
            "auto_equivalent": 2.5
        },
    ]
    setup = SetupEmme(model_tables, [{"name": "AM", "duration": 4.0, "id": 10000}], out_dir, _NAME, include_transit)
    setup.run()

def route_properties_gtfs_to_emme(
    transit_network = None,
    parameters = None,
    outpath: str = None
):
    """
    Prepare gtfs for cube lin file.

    Does the following operations:
    1. Combines route, frequency, trip, and shape information
    2. Converts time of day to time periods
    3. Calculates cube route name from gtfs route name and properties
    4. Assigns a cube-appropriate mode number
    5. Assigns a cube-appropriate operator number

    Returns:
        trip_df (DataFrame): DataFrame of trips with cube-appropriate values for:
            - NAME
            - ONEWAY
            - OPERATOR
            - MODE
            - HEADWAY
    """
    WranglerLogger.info(
        "Converting GTFS Standard Properties to MTC's Emme Standard"
    )

    shape_df = transit_network.feed.shapes.copy()
    trip_df = transit_network.feed.trips.copy()

    mode_crosswalk = pd.read_csv(parameters.mode_crosswalk_file)
    mode_crosswalk.drop_duplicates(subset = ["agency_raw_name", "route_type", "is_express_bus"], inplace = True)
    """
    faresystem_crosswalk = pd.read_csv(_os.path.join(_os.path.dirname(outpath), "faresystem_crosswalk.txt"),
        dtype = {"route_id" : "object"}
    )
    """
    veh_cap_crosswalk = pd.read_csv(parameters.veh_cap_crosswalk_file)

    """
    Add information from: routes, frequencies, and routetype to trips_df
    """
    trip_df = pd.merge(trip_df, transit_network.feed.routes.drop("agency_raw_name", axis = 1), how="left", on="route_id")

    trip_df = pd.merge(trip_df, transit_network.feed.frequencies, how="left", on="trip_id")

    trip_df["tod"] = trip_df.start_time.apply(transit_network.time_to_cube_time_period, as_str = False)
    trip_df["time_period"] = trip_df.start_time.apply(transit_network.time_to_cube_time_period)

    # add shape_id to name when N most common pattern is used for routes*tod*direction
    trip_df["shp_id"] = trip_df.groupby(["route_id", "tod", "direction_id"]).cumcount()
    trip_df["shp_id"] = trip_df["shp_id"].astype(str)
    trip_df["shp_id"] = "s" + trip_df["shp_id"]

    trip_df["route_short_name"] = trip_df["route_short_name"].str.replace("-", "_").str.replace(" ", ".").str.replace(",", "_").str.slice(stop = 50)

    trip_df["route_long_name"] = trip_df["route_long_name"].str.replace(",", "_").str.slice(stop = 50)

    # make tri-delta-transit name shorter
    trip_df["agency_id"] = np.where(
        trip_df.agency_id == "tri-delta-transit",
        "tri-delta",
        trip_df.agency_id
    )

    trip_df["LONGNAME"] = trip_df["route_long_name"]
    trip_df["headway_minutes"] = (trip_df["headway_secs"] / 60).astype(int)

    trip_df = pd.merge(trip_df, transit_network.feed.agency[["agency_name", "agency_raw_name", "agency_id"]], how = "left", on = ["agency_raw_name", "agency_id"])

    # identify express bus
    trip_df["is_express_bus"] = trip_df.apply(lambda x: _is_express_bus(x), axis = 1)
    trip_df.drop("agency_name", axis = 1 , inplace = True)

    trip_df = pd.merge(
        trip_df,
        mode_crosswalk.drop("agency_id", axis = 1),
        how = "left",
        on = ["agency_raw_name", "route_type", "is_express_bus"]
    )

    trip_df['TM2_mode'].fillna(11, inplace = True)
    trip_df['mode'] = trip_df['TM2_mode'].astype(int)

    trip_df["agency_id"].fillna("", inplace = True)

    trip_df["line_id"] = trip_df.apply(
        lambda x: str(x.TM2_operator)
        + "_"
        + str(x.route_id)
        + "_"
        + x.time_period
        + "_"
        + "d"
        + str(int(x.direction_id))
        + "_s"
        + x.shape_id,
        axis=1,
    )

    trip_df["line_id"] = trip_df["line_id"].str.slice(stop = 28)
    """
    # faresystem
    zonal_fare_dict = faresystem_crosswalk[
        (faresystem_crosswalk.route_id_original.isnull())
    ].copy()
    zonal_fare_dict = dict(zip(zonal_fare_dict.agency_raw_name, zonal_fare_dict.faresystem))

    trip_df = pd.merge(
        trip_df,
        faresystem_crosswalk,
        how = "left",
        on = ["agency_raw_name", "route_id"]
    )

    trip_df["faresystem"] = np.where(
        trip_df["faresystem"].isnull(),
        trip_df["agency_raw_name"].map(zonal_fare_dict),
        trip_df["faresystem"]
    )

    # GTFS fare info is incomplete
    trip_df["faresystem"].fillna(99,inplace = True)
    """
    # special vehicle types
    #trip_df["vehicle_type"] = trip_df.apply(lambda x: _special_vehicle_type(x), axis = 1)
    trip_df["vehicle_type"] = 1
    # get vehicle capacity
    trip_df = pd.merge(trip_df, veh_cap_crosswalk, how = "left", on = "VEHTYPE")

    trip_df['USERA1'] = trip_df.apply(lambda row: row.agency_id if row.agency_id != "" else row.agency_raw_name, axis = 1)
    trip_df['USERA2'] = trip_df.apply(lambda row: row.TM2_line_haul_name, axis = 1)
    trip_df['OPERATOR'] = trip_df.apply(lambda row: int(row.TM2_operator) if ~math.isnan(row.TM2_operator) else 99, axis = 1)

    return trip_df

def shape_gtfs_to_emme(transit_network, trip_row):
        """
        Creates transit segment for the trips in appropriate
        emme format.

        Args:
            row: DataFrame row with both shape_id and trip_id

        Returns: a dataframe representation of the transit segment
            for a trip in emme format.

        """
        trip_stop_times_df = transit_network.feed.stop_times.copy()
        trip_stop_times_df = trip_stop_times_df[
            trip_stop_times_df.trip_id == trip_row.trip_id
        ]

        trip_node_df = transit_network.feed.shapes.copy()
        trip_node_df = trip_node_df[trip_node_df.shape_id == trip_row.shape_id]
        trip_node_df.sort_values(by = ["shape_pt_sequence"], inplace = True)

        trip_stop_times_df = pd.merge(
            trip_stop_times_df, transit_network.feed.stops, how="left", on="stop_id"
        )

        stop_node_id_list = trip_stop_times_df["model_node_id"].tolist()
        trip_node_list = trip_node_df["shape_model_node_id"].tolist()

        trip_stop_times_df.sort_values(by = ["stop_sequence"], inplace = True)
        # sometimes GTFS `stop_sequence` does not start with 1, e.g. SFMTA light rails
        trip_stop_times_df["internal_stop_sequence"] = range(1, 1+len(trip_stop_times_df))
        # sometimes GTFS `departure_time` is not recorded for every stop, e.g. VTA light rails
        trip_stop_times_df["departure_time"].fillna(method = "ffill", inplace = True)
        trip_stop_times_df["departure_time"].fillna(0, inplace = True)
        trip_stop_times_df["NNTIME"] = trip_stop_times_df["departure_time"].diff() / 60
        # CUBE NNTIME takes 2 decimals
        trip_stop_times_df["NNTIME"] = trip_stop_times_df["NNTIME"].round(2)
        trip_stop_times_df["NNTIME"].fillna(-1, inplace = True)

        # node list
        stop_seq = 0
        nntimes = []
        allow_alightings=[]
        allow_boardings=[]
        stop_names=[]
        #print(trip_row.trip_id)
        #print(trip_row.line_id)
        #print(trip_node_list)
        #print(stop_node_id_list)

        if trip_row.TM2_line_haul_name in ["Light rail", "Heavy rail", "Commuter rail", "Ferry service"]:
            add_nntime = True
        else:
            add_nntime = False

        for nodeIdx in range(len(trip_node_list)):
            #print(nodeIdx)
            #print(trip_node_list[nodeIdx])
            if trip_node_list[nodeIdx] in stop_node_id_list:
                # in case a route stops at a stop more than once, e.g. circular route
                stop_seq += 1
                #print("stop seq {}".format(stop_seq))
                #print(trip_stop_times_df[
                #        trip_stop_times_df["model_node_id"] == trip_node_list[nodeIdx]])
                if (add_nntime) & (stop_seq > 1):
                    if len(trip_stop_times_df[
                        trip_stop_times_df["model_node_id"] == trip_node_list[nodeIdx]]) > 1:
                    
                        nntime_v = trip_stop_times_df.loc[
                            (trip_stop_times_df["model_node_id"] == trip_node_list[nodeIdx]) &
                            (trip_stop_times_df["internal_stop_sequence"] == stop_seq),
                            "NNTIME"].iloc[0]
                    else:
                        nntime_v = trip_stop_times_df.loc[
                            (trip_stop_times_df["model_node_id"] == trip_node_list[nodeIdx]),"NNTIME"].iloc[0]

                    nntimes.append(nntime_v)
                else:
                    nntimes.append(0)

                pickup_type = trip_stop_times_df.loc[
                    (trip_stop_times_df["model_node_id"] == trip_node_list[nodeIdx]),"pickup_type"].iloc[0]
                if pickup_type in [1, "1"]:
                    allow_alightings.append(0)
                else:
                    allow_alightings.append(1)

                drop_off_type = trip_stop_times_df.loc[
                    (trip_stop_times_df["model_node_id"] == trip_node_list[nodeIdx]),"drop_off_type"].iloc[0]
                if drop_off_type in [1, "1"]:
                    allow_boardings.append(0)
                else:
                    allow_boardings.append(1)

                stop_name = trip_stop_times_df.loc[
                    (trip_stop_times_df["model_node_id"] == trip_node_list[nodeIdx]),"stop_name"].iloc[0]
                stop_names.append(stop_name)
                
            else:
                nntimes.append(0)
                allow_alightings.append(0)
                allow_boardings.append(0)
                stop_names.append("")

        trip_node_df['time_minutes'] = nntimes
        trip_node_df['allow_alightings'] = allow_alightings
        trip_node_df['allow_boardings'] = allow_boardings
        trip_node_df['stop_name'] = stop_names
        trip_node_df['line_id'] = trip_row['line_id']
        trip_node_df['node_id'] = trip_node_df['shape_model_node_id'].astype(int)
        trip_node_df['stop_order'] = trip_node_df['shape_pt_sequence']

        return trip_node_df

class SetupEmme(object):
    """Class to run Emme import and data management operations."""

    def __init__(self, model_tables, time_periods, directory, name, include_transit):
        """
        Initialize Python class to run setup of Emme project.

        Arguments:
            model_tables -- interface to tables of model data
            time_periods -- list of time period objects with .name and .duration
            directory -- the output directory for the Emme project and database
            include_transit -- if True, process transit data from database
        """
        self._model_tables = model_tables
        self._time_periods = time_periods
        self._directory = directory
        self._NAME = name
        self._include_transit = bool(include_transit)

        # TODO: do not hard code this section
        attributes = [
            NetworkAttribute("NODE", "#node_id", "N", "NETWORK_FIELD", "INTEGER32"),
            NetworkAttribute("NODE", "x", "X"),
            NetworkAttribute("NODE", "y", "Y"),
            NetworkAttribute("NODE", "@bike_node", "bike_access", "EXTRA", "INTEGER32"),
            NetworkAttribute("NODE", "@drive_node", "drive_access", "EXTRA", "INTEGER32"),
            NetworkAttribute("NODE", "@walk_node", "walk_access", "EXTRA", "INTEGER32"),
            NetworkAttribute("NODE", "#node_county", "county", "NETWORK_FIELD", "STRING"),
            NetworkAttribute("NODE", "#osm_node_id", "osm_node_id", "NETWORK_FIELD", "STRING"),
            NetworkAttribute("NODE", "@rail_node", "rail_only", "EXTRA", "INTEGER32"),
            NetworkAttribute("NODE", "@farezone", "farezone", "EXTRA", "INTEGER32"),
            NetworkAttribute("NODE", "@tap_id", "tap_id", "EXTRA", "INTEGER32"),
            NetworkAttribute("CENTROID", "#zone_id", "N", "NETWORK_FIELD", "INTEGER32"),
            NetworkAttribute("CENTROID", "#node_id", "N", "NETWORK_FIELD", "INTEGER32"),
            NetworkAttribute("CENTROID", "x", "X"),
            NetworkAttribute("CENTROID", "y", "Y"),
            NetworkAttribute("CENTROID", "@bike_node", "bike_access", "EXTRA", "INTEGER32"),
            NetworkAttribute("CENTROID", "@drive_node", "drive_access", "EXTRA", "INTEGER32"),
            NetworkAttribute("CENTROID", "@walk_node", "walk_access", "EXTRA", "INTEGER32"),
            NetworkAttribute("CENTROID", "#node_county", "county", "NETWORK_FIELD", "STRING"),
            NetworkAttribute("CENTROID", "#osm_node_id", "osm_node_id", "NETWORK_FIELD", "STRING"),
            NetworkAttribute("CENTROID", "@rail_node", "rail_only", "EXTRA", "INTEGER32"),
            NetworkAttribute("CENTROID", "@farezone", "farezone", "EXTRA", "INTEGER32"),
            NetworkAttribute("CENTROID", "@tap_id", "tap_id", "EXTRA", "INTEGER32"),
            NetworkAttribute("LINK", "#link_id", "model_link_id", "NETWORK_FIELD", "INTEGER32"),
            NetworkAttribute("LINK", src_name="A"),
            NetworkAttribute("LINK", src_name="B"),
            NetworkAttribute("LINK", "@assignable", "assignable", "EXTRA", "INTEGER32"),
            NetworkAttribute("LINK", "@bike_link", "bike_access", "EXTRA", "INTEGER32"),
            NetworkAttribute("LINK", "@drive_link", "drive_access", "EXTRA", "INTEGER32"),
            NetworkAttribute("LINK", "@walk_link", "walk_access", "EXTRA", "INTEGER32"),
            NetworkAttribute("LINK", "@bus_only", "bus_only", "EXTRA", "INTEGER32"),
            NetworkAttribute("LINK", "#link_county", "county", "NETWORK_FIELD", "STRING"),
            NetworkAttribute("LINK", "length", "distance"),
            NetworkAttribute("LINK", "@ft", "ft", "EXTRA", "INTEGER32"),
            NetworkAttribute("LINK", "@managed", "managed", "EXTRA", "INTEGER32"),
            NetworkAttribute("LINK", "@rail_link", "rail_only", "EXTRA", "INTEGER32"),
            NetworkAttribute("LINK", "@segment_id", "segment_id", "EXTRA", "INTEGER32"),
            NetworkAttribute("LINK", "#shstgeometryid", "shstGeometryId", "NETWORK_FIELD", "STRING"),
            NetworkAttribute("LINK", "@tollbooth", "tollbooth", "EXTRA", "INTEGER32"),
            NetworkAttribute("LINK", "@tollseg", "tollseg", "EXTRA", "INTEGER32"),
            NetworkAttribute("LINK", "@transit", "transit", "EXTRA", "INTEGER32"),
            NetworkAttribute("LINK", "#cntype", "cntype", "NETWORK_FIELD", "STRING"),
            #NetworkAttribute("LINK", "@speed", "posted_speed", "EXTRA"),
            NetworkAttribute("LINK", "@lanes_ea", "lanes_EA", "EXTRA", "INTEGER32"),
            NetworkAttribute("LINK", "@lanes_am", "lanes_AM", "EXTRA", "INTEGER32"),
            NetworkAttribute("LINK", "@lanes_md", "lanes_MD", "EXTRA", "INTEGER32"),
            NetworkAttribute("LINK", "@lanes_pm", "lanes_PM", "EXTRA", "INTEGER32"),
            NetworkAttribute("LINK", "@lanes_ev", "lanes_EV", "EXTRA", "INTEGER32"),
            NetworkAttribute("LINK", "@useclass_ea", "useclass_EA", "EXTRA", "INTEGER32"),
            NetworkAttribute("LINK", "@useclass_am", "useclass_AM", "EXTRA", "INTEGER32"),
            NetworkAttribute("LINK", "@useclass_md", "useclass_MD", "EXTRA", "INTEGER32"),
            NetworkAttribute("LINK", "@useclass_pm", "useclass_PM", "EXTRA", "INTEGER32"),
            NetworkAttribute("LINK", "@useclass_ev", "useclass_EV", "EXTRA", "INTEGER32"),
            #NetworkAttribute("LINK", "length", "length", cast=lambda x: int(x) / 1000.0),
            #NetworkAttribute("LINK", "volume_delay_func", "vdf_id", "STANDARD", cast=int),
            NetworkAttribute("LINK", "_vertices", "geometry_wkt", storage_type="STANDARD", dtype="WKT_GEOMETRY"),
            NetworkAttribute("CONNECTOR", "#link_id", "model_link_id", "NETWORK_FIELD", "INTEGER32"),
            NetworkAttribute("CONNECTOR", src_name="A"),
            NetworkAttribute("CONNECTOR", src_name="B"),
            NetworkAttribute("CONNECTOR", "@assignable", "assignable", "EXTRA", "INTEGER32"),
            NetworkAttribute("CONNECTOR", "@bike_link", "bike_access", "EXTRA", "INTEGER32"),
            NetworkAttribute("CONNECTOR", "@drive_link", "drive_access", "EXTRA", "INTEGER32"),
            NetworkAttribute("CONNECTOR", "@walk_link", "walk_access", "EXTRA", "INTEGER32"),
            NetworkAttribute("CONNECTOR", "@bus_only", "bus_only", "EXTRA", "INTEGER32"),
            NetworkAttribute("CONNECTOR", "#link_county", "county", "NETWORK_FIELD", "STRING"),
            NetworkAttribute("CONNECTOR", "length", "distance"),
            NetworkAttribute("CONNECTOR", "@ft", "ft", "EXTRA", "INTEGER32"),
            NetworkAttribute("CONNECTOR", "@managed", "managed", "EXTRA", "INTEGER32"),
            NetworkAttribute("CONNECTOR", "@rail_link", "rail_only", "EXTRA", "INTEGER32"),
            NetworkAttribute("CONNECTOR", "@segment_id", "segment_id", "EXTRA", "INTEGER32"),
            NetworkAttribute("CONNECTOR", "#shstgeometryid", "shstGeometryId", "NETWORK_FIELD", "STRING"),
            NetworkAttribute("CONNECTOR", "@tollbooth", "tollbooth", "EXTRA", "INTEGER32"),
            NetworkAttribute("CONNECTOR", "@tollseg", "tollseg", "EXTRA", "INTEGER32"),
            NetworkAttribute("CONNECTOR", "@transit", "transit", "EXTRA", "INTEGER32"),
            NetworkAttribute("CONNECTOR", "#cntype", "cntype", "NETWORK_FIELD", "STRING"),
            NetworkAttribute("CONNECTOR", "@lanes_ea", "lanes_EA", "EXTRA", "INTEGER32"),
            NetworkAttribute("CONNECTOR", "@lanes_am", "lanes_AM", "EXTRA", "INTEGER32"),
            NetworkAttribute("CONNECTOR", "@lanes_md", "lanes_MD", "EXTRA", "INTEGER32"),
            NetworkAttribute("CONNECTOR", "@lanes_pm", "lanes_PM", "EXTRA", "INTEGER32"),
            NetworkAttribute("CONNECTOR", "@lanes_ev", "lanes_EV", "EXTRA", "INTEGER32"),
            NetworkAttribute("CONNECTOR", "@useclass_ea", "useclass_EA", "EXTRA", "INTEGER32"),
            NetworkAttribute("CONNECTOR", "@useclass_am", "useclass_AM", "EXTRA", "INTEGER32"),
            NetworkAttribute("CONNECTOR", "@useclass_md", "useclass_MD", "EXTRA", "INTEGER32"),
            NetworkAttribute("CONNECTOR", "@useclass_pm", "useclass_PM", "EXTRA", "INTEGER32"),
            NetworkAttribute("CONNECTOR", "@useclass_ev", "useclass_EV", "EXTRA", "INTEGER32"),
            NetworkAttribute("CONNECTOR", "_vertices", "geometry_wkt", storage_type="STANDARD", dtype="WKT_GEOMETRY"),
            NetworkAttribute("TRANSIT_LINE", "description", "route_long_name", dtype="STRING"),
            NetworkAttribute("TRANSIT_LINE", "#short_name", "route_short_name", "NETWORK_FIELD", "STRING"),
            NetworkAttribute("TRANSIT_LINE", "headway", "headway_minutes"),
            NetworkAttribute("TRANSIT_LINE", src_name="time_period"),
            NetworkAttribute("TRANSIT_LINE", src_name="line_id"),
            NetworkAttribute("TRANSIT_LINE", src_name="mode"),
            NetworkAttribute("TRANSIT_LINE", src_name="vehicle_type"),
            NetworkAttribute("TRANSIT_SEGMENT", "allow_alightings", "allow_alightings", dtype="BOOLEAN"),
            NetworkAttribute("TRANSIT_SEGMENT", "allow_boardings", "allow_boardings", dtype="BOOLEAN"),
            NetworkAttribute("TRANSIT_SEGMENT", "data1", "time_minutes"),
            #NetworkAttribute("TRANSIT_SEGMENT", "dwell_time", "dwell_time_minutes"),
            #NetworkAttribute("TRANSIT_SEGMENT", "transit_time_func", "transit_time_function", dtype="INTEGER32"),
            NetworkAttribute("TRANSIT_SEGMENT", "#stop_name", "stop_name", "NETWORK_FIELD", "STRING"),
            NetworkAttribute("TRANSIT_SEGMENT", src_name="line_id"),
            NetworkAttribute("TRANSIT_SEGMENT", src_name="node_id"),
            NetworkAttribute("TRANSIT_SEGMENT", src_name="stop_order"),
        ]
        self._time_attrs = [
            #NetworkAttribute("LINK", "@capacity", "capacity", "EXTRA"),
        ]
        self._attrs = attributes + self._time_attrs
        self._networks = []
        self._emmebank = None
        self._app = None

    def run(self):
        """Import data and prepare Emme project."""
        self.setup_project()
        centroid_table = self._model_tables["centroid_table"]
        node_table = self._model_tables["node_table"]
        connector_table = self._model_tables["connector_table"]
        link_table = self._model_tables["link_table"]
        for period in self._time_periods:
            for attr in self._time_attrs:
                attr.cast = lambda x: float(x) * period["duration"]
            proc = ProcessNetwork(period, self._attrs)
            proc.process_base_network(
                centroid_table, node_table, connector_table, link_table)
            if self._include_transit:
                line_table = self._model_tables["line_table"]
                itinerary_table = self._model_tables["itinerary_table"]
                vehicle_table = self._model_tables["vehicle_table"]
                proc.process_transit_network(
                    line_table, itinerary_table, vehicle_table, walk_speed=3.0)
            self._networks.append(proc.network)
        self.create_emmebank()
        self.save_networks()

        # Add database to Emme desktop project (if not already added)
        expected_path = _norm(self._emmebank.path)
        valid_db = [db for db in self._app.data_explorer().databases() if _norm(db.path) == expected_path]
        if valid_db:
            db = valid_db[0]
        else:
            db = self._app.data_explorer().add_database(expected_path)
        db.open()
        self._app.refresh_data()
        self._app.project.save()

        return self._emmebank

    def setup_project(self):
        """
        Start and configure Emme project.

        If there is an already existing project at this location it will be used instead.
        """
        dir_path = _dir(self._directory)
        name = self._NAME
        project_path = _norm(_join(dir_path, name, name + ".emp"))
        if not _os.path.exists(project_path):
            _app.create_project(dir_path, name)
        app = _app.start_dedicated(visible=True, user_initials="inro", project=project_path)
        app.project.name = name
        self._app = app

    def create_emmebank(self):
        """Create Emmebank file to hold network and demand data. Existing data is overwritten."""
        # Calculate required emmebank dimensions

        def calc_extra_attribute_values(network, dims=None):
            dims = dims if dims else network.element_totals
            extras = 0
            elem_name_mapping = [
                ("links", "LINK"),
                ("nodes", "NODE"),
                ("turn_entries", "TURN"),
                ("transit_lines", "TRANSIT_LINE"),
                ("transit_segments", "TRANSIT_SEGMENT")
            ]
            dims = _copy(dims)
            dims["nodes"] = dims["regular_nodes"] + dims["centroids"]
            for elem_name, name in elem_name_mapping:
                xatts = [a for a in network.attributes(name) if a.startswith("@")]
                extras += (dims[elem_name] + 2) * len(xatts) * 2
            return extras

        totals = {}
        for network in self._networks:
            for k, v in network.element_totals.items():
                totals[k] = max(totals.get(k, 1), v)
            if totals.get("extra_attribute_values"):
                totals["extra_attribute_values"] = max(
                    totals.get("extra_attribute_values"), calc_extra_attribute_values(network)
                )
            else:
                totals["extra_attribute_values"] = calc_extra_attribute_values(network)

        num_matrices = 5
        num_periods = len(self._time_periods)
        dimensions = {
            "scalar_matrices": 999,
            "destination_matrices": 999,
            "origin_matrices": 999,
            "full_matrices": num_matrices * num_periods * 2,
            "scenarios": num_periods * 2,
            "centroids": totals["centroids"],
            "regular_nodes": totals["regular_nodes"],
            "links": totals["links"],
            "turn_entries": totals["turn_entries"],
            "transit_vehicles": totals["transit_vehicles"],
            "transit_lines": totals["transit_lines"],
            "transit_segments": totals["transit_segments"],
            "extra_attribute_values": totals["extra_attribute_values"],
            "sola_analyses": 240,
            "functions": 99,
            "operators": 5000,
        }

        # Access Emmebank database, or create if it does not exist
        project_path = self._app.project.path
        project_root = _dir(project_path)
        db_root = _join(project_root, "Database")
        if not _os.path.exists(db_root):
            _os.mkdir(db_root)
        emmebank_path = _norm(_join(project_root, "Database", "emmebank"))
        print(emmebank_path)
        if _os.path.exists(emmebank_path):
            _os.remove(emmebank_path)
        emmebank = _eb.create(emmebank_path, dimensions)
        emmebank.title = self._NAME
        emmebank.coord_unit_length = 0.0001  # Meters to kilometers
        emmebank.unit_of_length = "km"
        emmebank.unit_of_cost = "$"
        emmebank.unit_of_energy = "MJ"
        emmebank.node_number_digits = 6
        emmebank.use_engineering_notation = True
        self._emmebank = emmebank

    def save_networks(self):
        """Save processed networks in Emmebank."""
        for time, network in zip(self._time_periods, self._networks):
            scen_id = time["id"]
            scenario = self._emmebank.scenario(scen_id)
            if scenario:
                self._emmebank.delete_scenario(scen_id)
            scenario = self._emmebank.create_scenario(scen_id)
            scenario.title = "Time period %s" % time["name"]
            for attr in self._attrs:
                if attr.storage_type == "EXTRA":
                    if scenario.extra_attribute(attr.name) is None:
                        scenario.create_extra_attribute(attr.network_domain, attr.name)
                elif attr.storage_type == "NETWORK_FIELD":
                    if scenario.network_field(attr.network_domain, attr.name) is None:
                        scenario.create_network_field(attr.network_domain, attr.name, attr.dtype)
            scenario.publish_network(network)

    def close(self):
        self._app.close()
        self._emmebank.dispose()


class ProcessNetwork(object):
    """Class to process and import network data from specified tables."""

    def __init__(self, time, attributes):
        """
        Initialize Python class to run setup of Emme project.

        Arguments:
            model_tables -- interface to network data (model_nodes, model_links etc.)
            time -- {"name": <>, "duration": <>} for time period, duration in hours
            attributes -- list of NetworkAttributes mapping input data fields to Emme data
                (extra attributes, network fields) with cast details as required
        """
        self._time = time["name"]
        self._time_duration = time["duration"]
        self._attrs = attributes
        self._network = _network.Network()
        self._ignore_index_errors = True

    @property
    def network(self):
        """Return the network object."""
        return self._network

    def process_base_network(self, centroid_table, node_table, connector_table, link_table):
        """Import network data from model_nodes and model_links tables."""
        zone_id_name = "N"

        network = self._network
        # Initialize network object with attributes
        for attr in self._attrs:
            if attr.name not in network.attributes(attr.network_domain) and (
                attr.storage_type != "STANDARD" or attr.name == "_vertices"
            ):
                network.create_attribute(attr.network_domain, attr.name)

        # Process nodes from model_nodes and centroids from model_centroids tables
        nodes = {}
        centroid_attrs = [attr for attr in self._attrs if attr.domain == "CENTROID"]
        for row in centroid_table:
            node = network.create_centroid(int(row[zone_id_name]))
            for attr in centroid_attrs:
                attr.set(node, row)
            nodes[node["#node_id"]] = node
        # assumes centroid IDs are in the range 1-10000
        id_generator = IDGenerator(10001, network)
        node_attrs = [attr for attr in self._attrs if attr.domain == "NODE"]
        for row in node_table:
            node = network.create_regular_node(next(id_generator))
            for attr in node_attrs:
                attr.set(node, row)
            nodes[node["#node_id"]] = node

        # Process links from model_links and connectors from model_connectors tables
        # No network restrictions, all same mode
        auto_mode = network.create_mode("AUTO", "c")
        auto_mode.description = "car"

        # NOTE: could determin list of mode IDs from attribute dictionary
        def mode_map(row):
            return {auto_mode}

        # Index errors if a link row references a node / centroid which does not exist
        links = {}
        index_errors = []
        connector_attrs = [attr for attr in self._attrs if attr.domain == "CONNECTOR"]
        for row in connector_table:
            try:
                i_node = nodes[int(row["A"])]
                j_node = nodes[int(row["B"])]
            except KeyError:
                index_errors.append("-".join([str(row["A"]), str(row["B"])]))
                continue
            link = network.create_link(i_node, j_node, mode_map(row))
            for attr in connector_attrs:
                attr.set(link, row)
            # Default values for connectors, as these do not exist in the input data
            #link["@capacity"] = 9999.0
            link["num_lanes"] = 1.0
            link["volume_delay_func"] = 1
            #link["@speed"] = 60.0
            link["length"] = 0.1
            links[link["#link_id"]] = link

        link_attrs = [attr for attr in self._attrs if attr.domain == "LINK"]
        for row in link_table:
            try:
                i_node = nodes[int(row["A"])]
                j_node = nodes[int(row["B"])]
            except KeyError:
                index_errors.append("-".join([str(row["A"]), str(row["B"])]))
                continue
            link = network.create_link(i_node, j_node, mode_map(row))
            for attr in link_attrs:
                attr.set(link, row)
            links[link["#link_id"]] = link
        # Copy link verticies to correct attribute name, if they are present
        if "_vertices" in network.attributes("LINK"):
            for link in network.links():
                link.vertices = link._vertices
            network.delete_attribute("LINK", "_vertices")

        if index_errors:
            self._link_index_errors = index_errors
            if not self._ignore_index_errors:
                raise Exception(
                    "{0} links start / end at nodes which do not exist; e.g. {1}".format(
                        len(index_errors), index_errors[0]
                    )
                )
        return

    def process_transit_network(self, line_table, itinerary_table, vehicle_table, walk_speed):
        """Import network data from vehicle_table, line_table and itinerary_table tables."""
        network = self._network

        line_attrs = [attr for attr in self._attrs if attr.domain == "TRANSIT_LINE"]
        seg_attrs = [attr for attr in self._attrs if attr.domain == "TRANSIT_SEGMENT"]

        # Add only auxilary transit mode, hard coded as "w"
        walk = network.create_mode("AUX_TRANSIT", "w")
        walk.speed = walk_speed
        for link in network.links():
            link.modes |= set([walk])
        # adding the transit modes to the network
        for vehicle_data in vehicle_table:
            mode = network.mode(vehicle_data["mode"])
            if mode is None:
                mode = network.create_mode("TRANSIT", vehicle_data["mode"])
            vehicle = network.create_transit_vehicle(vehicle_data["id"], mode.id)
            if vehicle_data.get("total_capacity"):
                vehicle.total_capacity = int(vehicle_data["total_capacity"] * self._time_duration)
            if vehicle_data.get("seated_capacity"):
                vehicle.seated_capacity = int(vehicle_data["seated_capacity"] * self._time_duration)
            if vehicle_data.get("auto_equivalent"):
                vehicle.auto_equivalent = float(vehicle_data["auto_equivalent"])

        # Get map of external node ID to node objects
        node_map = dict((n["#node_id"], n) for n in network.nodes())
        network.create_attribute("LINK", "is_connector")
        for link in network.links():
            link.is_connector = link.i_node.is_centroid or link.j_node.is_centroid
        # Group stops together by line along with sort order
        all_stops = _defaultdict(lambda: {})

        for stop in itinerary_table:
            all_stops[stop["line_id"]][stop["stop_order"]] = stop

        for line_data in line_table:
            # filter for lines from other periods or with invalid headways
            if line_data["time_period"] != self._time or line_data["headway_minutes"] > 999:
                continue

            mode = network.transit_vehicle(line_data["vehicle_type"]).mode
            # Get the sequence of stops for this line and sort by "stop_order"
            stop_data = all_stops[line_data["line_id"]]
            print(line_data['line_id'])
            #print(stop_data)
            stop_seq_iter = iter(sorted([(k, node_map[v["node_id"]]) for k, v in stop_data.items()]))
            seq_num, i_node = next(stop_seq_iter)
            node_seq = []
            node_data = {len(node_seq): stop_data[seq_num]}
            # check that links exist and network and add the required mode
            for seq_num, j_node in stop_seq_iter:
                link = network.link(i_node, j_node)
                if link is None:
                    raise Exception("No link from {} to {} on transit line {}".format(i_node, j_node, line_data["line_id"]))
                link.modes |= set([mode])
                node_seq.append(link.i_node)
                node_data[len(node_seq)] = stop_data[seq_num]
                i_node = j_node
            node_seq.append(link.j_node)
            # Create the transit line and copy the line data
            line = network.create_transit_line(line_data["line_id"], line_data["vehicle_type"], node_seq)
            for attr in line_attrs:
                attr.set(line, line_data)
            # Copy the transit segment and stop data to the network
            for i, seg in enumerate(line.segments(include_hidden=True)):
                for attr in seg_attrs:
                    attr.set(seg, node_data[i])
        network.delete_attribute("LINK", "is_connector")


class NetworkAttribute(object):
    """Container for network attribute definitions and mapping from source data to Emme network."""

    def __init__(self, domain, name=None, src_name=None, storage_type="STANDARD", dtype=None, cast=None):
        """Return new Emme network attribute with details as defined.

        Arguments:
            domain -- the network element type of the attribute.
            name -- name to use in the Emme network
            src_name -- name of the attribute in the source network data
            storage_type -- type of the network attribute, either "EXTRA", "NETWORK_FIELD", "STANDARD"
            dtype -- the data type, one of 'BOOLEAN', 'INTEGER32', 'REAL', 'STRING', "WKT_GEOMETRY"
            cast -- custom function to cast from source data to Emme data,
                    if not specified it is derived from storage_type and dtype
        """

        self.name = name
        self.src_name = src_name
        self.domain = domain
        if domain == "CENTROID":
            self.network_domain = "NODE"
        elif domain == "CONNECTOR":
            self.network_domain = "LINK"
        else:
            self.network_domain = domain
        self.storage_type = storage_type
        self.dtype = dtype
        if cast is not None:
            self.cast = cast
        elif dtype == "WKT_GEOMETRY":
            self.cast = wkt_to_vertices
        elif storage_type in ["EXTRA", "STANDARD"] and dtype is None:
            self.cast = float
        elif dtype == "REAL":
            self.cast = float
        elif dtype == "INTEGER32":
            self.cast = int
        elif dtype == "BOOLEAN":
            self.cast = bool
        else:
            self.cast = str

    def set(self, element, row):
        """Set the value for this attribute from the row of data to the Emme network element."""
        if self.name is None or self.src_name is None:
            return
        element[self.name] = self.cast(row[self.src_name])


def wkt_to_vertices(wkt_geometry):
    """Convert Well Known Text format to list of coordinate points."""
    geo_obj = _ogr.CreateGeometryFromWkt(wkt_geometry)
    return geo_obj.GetPoints()[1:-1]


class IDGenerator(object):
    """Generate available Node IDs."""

    def __init__(self, start, network):
        """Return new Emme network attribute with details as defined."""
        self._number = start
        self._network = network

    def next(self):
        """Return the next valid node ID number."""
        while True:
            if self._network.node(self._number) is None:
                break
            self._number += 1
        return self._number

    # For Python 3+ iterators must define __next__(), for python < 3 they must define next()
    def __next__(self):
        """Return the next valid node ID number."""
        return self.next()