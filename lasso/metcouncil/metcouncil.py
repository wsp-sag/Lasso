import os

import numpy as np
import pandas as pd
import geopandas as gpd

from geopandas import GeoDataFrame

from network_wrangler import RoadwayNetwork
from network_wrangler import update_df

from ..parameters import Parameters, RoadwayNetworkModelParameters
from ..roadway import ModelRoadwayNetwork
from ..data import update_df
from ..logger import WranglerLogger


def calculate_number_of_lanes(
    roadway_net: ModelRoadwayNetwork,
    roadway_ps: RoadwayNetworkModelParameters = None,
    network_variable="lanes",
    update_method: str = "update if found",
):

    """
    Computes the number of lanes using a heuristic defined in this method.

    Args:
        roadway_net: Network Wrangler RoadwayNetwork object
        roadway_ps: RoadwayNetworkModel Parameters class, defaults to one associated with roadway network. 
        lanes_lookup_file: File path to lanes lookup file. Defaults to file set in 
        network_variable: Name of the lanes variable
        update_method: update_method: update method to use in network_wrangler.update_df. One of "overwrite all", 
            "update if found", or "update nan". Defaults to "update if found"

    Returns:
        ModelRoadwayNetwork
    """

    if not roadway_ps: roadway_ps = roadway_net.parameters.roadway_network_ps

    msg = "Calculatung # of lanes using roadway_value_lookups['lanes'] = {}".format(
        roadway_ps.roadway_value_lookups['lanes'],
    )
    WranglerLogger.info(msg)

    join_df = roadway_ps.roadway_value_lookups['lanes'].apply_mapping(roadway_net.links_df)

    def _set_lanes(x):
        try:
            if x.centroidconnect == roadway_ps.centroid_connector_properties['lanes']:
                return int(1)
            elif max([x.anoka, x.hennepin, x.carver, x.dakota, x.washington]) > 0:
                return int(max([x.anoka, x.hennepin, x.carver, x.dakota, x.washington]))
            elif max([x.widot, x.mndot]) > 0:
                return int(max([x.widot, x.mndot]))
            elif x.osm_min > 0:
                return int(x.osm_min)
            elif x.naive > 0:
                return int(x.naive)
        except:
            return int(0)

    join_df[network_variable] = join_df.apply(lambda x: _set_lanes(x), axis=1)

    roadway_net.links_df = update_df(
        roadway_net.links_df,
        join_df,
        "model_link_id",
        update_fields=[network_variable],
        method=update_method,
    )

    WranglerLogger.info(
        "Finished calculating number of lanes to: {}".format(network_variable)
    )

    return roadway_net

def _set_final_assignment_group(x):
    """

    Args:
        x: row in link dataframe
    """
    try:
        if x.centroidconnect == 1:
            return 9
        elif x.bus_only == 1:
            return 98
        elif x.rail_only == 1:
            return 100
        elif x.drive_access == 0:
            return 101
        elif x.assignment_group_mrcc > 0:
            return int(x.assignment_group_mrcc)
        elif x.assignment_group_widot > 0:
            return int(x.assignment_group_widot)
        else:
            return int(x.assignment_group_osm)
    except:
        return 0

def _set_final_roadway_class(x):
    """

    Args:
        x: row in link dataframe
    """
    try:
        if x.centroidconnect == 1:
            return 99
        elif x.bus_only == 1:
            return 50
        elif x.rail_only == 1:
            return 100
        elif x.drive_access == 0:
            return 101
        elif x.roadway_class_mrcc > 0:
            return int(x.roadway_class_mrcc)
        elif x.roadway_class_widot > 0:
            return int(x.roadway_class_widot)
        else:
            return int(x.roadway_class_osm)
    except:
        return 0

def _calculate_mrcc_route_sys(
    links_df: GeoDataFrame, 
    roadway_ps: RoadwayNetworkModelParameters,
    update_method: str = "update if found",
):
    """
    Get MRCC route_sys variable from shstGeometryID.
    1. shstGeometryId --> LINK_ID
    2. LINK_ID --> route_sys

    Args:
        links_df: links GeoDataFrame
        roadway_ps: RoadwayNetworkModelParameters set.
        update_method: update method to use in network_wrangler.update_df. One of "overwrite all", 
            "update if found", or "update nan". Defaults to "update if found"

    Returns:
        link_df with route_sys column added
    """

    # 1. shstGeometryId --> LINK_ID
    # Expected columns shstReferenceId,shstGeometryId,pp_link_id (which is the LINK_ID),score
    _join_df  = roadway_ps.roadway_value_lookups["mrcc_shst_2_pp_link_id"].apply_mapping(links_df)

    # LINK_ID --> route_sys
    # Expected columns  LINK_ID, route_sys
    _join_df = roadway_ps.roadway_value_lookups["pp_link_id_2_route_sys"].apply_mapping(_join_df)
    
    # drop duplicated records with same field value
    _join_df.drop_duplicates(
        subset=["model_link_id", "shstGeometryId", "mrcc_route_sys"], inplace=True
    )

    # more than one match, take the best score
    _join_df.sort_values(
        by=["model_link_id", "mrcc_score"],
        ascending=True,
        na_position="first",
        inplace=True,
    )

    _join_df.drop_duplicates(
        subset=["model_link_id"], keep="last", inplace=True
    )

    links_route_sys_df = update_df(
        links_df,
        _join_df,
        "model_link_id",
        update_fields=["mrcc_route_sys"],
        method = update_method,
    )

    return links_route_sys_df

def _calculate_widot_rdwy_ctgy(
    links_df: GeoDataFrame, 
    roadway_ps: RoadwayNetworkModelParameters,
    update_method: str = "update if found",
):
    """
    Get WiDot rdwy ctgy from shstGeometryID.
    1. shstGeometryId --> LINK_ID
    2. LINK_ID --> rdwy_ctgy

    links_df: links GeoDataFrame
        roadway_ps: RoadwayNetworkModelParameters set.
        update_method: update method to use in network_wrangler.update_df. One of "overwrite all", 
        "update if found", or "update nan". Defaults to "update if found"

    Returns:
        link_df with rdwy_ctgy column added
    """

     # 1. shstGeometryId --> LINK_ID
    # Expected columns shstReferenceId,shstGeometryId,pp_link_id (which is the LINK_ID),score
    _join_df  = roadway_ps.roadway_value_lookups["widot_shst_2_link_id"].apply_mapping(links_df)

    # LINK_ID --> route_sys
    # Expected columns  LINK_ID, rdwy_ctgy
    _join_df = roadway_ps.roadway_value_lookups["widot_id_2_rdwy_ctgy"].apply_mapping(_join_df)

    # drop duplicated records with same field value
    _join_df.drop_duplicates(
        subset=["model_link_id", "shstGeometryId", "widot_rdwy_ctgy"], inplace=True
    )

    # more than one match, take the best score
    _join_df.sort_values(
        by=["model_link_id", "widot_score"],
        ascending=True,
        na_position="first",
        inplace=True,
    )

    _join_df.drop_duplicates(
        subset=["model_link_id"], keep="last", inplace=True
    )

    links_rdwy_ctgy_df = update_df(
        links_df,
        _join_df,
        "model_link_id",
        update_fields=["widot_rdwy_ctgy"],
        method = update_method,
    )

    return links_rdwy_ctgy_df

def add_id_field_to_shapefile(shapefile_filename, fieldname):
    gdf = gpd.read_file(shapefile_filename)
    if fieldname in gdf.columns:
        raise ValueError("Field {} already a column in shapefile".format(fieldname))
    gdf[fieldname]= range(1, 1 + len(gdf))
    outfile_filename = "{0}_{2}{1}".format(*os.path.splitext(shapefile_filename) + ("with_id",))
    gdf.to_file(outfile_filename)
    return outfile_filename

def calculate_assign_group_and_roadway_class(
    roadway_net: ModelRoadwayNetwork,
    roadway_ps: RoadwayNetworkModelParameters = None,
    assign_group_variable_name="assign_group",
    road_class_variable_name="roadway_class",
    update_method = "update if found",
):
    """
    Calculates assignment group and roadway class variables.

    Assignment Group is used in MetCouncil's traffic assignment to segment the volume/delay curves.
    Original source is from the MRCC data for the Minnesota: "route system" which is a roadway class
    For Wisconsin, it is from the Wisconsin DOT database, which has a variable called "roadway category"

    There is a crosswalk between the MRCC Route System and Wisconsin DOT --> Met Council Assignment group

    This method joins the network with mrcc and widot roadway data by shst js matcher returns

    Args:
        roadway_net (RoadwayNetwork): A Network Wrangler Roadway Network.
        parameters (Parameters): A Lasso Parameters, which stores input files.
        assign_group_variable_name (str): Name of the variable assignment group should be written to.  Default to "assign_group".
        road_class_variable_name (str): Name of the variable roadway class should be written to. Default to "roadway_class".
        update_method: update method to use in network_wrangler.update_df. One of "overwrite all", 
        "update if found", or "update nan". Defaults to "update if found"
    Returns:
        RoadwayNetwork
    """
    if not roadway_ps: roadway_ps = roadway_net.parameters.roadway_network_ps

    WranglerLogger.info(
        "Calculating Assignment Group and Roadway Class as network variables: '{}' and '{}'".format(
            assign_group_variable_name,
            road_class_variable_name,
        )
    )

    """
    Start actual process
    """
    # Get roadway category variables from ShSt spatial joins from MnDOT MRCC network and WiDOT
    links_route_sys_df = _calculate_mrcc_route_sys(roadway_net.links_df,roadway_ps)
    _join_df = _calculate_widot_rdwy_ctgy(links_route_sys_df,roadway_ps)

    # Get initial assignment group and roadway class from lookup tables starting with OSM and overlaying with MnDOT and then WiDOT
    _join_df = roadway_ps.roadway_value_lookups["osm_roadway_assigngrp_mapping"].apply_mapping(_join_df)
    _join_df = roadway_ps.roadway_value_lookups["mrcc_roadway_assigngrp_mapping"].apply_mapping(_join_df)
    _join_df = roadway_ps.roadway_value_lookups["widot_roadway_assigngrp_mapping"].apply_mapping(_join_df)

    # Apply more sophisticated rules for final variable calculation
    _join_df[assign_group_variable_name] = _join_df.apply(
        lambda x: _set_final_assignment_group(x), axis=1
    )
    _join_df[road_class_variable_name] = _join_df.apply(
        lambda x: _set_final_roadway_class(x), axis=1
    )

    # Update roadway class and assignment group variables in the roadway network 
    roadway_net.links_df = update_df(
        roadway_net.links_df,
        _join_df,
        "model_link_id",
        update_fields=[assign_group_variable_name, road_class_variable_name],
        method = update_method,
    )

    WranglerLogger.info(
        "Finished calculating assignment group variable {} and roadway class variable {}".format(
            assign_group_variable_name,
            road_class_variable_name,
        )
    )

    return roadway_net

def calculate_mpo(
    road_net,
    county_network_variable="county",
    network_variable="mpo",
    as_integer=True,
    mpo_counties=None,
    update_method: str = "update if found"
):
    """
    Calculates mpo variable.

    Args:
        county_variable (str): Name of the variable where the county names are stored.  Default to "county".
        network_variable (str): Name of the variable that should be written to.  Default to "mpo".
        as_integer (bool): If true, will convert true/false to 1/0s.
        mpo_counties (list): List of county names that are within mpo region.
        update_method: update method to use in network_wrangler.update_df. One of "overwrite all", 
            "update if found", or "update nan". Defaults to "update if found"

    Returns:
        None
    """

    WranglerLogger.info(
        "Calculating MPO as roadway network variable: {}".format(network_variable)
    )
    """
    Verify inputs
    """
    county_network_variable = (
        county_network_variable
        if county_network_variable
        else road_net.parameters.county_network_variable
    )

    if not county_network_variable:
        msg = "No variable specified as containing 'county' in the network."
        WranglerLogger.error(msg)
        raise ValueError(msg)
    if county_network_variable not in road_net.links_df.columns:
        msg = "Specified county network variable: {} does not exist in network. Try running or debuging county calculation."
        WranglerLogger.error(msg)
        raise ValueError(msg)

    mpo_counties = mpo_counties if mpo_counties else road_net.parameters.mpo_counties

    if not mpo_counties:
        msg = "No MPO Counties specified in method call or in parameters."
        WranglerLogger.error(msg)
        raise ValueError(msg)

    WranglerLogger.debug("MPO Counties: {}".format(",".join(str(mpo_counties))))

    """
    Start actual process
    """

    mpo = road_net.links_df[county_network_variable].isin(mpo_counties)

    if as_integer:
        mpo = mpo.astype(int)

    road_net.links_df[network_variable] = mpo

    WranglerLogger.info(
        "Finished calculating MPO variable: {}".format(network_variable)
    )

def add_met_council_calculated_roadway_variables(roadway_net: lasso.ModelRoadwayNetwork):
    """
    Creates calculated roadway variables.

    Args:
        roadway_net: input roadway network to add variables to

    Returns: roadway network with added variables. 
    """
    WranglerLogger.info("Creating metcouncil calculated roadway variables.")

    roadway_net = calculate_area_type(roadway_net)
    roadway_net = calculate_county_mpo(roadway_net)

    self.add_counts()
    self.create_ML_variable()
    self.create_hov_corridor_variable()
    self.create_managed_variable()

    return roadway_net

def calculate_area_type(roadway_net: ModelRoadwayNetwork, roadway_ps: RoadwayNetworkModelParameters= None):
    """
    Add area type values to roadway network.
    Uses the RoadwayNetworkModelParameters:
    - roadway_overlays["area_type"]
    - roadway_value_lookups["area_type_codes_dict"]
    - roadway_value_lookups["mc_mpo_counties_dict"]

    Args:
        roadway_net: the input ModelRoadwayNetwork.
        roadway_ps: overrides roadway_ps from roadway_net

    Returns: ModelRoadwayNetwork
    """
    roadway_ps = roadway_net.parameters.roadway_network_ps if roadway_ps is None else roadway_ps

    roadway_net.links_df["area_type_name"] = roadway_net.add_polygon_overlay_to_links(
        roadway_ps.roadway_overlays["area_type"],
        method = "link_centroid",
    )

    roadway_net.links_df["area_type_name"] = roadway_net.add_polygon_overlay_to_links(
        roadway_ps.roadway_overlays["downtown_area"],
        method = "link_centroid",
        fill_value = "downtown"
    )

    roadway_net.links_df["area_type"] = (
        roadway_net.links_df["area_type_name"]
        .map(roadway_ps.roadway_value_lookups["area_type_codes_dict"]
        .fillna(1)
        .astype(int)
    )

def calculate_county_mpo(roadway_net: ModelRoadwayNetwork, roadway_ps: RoadwayNetworkModelParameters= None):
    """
    Add County and MPO variables to roadway network.
    Uses the RoadwayNetworkModelParameters:
    - roadway_overlays["counties"]
    - roadway_value_lookups["county_codes_dict"]
    - roadway_value_lookups["mc_mpo_counties_dict"]

    Args:
        roadway_net: the input ModelRoadwayNetwork.
        roadway_ps: overrides roadway_ps from roadway_net

    Returns: ModelRoadwayNetwork
    """
    roadway_ps = roadway_net.parameters.roadway_network_ps if roadway_ps is None else roadway_ps

    roadway_net.links_df["county_name"] = roadway_net.add_polygon_overlay_to_links(
        roadway_ps.roadway_overlays["counties"],
        method = "link_centroid",
    )

    roadway_net.links_df["county"] = (
        roadway_net.links_df["county_name"]
        .map(roadway_ps.roadway_value_lookups["county_codes_dict"]
        .fillna(10)
        .astype(int)
    )

    roadway_net.links_df["mpo"] = (
        roadway_net.links_df["county_name"]
        .map(roadway_ps.roadway_value_lookups["mc_mpo_counties_dict"]
        .fillna(0)
        .astype(int)
    )
    
    return roadway_net


def roadway_standard_to_met_council_network(self, output_epsg=None):
    """
    Rename and format roadway attributes to be consistent with what metcouncil's model is expecting.

    Args:
        output_epsg (int): epsg number of output network.

    Returns:
        None
    """

    WranglerLogger.info(
        "Renaming roadway attributes to be consistent with what metcouncil's model is expecting"
    )

    """
    Verify inputs
    """

    output_epsg = output_epsg if output_epsg else self.parameters.output_epsg

    """
    Start actual process
    """
    if "managed" in self.links_df.columns:
        WranglerLogger.info("Creating managed lane network.")
        self.create_managed_lane_network(in_place=True)

        # when ML and assign_group projects are applied together, assign_group is filled as "" by wrangler for ML links
        for c in ModelRoadwayNetwork.CALCULATED_VALUES:
            if c in self.links_df.columns and c in self.parameters.int_col:
                self.links_df[c] = self.links_df[c].replace("", 0)
    else:
        WranglerLogger.info("Didn't detect managed lanes in network.")

    self.calculate_centroidconnect(self.parameters)
    self.create_calculated_variables()
    self.calculate_distance(overwrite=True)
    self.coerce_types()
    # no method to calculate price yet, will be hard coded in project card
    WranglerLogger.info("Splitting variables by time period and category")
    self.split_properties_by_time_period_and_category()
    

    self.links_metcouncil_df = self.links_df.copy()
    self.nodes_metcouncil_df = self.nodes_df.copy()

    self.links_metcouncil_df = pd.merge(
        self.links_metcouncil_df.drop(
            "geometry", axis=1
        ),  # drop the stick geometry in links_df
        self.shapes_df[["shape_id", "geometry"]],
        how="left",
        on="shape_id",
    )

    self.links_metcouncil_df.crs = "EPSG:4326"
    self.nodes_metcouncil_df.crs = "EPSG:4326"
    WranglerLogger.info("Setting Coordinate Reference System to EPSG 26915")
    self.links_metcouncil_df = self.links_metcouncil_df.to_crs(epsg=26915)
    self.nodes_metcouncil_df = self.nodes_metcouncil_df.to_crs(epsg=26915)

    self.nodes_metcouncil_df["X"] = self.nodes_metcouncil_df.geometry.apply(
        lambda g: g.x
    )
    self.nodes_metcouncil_df["Y"] = self.nodes_metcouncil_df.geometry.apply(
        lambda g: g.y
    )

    # CUBE expect node id to be N
    self.nodes_metcouncil_df.rename(columns={"model_node_id": "N"}, inplace=True)
