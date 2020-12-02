import os

import numpy as np
import pandas as pd
import geopandas as gpd

from network_wrangler import RoadwayNetwork

from defaults import *
from ..parameters import Parameters
from ..roadway import ModelRoadwayNetwork
from ..data import update_df
from ..logger import WranglerLogger




def calculate_number_of_lanes(
    roadway_net=None,
    parameters=None,
    lanes_lookup_file=None,
    network_variable="lanes",
    overwrite=False,
    centroid_connect_lanes=1,
):

    """
    Computes the number of lanes using a heuristic defined in this method.

    Args:
        roadway_net (RoadwayNetwork): Network Wrangler RoadwayNetwork object
        parameters (Parameters): Lasso parameters object
        lanes_lookup_file (str): File path to lanes lookup file.
        network_variable (str): Name of the lanes variable
        overwrite (boolean): Overwrite existing values
        centroid_connect_lanes (int): Number of lanes on centroid connectors

    Returns:
        RoadwayNetwork
    """

    WranglerLogger.info(
        "Calculating Number of Lanes as network variable: '{}'".format(
            network_variable,
        )
    )

    if type(parameters) is dict:
        parameters = Parameters(**parameters)
    elif isinstance(parameters, Parameters):
        parameters = Parameters(**parameters.__dict__)
    else:
        msg = "Parameters should be a dict or instance of Parameters: found {} which is of type:{}".format(
            parameters, type(parameters)
        )
        WranglerLogger.error(msg)
        raise ValueError(msg)

    """
    Verify inputs
    """

    if not roadway_net:
        msg = "'roadway_net' is missing from the method call.".format(roadway_net)
        WranglerLogger.error(msg)
        raise ValueError(msg)

    lanes_lookup_file = (
        lanes_lookup_file if lanes_lookup_file else parameters.lanes_lookup_file
    )
    if not lanes_lookup_file:
        msg = "'lanes_lookup_file' not found in method or lasso parameters.".format(
            lanes_lookup_file
        )
        WranglerLogger.error(msg)
        raise ValueError(msg)

    centroid_connect_lanes = (
        centroid_connect_lanes
        if centroid_connect_lanes
        else parameters.centroid_connect_lanes
    )

    update_lanes = False

    if network_variable in roadway_net.links_df:
        if overwrite:
            WranglerLogger.info(
                "Overwriting existing number of lanes variable '{}' already in network".format(
                    network_variable
                )
            )
            readway_net.links_df = roadway_net.links_df.drop([network_variable], axis=1)
        else:
            WranglerLogger.info(
                "Number of lanes variable '{}' updated for some links. Returning without overwriting for those links. Calculating for other links".format(
                    network_variable
                )
            )
            update_lanes = True

    """
    Start actual process
    """
    WranglerLogger.debug("Calculating Centroid Connectors")
    calculate_centroidconnect(
        roadway_net=roadway_net,
        parameters=parameters,
        number_of_lanes=centroid_connect_lanes,
    )

    WranglerLogger.debug(
        "Computing number lanes using: {}".format(
            lanes_lookup_file,
        )
    )

    lanes_df = pd.read_csv(lanes_lookup_file)

    join_df = pd.merge(
        roadway_net.links_df,
        lanes_df,
        how="left",
        on="model_link_id",
    )

    def _set_lanes(x):
        try:
            if x.centroidconnect == 1:
                return int(centroid_connect_lanes)
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
        update_fields = [network_variable],
        overwrite = overwrite,
    )

    WranglerLogger.info(
        "Finished calculating number of lanes to: {}".format(network_variable)
    )

    return roadway_net


def calculate_assign_group_and_roadway_class(
    roadway_net=None,
    parameters=None,
    assign_group_variable_name="assign_group",
    road_class_variable_name="roadway_class",
    mrcc_roadway_class_shape=None,
    mrcc_shst_data=None,
    mrcc_roadway_class_variable_shp=None,
    mrcc_assgngrp_dict=None,
    widot_roadway_class_shape=None,
    widot_shst_data=None,
    widot_roadway_class_variable_shp=None,
    widot_assgngrp_dict=None,
    osm_assgngrp_dict=None,
    overwrite_assign_group=False,
    overwrite_roadway_class= False,
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
        mrcc_roadway_class_shape (str): File path to the MRCC route system geodatabase.
        mrcc_shst_data (str): File path to the MRCC SHST match return.
        mrcc_roadway_class_variable_shp (str): Name of the variable where MRCC route system are stored.
        mrcc_assgngrp_dict (dict): Dictionary to map MRCC route system variable to assignment group.
        widot_roadway_class_shape (str): File path to the WIDOT roadway category geodatabase.
        widot_shst_data (str): File path to the WIDOT SHST match return.
        widot_roadway_class_variable_shp (str): Name of the variable where WIDOT roadway category are stored.
        widot_assgngrp_dict (dict): Dictionary to map WIDOT roadway category variable to assignment group.
        osm_assgngrp_dict (dict): Dictionary to map OSM roadway class to assignment group.

    Returns:
        RoadwayNetwork
    """

    WranglerLogger.info(
        "Calculating Assignment Group and Roadway Class as network variables: '{}' and '{}'".format(
            assign_group_variable_name,
            road_class_variable_name,
        )
    )

    """
    Start actual process
    """

    WranglerLogger.debug("Calculating Centroid Connectors")
    calculate_centroidconnect(roadway_net=roadway_net, parameters=parameters)

    # returns shstreets dataframe with geometry ID, pp_link_id (which is the LINK_ID)

    # shstReferenceId,shstGeometryId,pp_link_id
    mrcc_shst_ref_df = pd.read_csv(mrcc_shst_data)
    WranglerLogger.debug(
        "mrcc shst ref df columns\n{}".format(mrcc_shst_ref_df.columns)
    )
    
    widot_shst_ref_df = ModelRoadwayNetwork.read_match_result(widot_shst_data)
    WranglerLogger.debug("widot shst ref df columns".format(widot_shst_ref_df.columns))
    # join MRCC geodataframe with MRCC shared street return to get MRCC route_sys and shared street geometry id
    #
    # get route_sys from MRCC
    # end up with OSM data with MRCC attributes
    join_gdf = ModelRoadwayNetwork.get_attribute(
        roadway_net.links_df,
        "shstGeometryId",
        mrcc_shst_ref_df,
        mrcc_gdf,
        mrcc_roadway_class_variable_shp,
    )

    # for exporting mrcc_id
    if "mrcc_id" in roadway_net.links_df.columns:
        join_gdf.drop(["source_link_id"], axis=1, inplace=True)
    else:
        join_gdf.rename(columns={"source_link_id": "mrcc_id"}, inplace=True)

    join_gdf = ModelRoadwayNetwork.get_attribute(
        join_gdf,
        "shstGeometryId",
        widot_shst_ref_df,
        widot_gdf,
        widot_roadway_class_variable_shp,
    )

    _initial_variables = join_gdf.columns
    join_gdf = MC_OSM_ROADWAY_ASSIGNGRP_MAPPING.apply_mapping(join_gdf)
    join_gdf = MC_MRCC_ROADWAY_ASSIGNGRP_MAPPING.apply_mapping(join_gdf)
    join_gdf = MC_WIDOT_ROADWAY_ASSIGNGRP_MAPPING.apply_mapping(join_gdf)
    
    def _set_final_assignment_group(x):
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

    join_gdf[assign_group_variable_name] = join_gdf.apply(
        lambda x: _set_final_assignment_group(x), axis=1
    )

    def _set_final_roadway_class(x):
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

    join_gdf[road_class_variable_name] = join_gdf.apply(
        lambda x: _set_final_roadway_class(x), axis=1
    )

    roadway_net.links_df = update_df(
        roadway_net.links_df,
        join_gdf, 
        "model_link_id",
        update_fields = [assign_group_variable_name,road_class_variable_name],
        overwrite = False,
    )

    WranglerLogger.info(
        "Finished calculating assignment group variable {} and roadway class variable {}".format(
            assign_group_variable_name,
            road_class_variable_name,
        )
    )

    return roadway_net



