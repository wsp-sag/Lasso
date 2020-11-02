import copy
import glob
import os

import geopandas as gpd
import pandas as pd

from geopandas import GeoDataFrame
from pandas import DataFrame
import numpy as np
import math

from .parameters import Parameters
from .logger import WranglerLogger
from network_wrangler import RoadwayNetwork


def calculate_facility_type(
    roadway_network=None,
    parameters=None,
    network_variable="ft",
    network_variable_lanes="lanes",
    facility_type_dict = None,
    overwrite:bool = False,
    update_network_variable: bool = False,
):
    """
    Calculates facility type variable.

    facility type is a lookup based on OSM roadway

    Args:
        roadway_network (RoadwayNetwork): Input Wrangler roadway network
        parameters (Parameters): Lasso parameters object
        network_variable (str): Name of the variable that should be written to.  Default to "facility_type".
        facility_type_dict (dict): Dictionary to map OSM roadway to facility type.

    Returns:
        RoadwayNetwork with facility type computed
    """

    WranglerLogger.info("Calculating Facility Type")

    """
    Verify inputs
    """
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

    if not roadway_network:
        msg = "'roadway_network' is missing from the method call."
        WranglerLogger.error(msg)
        raise ValueError(msg)

    facility_type_dict = (
        facility_type_dict
        if facility_type_dict
        else parameters.osm_facility_type_dict
    )

    if not facility_type_dict:
        msg = msg = "'facility_type_dict' not found in method or lasso parameters."
        WranglerLogger.error(msg)
        raise ValueError(msg)

    if network_variable in roadway_network.links_df:
        if overwrite:
            WranglerLogger.info(
                "Overwriting existing Variable '{}' already in network".format(
                    network_variable
                )
            )
        else:
            WranglerLogger.info(
                "Variable '{}' updated for some links. Returning without overwriting for those links. Calculating for other links".format(
                    network_variable
                )
            )
            update_network_variable = True

    """
    Start actual process
    """

    join_gdf = roadway_network.links_df.copy()

    join_gdf["oneWay"].fillna("", inplace = True)
    join_gdf["oneWay"] = join_gdf["oneWay"].apply(lambda x: "NA" if x in [None, np.nan, float('nan')] else x)
    join_gdf["oneWay"] = join_gdf["oneWay"].apply(lambda x: x if type(x) == str else ','.join(map(str, x)))
    join_gdf["oneWay_binary"] = join_gdf["oneWay"].apply(lambda x: 0 if "False" in x else 1)

    def _calculate_facility_type(x):
        # facility_type heuristics

        if x.roadway == "motorway":
            return 1

        if x.roadway == "trunk":
            if x.oneWay_binary == 1:
                return 2

        if x.roadway in ["motorway_link", "trunk_link"]:
            return 3

        if x.roadway in ["primary", "secondary", "tertiary"]:
            if x.oneWay_binary == 1:
                if x[network_variable_lanes] > 1:
                    return 4

        if x.roadway in ["trunk", "primary", "secondary", "tertiary"]:
            if x.oneWay_binary == 0:
                if x[network_variable_lanes] > 1:
                    return 5

        if x.roadway == "trunk":
            if x.oneWay_binary == 0:
                if x[network_variable_lanes] == 1:
                    return 6

        if x.roadway in ["primary", "secondary", "tertiary"]:
            if x.oneWay_binary in [0,1]:
                return 6

        if x.roadway in ["primary_link", "secondary_link", "tertiary_link"]:
            if x.oneWay_binary in [0,1]:
                return 6

        if x.roadway in ["residential", "residential_link"]:
            if x.oneWay_binary in [0,1]:
                return 7

        return 99

    join_gdf[network_variable] = join_gdf.apply(lambda x : _calculate_facility_type(x), axis = 1)

    roadway_network.links_df[network_variable + "_cal"] = join_gdf[network_variable]

    if update_network_variable:
        roadway_network.links_df[network_variable] = np.where(
                roadway_network.links_df[network_variable].notnull(),
                roadway_network.links_df[network_variable],
                roadway_network.links_df[network_variable + "_cal"]
            )
    else:
        roadway_network.links_df[network_variable] = roadway_network.links_df[network_variable + "_cal"]

    WranglerLogger.info(
        "Finished calculating roadway class variable: {}".format(network_variable)
    )

    return roadway_network


def determine_number_of_lanes(
    roadway_network=None,
    parameters=None,
    network_variable:str="lanes",
    osm_lanes_attributes:str=None,
    tam_tm2_attributes:str=None,
    sfcta_attributes:str=None,
    pems_attributes:str=None,
    tomtom_attributes:str=None,
    overwrite:bool=False,
):
    """
    Uses a series of rules to determine the number of lanes.

    Args:
        roadway_network (RoadwayNetwork): Input Wrangler roadway network
        parameters (Parameters): Lasso parameters object
        network_variable (str): Name of lanes variable
        tam_tm2_attributes (str): Transportation Authority of Marin
            (TAM) version of TM2 attributes lookup filename
        sfcta_attributes (str): San Francisco County Transportation
            Authority (SFCTA) attributes lookup filename
        pems_attributes (str): Caltrans performance monitoring
            system (PeMS) attributes lookup filename
        tomtom_attributes (str): TomTom attributes lookup filename
        overwrite (bool): True to overwrite existing variables

    Returns:
        RoadwayNetwork with number of lanes computed

    """

    WranglerLogger.info("Determining number of lanes")

    """
    Verify inputs
    """
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

    if not roadway_network:
        msg = "'roadway_network' is missing from the method call.".format(roadway_network)
        WranglerLogger.error(msg)
        raise ValueError(msg)

    osm_lanes_attributes = (
        osm_lanes_attributes
        if osm_lanes_attributes
        else parameters.osm_lanes_attributes
    )

    tam_tm2_attributes = (
        tam_tm2_attributes
        if tam_tm2_attributes
        else parameters.tam_tm2_attributes
    )

    if not tam_tm2_attributes:
        msg = "'tam_tm2_attributes' not found in method or lasso parameters."
        WranglerLogger.error(msg)
        raise ValueError(msg)

    sfcta_attributes = (
        sfcta_attributes
        if sfcta_attributes
        else parameters.sfcta_attributes
    )

    if not sfcta_attributes:
        msg = "'sfcta_attributes' not found in method or lasso parameters."
        WranglerLogger.error(msg)
        raise ValueError(msg)

    pems_attributes = (
        pems_attributes
        if pems_attributes
        else parameters.pems_attributes
    )

    if not pems_attributes:
        msg = "'pems_attributes' not found in method or lasso parameters."
        WranglerLogger.error(msg)
        raise ValueError(msg)

    tomtom_attributes = (
        tomtom_attributes
        if tomtom_attributes
        else parameters.tomtom_attributes
    )

    if not tomtom_attributes:
        msg = "'tomtom_attributes' not found in method or lasso parameters."
        WranglerLogger.error(msg)
        raise ValueError(msg)

    """
    Start actual process
    """
    osm_df = pd.read_csv(osm_lanes_attributes)
    osm_df = osm_df.rename(columns = {"min_lanes": "osm_min_lanes", "max_lanes": "osm_max_lanes"})

    tam_df = pd.read_csv(tam_tm2_attributes)
    tam_df = tam_df[['shstReferenceId', 'lanes']].rename(columns = {"lanes": "tm2_lanes"})

    sfcta_df = pd.read_csv(sfcta_attributes)
    sfcta_df = sfcta_df[['shstReferenceId', 'min_lanes', 'max_lanes']].rename(
        columns = {"min_lanes": "sfcta_min_lanes",
                    "max_lanes": "sfcta_max_lanes"}
    )

    pems_df = pd.read_csv(pems_attributes)
    pems_df = pems_df[['shstReferenceId', 'lanes']].rename(columns = {"lanes": "pems_lanes"})

    tom_df = pd.read_csv(tomtom_attributes)
    tom_df = tom_df[['shstReferenceId', 'lanes']].rename(columns = {"lanes": "tom_lanes"})

    join_gdf = pd.merge(
        roadway_network.links_df, osm_df, how = "left", on = "shstReferenceId"
    )

    join_gdf = pd.merge(
        join_gdf, tam_df, how = "left", on = "shstReferenceId"
    )

    join_gdf = pd.merge(
        join_gdf, sfcta_df, how = "left", on = "shstReferenceId"
    )

    join_gdf = pd.merge(
        join_gdf, pems_df, how = "left", on = "shstReferenceId"
    )

    join_gdf = pd.merge(
        join_gdf, tom_df, how = "left", on = "shstReferenceId"
    )

    def _determine_lanes(x):
            # heuristic 1
            if pd.notna(x.pems_lanes):
                if pd.notna(x.osm_min_lanes):
                    if x.pems_lanes == x.osm_min_lanes:
                        if x.roadway == "motorway":
                            return int(x.pems_lanes)
            # heuristic 2
            if x.county == "San Francisco":
                if pd.notna(x.sfcta_min_lanes):
                    if x.sfcta_min_lanes > 0:
                        if x.sfcta_min_lanes == x.sfcta_max_lanes:
                            if x.roadway != "motorway":
                                if x.roadway != "motorway_link":
                                    if x.osm_min_lanes >= x.sfcta_min_lanes:
                                        if x.osm_max_lanes <= x.sfcta_max_lanes:
                                            return int(x.sfcta_min_lanes)
            # heuristic 3
            if pd.notna(x.pems_lanes):
                if pd.notna(x.osm_min_lanes):
                    if x.pems_lanes >= x.osm_min_lanes:
                        if x.pems_lanes <= x.osm_max_lanes:
                            if x.roadway == "motorway":
                                return int(x.pems_lanes)
            # heuristic 4
            if x.roadway in ["motorway", "motorway_link"]:
                if pd.notna(x.osm_min_lanes):
                    if x.osm_min_lanes <= x.tom_lanes:
                        if x.osm_max_lanes >= x.tom_lanes:
                            return int(x.osm_min_lanes)
            # heuristic 5
            if x.county != "San Francisco":
                if pd.notna(x.osm_min_lanes):
                    if pd.notna(x.tm2_lanes):
                        if x.tm2_lanes > 0:
                            if x.osm_min_lanes <= x.tm2_lanes:
                                if x.osm_max_lanes >= x.tm2_lanes:
                                    return int(x.tm2_lanes)
            # heuristic 6
            if x.county == "San Francisco":
                if pd.notna(x.sfcta_min_lanes):
                    if x.sfcta_min_lanes > 0:
                        if x.sfcta_min_lanes == x.sfcta_max_lanes:
                            if x.roadway != "motorway":
                                if x.roadway != "motorway_link":
                                    return int(x.sfcta_min_lanes)
            # heuristic 7
            if x.roadway in ["motorway", "motorway_link"]:
                if pd.notna(x.osm_min_lanes):
                    if x.osm_min_lanes == x.osm_max_lanes:
                        return int(x.osm_min_lanes)
            # heuristic 8
            if x.roadway in ["motorway", "motorway_link"]:
                if pd.notna(x.osm_min_lanes):
                    if (x.osm_max_lanes - x.osm_min_lanes) == 1:
                        return int(x.osm_min_lanes)
            # heuristic 9
            if x.roadway == "motorway":
                if pd.notna(x.pems_lanes):
                    return int(x.pems_lanes)
            # heuristic 10
            if x.county == "San Francisco":
                if pd.notna(x.sfcta_min_lanes):
                    if x.sfcta_min_lanes > 0:
                        if x.roadway != "motorway":
                            if x.roadway != "motorway_link":
                                return int(x.sfcta_min_lanes)
            # heuristic 11
            if pd.notna(x.osm_min_lanes):
                if x.osm_min_lanes == x.osm_max_lanes:
                    return int(x.osm_min_lanes)
            # heuristic 12
            if pd.notna(x.osm_min_lanes):
                if x.roadway in ["motorway", "motorway_link"]:
                    if (x.osm_max_lanes - x.osm_min_lanes) >= 2:
                        return int(x.osm_min_lanes)
            # heuristic 13
            if pd.notna(x.osm_min_lanes):
                if (x.osm_max_lanes - x.osm_min_lanes) == 1:
                    return int(x.osm_min_lanes)
            # heuristic 14
            if pd.notna(x.osm_min_lanes):
                if (x.osm_max_lanes - x.osm_min_lanes) >= 2:
                    return int(x.osm_min_lanes)
            # heuristic 15
            if pd.notna(x.tm2_lanes):
                if x.tm2_lanes > 0:
                    return int(x.tm2_lanes)
            # heuristic 16
            if pd.notna(x.tom_lanes):
                if x.tom_lanes > 0:
                    return int(x.tom_lanes)
            # heuristic 17
            if x.roadway in ["residential", "service"]:
                return int(1)
            # heuristic 18
            return int(1)

    join_gdf[network_variable] = join_gdf.apply(lambda x: _determine_lanes(x), axis = 1)

    roadway_network.links_df[network_variable] = join_gdf[network_variable]

    WranglerLogger.info(
        "Finished determining number of lanes using variable: {}".format(network_variable)
    )

    return roadway_network


def calculate_assignable(
    roadway_network = None,
    parameters = None,
    network_variable: str = "assignable",
    legacy_tm2_attributes: str = None,
    overwrite:bool = False,
):
    """
    Calculates assignable variable.
    Currently just use the conflation with legacy TM2

    Args:
        roadway_network (RoadwayNetwork): Input Wrangler roadway network
        parameters (Parameters): Lasso parameters object
        network_variable (str): Variable that should be written to in the network. Default to "assignable"
        legacy_tm2_attributes (str): MTC travel mode two attributes lookup filename
        overwrite (Bool): True if overwriting existing variable in network.  Default to False.

    Returns:
        roadway object
    """

    WranglerLogger.info("Determining assignable")

    """
    Verify inputs
    """
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

    if not roadway_network:
        msg = "'roadway_network' is missing from the method call.".format(roadway_network)
        WranglerLogger.error(msg)
        raise ValueError(msg)

    if network_variable in roadway_network.links_df:
        if overwrite:
            WranglerLogger.info(
                "Overwriting existing Variable '{}' already in network".format(
                    network_variable
                )
            )
        else:
            WranglerLogger.info(
                "Variable '{}' already in network. Returning without overwriting.".format(
                    network_variable
                )
            )
            return roadway_network

    legacy_tm2_attributes = (
        legacy_tm2_attributes
        if legacy_tm2_attributes
        else parameters.legacy_tm2_attributes
    )

    if not legacy_tm2_attributes:
        msg = "'legacy_tm2_attributes' not found in method or lasso parameters."
        WranglerLogger.error(msg)
        raise ValueError(msg)

    """
    Start actual process
    """

    WranglerLogger.info(
        "Calculating and adding roadway network variable: {}".format(
            network_variable
        )
    )

    legacy_df = pd.read_csv(legacy_tm2_attributes)

    join_gdf = pd.merge(
        roadway_network.links_df,
        legacy_df[["shstReferenceId", network_variable]],
        how = "left",
        on = "shstReferenceId"
    )

    roadway_network.links_df[network_variable] = join_gdf[network_variable]

    WranglerLogger.info(
        "Finished determining assignable using variable: {}".format(network_variable)
    )

    return roadway_network


def calculate_cntype(
    roadway_network=None,
    parameters=None,
    network_variable: str = "cntype",
    overwrite:bool = False,
):
    """
    Calculates cntype variable.

    Args:
        roadway_network (RoadwayNetwork): Input Wrangler roadway network
        parameters (Parameters): Lasso parameters object
        network_variable (str): Variable that should be written to in the network. Default to "cntype"
        overwrite (Bool): True if overwriting existing variable in network.  Default to False.

    Returns:
        roadway object
    """

    WranglerLogger.info("Determining cntype")

    """
    Verify inputs
    """
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

    if not roadway_network:
        msg = "'roadway_network' is missing from the method call.".format(roadway_network)
        WranglerLogger.error(msg)
        raise ValueError(msg)

    if network_variable in roadway_network.links_df:
        if overwrite:
            WranglerLogger.info(
                "Overwriting existing Variable '{}' already in network".format(
                    network_variable
                )
            )
        else:
            WranglerLogger.info(
                "Variable '{}' already in network. Returning without overwriting.".format(
                    network_variable
                )
            )
            return roadway_network

    """
    Start actual process
    """

    WranglerLogger.info(
        "Calculating and adding roadway network variable: {}".format(
            network_variable
        )
    )

    # TODO this logic needs to be revised
    def _calculate_cntype(x):
        if x.roadway == "taz":
            return "TAZ"
        if x.roadway == "maz":
            return "MAZ"
        if x.drive_access == 1:
            return "TANA"
        elif x.walk_access == 1:
            return "PED"
        elif x.bike_access == 1:
            return "BIKE"
        elif x.rail_only == 1:
            return "CRAIL"
        else:
            return "NA"

    roadway_network.links_df[network_variable] = roadway_network.links_df.apply(lambda x: _calculate_cntype(x), axis = 1)

    WranglerLogger.info(
        "Finished determining variable: {}".format(network_variable)
    )

    return roadway_network


def calculate_transit(
    roadway_network = None,
    parameters = None,
    network_variable: str = "transit",
    overwrite:bool = False,
    update_network_variable: bool = False,
):
    """
    Calculates transit-only variable.

    Args:
        roadway_network (RoadwayNetwork): Input Wrangler roadway network
        parameters (Parameters): Lasso parameters object
        network_variable (str): Variable that should be written to in the network. Default to "transit"
        overwrite (Bool): True if overwriting existing variable in network.  Default to False.

    Returns:
        roadway object
    """

    WranglerLogger.info("Determining transit")

    """
    Verify inputs
    """
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

    if not roadway_network:
        msg = "'roadway_network' is missing from the method call.".format(roadway_network)
        WranglerLogger.error(msg)
        raise ValueError(msg)

    if network_variable in roadway_network.links_df:
        if overwrite:
            WranglerLogger.info(
                "Overwriting existing Variable '{}' already in network".format(
                    network_variable
                )
            )
        else:
            WranglerLogger.info(
                "Variable '{}' updated for some links. Returning without overwriting for those links. Calculating for other links".format(
                    network_variable
                )
            )
            update_network_variable = True

    """
    Start actual process
    """

    WranglerLogger.info(
        "Calculating and adding roadway network variable: {}".format(
            network_variable
        )
    )

    if update_network_variable:
        roadway_network.links_df[network_variable] = np.where(
            roadway_network.links_df[network_variable].notnull(),
            roadway_network.links_df[network_variable],
            0
        )

    if "bus_only" in roadway_network.links_df.columns:
        roadway_network.links_df[network_variable] = np.where(
            (roadway_network.links_df.bus_only == 1) |
                (roadway_network.links_df.rail_only == 1),
            1,
            0
        )
    else:
        roadway_network.links_df[network_variable] = np.where(
            (roadway_network.links_df.rail_only == 1),
            1,
            0
        )

    WranglerLogger.info(
        "Finished determining transit-only variable: {}".format(network_variable)
    )

    return roadway_network


def calculate_useclass(
    roadway_network = None,
    parameters = None,
    network_variable: str = "useclass",
    overwrite:bool = False,
    update_network_variable: bool = False,
):
    """
    Calculates useclass variable.
    Use value from project cards if available, if not default to 0

    Args:
        roadway_network (RoadwayNetwork): Input Wrangler roadway network
        parameters (Parameters): Lasso parameters object
        network_variable (str): Variable that should be written to in the network. Default to "useclass"
        overwrite (Bool): True if overwriting existing variable in network.  Default to False.

    Returns:
        roadway object
    """

    WranglerLogger.info("Determining useclass")

    """
    Verify inputs
    """
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

    if not roadway_network:
        msg = "'roadway_network' is missing from the method call.".format(roadway_network)
        WranglerLogger.error(msg)
        raise ValueError(msg)

    if network_variable in roadway_network.links_df:
        if overwrite:
            WranglerLogger.info(
                "Overwriting existing Variable '{}' already in network".format(
                    network_variable
                )
            )
        else:
            WranglerLogger.info(
                "Variable '{}' updated for some links. Returning without overwriting for those links. Calculating for other links".format(
                    network_variable
                )
            )
            update_network_variable = True

    """
    Start actual process
    """

    WranglerLogger.info(
        "Calculating and adding roadway network variable: {}".format(
            network_variable
        )
    )

    if update_network_variable:
        roadway_network.links_df[network_variable] = np.where(
                roadway_network.links_df[network_variable].notnull(),
                roadway_network.links_df[network_variable],
                0
            )
    else:
        roadway_network.links_df[network_variable] = 0

    WranglerLogger.info(
        "Finished determining variable: {}".format(network_variable)
    )

    return roadway_network


def add_centroid_and_centroid_connector(
    roadway_network = None,
    parameters = None,
    centroid_file: str = None,
    centroid_connector_link_file: str = None,
    centroid_connector_shape_file: str = None,
):
    """
    Add centorid and centroid connectors from pickles.

    Args:
        roadway_network (RoadwayNetwork): Input Wrangler roadway network
        parameters (Parameters): Lasso parameters object
        centroid_file (str): centroid node gdf pickle filename
        centroid_connector_link_file (str): centroid connector link pickle filename
        pems_attributes (str): centroid connector shape pickle filename

    Returns:
        roadway network object

    """

    WranglerLogger.info("Adding centroid and centroid connector to standard network")

    """
    Verify inputs
    """
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

    if not roadway_network:
        msg = "'roadway_network' is missing from the method call.".format(roadway_network)
        WranglerLogger.error(msg)
        raise ValueError(msg)

    centroid_file = (
        centroid_file
        if centroid_file
        else parameters.centroid_file
    )

    centroid_connector_link_file = (
        centroid_connector_link_file
        if centroid_connector_link_file
        else parameters.centroid_connector_link_file
    )

    if not centroid_connector_link_file:
        msg = "'centroid_connector_link_file' not found in method or lasso parameters."
        WranglerLogger.error(msg)
        raise ValueError(msg)

    centroid_connector_shape_file = (
        centroid_connector_shape_file
        if centroid_connector_shape_file
        else parameters.centroid_connector_shape_file
    )

    if not centroid_connector_shape_file:
        msg = "'centroid_connector_shape_file' not found in method or lasso parameters."
        WranglerLogger.error(msg)
        raise ValueError(msg)

    """
    Start actual process
    """

    centroid_gdf = pd.read_pickle(centroid_file)
    centroid_connector_link_gdf = pd.read_pickle(centroid_connector_link_file)
    centroid_connector_shape_gdf = pd.read_pickle(centroid_connector_shape_file)

    centroid_connector_link_gdf["lanes"] = 1
    centroid_connector_link_gdf["ft"] = 8
    centroid_connector_link_gdf["managed"] = 0

    roadway_network.nodes_df = pd.concat(
        [roadway_network.nodes_df,
        centroid_gdf[
            list(set(roadway_network.nodes_df.columns) &
            set(centroid_gdf.columns))
        ]],
        sort = False,
        ignore_index = True
    )

    roadway_network.links_df = pd.concat(
        [roadway_network.links_df,
        centroid_connector_link_gdf[
            list(set(roadway_network.links_df.columns) &
            set(centroid_connector_link_gdf.columns))
        ]],
        sort = False,
        ignore_index = True
    )

    roadway_network.shapes_df = pd.concat(
        [roadway_network.shapes_df,
        centroid_connector_shape_gdf[
            list(set(roadway_network.shapes_df.columns) &
            set(centroid_connector_shape_gdf.columns))
        ]],
        sort = False,
        ignore_index = True
    )



    WranglerLogger.info(
        "Finished adding centroid and centroid connectors"
    )

    return roadway_network

def roadway_standard_to_mtc_network(
    roadway_network = None,
    parameters = None,
    output_proj = None
):
    """
    Rename and format roadway attributes to be consistent with what mtc's model is expecting.

    Args:
        output_epsg (int): epsg number of output network.

    Returns:
        None
    """

    WranglerLogger.info(
        "Renaming roadway attributes to be consistent with what mtc's model is expecting"
    )

    """
    Verify inputs
    """

    output_proj = output_proj if output_proj else parameters.output_proj

    """
    Start actual process
    """
    if "managed" in roadway_network.links_df.columns:
        WranglerLogger.info("Creating managed lane network.")
        roadway_network.create_managed_lane_network(in_place=True)
    else:
        WranglerLogger.info("Didn't detect managed lanes in network.")

    # make managed lane access and egress dummy links assignable
    roadway_network.links_df["assignable"] = np.where(
        roadway_network.links_df["roadway"].isin(["ml_access", "ml_egress"]),
        1,
        roadway_network.links_df["assignable"]
    )

    roadway_network = calculate_cntype(roadway_network, parameters)
    roadway_network = calculate_transit(roadway_network, parameters)
    roadway_network = calculate_useclass(roadway_network, parameters)

    roadway_network.calculate_distance(overwrite = True)

    roadway_network.fill_na()
    WranglerLogger.info("Splitting variables by time period and category")
    roadway_network.split_properties_by_time_period_and_category()
    roadway_network.convert_int()

    roadway_network.links_mtc_df = roadway_network.links_df.copy()
    roadway_network.nodes_mtc_df = roadway_network.nodes_df.copy()

    roadway_network.links_mtc_df.crs = RoadwayNetwork.CRS
    roadway_network.nodes_mtc_df.crs = RoadwayNetwork.CRS
    WranglerLogger.info("Setting Coordinate Reference System to {}".format(output_proj))
    roadway_network.links_mtc_df = roadway_network.links_mtc_df.to_crs(crs = output_proj)
    roadway_network.nodes_mtc_df = roadway_network.nodes_mtc_df.to_crs(crs = output_proj)

    roadway_network.nodes_mtc_df["X"] = roadway_network.nodes_mtc_df.geometry.apply(
        lambda g: g.x
    )
    roadway_network.nodes_mtc_df["Y"] = roadway_network.nodes_mtc_df.geometry.apply(
        lambda g: g.y
    )

    # CUBE expect node id to be N
    roadway_network.nodes_mtc_df.rename(columns={"model_node_id": "N"}, inplace=True)

    return roadway_network

def route_properties_gtfs_to_cube(
    transit_network = None,
    parameters = None,
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
        "Converting GTFS Standard Properties to MTC's Cube Standard"
    )
    # TODO edit as GTFS is consumed
    mtc_operator_dict = {
        "0": 3,
        "1": 3,
        "2": 3,
        "3": 4,
        "4": 2,
        "5": 5,
        "6": 8,
        "7": 1,
        "8": 1,
        "9": 10,
        "10": 3,
        "11": 9,
        "12": 3,
        "13": 4,
        "14": 4,
        "15": 3,
    }

    shape_df = transit_network.feed.shapes.copy()
    trip_df = transit_network.feed.trips.copy()

    mode_crosswalk = pd.read_csv(parameters.mode_crosswalk_file)
    mode_crosswalk.drop_duplicates(subset = ["agency_raw_name", "route_type"], inplace = True)

    """
    Add information from: routes, frequencies, and routetype to trips_df
    """
    trip_df = pd.merge(trip_df, transit_network.feed.routes.drop("agency_raw_name", axis = 1), how="left", on="route_id")
    trip_df = pd.merge(trip_df, transit_network.feed.frequencies, how="left", on="trip_id")

    trip_df["tod"] = trip_df.start_time.apply(transit_network.time_to_cube_time_period, as_str = False)

    trip_df["route_short_name"] = trip_df["route_short_name"].str.replace("-", "_")

    trip_df["NAME"] = trip_df.apply(
        lambda x: str(x.agency_id)
        + "_"
        + str(x.route_id)
        + "_"
        + str(x.route_short_name),
        axis=1,
    )

    trip_df["LONGNAME"] = trip_df["route_long_name"]
    trip_df["HEADWAY"] = (trip_df["headway_secs"] / 60).astype(int)

    trip_df = pd.merge(
        trip_df,
        mode_crosswalk.drop("agency_id", axis = 1),
        how = "left",
        on = ["agency_raw_name", "route_type"]
    )

    trip_df['TM2_mode'].fillna(11, inplace = True)
    trip_df['TM2_mode'] = trip_df['TM2_mode'].astype(int)


    trip_df["ONEWAY"] = "T"
    trip_df["OPERATOR"] = trip_df["agency_id"].map(mtc_operator_dict)

    return trip_df

def cube_format(transit_network, row):
    """
    Creates a string represnting the route in cube line file notation.

    Args:
        row: row of a DataFrame representing a cube-formatted trip, with the Attributes
            trip_id, shape_id, NAME, LONGNAME, tod, HEADWAY, MODE, ONEWAY, OPERATOR

    Returns:
        string representation of route in cube line file notation
    """

    s = '\nLINE NAME="{}",'.format(row.NAME)
    s += '\n LONGNAME="{}",'.format(row.LONGNAME)
    s += '\n USERA1=\"%s",' % (row.agency_id,)
    s += '\n USERA2=\"%s",' % (row.TM2_line_haul_name,)
    s += "\n HEADWAY[{}]={},".format(row.tod, row.HEADWAY)
    s += "\n MODE={},".format(row.TM2_mode)
    if row.TM2_faresystem > 0:
        s += "\n FARESYSTEM={},".format(int(row.TM2_faresystem))
    s += "\n ONEWAY={},".format(row.ONEWAY)
    s += "\n OPERATOR={},".format(int(row.TM2_operator) if ~math.isnan(row.TM2_operator) else 99)
    s += '\n SHORTNAME=%s,' % (row.route_short_name,)
    s += "\n N={}".format(transit_network.shape_gtfs_to_cube(row))

    # TODO: need NNTIME, ACCESS_C

    return s

def write_as_cube_lin(
    transit_network = None,
    parameters = None,
    outpath: str  = None
    ):
    """
    Writes the gtfs feed as a cube line file after
    converting gtfs properties to MetCouncil cube properties.

    Args:
        outpath: File location for output cube line file.

    """
    if not outpath:
        outpath  = os.path.join(parameters.scratch_location,"outtransit.lin")
    trip_cube_df = route_properties_gtfs_to_cube(transit_network, parameters)

    trip_cube_df["LIN"] = trip_cube_df.apply(lambda x: cube_format(transit_network, x), axis=1)

    l = trip_cube_df["LIN"].tolist()
    l = [";;<<PT>><<LINE>>;;"] + l

    with open(outpath, "w") as f:
        f.write("\n".join(l))
