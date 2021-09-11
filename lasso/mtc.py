import copy
import glob
import os

import geopandas as gpd
import pandas as pd

from geopandas import GeoDataFrame
from pandas import DataFrame
import numpy as np
import math
from scipy.spatial import cKDTree
from sklearn.cluster import KMeans
from pyproj import CRS
from shapely.geometry import Point, LineString

from .parameters import Parameters
from .logger import WranglerLogger
from network_wrangler import RoadwayNetwork
from .util import geodesic_point_buffer, create_locationreference


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

        if x.roadway in ["taz", "maz", "tap"]:
            return 8

        if x.roadway in ["ml_access", "ml_egress"]:
            return 8

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
    assignable_analysis: str = None,
    overwrite:bool = False,
    use_assignable_analysis: bool = True,
    update_network_variable: bool = False,
):
    """
    Calculates assignable variable.
    Currently just use the conflation with legacy TM2

    Args:
        roadway_network (RoadwayNetwork): Input Wrangler roadway network
        parameters (Parameters): Lasso parameters object
        network_variable (str): Variable that should be written to in the network. Default to "assignable"
        legacy_tm2_attributes (str): MTC travel mode two attributes lookup filename
        assignable_analysis (str): assignable lookup filename
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

    assignable_analysis = (
        assignable_analysis
        if assignable_analysis
        else parameters.assignable_analysis
    )

    if not assignable_analysis:
        msg = "'assignable_analysis' not found in method or lasso parameters, will use TM2 legacy."
        WranglerLogger.warning(msg)
        use_assignable_analysis = False

    legacy_tm2_attributes = (
        legacy_tm2_attributes
        if legacy_tm2_attributes
        else parameters.legacy_tm2_attributes
    )

    if not legacy_tm2_attributes:
        msg = "'legacy_tm2_attributes' not found in method or lasso parameters."
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

    if use_assignable_analysis:
        assignable_df = pd.read_csv(assignable_analysis)

        join_gdf = pd.merge(
            roadway_network.links_df,
            assignable_df[["A", "B", network_variable]],
            how = "left",
            on = ["A", "B"]
        )
    else:
        legacy_df = pd.read_csv(legacy_tm2_attributes)

        join_gdf = pd.merge(
            roadway_network.links_df,
            legacy_df[["shstReferenceId", network_variable]],
            how = "left",
            on = "shstReferenceId"
        )

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
        if x.roadway == "tap":
            return "TAP"
        if x.bus_only == 1:
            return "TANA"
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

def calculate_farezone(
    roadway_network=None,
    transit_network=None,
    parameters=None,
    network_variable: str = "farezone",
    overwrite:bool = False,
    update_network_variable: bool = False,
    use_old: bool = False,
):
    """
    Calculates farezone variable.

    Args:
        roadway_network (RoadwayNetwork): Input Wrangler roadway network
        transit_network (CubeTransit): Input Wrangler transit network
        parameters (Parameters): Lasso parameters object
        network_variable (str): Variable that should be written to in the network. Default to "farezone"
        overwrite (Bool): True if overwriting existing variable in network.  Default to False.

    Returns:
        roadway object
    """

    WranglerLogger.info("Determining farezone")

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

    if not transit_network:
        msg = "'transit_network' is missing from the method call.".format(transit_network)
        WranglerLogger.error(msg)
        raise ValueError(msg)

    if network_variable in roadway_network.nodes_df:
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

    if use_old:

        stop_nodes_df = list(map(int, transit_network.feed.stops.model_node_id.tolist()))

        def _calculate_farezone(x):
            if x.model_node_id in stop_nodes_df:
                return 1

        roadway_network.nodes_df[network_variable] = roadway_network.nodes_df.apply(lambda x: _calculate_farezone(x), axis = 1)

        return roadway_network

    # get the agency names for each stop
    stops_df = transit_network.feed.stops.copy()

    # get the fare rules table for zonal fare agencies
    fare_rules_df = transit_network.feed.fare_rules.copy()
    zonal_fare_df = fare_rules_df[
        ((fare_rules_df.origin_id.notnull()) | (fare_rules_df.origin_id.notnull())) &
        (fare_rules_df.origin_id != " ")].copy()

    stop_with_zone_id_df = stops_df[
        (stops_df.zone_id.notnull()) &
        (stops_df.agency_raw_name.isin(zonal_fare_df.agency_raw_name.unique()))
    ].copy()

    model_node_zone_df = stop_with_zone_id_df.groupby(
        ["model_node_id", "agency_raw_name", "zone_id"]
    ).count().reset_index()[
        ["model_node_id", "agency_raw_name", "zone_id"]
    ]

    # most stop nodes serve only one agency, but some serve multiple
    unique_model_node_zone_df = pd.DataFrame(
        {"model_node_id" : model_node_zone_df.model_node_id.value_counts().keys(),
         "count" : model_node_zone_df.model_node_id.value_counts().values}
    )

    unique_model_node_zone_df = unique_model_node_zone_df[unique_model_node_zone_df["count"] == 1].copy()

    unique_model_node_zone_df = pd.merge(
        unique_model_node_zone_df.drop(["count"], axis = 1),
        model_node_zone_df,
        how = "left",
        on = "model_node_id"
    )

    unique_zone_df = unique_model_node_zone_df.groupby(
        ["agency_raw_name", "zone_id"]
    ).count().reset_index()[["agency_raw_name", "zone_id"]]

    unique_zone_df[network_variable] = range(1, 1+len(unique_zone_df))

    unique_model_node_zone_df = pd.merge(
        unique_model_node_zone_df,
        unique_zone_df,
        how = "left",
        on = ["agency_raw_name", "zone_id"]
    )

    non_unique_model_node_zone_df = model_node_zone_df[
        ~model_node_zone_df.model_node_id.isin(
            unique_model_node_zone_df.model_node_id
            )
        ].copy()

    non_unique_model_node_zone_df["agencies"] = non_unique_model_node_zone_df.groupby(
        ["model_node_id"]
    )['agency_raw_name'].transform(lambda x:','.join(x))

    non_unique_model_node_zone_df["zone_ids"] = non_unique_model_node_zone_df.groupby(
        ["model_node_id"]
    )['zone_id'].transform(lambda x:','.join(x))

    new_zone_combinations_df = non_unique_model_node_zone_df.groupby(
        ["agencies", "zone_ids"]).count().reset_index()[
        ["agencies", "zone_ids"]
    ]

    new_zone_combinations_df['farezone'] = range(
        unique_model_node_zone_df['farezone'].max() + 1,
        unique_model_node_zone_df['farezone'].max() + 1 + len(new_zone_combinations_df)
    )

    non_unique_model_node_zone_df = pd.merge(
        non_unique_model_node_zone_df,
        new_zone_combinations_df,
        how ='left',
        on = ['agencies', 'zone_ids']
    )

    final_model_node_zone_df = pd.concat(
        [unique_model_node_zone_df, non_unique_model_node_zone_df],
        sort = False,
        ignore_index = True
    )

    final_model_node_zone_df = final_model_node_zone_df.drop_duplicates(
        subset = ["model_node_id", network_variable]
    )

    final_model_node_zone_df["model_node_id"] = final_model_node_zone_df["model_node_id"].astype(int)

    roadway_network.nodes_df = pd.merge(
        roadway_network.nodes_df,
        final_model_node_zone_df[["model_node_id", network_variable]].rename(
            columns = {network_variable : network_variable + "_cal"}
        ),
        how = "left",
        on = "model_node_id"
    )

    if update_network_variable:
        roadway_network.nodes_df[network_variable] = np.where(
                roadway_network.nodes_df[network_variable].notnull(),
                roadway_network.nodes_df[network_variable],
                roadway_network.nodes_df[network_variable + "_cal"]
        )
    else:
        roadway_network.nodes_df[network_variable] = roadway_network.nodes_df[network_variable + "_cal"]

    roadway_network.nodes_df.drop(network_variable + "_cal", axis = 1, inplace = True)

    WranglerLogger.info(
        "Finished determining variable: {}".format(network_variable)
    )

    return roadway_network

def write_cube_fare_files(
    roadway_network=None,
    transit_network=None,
    parameters=None,
    outpath: str = None,
):
    """
    create cube transit files
    """

    if not outpath:
        outpath  = os.path.join(parameters.scratch_location)

    # read fare_attributes and fare_rules
    # TODO debug partridge i/o fare as part of transit object

    fare_attributes_df = transit_network.feed.fare_attributes.copy()
    fare_rules_df = transit_network.feed.fare_rules.copy()

    # deflate 2015 fare to 2010 dollars
    fare_attributes_df["price"] = fare_attributes_df["price"] * parameters.fare_2015_to_2010_deflator
    fare_attributes_df["price"] = fare_attributes_df["price"].round(2)

    fare_df = pd.merge(
        fare_attributes_df,
        fare_rules_df,
        how = "outer",
        on = ["fare_id", "agency_raw_name"])

    zonal_fare_df = fare_df[((fare_df.origin_id.notnull()) | (fare_df.destination_id.notnull())) & (fare_df.origin_id != " ")].copy()
    flat_fare_df = fare_df[~(((fare_df.origin_id.notnull()) | (fare_df.destination_id.notnull())) & (fare_df.origin_id != " "))].copy()

    # get the agency names for each stop
    stops_df = transit_network.feed.stops.copy()

    stop_with_zone_id_df = stops_df[stops_df.zone_id.notnull()].copy()

    model_node_zone_df = stop_with_zone_id_df.groupby(
        ["model_node_id", "agency_raw_name", "zone_id"]
    ).count().reset_index()[
        ["model_node_id", "agency_raw_name", "zone_id"]
    ]

    model_node_zone_df["model_node_id"] = model_node_zone_df["model_node_id"].astype(int)

    final_zone_farezone_df = pd.merge(
        model_node_zone_df,
        roadway_network.nodes_df[["model_node_id", "farezone"]],
        how = "left",
        on = "model_node_id"
    )

    final_zone_farezone_df = final_zone_farezone_df.drop_duplicates(subset = ["agency_raw_name", "zone_id", "farezone"])

    zonal_fare_df = pd.merge(
        zonal_fare_df,
        final_zone_farezone_df[["agency_raw_name", "zone_id", "farezone"]].rename(
            columns = {"farezone" : "origin_farezone", "zone_id" : "origin_id"}
        ),
        how = "left",
        on = ["origin_id", "agency_raw_name"]
    )

    zonal_fare_df = pd.merge(
        zonal_fare_df,
        final_zone_farezone_df[["agency_raw_name", "zone_id", "farezone"]].rename(
            columns = {"farezone" : "destination_farezone", "zone_id" : "destination_id"}
        ),
        how = "left",
        on = ["destination_id", "agency_raw_name"]
    )

    zonal_fare_df = zonal_fare_df[
        (zonal_fare_df.origin_farezone.notnull()) &
        (zonal_fare_df.destination_farezone.notnull())]

    zonal_fare_system_df = pd.DataFrame(
        {"agency_raw_name" : zonal_fare_df.agency_raw_name.unique(),
         "faresystem" : range(1, 1+len(zonal_fare_df.agency_raw_name.unique()))}
    )

    zonal_fare_df = pd.merge(
        zonal_fare_df,
        zonal_fare_system_df,
        how = "left",
        on = "agency_raw_name"
    )

    flat_fare_system_df = flat_fare_df.groupby(["agency_raw_name", "fare_id", "price"])['transfers'].count().reset_index().drop('transfers', axis = 1)
    flat_fare_system_df["faresystem"] = range(
        zonal_fare_system_df.faresystem.max() + 1,
        zonal_fare_system_df.faresystem.max() + 1 + len(flat_fare_system_df)
    )

    flat_fare_df = pd.merge(
        flat_fare_df,
        flat_fare_system_df[["agency_raw_name", "fare_id", "faresystem"]],
        how = "left",
        on = ["agency_raw_name", "fare_id"]
    )

    flat_fare_df.drop_duplicates(["route_id", "agency_raw_name"], inplace = True)
    flat_fare_df["route_id"] = flat_fare_df["route_id"].fillna(0).astype(int).astype(str)

    transfer_df = pd.read_csv(os.path.join(outpath, "transfer.csv"))
    transfer_df.drop_duplicates(inplace = True)
    # write out fare system file
    fare_file = os.path.join(outpath, "fares.far")
    far = cube_fare_format(zonal_fare_system_df, flat_fare_system_df, transfer_df)
    with open(fare_file, "w") as f:
        f.write(far)

    # write out fare matrix file
    fare_matrix_file = os.path.join(outpath, "fareMatrix.txt")
    write_fare_matrix(zonal_fare_df, fare_matrix_file, parameters)

    # write out faresystem - route crosswalk
    faresystem_crosswalk_file = os.path.join(outpath, "faresystem_crosswalk.txt")
    faresystem_crosswalk_df = pd.concat(
        [zonal_fare_system_df,
        flat_fare_df[["agency_raw_name", "route_id", "route_id_original", "faresystem"]]],
        sort = False,
        ignore_index = True
    )
    faresystem_crosswalk_df.to_csv(
        faresystem_crosswalk_file,
        index = False
    )

def cube_fare_format(zonal_fare_system_df, flat_fare_system_df, transfer = DataFrame()):
    """
    Create a .far file
    """
    fare_system_df = pd.concat(
        [zonal_fare_system_df, flat_fare_system_df],
        sort = False,
        ignore_index = True
    )

    transfer_fare_df = DataFrame(
        {
        "fromfs" : np.repeat(range(1, len(fare_system_df) + 1), len(fare_system_df)),
        "tofs" : np.tile(range(1, len(fare_system_df) + 1), len(fare_system_df)),
        }
    )

    transfer_fare_df = pd.merge(transfer_fare_df, transfer, how = "left", on = ["fromfs", "tofs"])

    for i in range(len(zonal_fare_system_df)):
        row = zonal_fare_system_df.iloc[i]
        tofs = row["faresystem"]
        transfer_fare_df.loc[(transfer_fare_df["tofs"] == tofs) & (transfer_fare_df["price"].isnull()), "price"] = 0

    for i in range(len(flat_fare_system_df)):
        row = flat_fare_system_df.iloc[i]
        tofs = row["faresystem"]
        transfer_fare_df.loc[(transfer_fare_df["tofs"] == tofs) & (transfer_fare_df["price"].isnull()), "price"] = row["price"]

    transfer_df = pd.pivot_table(transfer_fare_df, values=["price"], index=["tofs"], columns="fromfs", dropna = False)

    far = ""
    for i in range(len(zonal_fare_system_df)):
        row = zonal_fare_system_df.iloc[i]
        far += "FARESYSTEM NUMBER={}".format(row["faresystem"])
        far += ',NAME=\"%s"'%(row["agency_raw_name"])
        far += ",STRUCTURE=FROMTO,FAREMATRIX=FMI.1.{},".format(row["faresystem"])
        far += "FAREZONES=NI.FAREZONE,FAREFROMFS={}".format(",".join(str(x) for x in transfer_df.loc[row["faresystem"]]))
        far += "\n"

    for i in range(len(flat_fare_system_df)):
        row = flat_fare_system_df.iloc[i]
        far += "FARESYSTEM NUMBER={}".format(row["faresystem"])
        far += ',NAME=\"%s"'%(row["agency_raw_name"])
        if row["price"] == 0:
            far += ",STRUCTURE=FREE\n"
        else:
            far += ",STRUCTURE=FLAT,IBOARDFARE={},".format(row["price"])
            far += "FAREFROMFS={}".format(",".join(str(x) for x in transfer_df.loc[row["faresystem"]]))
            far += "\n"

    return far

def assign_link_id_by_county(
    roadway_network = None,
    add_links_df = None
):
    """
    when adding new links, assign id by county rules

    Args:
        roadway_network (RoadwayNetwork): Input Wrangler roadway network
        add_links_df: new links

    Returns:
        add_links_df with unique link ids

    """

    county_last_link_id_df = roadway_network.links_df.groupby("county")["model_link_id"].max().reset_index().rename(
        columns = {"model_link_id" : "county_last_id"}
    )

    if "model_link_id" in add_links_df.columns:
        add_links_df.drop(["model_link_id"], axis = 1, inplace = True)

    if "county_last_id" in add_links_df.columns:
        add_links_df.drop(["county_last_id"], axis = 1, inplace = True)

    add_links_df = pd.merge(
        add_links_df,
        county_last_link_id_df,
        how = "left",
        on = "county"
    )

    add_links_df["model_link_id"] = add_links_df.groupby(["county"]).cumcount() + 1

    add_links_df["model_link_id"] = add_links_df["model_link_id"] + add_links_df["county_last_id"]

    return add_links_df

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
        centroid_connector_shape_file (str): centroid connector shape pickle filename

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

    centroid_connector_link_gdf["assignable"] = 1

    centroid_gdf["X"] = centroid_gdf.geometry.apply(
        lambda g: g.x
    )
    centroid_gdf["Y"] = centroid_gdf.geometry.apply(
        lambda g: g.y
    )

    roadway_network.nodes_df = pd.concat(
        [roadway_network.nodes_df,
        centroid_gdf[
            list(set(roadway_network.nodes_df.columns) &
            set(centroid_gdf.columns))
        ]],
        sort = False,
        ignore_index = True
    )

    centroid_connector_link_gdf = assign_link_id_by_county(roadway_network, centroid_connector_link_gdf)

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

def add_tap_id_to_node(
    roadway_network = None,
    parameters = None,
    network_variable: str = "tap_id",
    overwrite:bool = False,
    update_network_variable: bool = False,
):
    """
    adds tap_id as a node attribute

    Args:
        roadway_network (RoadwayNetwork): Input Wrangler roadway network
        parameters (Parameters): Lasso parameters object
        network_variable (str): Variable that should be written to in the network. Default to "tap_id"
        overwrite (Bool): True if overwriting existing variable in network.  Default to False.

    Returns:
        roadway object
    """

    WranglerLogger.info("Adding tap_id to node layer")

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

    if network_variable in roadway_network.nodes_df:
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
        "Adding roadway network variable: {}".format(
            network_variable
        )
    )

    tap_links_df = roadway_network.links_df[roadway_network.links_df["roadway"] == "tap"].copy()

    tap_links_df["tap_id"] = tap_links_df.apply(lambda x: x.A if x.A in parameters.tap_N_list else x.B, axis = 1)
    tap_links_df["model_node_id"] = tap_links_df.apply(lambda x: x.B if x.A in parameters.tap_N_list else x.A, axis = 1)

    node_tap_dict = dict(zip(tap_links_df["model_node_id"], tap_links_df["tap_id"]))

    roadway_network.nodes_df[network_variable] = roadway_network.nodes_df["model_node_id"].map(node_tap_dict)

    WranglerLogger.info(
        "Finished adding {} to node layer".format(
            network_variable
        )
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
    roadway_network = calculate_facility_type(roadway_network, parameters, update_network_variable = True)
    roadway_network = calculate_assignable(roadway_network, parameters, update_network_variable = True)
    roadway_network = add_tap_id_to_node(roadway_network, parameters, update_network_variable = True)

    roadway_network.calculate_distance(overwrite = True)

    roadway_network.fill_na()
    WranglerLogger.info("Splitting variables by time period and category")
    roadway_network.split_properties_by_time_period_and_category()
    roadway_network.convert_int()

    roadway_network.links_mtc_df = roadway_network.links_df.copy()
    roadway_network.nodes_mtc_df = roadway_network.nodes_df.copy()

    roadway_network.links_mtc_df = pd.merge(
        roadway_network.links_mtc_df.drop("geometry", axis = 1),
        roadway_network.shapes_df[["id", "geometry"]],
        how = "left",
        on = "id"
    )

    roadway_network.links_mtc_df.crs = roadway_network.crs
    roadway_network.nodes_mtc_df.crs = roadway_network.crs
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
    mode_crosswalk.drop_duplicates(subset = ["agency_raw_name", "route_type", "is_express_bus"], inplace = True)

    faresystem_crosswalk = pd.read_csv(os.path.join(os.path.dirname(outpath), "faresystem_crosswalk.txt"),
        dtype = {"route_id" : "object"}
    )

    veh_cap_crosswalk = pd.read_csv(parameters.veh_cap_crosswalk_file)

    """
    Add information from: routes, frequencies, and routetype to trips_df
    """
    trip_df = pd.merge(trip_df, transit_network.feed.routes.drop("agency_raw_name", axis = 1), how="left", on="route_id")

    trip_df = pd.merge(trip_df, transit_network.feed.frequencies, how="left", on="trip_id")

    trip_df["tod"] = trip_df.start_time.apply(transit_network.time_to_cube_time_period, as_str = False)
    trip_df["tod_name"] = trip_df.start_time.apply(transit_network.time_to_cube_time_period)

    # add shape_id to name when N most common pattern is used for routes*tod*direction
    trip_df["shp_id"] = trip_df.groupby(["route_id", "tod", "direction_id"]).cumcount()
    trip_df["shp_id"] = trip_df["shp_id"].astype(str)
    trip_df["shp_id"] = "s" + trip_df["shp_id"]

    trip_df["route_short_name"] = trip_df["route_short_name"].str.replace("-", "_").str.replace(" ", ".").str.replace(",", "_").str.slice(stop = 50)

    trip_df["route_long_name"] = trip_df["route_long_name"].str.replace(",", "_").str.slice(stop = 50)

    trip_df["LONGNAME"] = trip_df["route_long_name"]
    trip_df["HEADWAY"] = (trip_df["headway_secs"] / 60).astype(int)

    trip_df = pd.merge(trip_df, transit_network.feed.agency[["agency_name", "agency_raw_name", "agency_id"]], how = "left", on = ["agency_raw_name", "agency_id"])

    # make tri-delta-transit name shorter
    trip_df["agency_id"] = np.where(
        trip_df.agency_id == "tri-delta-transit",
        "tri-delta",
        trip_df.agency_id
    )

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
    trip_df['TM2_mode'] = trip_df['TM2_mode'].astype(int)

    trip_df["ONEWAY"] = "T"

    trip_df["agency_id"].fillna("", inplace = True)

    trip_df["NAME"] = trip_df.apply(
        lambda x: str(x.TM2_operator)
        + "_"
        + str(x.route_id)
        + "_"
        + x.tod_name
        + "_"
        + "d"
        + str(int(x.direction_id))
        + "_s"
        + x.shape_id,
        axis=1,
    )

    trip_df["NAME"] = trip_df["NAME"].str.slice(stop = 28)

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

    # special vehicle types
    trip_df["VEHTYPE"] = trip_df.apply(lambda x: _special_vehicle_type(x), axis = 1)
    # get vehicle capacity
    trip_df = pd.merge(trip_df, veh_cap_crosswalk, how = "left", on = "VEHTYPE")

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
    s += '\n LONGNAME="{}",'.format(str(row.LONGNAME).replace('"', ''))
    s += '\n USERA1=\"%s",' % (row.agency_id if row.agency_id != "" else row.agency_raw_name)
    s += '\n USERA2=\"%s",' % (row.TM2_line_haul_name,)
    s += "\n HEADWAY[{}]={},".format(row.tod, row.HEADWAY)
    s += "\n MODE={},".format(row.TM2_mode)
    s += "\n FARESYSTEM={},".format(int(row.faresystem))
    s += "\n ONEWAY={},".format(row.ONEWAY)
    s += "\n OPERATOR={},".format(int(row.TM2_operator) if ~math.isnan(row.TM2_operator) else 99)
    s += '\n SHORTNAME=\"%s",' % (row.route_short_name,)
    s += '\n VEHICLETYPE={},'.format(row.vehtype_num)
    if row.TM2_line_haul_name in ["Light rail", "Heavy rail", "Commuter rail", "Ferry service"]:
        add_nntime = True
    else:
        add_nntime = False
    s += "\n N={}".format(transit_network.shape_gtfs_to_cube(row, add_nntime))

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
    trip_cube_df = route_properties_gtfs_to_cube(transit_network, parameters, outpath)

    vehicle_type_df = trip_cube_df.drop_duplicates(subset = ["VEHTYPE"]).copy()
    vehicle_type_df.sort_values(by = "vehtype_num", inplace = True)
    vehicle_type_df["VEHICLETYPE"] = vehicle_type_df.apply(lambda x: vehicle_type_pts_format(x), axis=1)

    p = vehicle_type_df["VEHICLETYPE"].tolist()

    with open(os.path.join(os.path.dirname(outpath), "vehtype.pts") , "w") as f:
        f.write("\n".join(p))

    trip_cube_df["LIN"] = trip_cube_df.apply(lambda x: cube_format(transit_network, x), axis=1)

    l = trip_cube_df["LIN"].tolist()
    l = [";;<<PT>><<LINE>>;;"] + l

    with open(outpath, "w") as f:
        f.write("\n".join(l))

def vehicle_type_pts_format(row):
    """
    Creates a string representing the vehicle type in cube pts file notation.

    Args:
        row: row of a DataFrame representing a cube-formatted vehicle type, with the Attributes
            NUMBER, NAME, SEATCAP, CRUSHCAP

    Returns:
        string representation of vehicle type in cube pts file notation
    """

    s = '\nVEHICLETYPE NUMBER={} '.format(row.vehtype_num)
    s += 'NAME="{}" '.format(row.VEHTYPE)
    s += 'SEATCAP={} '.format(row.seatcap)
    s += 'CRUSHCAP={} '.format(row["100%Capacity"])
    s += 'LOADDISTFAC=0.8 ' # use the same made-up value from TM2
    s += 'CROWDCURVE[1]=3 CROWDCURVE[2]=3 CROWDCURVE[3]=3 ' # use the same made-up value from TM2

    return s

def _is_express_bus(x):
    if x.agency_name == "AC Transit":
        if x.route_short_name[0] not in map(str,range(1,10)):
            if x.route_short_name != "BSD":
                return 1
    if x.agency_name == "County Connection":
        if x.route_short_name[-1] == "X":
            return 1
    if x.agency_name == "Fairfield and Suisun Transit":
        if x.route_short_name in ["40", "90"]:
            return 1
    if x.agency_name == "Golden Gate Transit":
        if x.route_short_name in ["2", "4", "8", "10", "18", "24", "27", "37", "38", "40", "42",
        "44", "54", "56", "58", "70", "71", "72", "72X", "76", "92", "101", "101X"]:
            return 1
    if x.agency_name == "VTA":
        if int(x.route_short_name) >= 100:
            if int(x.route_short_name) <= 200:
                return 1
    if x.agency_name == "SamTrans":
        if x.route_short_name == "KX":
            return 1
    if x.agency_name == "WestCat (Western Contra Costa)":
        if (x.route_short_name.startswith("J")) | (x.route_short_name.startswith("Lynx")):
            return 1
    if x.agency_name == "SolTrans":
        if (x.route_short_name in ["80", "92", "78"]) | (x.route_long_name in ["80", "92", "78"]):
            return 1
    if x.agency_name == "Vine (Napa County)":
        if x.route_short_name in ["29"]:
            return 1
    if x.agency_name == "Tri Delta Transit":
        if x.route_short_name in ["300"]:
            return 1
    return 0

def _special_vehicle_type(x):
    if x.agency_name == "AC Transit":
        if x.route_short_name in ["1", "6", "72R", "OX", "J", "LA", "SB", "OX", "O", "N", "NX", "NX3", "NX4", "NL", "F", "FS"]:
            return "Motor Articulated Bus"
        if x.route_short_name in ["40", "40L", "57", "60", "52", "217", "97", "99"]:
            return "Motor Bus Mix of Standard and Artics"
    if x.agency_name == "Bay Area Rapid Transit":
        if x.route_color == "0099cc": # blue
            if x.tod_name in ["AM", "PM"]:
                return "9 Car BART"
            else:
                return "5 Car BART RENOVATED"
        if x.route_color == "339933": # green
            if x.tod_name not in ["AM", "PM"]:
                return "5 Car BART RENOVATED"
        if x.route_color == "ff9933": # orange
            return "7 Car BART"
        if x.route_color == "ff0000": # red
            if x.tod_name in ["AM", "PM"]:
                return "9 Car BART"
            else:
                return "5 Car BART RENOVATED"
        if x.route_color == "ffff33": # yellow
            if x.tod_name not in ["AM", "PM"]:
                return "5 Car BART RENOVATED"
    if x.route_short_name == "Vallejo Ferry":
        return "Ferry Vallejo"
    if x.agency_name == "SamTrans":
        if x.route_short_name in ["294", "295", "296", "297"]:
            return "SamTrans Plus Bus"
        if x.route_short_name in ["292", "398", "ECR"]:
            return "Motor Articulated Bus"
    if x.agency_name == "San Francisco Municipal Transportation Agency":
        if x.route_short_name in ["59", "60", "61"]:
            return "Cable Car"
        if x.route_short_name in ["J"]:
            return "LRV1"
        if x.route_short_name in ["KT", "L", "M", "N"]:
            return "LRV2"
        if x.route_short_name in ["38", "38R"]:
            return "Motor Articulated Bus"
        if x.route_short_name in ["1BX", "8", "8AX", "8BX", "14X", "14R"]:
            return "Motor Articulated Bus ALLDOOR"
        if x.route_short_name in ["35", "66"]:
            return "Motor Small Bus"
        if x.route_short_name in ["36", "37", "39", "52", "56", "67"]:
            return "Motor Small Bus ALLDOOR"
        if x.route_short_name in ["1AX", "10", "12", "18", "19", "2",
        "23", "27", "28", "29", "30X", "31AX", "31BX", "43", "44", "47",
        "48", "52", "54", "5R", "67", "82X", "83X", "88", "9", "91", "9R"]:
            return "Motor Standard Bus ALLDOOR"
        if x.route_short_name in ["E", "F"]:
            return "Streetcar"
        if x.route_short_name in ["14", "30", "49"]:
            return "Trolley Articulated Bus ALLDOOR"
        if x.route_short_name in ["55", "7", "7R", "7X"]:
            return "Trolley Standard Bus"
        if x.route_short_name in ["1", "21", "22", "24", "3", "31", "41", "45", "5", "6"]:
            return "Trolley Standard Bus ALLDOOR"
    if x.agency_name == "VTA":
        if x.route_short_name in ["22", "122", "522"]:
            return "Motor Articulated Bus"
        if x.route_short_name in ["140", "40", "55"]:
            return "Motor Bus Mix of Standard and Artics"
        if x.route_short_name in ["10", "201", "822", "823", "824", "825",
        "826", "827", "831", "828"]:
            return "Motor Small Bus"
    return x.VEHTYPE

def create_taps_tm2(
    transit_network = None,
    roadway_network = None,
    parameters = None,
    outpath: str = None,
    ):
    """
    creates taps

    Args:
        transit_network: transit network object
        roadway_network: roadway network object
        parameters
        outpath: output file path

    Return:
        taps nodes and taps connectors

    """
    # get stops
    stops_df = transit_roadway.feed.stops.copy()
    # create buffer for each stop
    stops_df["buffer"] = stops_df.apply(lambda x: geodesic_point_buffer(stop_lat, stop_lon, 300))

    # get neighbor stops within buffer - sjoin btw buffer polygons and stops
    stops_copy_df = stops_df[["stop_id", "stop_lat", "stop_lon", "model_node_id"]].copy()
    stops_copy_df.rename(
        columns = {"stop_id" : "neighbor_stop_id",
        "stop_lat" : "neighbor_stop_lat",
        "stop_lon" : "neighbor_stop_lon",
        "model_node_id" : "neighbor_model_node_id"},
        inplace = True
    )

    stops_buffer_gdf = GeoDataFrame(
        stops_df[["stop_id", "stop_lat", "stop_lon", "model_node_id", "buffer"]],
        geometry = stops_df["buffer"],
        crs = "EPSG:4326"
    )
    stops_copy_df = GeoDataFrame(
        stops_copy_df,
        geometry = gpd.points_from_xy(stops_copy_df.neighbor_stop_lon, stops_copy_df.neighbor_stop_lat),
        crs = "EPSG:4326"
    )

    # stop - stop pairs
    stops_buffer_neighbor_gdf = gpd.sjoin(stops_copy_df, stops_buffer_gdf, op = "intersects")

    # drop duplicated stop-stop pairs
    stops_buffer_neighbor_gdf = stops_buffer_neighbor_gdf[
        stops_buffer_neighbor_gdf.neighbor_stop_id >= stops_buffer_neighbor_gdf.stop_id
    ]

    # count neighbor stops for each stop
    stops_buffer_neighbor_num_df = pd.concat(
        [
        pd.DataFrame(stops_buffer_neighbor_gdf.stop_id.value_counts()).reset_index().rename(
            columns = {"stop_id" : "neighbor_num", "index" : "stop_id"}),
        pd.DataFrame(stops_buffer_neighbor_gdf.neighbor_stop_id.value_counts()).reset_index().rename(
            columns = {"neighbor_stop_id" : "neighbor_num", "index" : "stop_id"})
        ],
        sort = False,
        ignore_index = True
    )

    stops_buffer_neighbor_num_df = stops_buffer_neighbor_num_df.groupby("stop_id").sum().reset_index()

    # double counting self
    stops_buffer_neighbor_num_df["neighbor_num"] = stops_buffer_neighbor_num_df["neighbor_num"] - 1

    # assign a TAP to every stop, this is problematic
    taps_dict = create_tap_dict(stops_df, stops_buffer_neighbor_gdf)

    stop_taps_df = pd.DataFrame(taps_dict.items(), columns = ["stops", "TAP"])

    stop_taps_df = pd.merge(stops_df, stop_taps_df, how = "left", left_on = "stop_id", right_on = "stops")

    tap_num_connections_df = pd.DataFrame(stop_taps_df.TAP.value_counts()).reset_index()
    tap_num_connections_df.columns = ['TAP', 'num_connections']

    # what is this trying to achieve?
    lone_taps_df = tap_num_connections_df[tap_num_connections_df.num_connections == 1]
    lone_tap_stops_df = pd.merge(lone_taps_df, stop_taps_df, on = "TAP", how = "inner")

    test = pd.merge(lone_tap_stops_df, stop_taps_df, left_on='TAP', right_on='stops', how='left')
    test = test[['TAP_x', 'TAP_y']]
    test.columns = ['OldTAP', 'TAP']
    test_dict = dict(zip(test.OldTAP, test.TAP))
    stop_taps_df['TAP'] = stop_taps_df['TAP'].map(test_dict).fillna(stop_taps_df['TAP'])

    # join tap assignment with stops
    taps = pd.merge(stops_df[["stop_id", "stop_lat", "stop_lon"]], stop_taps_df[["stops", "TAP", "WithinRange"]],
               how = "left", left_on = "stop_id", right_on = "stops")

    # generate tap point by averaging the stops with same tap assignment
    tap_locations = taps.groupby('TAP')['stop_lon', 'stop_lat', 'WithinRange'].mean()
    tap_locations = tap_locations.reset_index()

    tap_locations_gdf = gpd.GeoDataFrame(
        tap_locations,
        geometry = gpd.points_from_xy(tap_locations.stop_lon, tap_locations.stop_lat),
        crs = "EPSG:4326"
    )

    return tap_locations_gdf

def create_tap_dict(stops_df, stops_buffer_neighbor_gdf):
    """
    old logic, needs to be reviewed
    """
    taps = {}
    for stop_id in stops_df.stop_id:
        neighbors_df = stops_buffer_neighbor_gdf[(stops_buffer_neighbor_gdf.stop_id == stop_id) |
                                                 (stops_buffer_neighbor_gdf.neighbor_stop_id == stop_id)].copy()
        for _, row in neighbors_df.iterrows():
            if row["stop_id"] == stop_id:
                if row["neighbor_stop_id"] not in taps:
                    taps[row["neighbor_stop_id"]] = stop_id
                if stop_id not in taps:
                    taps[stop_id] = stop_id
            else:
                if row["stop_id"] not in taps:
                    taps[row["stop_id"]] = stop_id
                if stop_id not in taps:
                    taps[stop_id] = stop_id
    return taps

def create_taps_kmeans_location_based(
    transit_network = None,
    roadway_network = None,
    parameters = None,
    bus_clusters = 6000,
    outpath: str = None,
    ):
    """
    creates taps

    Args:
        transit_network: transit network object
        roadway_network: roadway network object
        parameters
        outpath: output file path

    Return:
        taps nodes and taps connectors

    """

    # get stops
    stops_df = transit_network.feed.stops.copy()
    stops_df["model_node_id"] = stops_df["model_node_id"].astype(int)

    # get stop types bus vs non-bus
    # route_type 3 = bus, 5 = SF cable car, 0 = street-level light rail
    stop_times_df = transit_network.feed.stop_times.copy()
    stop_times_df = pd.merge(
        stop_times_df,
        transit_network.feed.trips[["trip_id", "route_id"]],
        how = "left",
        on = "trip_id"
    )
    stop_times_df = pd.merge(
        stop_times_df,
        transit_network.feed.routes[["route_id", "route_type"]],
        how = "left",
        on = "route_id"
    )

    stop_type_df = stop_times_df.groupby(["stop_id", "route_type"]).count().reset_index()

    bus_stops_id = stop_type_df[stop_type_df.route_type.isin([0,3,5])].stop_id

    nonbus_stops_id = stops_df[~stops_df.stop_id.isin(bus_stops_id)].stop_id

    # use K-means cluster to locate TAPs based on node coordination
    stops_df = pd.merge(
        stops_df,
        roadway_network.nodes_df[["model_node_id", "X", "Y"]],
        how = "left",
        on = "model_node_id"
    )

    bus_stops_df = stops_df[stops_df.stop_id.isin(bus_stops_id)].copy()
    nonbus_stops_df = stops_df[stops_df.stop_id.isin(nonbus_stops_id)].copy()

    kmeans = KMeans(n_clusters = bus_clusters)
    kmeans.fit(bus_stops_df[["X", "Y"]])

    taps_gdf = DataFrame(
        {"tap_id" : range(0, bus_clusters),
         "X" : kmeans.cluster_centers_[:, 0],
         "Y" : kmeans.cluster_centers_[:, 1],
        })

    bus_stops_df["tap_id"] = kmeans.labels_
    nonbus_stops_df["tap_id"] = range(bus_clusters, bus_clusters + len(nonbus_stops_df))

    taps_gdf = pd.concat([taps_gdf, nonbus_stops_df[["tap_id","X", "Y"]]], sort = False, ignore_index = True)

    taps_gdf = GeoDataFrame(
        taps_gdf,
        geometry = gpd.points_from_xy(taps_gdf.X, taps_gdf.Y),
        crs = "EPSG:4326"
    )

    stops_taps_df = pd.concat([bus_stops_df, nonbus_stops_df], ignore_index = True, sort = False)

    return taps_gdf, stops_taps_df

def create_taps_kmeans_frequency_based(
    transit_network = None,
    roadway_network = None,
    parameters = None,
    bus_clusters = 6000,
    outpath: str = None,
    ):
    """
    creates taps

    Args:
        transit_network: transit network object
        roadway_network: roadway network object
        parameters
        outpath: output file path

    Return:
        taps nodes and taps connectors

    """

    # get stops
    stops_df = transit_network.feed.stops.copy()
    stops_df["model_node_id"] = stops_df["model_node_id"].astype(int)

    # get stop types bus vs non-bus
    # route_type 3 = bus, 5 = SF cable car, 0 = street-level light rail
    stop_times_df = transit_network.feed.stop_times.copy()
    stop_times_df = pd.merge(
        stop_times_df,
        transit_network.feed.trips[["trip_id", "route_id"]],
        how = "left",
        on = "trip_id"
    )
    stop_times_df = pd.merge(
        stop_times_df,
        transit_network.feed.routes[["route_id", "route_type"]],
        how = "left",
        on = "route_id"
    )

    stop_type_df = stop_times_df.groupby(["stop_id", "route_type"]).count().reset_index()

    bus_stops_id = stop_type_df[stop_type_df.route_type.isin([0,3,5])].stop_id

    nonbus_stops_id = stops_df[~stops_df.stop_id.isin(bus_stops_id)].stop_id

    # use K-means cluster to locate TAPs based on node coordination
    stops_df = pd.merge(
        stops_df,
        roadway_network.nodes_df[["model_node_id", "X", "Y"]],
        how = "left",
        on = "model_node_id"
    )

    bus_stops_df = stops_df[stops_df.stop_id.isin(bus_stops_id)].copy()
    nonbus_stops_df = stops_df[stops_df.stop_id.isin(nonbus_stops_id)].copy()

    # count number of trips at each bus stop
    stop_trip_num_df = pd.merge(
        bus_stops_df,
        transit_network.feed.stop_times[["stop_id", "trip_id"]],
        how = "left",
        on = "stop_id"
    )

    frequencies_df = transit_network.feed.frequencies.copy()
    frequencies_df["duration"] = np.where(
        frequencies_df.start_time < frequencies_df.end_time,
        frequencies_df.end_time - frequencies_df.start_time,
        frequencies_df.end_time - frequencies_df.start_time + 24 * 3600
    )
    frequencies_df["num_trip"] = frequencies_df["duration"] / frequencies_df["headway_secs"]

    stop_trip_num_df = pd.merge(
        stop_trip_num_df,
        frequencies_df[["trip_id", "num_trip"]],
        how = "left",
        on = "trip_id"
    )

    stop_trip_num_df = stop_trip_num_df.groupby(["stop_id"])["num_trip"].sum().reset_index()

    bus_stops_df = pd.merge(
        bus_stops_df,
        stop_trip_num_df[["stop_id", "num_trip"]],
        how = "left",
        on = "stop_id"
    )

    kmeans = KMeans(n_clusters = bus_clusters)
    kmeans.fit(bus_stops_df[["X", "Y"]], sample_weight = bus_stops_df["num_trip"])

    taps_gdf = DataFrame(
        {"tap_id" : range(0, bus_clusters),
         "X" : kmeans.cluster_centers_[:, 0],
         "Y" : kmeans.cluster_centers_[:, 1],
        })

    bus_stops_df["tap_id"] = kmeans.labels_
    nonbus_stops_df["tap_id"] = range(bus_clusters, bus_clusters + len(nonbus_stops_df))

    taps_gdf = pd.concat([taps_gdf, nonbus_stops_df[["tap_id","X", "Y"]]], sort = False, ignore_index = True)

    taps_gdf = GeoDataFrame(
        taps_gdf,
        geometry = gpd.points_from_xy(taps_gdf.X, taps_gdf.Y),
        crs = "EPSG:4326"
    )

    stops_taps_df = pd.concat([bus_stops_df, nonbus_stops_df], ignore_index = True, sort = False)

    return taps_gdf, stops_taps_df

def snap_stop_to_node(stops, node_gdf):

    """
    map gtfs stops to roadway nodes

    Parameters:
    ------------
    stops
    network nodes

    return
    ------------
    stops with network nodes id
    """

    print('snapping gtfs stops to roadway node osmid...')

    node_non_c_gdf = node_gdf.copy()
    node_non_c_gdf = node_non_c_gdf.to_crs(CRS('epsg:26915'))
    node_non_c_gdf['X'] = node_non_c_gdf.geometry.map(lambda g:g.x)
    node_non_c_gdf['Y'] = node_non_c_gdf.geometry.map(lambda g:g.y)
    inventory_node_ref = node_non_c_gdf[['X', 'Y']].values
    tree = cKDTree(inventory_node_ref)

    stop_df = stops.copy()
    stop_df['geometry'] = [Point(xy) for xy in zip(stop_df['stop_lon'], stop_df['stop_lat'])]
    stop_df = gpd.GeoDataFrame(stop_df)
    stop_df.crs = CRS("EPSG:4326")
    stop_df = stop_df.to_crs(CRS('epsg:26915'))
    stop_df['X'] = stop_df['geometry'].apply(lambda p: p.x)
    stop_df['Y'] = stop_df['geometry'].apply(lambda p: p.y)

    for i in range(len(stop_df)):
        point = stop_df.iloc[i][['X', 'Y']].values
        dd, ii = tree.query(point, k = 1)
        add_snap_gdf = gpd.GeoDataFrame(node_non_c_gdf.iloc[ii]).transpose().reset_index(drop = True)
        add_snap_gdf['stop_id'] = stop_df.iloc[i]['stop_id']
        if i == 0:
            stop_to_node_gdf = add_snap_gdf.copy()
        else:
            stop_to_node_gdf = stop_to_node_gdf.append(add_snap_gdf, ignore_index=True, sort=False)

    stop_df.drop(['X','Y'], axis = 1, inplace = True)
    stop_to_node_gdf = pd.merge(stop_df, stop_to_node_gdf, how = 'left', on = 'stop_id')

    #column_list = ["stop_id", 'osm_node_id', 'shst_node_id', "model_node_id"]

    return stop_to_node_gdf#[column_list]

def create_taps_kmeans(
    transit_network = None,
    roadway_network = None,
    parameters = None,
    clusters = 6000,
    outpath: str = None,
    ):
    """
    creates taps

    Args:
        transit_network: transit network object
        roadway_network: roadway network object
        parameters
        outpath: output file path

    Return:
        taps nodes and taps connectors

    """

    # get stops
    stops_df = transit_network.feed.stops.copy()
    stops_df["model_node_id"] = stops_df["model_node_id"].astype(int)

    # use K-means cluster to locate TAPs based on node coordination
    stops_df = pd.merge(
        stops_df,
        roadway_network.nodes_df[["model_node_id", "X", "Y"]],
        how = "left",
        on = "model_node_id"
    )

    kmeans = KMeans(n_clusters = clusters)
    kmeans.fit(stops_df[["X", "Y"]])

    taps_gdf = DataFrame(
        {"tap_id" : range(0, clusters),
         "X" : kmeans.cluster_centers_[:, 0],
         "Y" : kmeans.cluster_centers_[:, 1],
        })

    stops_df["tap_id"] = kmeans.labels_

    taps_gdf = GeoDataFrame(
        taps_gdf,
        geometry = gpd.points_from_xy(taps_gdf.X, taps_gdf.Y),
        crs = "EPSG:4326"
    )

    return taps_gdf, stops_df

def create_tap_nodes_and_links(
    transit_network = None,
    roadway_network = None,
    parameters = None,
    num_taps = 6000,
    ):

    taps_gdf, stop_tap_df = create_taps_kmeans(
        transit_network = transit_network,
        roadway_network = roadway_network,
        parameters = parameters,
        clusters = num_taps,
    )

    # check if centroids are added, because need to number tap links
    if "taz" not in roadway_network.links_df.roadway.unique():
        roadway_network = add_centroid_and_centroid_connector(
            roadway_network = roadway_network,
            parameters = parameters
        )

    # numbering tap nodes
    county_gdf = gpd.read_file(parameters.county_shape)
    county_gdf = county_gdf.to_crs("EPSG:4326")

    tap_nodes_gdf = gpd.sjoin(
        taps_gdf,
        county_gdf[["NAME", "geometry"]].rename(columns = {"NAME" : "county"}),
        how = "left",
        op = "intersects"
    )

    tap_nodes_gdf["county"].fillna("Marin", inplace = True)

    tap_nodes_gdf["tap_node_county_start"] = tap_nodes_gdf["county"].map(parameters.tap_N_start)
    tap_nodes_gdf["model_node_id"] = tap_nodes_gdf.groupby(["county"]).cumcount()

    tap_nodes_gdf["model_node_id"] = tap_nodes_gdf["model_node_id"] + tap_nodes_gdf["tap_node_county_start"]

    # tap shapes
    tap_shapes_gdf = pd.merge(stop_tap_df,
                  tap_nodes_gdf[["tap_id", "X", "Y"]].rename(columns = {"X" : "tap_X", "Y" : "tap_Y"}),
                  how = 'left',
                  on = ["tap_id"])

    tap_shapes_gdf.drop_duplicates(subset = ["model_node_id", "tap_id"], inplace = True)

    tap_shapes_gdf["id"] = range(1, 1 + len(tap_shapes_gdf))
    tap_shapes_gdf["id"] = tap_shapes_gdf["id"].apply(lambda x : "tap_" + str(x))
    tap_shapes_gdf["shstGeometryId"] = tap_shapes_gdf["id"]

    tap_shapes_gdf["geometry"] = tap_shapes_gdf.apply(
        lambda x: LineString([Point(x.X, x.Y), Point(x.tap_X, x.tap_Y)]),
        axis = 1
    )

    tap_shapes_gdf = gpd.GeoDataFrame(
        tap_shapes_gdf,
        geometry = tap_shapes_gdf["geometry"],
        crs = "EPSG:4326"
    )

    # tap links
    tap_links_gdf = tap_shapes_gdf.copy()
    tap_links_gdf.rename(columns = {"model_node_id" : "A"}, inplace = True)

    tap_dict = dict(zip(tap_nodes_gdf.tap_id, tap_nodes_gdf.model_node_id))
    tap_links_gdf["B"] = tap_links_gdf["tap_id"].map(tap_dict)

    tap_links_gdf = pd.merge(
        tap_links_gdf,
        tap_nodes_gdf[["tap_id", "county"]],
        how = "left",
        on = "tap_id"
    )

    tap_links_gdf_copy = tap_links_gdf.copy()
    tap_links_gdf_copy.rename(columns = {"A" : "B", "B" : "A"}, inplace = True)

    tap_links_gdf = pd.concat(
        [tap_links_gdf, tap_links_gdf_copy],
        sort = False,
        ignore_index = True
    )

    tap_links_gdf["roadway"] = "tap"

    tap_links_gdf["walk_access"] = 1

    # numbering tap links
    county_last_link_id_df = roadway_network.links_df.groupby("county")["model_link_id"].max().reset_index().rename(
    columns = {"model_link_id" : "county_last_id"})

    tap_links_gdf = pd.merge(
        tap_links_gdf,
        county_last_link_id_df,
        how = "left",
        on = "county"
    )

    tap_links_gdf["model_link_id"] = tap_links_gdf.groupby(["county"]).cumcount() + 1

    tap_links_gdf["model_link_id"] = tap_links_gdf["model_link_id"] + tap_links_gdf["county_last_id"]

    geom_length = tap_links_gdf[['geometry']].copy()
    geom_length = geom_length.to_crs(epsg = 26915)
    geom_length["length"] = geom_length.length

    tap_links_gdf["length"] = geom_length["length"]

    tap_links_gdf["fromIntersectionId"] = np.nan
    tap_links_gdf["toIntersectionId"] = np.nan

    all_node_gdf = pd.concat([roadway_network.nodes_df,
                         tap_nodes_gdf],
                        sort = False,
                        ignore_index = True)

    create_locationreference(all_node_gdf, tap_links_gdf)

    return tap_nodes_gdf, tap_links_gdf, tap_shapes_gdf

def add_tap_and_tap_connector(
    roadway_network = None,
    transit_network = None,
    parameters = None,
    tap_file: str = None,
    tap_connector_link_file: str = None,
    tap_connector_shape_file: str = None,
):
    """
    Add centorid and centroid connectors from pickles.

    Args:
        roadway_network (RoadwayNetwork): Input Wrangler roadway network
        parameters (Parameters): Lasso parameters object
        tap_file (str): tap node gdf pickle filename
        tap_connector_link_file (str): tap connector link pickle filename
        tap_connector_shape_file (str): tap connector shape pickle filename

    Returns:
        roadway network object

    """

    WranglerLogger.info("Adding tap and tap connector to standard network")

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

    if not transit_network:
        msg = "'transit_network' is missing from the method call.".format(transit_network)
        WranglerLogger.error(msg)
        raise ValueError(msg)

    tap_file = (
        tap_file
        if tap_file
        else parameters.tap_file
    )

    tap_connector_link_file = (
        tap_connector_link_file
        if tap_connector_link_file
        else parameters.tap_connector_link_file
    )

    if not tap_connector_link_file:
        msg = "'tap_connector_link_file' not found in method or lasso parameters."
        WranglerLogger.error(msg)
        raise ValueError(msg)

    tap_connector_shape_file = (
        tap_connector_shape_file
        if tap_connector_shape_file
        else parameters.tap_connector_shape_file
    )

    if not tap_connector_shape_file:
        msg = "'tap_connector_shape_file' not found in method or lasso parameters."
        WranglerLogger.error(msg)
        raise ValueError(msg)

    """
    Start actual process
    """

    tap_gdf = pd.read_pickle(tap_file)
    tap_connector_link_gdf = pd.read_pickle(tap_connector_link_file)
    tap_connector_shape_gdf = pd.read_pickle(tap_connector_shape_file)

    tap_gdf["X"] = tap_gdf.geometry.apply(
        lambda g: g.x
    )
    tap_gdf["Y"] = tap_gdf.geometry.apply(
        lambda g: g.y
    )

    # check if there's stops that are not connected to TAPs, this happens when new stops are added
    # if so, connect it to the closest TAP
    stops_df = transit_network.feed.stops.copy()
    stops_not_connected_to_tap_df = stops_df[~(stops_df.model_node_id.astype(int).isin(tap_connector_link_gdf.A.tolist()))].copy()

    if len(stops_not_connected_to_tap_df) > 0:
        WranglerLogger.info(
            "There are {} stops not connected to taps, connecting them to the closest taps.".format(len(stops_not_connected_to_tap_df))
        )

        stops_not_connected_to_tap_df = snap_stop_to_node(stops_not_connected_to_tap_df, tap_gdf)

        stops_not_connected_to_tap_df.drop(["X", "Y", "model_node_id_y", "geometry_x", "geometry_y"], axis = 1, inplace = True)
        stops_not_connected_to_tap_df.rename(columns = {"model_node_id_x": "model_node_id"}, inplace = True)
        stops_not_connected_to_tap_df["model_node_id"] = stops_not_connected_to_tap_df["model_node_id"].astype(int)

        stops_not_connected_to_tap_df = pd.merge(
            stops_not_connected_to_tap_df,
            roadway_network.nodes_df[["model_node_id", "X", "Y"]],
            how = "left",
            on = ["model_node_id"]
        )

        # tap shapes
        add_tap_shapes_gdf = pd.merge(stops_not_connected_to_tap_df,
                      tap_gdf[["tap_id", "X", "Y"]].rename(columns = {"X" : "tap_X", "Y" : "tap_Y"}),
                      how = 'left',
                      on = ["tap_id"])

        add_tap_shapes_gdf.drop_duplicates(subset = ["model_node_id", "tap_id"], inplace = True)

        existing_tap_shape_id_max = tap_connector_shape_gdf.id.str.strip("tap_").astype(int).max()

        add_tap_shapes_gdf["id"] = range(existing_tap_shape_id_max + 1, 1 + existing_tap_shape_id_max + len(add_tap_shapes_gdf))
        add_tap_shapes_gdf["id"] = add_tap_shapes_gdf["id"].apply(lambda x : "tap_" + str(x))
        add_tap_shapes_gdf["shstGeometryId"] = add_tap_shapes_gdf["id"]

        add_tap_shapes_gdf["geometry"] = add_tap_shapes_gdf.apply(
            lambda x: LineString([Point(x.X, x.Y), Point(x.tap_X, x.tap_Y)]),
            axis = 1
        )

        add_tap_shapes_gdf = gpd.GeoDataFrame(
            add_tap_shapes_gdf,
            geometry = add_tap_shapes_gdf["geometry"],
            crs = "EPSG:4326"
        )

        # tap links
        add_tap_links_gdf = add_tap_shapes_gdf.copy()
        add_tap_links_gdf.rename(columns = {"model_node_id" : "A"}, inplace = True)

        tap_dict = dict(zip(tap_gdf.tap_id, tap_gdf.model_node_id))
        add_tap_links_gdf["B"] = add_tap_links_gdf["tap_id"].map(tap_dict)

        add_tap_links_gdf_copy = add_tap_links_gdf.copy()
        add_tap_links_gdf_copy.rename(columns = {"A" : "B", "B" : "A"}, inplace = True)

        add_tap_links_gdf = pd.concat(
            [add_tap_links_gdf, add_tap_links_gdf_copy],
            sort = False,
            ignore_index = True
        )

        add_tap_links_gdf["roadway"] = "tap"
        add_tap_links_gdf["walk_access"] = 1

        tap_connector_link_gdf = pd.concat(
            [tap_connector_link_gdf,
            add_tap_links_gdf],
            sort = False,
            ignore_index = True
        )

        tap_connector_shape_gdf = pd.concat(
            [tap_connector_shape_gdf,
            add_tap_shapes_gdf],
            sort = False,
            ignore_index = True
        )

    tap_connector_link_gdf["lanes"] = 1
    tap_connector_link_gdf["ft"] = 8
    tap_connector_link_gdf["managed"] = 0

    roadway_network.nodes_df = pd.concat(
        [roadway_network.nodes_df,
        tap_gdf[
            list(set(roadway_network.nodes_df.columns) &
            set(tap_gdf.columns))
        ]],
        sort = False,
        ignore_index = True
    )

    tap_connector_link_gdf = assign_link_id_by_county(roadway_network, tap_connector_link_gdf)

    roadway_network.links_df = pd.concat(
        [roadway_network.links_df,
        tap_connector_link_gdf[
            list(set(roadway_network.links_df.columns) &
            set(tap_connector_link_gdf.columns))
        ]],
        sort = False,
        ignore_index = True
    )

    roadway_network.shapes_df = pd.concat(
        [roadway_network.shapes_df,
        tap_connector_shape_gdf[
            list(set(roadway_network.shapes_df.columns) &
            set(tap_connector_shape_gdf.columns))
        ]],
        sort = False,
        ignore_index = True
    )

    WranglerLogger.info(
        "Finished adding tap and tap connectors"
    )

    return roadway_network

def write_fare_matrix(
    zonal_fare_df,
    output_filename,
    parameters=None,
    output_variables:list=None
    ):
    """
    write out zonal fare matrix to simple text
    """

    output_variables = (
        output_variables if output_variables else parameters.fare_matrix_output_variables
    )

    output_df = zonal_fare_df[output_variables].copy()

    for c in output_df.columns:
        if c not in parameters.float_col:
            output_df[c] = output_df[c].astype(int)

    output_df.to_csv(
        output_filename,
        index = False,
        header = False,
        sep = " "
    )
