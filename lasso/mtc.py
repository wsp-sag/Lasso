import copy
import glob
import os

import geopandas as gpd
import pandas as pd

from geopandas import GeoDataFrame
from pandas import DataFrame
import numpy as np

from network_wrangler import RoadwayNetwork
from .parameters import Parameters
from .roadway import ModelRoadwayNetwork
from .logger import WranglerLogger


def calculate_facility_type(
    roadway_network_object = None,
    parameters = {},
    network_variable = "facility_type",
    network_variable_lanes = "numlanes",
    facility_type_dict = None
):
    """
    Calculates facility type variable.

    facility type is a lookup based on OSM roadway

    Args:
        network_variable (str): Name of the variable that should be written to.  Default to "facility_type".
        facility_type_dict (dict): Dictionary to map OSM roadway to facility type.

    Returns:
        None
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

    if not roadway_network_object:
        msg = "'roadway_network_object' is missing from the method call.".format(roadway_net)
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

    """
    Start actual process
    """

    join_gdf = roadway_network_object.links_df.copy()

    join_gdf["oneWay"] = join_gdf["oneWay"].apply(lambda x: "NA" if x == None else x)
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

    roadway_network_object.links_df[network_variable] = join_gdf[network_variable]

    WranglerLogger.info(
        "Finished calculating roadway class variable: {}".format(network_variable)
    )

    return roadway_network_object

def determine_number_of_lanes(
    roadway_network_object = None,
    parameters = {},
    network_variable: str = "lanes",
    osm_lanes_attributes: str = None,
    tam_tm2_attributes: str = None,
    sfcta_attributes: str = None,
    pems_attributes: str = None,
    tomtom_attributes: str = None,
    overwrite:bool = False,
):
    """
    Uses a series of rules to determine the number of lanes.

    Args:
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
        None

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

    if not roadway_network_object:
        msg = "'roadway_network_object' is missing from the method call.".format(roadway_net)
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
        roadway_network_object.links_df, osm_df, how = "left", on = "shstReferenceId"
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

    roadway_network_object.links_df[network_variable] = join_gdf[network_variable]

    WranglerLogger.info(
        "Finished determining number of lanes using variable: {}".format(network_variable)
    )

    return roadway_network_object

def calculate_use(
    roadway_network_object = None,
    network_variable="use",
    as_integer=True,
    overwrite=False,
):
    """
    Calculates use variable.

    Args:
        network_variable (str): Variable that should be written to in the network. Default to "use"
        as_integer (bool): If True, will convert true/false to 1/0s.  Defauly to True.
        overwrite (Bool): True if overwriting existing county variable in network.  Default to False.

    Returns:
        None
    """

    if not roadway_network_object:
        msg = "'roadway_network_object' is missing from the method call.".format(roadway_net)
        WranglerLogger.error(msg)
        raise ValueError(msg)

    if network_variable in self.links_df:
        if overwrite:
            WranglerLogger.info(
                "Overwriting existing hov Variable '{}' already in network".format(
                    network_variable
                )
            )
        else:
            WranglerLogger.info(
                "'use' Variable '{}' already in network. Returning without overwriting.".format(
                    network_variable
                )
            )
            return

    WranglerLogger.info(
        "Calculating use and adding as roadway network variable: {}".format(
            network_variable
        )
    )
    """
    Verify inputs
    """

    if not network_variable:
        msg = "No network variable specified for centroid connector"
        WranglerLogger.error(msg)
        raise ValueError(msg)

    """
    Start actual process
    """
    roadway_network_object.links_df[network_variable] = int(1)

    if as_integer:
        roadway_network_object.links_df[network_variable] = roadway_network_object.links_df[network_variable].astype(
            int
        )
    WranglerLogger.info(
        "Finished calculating use variable: {}".format(
            network_variable
        )
    )

    return roadway_network_object

def roadway_standard_to_mtc_network(self, output_epsg=None):
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

    output_epsg = output_epsg if output_epsg else self.parameters.output_epsg

    """
    Start actual process
    """
    if "managed" in self.links_df.columns:
        WranglerLogger.info("Creating managed lane network.")
        self.create_managed_lane_network(in_place=True)
    else:
        WranglerLogger.info("Didn't detect managed lanes in network.")

    self.create_calculated_variables()
    self.calculate_distance(overwrite = True)

    self.fill_na()
    self.convert_int()
    WranglerLogger.info("Splitting variables by time period and category")
    self.split_properties_by_time_period_and_category()

    self.links_mtc_df = self.links_df.copy()
    self.nodes_mtc_df = self.nodes_df.copy()
    self.shapes_mtc_df = self.shapes_df.dropna().copy()

    self.links_mtc_df.crs = RoadwayNetwork.CRS
    self.nodes_mtc_df.crs = RoadwayNetwork.CRS
    self.shapes_mtc_df.crs = RoadwayNetwork.CRS
    WranglerLogger.info("Setting Coordinate Reference System to {}".format(output_epsg))
    self.links_mtc_df = self.links_mtc_df.to_crs(epsg=output_epsg)
    self.nodes_mtc_df = self.nodes_mtc_df.to_crs(epsg=output_epsg)
    self.shapes_mtc_df = self.shapes_mtc_df.to_crs(epsg=output_epsg)

    self.nodes_mtc_df["X"] = self.nodes_mtc_df.geometry.apply(
        lambda g: g.x
    )
    self.nodes_mtc_df["Y"] = self.nodes_mtc_df.geometry.apply(
        lambda g: g.y
    )

    # CUBE expect node id to be N
    self.nodes_mtc_df.rename(columns={"model_node_id": "N"}, inplace=True)
