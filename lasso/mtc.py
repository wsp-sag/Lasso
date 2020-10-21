import copy
import glob
import os

import geopandas as gpd
import pandas as pd

from geopandas import GeoDataFrame
from pandas import DataFrame
import numpy as np

from .parameters import Parameters
from .logger import WranglerLogger

class MTC:
    """
    """

    def __init__(
        self, **kwargs
    ):
        """
        """
        self.mpo = "mtc"

    @staticmethod
    def calculate_facility_type(
        roadway_network_object = None,
        #parameters = {},
        network_variable = "ft",
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
        """
        if type(parameters) is dict:
            parameters = Parameters(**parameters)
        else:
            parameters = Parameters(**parameters.__dict__)
        """
        if not roadway_network_object:
            msg = "'roadway_network_object' is missing from the method call."
            WranglerLogger.error(msg)
            raise ValueError(msg)

        facility_type_dict = (
            facility_type_dict
            if facility_type_dict
            else roadway_network_object.parameters.osm_facility_type_dict
        )

        if not facility_type_dict:
            msg = msg = "'facility_type_dict' not found in method or lasso parameters."
            WranglerLogger.error(msg)
            raise ValueError(msg)

        """
        Start actual process
        """

        join_gdf = roadway_network_object.links_df.copy()

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

        roadway_network_object.links_df[network_variable] = join_gdf[network_variable]

        WranglerLogger.info(
            "Finished calculating roadway class variable: {}".format(network_variable)
        )

        #return roadway_network_object

    @staticmethod
    def determine_number_of_lanes(
        roadway_network_object = None,
        #parameters = {},
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
        """
        if type(parameters) is dict:
            parameters = Parameters(**parameters)
        else:
            parameters = Parameters(**parameters.__dict__)
        """
        if not roadway_network_object:
            msg = "'roadway_network_object' is missing from the method call.".format(roadway_net)
            WranglerLogger.error(msg)
            raise ValueError(msg)

        osm_lanes_attributes = (
            osm_lanes_attributes
            if osm_lanes_attributes
            else roadway_network_object.parameters.osm_lanes_attributes
        )

        tam_tm2_attributes = (
            tam_tm2_attributes
            if tam_tm2_attributes
            else roadway_network_object.parameters.tam_tm2_attributes
        )

        if not tam_tm2_attributes:
            msg = "'tam_tm2_attributes' not found in method or lasso parameters."
            WranglerLogger.error(msg)
            raise ValueError(msg)

        sfcta_attributes = (
            sfcta_attributes
            if sfcta_attributes
            else roadway_network_object.parameters.sfcta_attributes
        )

        if not sfcta_attributes:
            msg = "'sfcta_attributes' not found in method or lasso parameters."
            WranglerLogger.error(msg)
            raise ValueError(msg)

        pems_attributes = (
            pems_attributes
            if pems_attributes
            else roadway_network_object.parameters.pems_attributes
        )

        if not pems_attributes:
            msg = "'pems_attributes' not found in method or lasso parameters."
            WranglerLogger.error(msg)
            raise ValueError(msg)

        tomtom_attributes = (
            tomtom_attributes
            if tomtom_attributes
            else roadway_network_object.parameters.tomtom_attributes
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

    @staticmethod
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

        if network_variable in roadway_network_object.links_df:
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

        #return roadway_network_object

    @staticmethod
    def calculate_assignable(
        roadway_network_object = None,
        #parameters = {},
        network_variable: str = "assignable",
        legacy_tm2_attributes: str = None,
        overwrite:bool = False,
    ):
        """
        Calculates assignable variable.
        Currently just use the conflation with legacy TM2

        Args:
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
        """
        if type(parameters) is dict:
            parameters = Parameters(**parameters)
        else:
            parameters = Parameters(**parameters.__dict__)
        """
        if not roadway_network_object:
            msg = "'roadway_network_object' is missing from the method call.".format(roadway_net)
            WranglerLogger.error(msg)
            raise ValueError(msg)

        if network_variable in roadway_network_object.links_df:
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
                return

        legacy_tm2_attributes = (
            legacy_tm2_attributes
            if legacy_tm2_attributes
            else roadway_network_object.parameters.legacy_tm2_attributes
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
            roadway_network_object.links_df,
            legacy_df[["shstReferenceId", network_variable]],
            how = "left",
            on = "shstReferenceId"
        )

        roadway_network_object.links_df[network_variable] = join_gdf[network_variable]

        WranglerLogger.info(
            "Finished determining assignable using variable: {}".format(network_variable)
        )

        #return roadway_network_object

    @staticmethod
    def calculate_cntype(
        roadway_network_object = None,
        #parameters = {},
        network_variable: str = "cntype",
        overwrite:bool = False,
    ):
        """
        Calculates cntype variable.

        Args:
            network_variable (str): Variable that should be written to in the network. Default to "cntype"
            overwrite (Bool): True if overwriting existing variable in network.  Default to False.

        Returns:
            roadway object
        """

        WranglerLogger.info("Determining cntype")

        """
        Verify inputs
        """
        """
        if type(parameters) is dict:
            parameters = Parameters(**parameters)
        else:
            parameters = Parameters(**parameters.__dict__)
        """
        if not roadway_network_object:
            msg = "'roadway_network_object' is missing from the method call.".format(roadway_net)
            WranglerLogger.error(msg)
            raise ValueError(msg)

        if network_variable in roadway_network_object.links_df:
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
                return

        """
        Start actual process
        """

        WranglerLogger.info(
            "Calculating and adding roadway network variable: {}".format(
                network_variable
            )
        )

        # TODO this logic needs to be revised
        def _calcualte_cntype(x):
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

        roadway_network_object.links_df[network_variable] = roadway_network_object.links_df.apply(lambda x: _calcualte_cntype(x), axis = 1)

        WranglerLogger.info(
            "Finished determining assignable using variable: {}".format(network_variable)
        )

        #return roadway_network_object

    @staticmethod
    def calculate_transit(
        roadway_network_object = None,
        #parameters = {},
        network_variable: str = "transit",
        overwrite:bool = False,
    ):
        """
        Calculates transit-only variable.

        Args:
            network_variable (str): Variable that should be written to in the network. Default to "transit"
            overwrite (Bool): True if overwriting existing variable in network.  Default to False.

        Returns:
            roadway object
        """

        WranglerLogger.info("Determining assignable")

        """
        Verify inputs
        """
        """
        if type(parameters) is dict:
            parameters = Parameters(**parameters)
        else:
            parameters = Parameters(**parameters.__dict__)
        """
        if not roadway_network_object:
            msg = "'roadway_network_object' is missing from the method call.".format(roadway_net)
            WranglerLogger.error(msg)
            raise ValueError(msg)

        if network_variable in roadway_network_object.links_df:
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
                return

        """
        Start actual process
        """

        WranglerLogger.info(
            "Calculating and adding roadway network variable: {}".format(
                network_variable
            )
        )

        if "bus_only" in roadway_network_object.links_df.columns:
            roadway_network_object.links_df[network_variable] = np.where(
                (roadway_network_object.links_df.bus_only == 1) |
                    (roadway_network_object.links_df.rail_only == 1),
                1,
                0
            )
        else:
            roadway_network_object.links_df[network_variable] = np.where(
                (roadway_network_object.links_df.rail_only == 1),
                1,
                0
            )

        WranglerLogger.info(
            "Finished determining transit-only variable: {}".format(network_variable)
        )

        #return roadway_network_object

    @staticmethod
    def calculate_useclass(
        roadway_network_object = None,
        #parameters = {},
        network_variable: str = "useclass",
        overwrite:bool = False,
        update_network_variable: bool = False,
    ):
        """
        Calculates useclass variable.
        Use value from project cards if available, if not default to 0

        Args:
            network_variable (str): Variable that should be written to in the network. Default to "useclass"
            overwrite (Bool): True if overwriting existing variable in network.  Default to False.

        Returns:
            roadway object
        """

        WranglerLogger.info("Determining useclass")

        """
        Verify inputs
        """
        """
        if type(parameters) is dict:
            parameters = Parameters(**parameters)
        else:
            parameters = Parameters(**parameters.__dict__)
        """
        if not roadway_network_object:
            msg = "'roadway_network_object' is missing from the method call.".format(roadway_net)
            WranglerLogger.error(msg)
            raise ValueError(msg)

        if network_variable in roadway_network_object.links_df:
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
            roadway_network_object.links_df[network_variable] = np.where(
                    roadway_network_object.links_df[network_variable].notnull(),
                    roadway_network_object.links_df[network_variable],
                    0
                )
        else:
            roadway_network_object.links_df[network_variable] = 0

        WranglerLogger.info(
            "Finished determining transit-only variable: {}".format(network_variable)
        )

        #return roadway_network_object

    @staticmethod
    def add_centroid_and_centroid_connector(
        roadway_network_object = None,
        #parameters = {},
        centroid_file: str = None,
        centroid_connector_link_file: str = None,
        centroid_connector_shape_file: str = None,
    ):
        """
        Add centorid and centroid connectors from pickles.

        Args:
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
        """
        if type(parameters) is dict:
            parameters = Parameters(**parameters)
        else:
            parameters = Parameters(**parameters.__dict__)
        """
        if not roadway_network_object:
            msg = "'roadway_network_object' is missing from the method call.".format(roadway_net)
            WranglerLogger.error(msg)
            raise ValueError(msg)

        centroid_file = (
            centroid_file
            if centroid_file
            else roadway_network_object.parameters.centroid_file
        )

        centroid_connector_link_file = (
            centroid_connector_link_file
            if centroid_connector_link_file
            else roadway_network_object.parameters.centroid_connector_link_file
        )

        if not centroid_connector_link_file:
            msg = "'centroid_connector_link_file' not found in method or lasso parameters."
            WranglerLogger.error(msg)
            raise ValueError(msg)

        centroid_connector_shape_file = (
            centroid_connector_shape_file
            if centroid_connector_shape_file
            else roadway_network_object.parameters.centroid_connector_shape_file
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

        roadway_network_object.nodes_df = pd.concat(
            [roadway_network_object.nodes_df,
            centroid_gdf[
                list(set(roadway_network_object.nodes_df.columns) &
                set(centroid_gdf.columns))
            ]],
            sort = False,
            ignore_index = True
        )

        roadway_network_object.links_df = pd.concat(
            [roadway_network_object.links_df,
            centroid_connector_link_gdf[
                list(set(roadway_network_object.links_df.columns) &
                set(centroid_connector_link_gdf.columns))
            ]],
            sort = False,
            ignore_index = True
        )

        roadway_network_object.shapes_df = pd.concat(
            [roadway_network_object.shapes_df,
            centroid_connector_shape_gdf[
                list(set(roadway_network_object.shapes_df.columns) &
                set(centroid_connector_shape_gdf.columns))
            ]],
            sort = False,
            ignore_index = True
        )



        WranglerLogger.info(
            "Finished adding centroid and centroid connectors"
        )

        return roadway_network_object
