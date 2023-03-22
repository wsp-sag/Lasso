import copy
import numpy as np
import pandas as pd
import geopandas as gpd
from pyproj import CRS
from scipy.spatial import cKDTree
from shapely.geometry import Point, LineString

from network_wrangler import RoadwayNetwork

from ..parameters import Parameters
from ..model_roadway import ModelRoadwayNetwork
from ..logger import WranglerLogger

class SEFloridaRoadwayNetwork(ModelRoadwayNetwork):
    """SE Florida specific methods for :py:class:`ModelRoadwayNetwork`

    .. highlight:: python
    Typical usage example:
    ::
        net = SEFloridaRoadwayNetwork.read(
            link_filename=STPAUL_LINK_FILE,
            node_filename=STPAUL_NODE_FILE,
            shape_filename=STPAUL_SHAPE_FILE,
            fast=True,
        )
        net.roadway_standard_to_seflorida_network()

    """

    @staticmethod
    def read(
        link_filename: str,
        node_filename: str,
        shape_filename: str,
        parameters: Parameters = None,
        **kwargs,
    ):
        """
        Reads in links and nodes network standard.

        Args:
            link_filename (str): File path to link json.
            node_filename (str): File path to node geojson.
            shape_filename (str): File path to link true shape geojson
            fast (bool): boolean that will skip validation to speed up read time.
            parameters: an instance of Parameters.
                If not specified, will use default SEFlorida parameters overridden by any relevant
                parameters in parameters_dict or in other, additional kwargs.
            parameters_dict: dictionary of parameter settings which override parameters instance.
                Defaults to {}.
        Returns:
            SEFloridaModelRoadwayNetwork
        """
        WranglerLogger.info("Reading as a SE Florida Model Roadway Network")

        WranglerLogger.debug(
            "Using SEFloridaRoadwayNetwork parameters:      {}".format(parameters)
        )

        model_roadway_network = RoadwayNetwork.read(
            link_filename,
            node_filename,
            shape_filename,
            parameters=parameters,
            **kwargs,
        )
        model_roadway_network.__class__ = SEFloridaRoadwayNetwork
        return model_roadway_network

    def calculate_number_of_lanes(
        self,
        roadway_network=None,
        parameters=None,
        network_variable: str = "lanes",
        osm_lanes_attributes: str = None,
        legacy_serpm8_attributes: str = None,
        navteq_attributes: str = None,
        fdot_attributes: str = None,
        county_attributes: str = None,
        overwrite: bool = False,
    ):
        """
        Uses a series of rules to determine the number of lanes.

        Args:
            roadway_network (RoadwayNetwork): Input Wrangler roadway network
            parameters (Parameters): Lasso parameters object
            network_variable (str): Name of lanes variable
            osm_lanes_attributes (str): OpenStreetMap attributes lookup filename
            legacy_serpm8_attributes (str): SERPM8 model attributes lookup filename
            navteq_attributes (str): NAVTEQ attributes lookup filename
            fdot_attributes (str): FDOT attributes lookup filename
            county_attributes (str): county agencies' attributes lookup filename
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
            msg = "'roadway_network' is missing from the method call.".format(
                roadway_network
            )
            WranglerLogger.error(msg)
            raise ValueError(msg)

        osm_lanes_attributes = (
            osm_lanes_attributes
            if osm_lanes_attributes
            else parameters.osm_lanes_attributes
        )

        if not osm_lanes_attributes:
            msg = "'osm_lanes_attributes' not found in method or lasso parameters."
            WranglerLogger.error(msg)
            raise ValueError(msg)

        legacy_serpm8_attributes = (
            legacy_serpm8_attributes
            if legacy_serpm8_attributes
            else parameters.legacy_serpm8_attributes
        )

        if not legacy_serpm8_attributes:
            msg = "'legacy_serpm8_attributes' not found in method or lasso parameters."
            WranglerLogger.error(msg)
            raise ValueError(msg)

        navteq_attributes = (
            navteq_attributes if navteq_attributes else parameters.navteq_attributes
        )

        if not navteq_attributes:
            msg = "'navteq_attributes' not found in method or lasso parameters."
            WranglerLogger.error(msg)
            raise ValueError(msg)

        fdot_attributes = (
            fdot_attributes if fdot_attributes else parameters.fdot_attributes
        )

        if not fdot_attributes:
            msg = "'fdot_attributes' not found in method or lasso parameters."
            WranglerLogger.error(msg)
            raise ValueError(msg)

        county_attributes = (
            county_attributes if county_attributes else parameters.county_attributes
        )

        if not county_attributes:
            msg = "'county_attributes' not found in method or lasso parameters."
            WranglerLogger.error(msg)
            raise ValueError(msg)

        """
        Start actual process
        """
        osm_df = pd.read_csv(osm_lanes_attributes)
        osm_df = osm_df[["shstReferenceId", "min_lanes", "max_lanes"]].rename(
            columns={"min_lanes": "osm_min_lanes", "max_lanes": "osm_max_lanes"}
        )

        serpm8_df = pd.read_csv(legacy_serpm8_attributes)
        serpm8_df = serpm8_df[["shstReferenceId", "lanes"]].rename(
            columns={"lanes": "serpm8_lanes"}
        )

        navteq_df = pd.read_csv(navteq_attributes)
        navteq_df = navteq_df[["shstReferenceId", "lanes"]].rename(
            columns={"lanes": "navteq_lanes"}
        )

        fdot_df = pd.read_csv(fdot_attributes)
        fdot_df = fdot_df[["shstReferenceId", "min_lanes", "max_lanes"]].rename(
            columns={"min_lanes": "fdot_min_lanes", "max_lanes": "fdot_max_lanes"}
        )

        county_df = pd.read_csv(county_attributes)
        county_df = county_df[["shstReferenceId", "lanes"]].rename(
            columns={"lanes": "county_lanes"}
        )

        join_gdf = pd.merge(
            roadway_network.links_df, osm_df, how="left", on="shstReferenceId"
        )

        join_gdf = pd.merge(join_gdf, serpm8_df, how="left", on="shstReferenceId")

        join_gdf = pd.merge(join_gdf, navteq_df, how="left", on="shstReferenceId")

        join_gdf = pd.merge(join_gdf, fdot_df, how="left", on="shstReferenceId")

        join_gdf = pd.merge(join_gdf, county_df, how="left", on="shstReferenceId")

        def _determine_lanes(x):
            # all 5 sources agree (very high confidence)
            if (
                (x.serpm8_lanes == x.navteq_lanes)
                and (x.serpm8_lanes >= x.fdot_min_lanes)
                and (x.serpm8_lanes <= x.fdot_max_lanes)
                and (x.serpm8_lanes == x.county_lanes)
                and (x.serpm8_lanes >= x.osm_min_lanes)
                and (x.serpm8_lanes <= x.osm_max_lanes)
            ):
                return x.osm_min_lanes
            elif (
                # 4 sources agree, except FDOT (high confidence)
                (x.serpm8_lanes == x.navteq_lanes)
                and (x.serpm8_lanes >= x.osm_min_lanes)
                and (x.serpm8_lanes <= x.osm_max_lanes)
                and (x.serpm8_lanes == x.county_lanes)
            ):
                return x.serpm8_lanes
            elif (
                # 4 sources agree, except NAVTEQ (high confidence)
                (x.serpm8_lanes == x.county_lanes)
                and (x.serpm8_lanes >= x.osm_min_lanes)
                and (x.serpm8_lanes <= x.osm_max_lanes)
                and (x.serpm8_lanes >= x.fdot_min_lanes)
                and (x.serpm8_lanes <= x.fdot_max_lanes)
            ):
                return x.serpm8_lanes
            elif (
                # 4 sources agree, except SERPM8 (high confidence)
                (x.navteq_lanes == x.county_lanes)
                and (x.navteq_lanes >= x.osm_min_lanes)
                and (x.navteq_lanes <= x.osm_max_lanes)
                and (x.navteq_lanes >= x.fdot_min_lanes)
                and (x.navteq_lanes <= x.fdot_max_lanes)
            ):
                return x.navteq_lanes
            elif (
                # 4 sources agree, except county (high confidence)
                (x.navteq_lanes == x.serpm8_lanes)
                and (x.navteq_lanes >= x.osm_min_lanes)
                and (x.navteq_lanes <= x.osm_max_lanes)
                and (x.navteq_lanes >= x.fdot_min_lanes)
                and (x.navteq_lanes <= x.fdot_max_lanes)
            ):
                return x.navteq_lanes
            elif (
                # 4 sources agree, except OSM (high confidence)
                (x.navteq_lanes == x.serpm8_lanes)
                and (x.navteq_lanes >= x.osm_min_lanes)
                and (x.navteq_lanes <= x.osm_max_lanes)
                and (x.navteq_lanes >= x.fdot_min_lanes)
                and (x.navteq_lanes <= x.fdot_max_lanes)
            ):
                return x.navteq_lanes
            elif (
                # 3 sources agree, except FDOT and county (medium high confidence)
                (x.serpm8_lanes == x.navteq_lanes)
                and (x.serpm8_lanes >= x.osm_min_lanes)
                and (x.serpm8_lanes <= x.osm_max_lanes)
            ):
                return x.serpm8_lanes
            elif (
                # 3 sources agree, except FDOT and NAVTEQ (medium high confidence)
                (x.serpm8_lanes == x.county_lanes)
                and (x.serpm8_lanes >= x.osm_min_lanes)
                and (x.serpm8_lanes <= x.osm_max_lanes)
            ):
                return x.serpm8_lanes
            elif (
                # 3 sources agree, except county and NAVTEQ (medium high confidence)
                (x.serpm8_lanes >= x.fdot_min_lanes)
                and (x.serpm8_lanes <= x.fdot_max_lanes)
                and (x.serpm8_lanes >= x.osm_min_lanes)
                and (x.serpm8_lanes <= x.osm_max_lanes)
            ):
                return x.serpm8_lanes
            elif (
                # 3 sources agree, except SERPM8 and FDOT (medium high confidence)
                (x.navteq_lanes == x.county_lanes)
                and (x.navteq_lanes >= x.osm_min_lanes)
                and (x.navteq_lanes <= x.osm_max_lanes)
            ):
                return x.navteq_lanes
            elif (
                # 3 sources agree, except SERPM8 and county (medium high confidence)
                (x.navteq_lanes >= x.fdot_min_lanes)
                and (x.navteq_lanes <= x.fdot_max_lanes)
                and (x.navteq_lanes >= x.osm_min_lanes)
                and (x.navteq_lanes <= x.osm_max_lanes)
            ):
                return x.navteq_lanes
            elif (
                # 3 sources agree, except SERPM8 and NAVTE (medium high confidence)Q
                (x.county_lanes >= x.fdot_min_lanes)
                and (x.county_lanes <= x.fdot_max_lanes)
                and (x.county_lanes >= x.osm_min_lanes)
                and (x.county_lanes <= x.osm_max_lanes)
            ):
                return x.county_lanes
            elif (
                # 3 sources agree, except OSM and county (medium high confidence)
                (x.serpm8_lanes == x.navteq_lanes)
                and (x.serpm8_lanes >= x.fdot_min_lanes)
                and (x.serpm8_lanes <= x.fdot_max_lanes)
            ):
                return x.navteq_lanes
            elif (
                # 3 sources agree, except OSM and FDO (medium high confidence)T
                (x.serpm8_lanes == x.navteq_lanes)
                and (x.serpm8_lanes == x.county_lanes)
            ):
                return x.navteq_lanes
            elif (
                # 3 sources agree, except OSM and NAVTEQ (medium high confidence)
                (x.serpm8_lanes == x.county_lanes)
                and (x.serpm8_lanes >= x.fdot_min_lanes)
                and (x.serpm8_lanes <= x.fdot_max_lanes)
            ):
                return x.serpm8_lanes
            elif (
                # 3 sources agree, except OSM and SERPM8 (medium high confidence)
                (x.navteq_lanes == x.county_lanes)
                and (x.navteq_lanes >= x.fdot_min_lanes)
                and (x.navteq_lanes <= x.fdot_max_lanes)
            ):
                return x.county_lanes
            elif (
                # 2 sources agree, SERPM8 and OSM (medium confidence)
                (x.serpm8_lanes >= x.osm_min_lanes)
                and (x.serpm8_lanes <= x.osm_max_lanes)
            ):
                return x.serpm8_lanes
            elif (
                # 2 sources agree, OSM and NAVTEQ (medium confidence)
                (x.navteq_lanes >= x.osm_min_lanes)
                and (x.navteq_lanes <= x.osm_max_lanes)
            ):
                return x.navteq_lanes
            elif (
                # 2 sources agree, OSM and county (medium confidence)
                (x.county_lanes >= x.osm_min_lanes)
                and (x.county_lanes <= x.osm_max_lanes)
            ):
                return x.county_lanes
            elif (
                # 2 sources agree, OSM and FDOT (medium confidence)
                (x.fdot_min_lanes >= x.osm_min_lanes)
                and (x.fdot_min_lanes <= x.osm_max_lanes)
            ):
                return x.osm_min_lanes
            elif (
                # 2 sources agree, SERPM8 and NAVTEQ (medium confidence)
                x.serpm8_lanes
                == x.navteq_lanes
            ):
                return x.serpm8_lanes
            elif (
                # 2 sources agree, SERPM8 and county (medium confidence)
                x.serpm8_lanes
                == x.county_lanes
            ):
                return x.serpm8_lanes
            elif (
                # 2 sources agree, SERPM8 and FDOT (medium confidence)
                (x.serpm8_lanes >= x.fdot_min_lanes)
                and (x.serpm8_lanes <= x.fdot_max_lanes)
            ):
                return x.serpm8_lanes
            elif (
                # 2 sources agree, NAVTEQ and county (medium confidence)
                x.navteq_lanes
                == x.county_lanes
            ):
                return x.county_lanes
            elif (
                # 2 sources agree, NAVTEQ and FDOT (medium confidence)
                (x.navteq_lanes >= x.fdot_min_lanes)
                and (x.navteq_lanes <= x.fdot_max_lanes)
            ):
                return x.navteq_lanes
            elif (
                # 2 sources agree, county and FDOT (medium confidence)
                (x.county_lanes >= x.fdot_min_lanes)
                and (x.county_lanes <= x.fdot_max_lanes)
            ):
                return x.county_lanes
            elif (
                # 1 source exists, OSM (low condidence)
                x.osm_max_lanes - x.osm_min_lanes
                < 2
            ):
                return x.osm_min_lanes
            elif (
                # 1 source exist, SERPM8 (low condidence)
                x.serpm8_lanes
                > 0
            ):
                return x.serpm8_lanes
            elif (
                # 1 source exist, NAVTEQ (low condidence)
                x.navteq_lanes
                > 0
            ):
                return x.navteq_lanes
            elif (
                # 1 source exist, county (low condidence)
                x.county_lanes
                > 0
            ):
                return x.county_lanes
            elif (
                # 1 source exist, FDOT (low condidence)
                x.fdot_min_lanes
                > 0
            ):
                return x.fdot_min_lanes
            else:  # no source available (low condidence)
                return 1

        print(join_gdf.columns)

        join_gdf[network_variable] = join_gdf.apply(
            lambda x: _determine_lanes(x), axis=1
        )

        roadway_network.links_df[network_variable] = join_gdf[network_variable]

        WranglerLogger.info(
            "Finished determining number of lanes using variable: {}".format(
                network_variable
            )
        )

        return roadway_network

    def calculate_facility_type(
        self,
        roadway_network=None,
        parameters=None,
        network_variable="ft",
        network_variable_lanes="lanes",
        facility_type_dict=None,
        overwrite: bool = False,
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

        join_gdf["lanes"] = join_gdf["lanes"].astype(int)

        print("unique oneWay values:")
        print(join_gdf["oneWay"].unique())

        join_gdf["oneWay"].fillna("False", inplace=True)
        join_gdf["oneWay"] = join_gdf["oneWay"].astype(str)
        join_gdf["oneWay"] = join_gdf["oneWay"].replace(
            {"True": "1", "False": "0", "": "0"}
        )
        print("unique oneWay values after type change:")
        print(join_gdf["oneWay"].unique())
        join_gdf["oneWay"] = join_gdf["oneWay"].astype(int)

        print("unique oneWay values after type change:")
        print(join_gdf["oneWay"].unique())

        print("oneway summary")
        print(join_gdf["oneWay"].value_counts())

        print("lanes summary")
        print(join_gdf["lanes"].value_counts())

        def _calculate_facility_type(x):
            # facility_type heuristics
            # freeway
            if x.roadway == "motorway" and x.oneWay == 1:
                return 1

            # expressway
            elif x.roadway == "trunk" and x.oneWay == 1:
                return 2

            # ramp
            elif x.roadway in ["motorway_link", "trunk_link"] and x.oneWay == 1:
                return 3

            # divided arterial
            elif (
                x.roadway in ["primary", "secondary", "tertiary"]
                and x.oneWay == 1
                and x.lanes > 1
            ):
                return 4

            # undivided arterial
            elif (
                x.roadway in ["trunk", "primary", "secondary", "tertiary"]
                and x.oneWay == 0
                and x.lanes > 1
            ):
                return 5

            # collector
            elif x.roadway == "trunk" and x.oneWay == 0 and x.lanes == 1:
                return 6
            elif x.roadway in [
                "primary",
                "secondary",
                "tertiary",
                "primary_link",
                "secondary_link",
                "tertiary_link",
            ]:
                return 6

            # local
            elif x.roadway in ["residential", "residential_link"]:
                return 7

            else:
                return 99

        join_gdf[network_variable] = join_gdf.apply(
            lambda x: _calculate_facility_type(x), axis=1
        )

        roadway_network.links_df[network_variable + "_cal"] = join_gdf[network_variable]

        if update_network_variable:
            roadway_network.links_df[network_variable] = np.where(
                roadway_network.links_df[network_variable].notnull(),
                roadway_network.links_df[network_variable],
                roadway_network.links_df[network_variable + "_cal"],
            )
        else:
            roadway_network.links_df[network_variable] = roadway_network.links_df[
                network_variable + "_cal"
            ]

        WranglerLogger.info(
            "Finished calculating roadway class variable: {}".format(network_variable)
        )

        return roadway_network

    def calculate_transit(
        self,
        roadway_network=None,
        parameters=None,
        network_variable: str = "transit",
        overwrite: bool = False,
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
            msg = "'roadway_network' is missing from the method call.".format(
                roadway_network
            )
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
                0,
            )

        if "bus_only" in roadway_network.links_df.columns:
            roadway_network.links_df[network_variable] = np.where(
                (roadway_network.links_df.bus_only == 1)
                | (roadway_network.links_df.rail_only == 1),
                1,
                0,
            )
        else:
            roadway_network.links_df[network_variable] = np.where(
                (roadway_network.links_df.rail_only == 1), 1, 0
            )

        WranglerLogger.info(
            "Finished determining transit-only variable: {}".format(network_variable)
        )

        return roadway_network

    def assign_link_id_by_county(roadway_network=None, add_links_df=None):
        """
        when adding new links, assign id by county rules
        Args:
            roadway_network (RoadwayNetwork): Input Wrangler roadway network
            add_links_df: new links
        Returns:
            add_links_df with unique link ids
        """

        county_last_link_id_df = (
            roadway_network.links_df.groupby("county")["model_link_id"]
            .max()
            .reset_index()
            .rename(columns={"model_link_id": "county_last_id"})
        )

        if "model_link_id" in add_links_df.columns:
            add_links_df.drop(["model_link_id"], axis=1, inplace=True)

        if "county_last_id" in add_links_df.columns:
            add_links_df.drop(["county_last_id"], axis=1, inplace=True)

        add_links_df = pd.merge(
            add_links_df, county_last_link_id_df, how="left", on="county"
        )

        add_links_df["model_link_id"] = add_links_df.groupby(["county"]).cumcount() + 1

        add_links_df["model_link_id"] = (
            add_links_df["model_link_id"] + add_links_df["county_last_id"]
        )

        return add_links_df

    def add_centroid_and_centroid_connector(
        self,
        roadway_network=None,
        parameters=None,
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

        WranglerLogger.info(
            "Adding centroid and centroid connector to standard network"
        )

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
            msg = "'roadway_network' is missing from the method call.".format(
                roadway_network
            )
            WranglerLogger.error(msg)
            raise ValueError(msg)

        centroid_file = centroid_file if centroid_file else parameters.centroid_file

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

        centroid_gdf["X"] = centroid_gdf.geometry.apply(lambda g: g.x)
        centroid_gdf["Y"] = centroid_gdf.geometry.apply(lambda g: g.y)

        roadway_network.nodes_df = pd.concat(
            [
                roadway_network.nodes_df,
                centroid_gdf[
                    list(
                        set(roadway_network.nodes_df.columns)
                        & set(centroid_gdf.columns)
                    )
                ],
            ],
            sort=False,
            ignore_index=True,
        )

        # TODO: should update this to assign model_link_id in sequential order
        centroid_connector_link_gdf = self.assign_link_id_by_county(
            roadway_network, centroid_connector_link_gdf
        )

        roadway_network.links_df = pd.concat(
            [
                roadway_network.links_df,
                centroid_connector_link_gdf[
                    list(
                        set(roadway_network.links_df.columns)
                        & set(centroid_connector_link_gdf.columns)
                    )
                ],
            ],
            sort=False,
            ignore_index=True,
        )

        roadway_network.shapes_df = pd.concat(
            [
                roadway_network.shapes_df,
                centroid_connector_shape_gdf[
                    list(
                        set(roadway_network.shapes_df.columns)
                        & set(centroid_connector_shape_gdf.columns)
                    )
                ],
            ],
            sort=False,
            ignore_index=True,
        )

        WranglerLogger.info("Finished adding centroid and centroid connectors")

        return roadway_network

    def add_opposite_direction_to_link(self, link_gdf, nodes_df, links_df):

        """
        create and add the opposite direction of links to a dataframe

        Args:
            links_gdf: Input link dataframe with A and B node information
            nodes_df: Input Wrangler roadway network nodes_df
            links_df: Input Wrangler roadway network links_df

        Returns:
            roadway object
        """

        link_gdf = pd.concat(
            [link_gdf, link_gdf.rename(columns={"A": "B", "B": "A"})], sort=False, ignore_index=True
        )

        link_gdf = pd.merge(
            link_gdf,
            nodes_df[["model_node_id", "X", "Y"]].rename(
                columns={"model_node_id": "A", "X": "A_X", "Y": "A_Y"}
            ),
            how="left",
            on="A",
        )

        link_gdf = pd.merge(
            link_gdf,
            nodes_df[["model_node_id", "X", "Y"]].rename(
                columns={"model_node_id": "B", "X": "B_X", "Y": "B_Y"}
            ),
            how="left",
            on="B",
        )

        link_gdf["geometry"] = link_gdf.apply(
            lambda g: LineString([Point(g.A_X, g.A_Y), Point(g.B_X, g.B_Y)]), axis=1
        )

        link_gdf = gpd.GeoDataFrame(link_gdf, geometry=link_gdf["geometry"], crs=links_df.crs)

        for c in links_df.columns:
            if c not in link_gdf.columns:
                if c not in ["county", "shstGeometryId", "cntype"]:
                    link_gdf[c] = 0
                else:
                    link_gdf[c] = ""

        link_gdf["A"] = link_gdf["A"].astype(int)
        link_gdf["B"] = link_gdf["B"].astype(int)
        link_gdf = link_gdf.drop(["A_X", "A_Y", "B_X", "B_Y"], axis=1)

        return link_gdf

    def build_pnr_connections(
        self,
        roadway_network=None,
        pnr_nodes: gpd.GeoDataFrame = None,
        parameters=None,
        build_taz_walk_connector: bool = True,
        output_proj=None,
    ):
        """
        (1) add pnr nodes;
        (2) build links connecting pnr nodes and nearest walk and drive nodes;
        (3) build taz walk access/egress connectors.

        Args:
            roadway_network (RoadwayNetwork): Input Wrangler roadway network
            parameters (Parameters): Lasso parameters object
            build_taz_walk_connector (Bool): True if building taz walk access/egress connectors
            output_epsg (int): epsg number of output network.

        Returns:
            roadway object
        """

        WranglerLogger.info("Building PNR connections")

        """
        Verify inputs
        """

        output_proj = output_proj if output_proj else parameters.output_proj

        """
        Start actual process
        """

        orig_crs = roadway_network.nodes_df.crs  # record original crs
        interim_crs = CRS("epsg:2236")  # crs for nearest calculation

        roadway_network.links_df = roadway_network.links_df.to_crs(interim_crs)
        roadway_network.shapes_df = roadway_network.shapes_df.to_crs(interim_crs)
        roadway_network.nodes_df = roadway_network.nodes_df.to_crs(interim_crs)
        roadway_network.nodes_df["X"] = roadway_network.nodes_df["geometry"].x
        roadway_network.nodes_df["Y"] = roadway_network.nodes_df["geometry"].y

        # (1) add pnr nodes
        # read pnr parking location
        print("read pnr parking location")
        pnr_nodes_df = pnr_nodes.copy()
        pnr_nodes_df = pnr_nodes_df.to_crs(interim_crs)
        pnr_nodes_df["X"] = pnr_nodes_df["geometry"].x
        pnr_nodes_df["Y"] = pnr_nodes_df["geometry"].y

        # assign a model_node_id to pnr parking node
        pnr_nodes_df["model_node_id"] = (
            roadway_network.nodes_df.model_node_id.max() + pnr_nodes_df.index + 1
        )
        pnr_nodes_df["pnr"] = 1

        # add pnr parking nodes to node_df
        print("add pnr parking nodes to node_df")
        roadway_network.nodes_df = pd.concat(
            [roadway_network.nodes_df, pnr_nodes_df],
            sort=False,
            ignore_index=True,
        )
        roadway_network.nodes_df["pnr"] = roadway_network.nodes_df["pnr"].fillna(0).astype(int)

        # (2) build links connecting pnr nodes and nearest walk and drive nodes
        # select walk and drive nodes, save to separate lists
        print("build links connecting pnr nodes and nearest walk and drive nodes")
        dr_wlk_nodes_df = roadway_network.nodes_df[
            ((roadway_network.nodes_df.drive_access == 1) & (roadway_network.nodes_df.walk_access == 1))
            & ~(roadway_network.nodes_df.model_node_id.isin(pnr_nodes_df["model_node_id"].to_list()))
            # exclude taz/maz/external nodes
            & (roadway_network.nodes_df.model_node_id > 20000)  # TODO: add zone list to parameter
        ].copy()

        # for each pnr nodes, search for the nearest walk and drive nodes
        dr_wlk_node_ref = dr_wlk_nodes_df[["X", "Y"]].values
        tree = cKDTree(dr_wlk_node_ref)

        for index, row in pnr_nodes_df.iterrows():
            point = row[["X", "Y"]].values
            dd, ii = tree.query(point, k=1)
            pnr_nodes_df.loc[index, "A"] = dr_wlk_nodes_df.iloc[ii].model_node_id

        # create links between pnr nodes and their nearest walk and drive nodes
        print("create links between pnr nodes and their nearest walk and drive nodes")
        if len(pnr_nodes_df) > 0 and (
            "A" in pnr_nodes_df.columns
        ):  #'A' is the nearest walk and drive node
            pnr_nodes_df = pnr_nodes_df[pnr_nodes_df["A"].notna()].reset_index(drop=True)
            pnr_link_gdf = pnr_nodes_df[["A", "model_node_id"]].copy()
            pnr_link_gdf.rename(columns={"model_node_id": "B"}, inplace=True)

            pnr_link_gdf = self.add_opposite_direction_to_link(
                pnr_link_gdf, nodes_df=roadway_network.nodes_df, links_df=roadway_network.links_df
            )

            # specify link variables
            pnr_link_gdf["model_link_id"] = (
                roadway_network.links_df["model_link_id"].max() + pnr_link_gdf.index + 1
            )
            pnr_link_gdf["shstGeometryId"] = range(1, 1 + len(pnr_link_gdf))
            pnr_link_gdf["shstGeometryId"] = pnr_link_gdf["shstGeometryId"].apply(
                lambda x: "pnr" + str(x)
            )
            pnr_link_gdf["id"] = pnr_link_gdf["shstGeometryId"]
            pnr_link_gdf["roadway"] = "pnr"
            pnr_link_gdf["lanes"] = 1
            pnr_link_gdf["walk_access"] = 1
            pnr_link_gdf["drive_access"] = 1
            pnr_link_gdf["ftype"] = 9

            roadway_network.links_df = pd.concat(
                [roadway_network.links_df, pnr_link_gdf], sort=False, ignore_index=True
            )
            roadway_network.links_df.drop_duplicates(subset=["A", "B"], inplace=True)

            # update shapes_df
            pnr_shapes_df = pnr_link_gdf.copy()
            pnr_shapes_df = pnr_shapes_df[["id", "geometry"]]
            roadway_network.shapes_df = pd.concat(
                [roadway_network.shapes_df, pnr_shapes_df]
            ).reset_index(drop=True)

        # (3) build TAZ walk connectors
        if build_taz_walk_connector:
            print("build taz walk connector")
            # select TAZ centroids
            centroids_df = roadway_network.nodes_df[
                roadway_network.nodes_df.model_node_id <= 6400  # TODO: add TAZ list to parameter
            ].copy()

            # for each TAZ centroid, make a connection to the nearest walkable node
            wlk_nodes_df = roadway_network.nodes_df[
                (roadway_network.nodes_df.walk_access == 1)
                # exclude pnr nodes
                & ~(
                    roadway_network.nodes_df.model_node_id.isin(pnr_nodes_df["model_node_id"].to_list())
                )
                # exclude taz/maz/external nodes
                & (roadway_network.nodes_df.model_node_id > 20000)  # TODO: add zone list to parameter
            ].copy()

            wlk_node_ref = wlk_nodes_df[["X", "Y"]].values
            walk_tree = cKDTree(wlk_node_ref)

            for index, row in centroids_df.iterrows():
                point = row[["X", "Y"]].values
                dd, ii = walk_tree.query(point, k=1)
                centroids_df.loc[index, "A"] = wlk_nodes_df.iloc[ii].model_node_id

            # create links between tazs and nearest walk nodes
            print("create links between tazs and nearest walk nodes")
            if len(centroids_df) > 0 and (
                "A" in centroids_df.columns
            ):  #'A' is the nearest walk and drive node

                taz_walk_access_df = centroids_df[centroids_df["A"].notna()].reset_index(drop=True)
                taz_walk_access_gdf = taz_walk_access_df[["A", "model_node_id"]].copy()
                taz_walk_access_gdf.rename(columns={"model_node_id": "B"}, inplace=True)

                taz_walk_access_gdf = self.add_opposite_direction_to_link(
                    taz_walk_access_gdf,
                    nodes_df=roadway_network.nodes_df,
                    links_df=roadway_network.links_df,
                )

                # specify link variables
                taz_walk_access_gdf["model_link_id"] = (
                    roadway_network.links_df["model_link_id"].max() + taz_walk_access_gdf.index + 1
                )
                taz_walk_access_gdf["shstGeometryId"] = range(1, 1 + len(taz_walk_access_gdf))
                taz_walk_access_gdf["shstGeometryId"] = taz_walk_access_gdf["shstGeometryId"].apply(
                    lambda x: "taz" + str(x)
                )
                taz_walk_access_gdf["id"] = taz_walk_access_gdf["shstGeometryId"]
                taz_walk_access_gdf["roadway"] = "taz"
                taz_walk_access_gdf["lanes"] = 1
                taz_walk_access_gdf["walk_access"] = 1
                taz_walk_access_gdf["ftype"] = 9

                roadway_network.links_df = pd.concat(
                    [roadway_network.links_df, taz_walk_access_gdf], sort=False, ignore_index=True
                )
                roadway_network.links_df.drop_duplicates(subset=["A", "B"], inplace=True)

                # update shapes_df
                taz_walk_access_shapes = taz_walk_access_gdf.copy()
                taz_walk_access_shapes = taz_walk_access_shapes[["id", "geometry"]]
                roadway_network.shapes_df = pd.concat(
                    [roadway_network.shapes_df, taz_walk_access_shapes]
                ).reset_index(drop=True)

        roadway_network.links_df = roadway_network.links_df.to_crs(orig_crs)
        roadway_network.shapes_df = roadway_network.shapes_df.to_crs(orig_crs)
        roadway_network.nodes_df = roadway_network.nodes_df.to_crs(orig_crs)
        roadway_network.nodes_df["X"] = roadway_network.nodes_df["geometry"].x
        roadway_network.nodes_df["Y"] = roadway_network.nodes_df["geometry"].y

        return roadway_network

    def roadway_standard_to_seflorida_network(
        self, roadway_network=None, parameters=None, output_proj=None
    ):
        """
        Rename and format roadway attributes to be consistent with what mtc's model is expecting.
        Args:
            output_epsg (int): epsg number of output network.
        Returns:
            None
        """

        WranglerLogger.info(
            "Renaming roadway attributes to be consistent with what seflorida's model is expecting"
        )

        """
        Verify inputs
        """
        output_proj = output_proj if output_proj else parameters.output_proj
        print(output_proj)

        """
        Start actual process
        """
        if "managed" in roadway_network.links_df.columns:
            WranglerLogger.info("Creating managed lane network.")
            roadway_network.create_managed_lane_network(in_place=True)
        else:
            WranglerLogger.info("Didn't detect managed lanes in network.")

        roadway_network = self.calculate_transit(roadway_network, parameters)
        roadway_network = self.calculate_facility_type(
            roadway_network, parameters, update_network_variable=True
        )

        roadway_network.calculate_distance(overwrite=True)

        roadway_network.fill_na()
        WranglerLogger.info("Splitting variables by time period and category")
        roadway_network.split_properties_by_time_period_and_category()
        roadway_network.convert_int()

        roadway_network.links_seflorida_df = roadway_network.links_df.copy()
        roadway_network.nodes_seflorida_df = roadway_network.nodes_df.copy()

        roadway_network.links_seflorida_df = pd.merge(
            roadway_network.links_seflorida_df.drop("geometry", axis=1),
            roadway_network.shapes_df[["id", "geometry"]],
            how="left",
            on="id",
        )

        print(f"roadway_network.crs: {roadway_network.crs}")

        roadway_network.links_seflorida_df.crs = roadway_network.crs
        roadway_network.nodes_seflorida_df.crs = roadway_network.crs
        WranglerLogger.info(
            "Setting Coordinate Reference System to {}".format(output_proj)
        )
        roadway_network.links_seflorida_df = roadway_network.links_seflorida_df.to_crs(
            crs=output_proj
        )
        roadway_network.nodes_seflorida_df = roadway_network.nodes_seflorida_df.to_crs(
            crs=output_proj
        )

        roadway_network.nodes_seflorida_df[
            "X"
        ] = roadway_network.nodes_seflorida_df.geometry.apply(lambda g: g.x)
        roadway_network.nodes_seflorida_df[
            "Y"
        ] = roadway_network.nodes_seflorida_df.geometry.apply(lambda g: g.y)

        # CUBE expect node id to be N
        roadway_network.nodes_seflorida_df.rename(
            columns={"model_node_id": "N"}, inplace=True
        )

        return roadway_network
