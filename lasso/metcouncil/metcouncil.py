import os
from typing import Collection

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
from ..util import add_id_field_to_shapefile

class MetCouncilRoadwayNetwork(ModelRoadwayNetwork):

    @staticmethod
    def convert_model_roadway_net_to_metcouncil(model_roadway_network: ModelRoadwayNetwork)
        WranglerLogger("Converting ModelRoadwayNetwork to MetCouncil flavor.")
        model_roadway_network.__class__ = MetCouncilModelRoadwayNetwork

    @staticmethod
    def read_as_metcouncil(
        link_filename: str,
        node_filename: str,
        shape_filename: str,
        fast: bool = False,
        parameters: Union[dict, Parameters] = {},
        **kwargs,
    ):
        """
        Reads in links and nodes network standard.

        Args:
            link_filename (str): File path to link json.
            node_filename (str): File path to node geojson.
            shape_filename (str): File path to link true shape geojson
            fast (bool): boolean that will skip validation to speed up read time.
            parameters: dictionary of parameter settings (see Parameters class) or an instance of Parameters. 
                If not specified, will use default MetCouncil parameters.
        Returns:
            MetCouncilModelRoadwayNetwork
        """
        WranglerLogger.info("Reading as a MetCouncil Model Roadway Network")
        model_roadway_network = read(link_filename, node_filename, shape_filename, fast, parameters, **kwargs)
        model_roadway_network.__class__ = MetCouncilModelRoadwayNetwork
        return model_roadway_network

    @staticmethod
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
            GeoDataFrame of links_df
        """

        if not roadway_ps: roadway_ps = roadway_net.parameters.roadway_network_ps

        msg = "Calculating MetCouncil number of lanes.\n\
            Calculatung # of lanes using roadway_value_lookups['lanes'] = {}".format(
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

        _links_df = update_df(
            roadway_net.links_df,
            join_df,
            "model_link_id",
            update_fields=[network_variable],
            method=update_method,
        )

        WranglerLogger.info(
            "Finished calculating number of lanes to: {}".format(network_variable)
        )

        return _links_df

    @staticmethod
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

    @staticmethod
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

    @staticmethod
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

    @staticmethod
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
    
    @staticmethod
    def calculate_assign_group_and_roadway_class(
        links_df: GeoDataFrame,
        roadway_ps: RoadwayNetworkModelParameters,
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
            links_df: Links dataframe
            roadway_ps: A RoadwayNetworkModelParameters
            assign_group_variable_name: Name of the variable assignment group should be written to.  Default to "assign_group".
            road_class_variable_name: Name of the variable roadway class should be written to. Default to "roadway_class".
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
        _links_update_df  = _calculate_mrcc_route_sys(links_df,roadway_ps)
        _links_update_df = _calculate_widot_rdwy_ctgy(_links_update_df,roadway_ps)

        # Get initial assignment group and roadway class from lookup tables starting with OSM and overlaying with MnDOT and then WiDOT
        _links_update_df = roadway_ps.roadway_value_lookups["osm_roadway_assigngrp_mapping"].apply_mapping(_links_update_df)
        _links_update_df = roadway_ps.roadway_value_lookups["mrcc_roadway_assigngrp_mapping"].apply_mapping(_links_update_df)
        _links_updatet_df = roadway_ps.roadway_value_lookups["widot_roadway_assigngrp_mapping"].apply_mapping(_links_update_df)

        # Apply more sophisticated rules for final variable calculation
        _links_updatet_df[assign_group_variable_name] = _links_out_df.apply(
            lambda x: _set_final_assignment_group(x), axis=1
        )
        _links_update_df[road_class_variable_name] = _links_out_df.apply(
            lambda x: _set_final_roadway_class(x), axis=1
        )

        # Update roadway class and assignment group variables in the roadway network 
        _links_out_df = update_df(
            roadway_net.links_df,
            _links_update_df,
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

        return _links_out_df

    def add_met_council_calculated_roadway_variables(self, links_df: GeoDataFrame) -> None:
        """
        Adds calculated roadway variables to specified link dataframe.

        Args:
            link_df: specified link dataframe (model_links_df or links_df)
        """
        WranglerLogger.info("Creating metcouncil calculated roadway variables.")

        links_df = self.calculate_area_type(links_df)
        links_df = self.calculate_county_mpo(links_df)

        links_df = self.add_counts(links_df)

        mc_variables_init_dict = {
            "count_year": 2017,
            "managed": 0,
            "ML_lanes": 0,
            "segment_id": 0,
            "HOV": 0,
        }

        links_df = self.add_placeholder_variables(links_df, variables_init_dict=mc_variables_init_dict)
        
        return links_df

    def calculate_area_type(self, links_df: GeoDataFrame, roadway_ps: RoadwayNetworkModelParameters= None) -> GeoDataFrame:
        """
        Add area type values to roadway network.
        Uses the RoadwayNetworkModelParameters:
        - roadway_overlays["area_type"]
        - roadway_value_lookups["area_type_codes_dict"]
        - roadway_value_lookups["mc_mpo_counties_dict"]

        Args:
            links_df: the input ModelRoadwayNetwork.
            roadway_ps: overrides roadway_ps from roadway_net

        Returns: ModelRoadwayNetwork with area type
        """
        roadway_ps = self.parameters.roadway_network_ps if roadway_ps is None else roadway_ps

        links_df["area_type_name"] = roadway_net.add_polygon_overlay_to_links(
            roadway_ps.roadway_overlays["area_type"],
            method = "link_centroid",
        )

        links_df["area_type_name"] = self.add_polygon_overlay_to_links(
            roadway_ps.roadway_overlays["downtown_area"],
            method = "link_centroid",
            fill_value = "downtown"
        )

        links_df["area_type"] = (
            links_df["area_type_name"]
            .map(
                roadway_ps.roadway_value_lookups["area_type_codes_dict"]
                .fillna(1)
                .astype(int)
            )
        )

        return links_df

    def calculate_county_mpo(self, links_df: DataFrame, roadway_ps: RoadwayNetworkModelParameters= None) -> DataFrame:
        """
        Add County and MPO variables to roadway network.
        Uses the RoadwayNetworkModelParameters:
        - roadway_overlays["counties"]
        - roadway_value_lookups["county_codes_dict"]
        - roadway_value_lookups["mc_mpo_counties_dict"]

        Args:
            roadway_net: the input ModelRoadwayNetwork.
            roadway_ps: overrides roadway_ps from roadway_net

        Returns: ModelRoadwayNetwork with counties and MPO variables
        """
        if roadway_ps is None: roadway_ps = self.parameters.roadway_network_ps 

        links_df["county_name"] = roadway_net.add_polygon_overlay_to_links(
            roadway_ps.roadway_overlays["counties"],
            method = "link_centroid",
        )

        links_df["county"] = (
            links_df["county_name"]
            .map(
                roadway_ps.roadway_value_lookups["county_codes_dict"]
                .fillna(10)
                .astype(int)
            )
        )

        links_df["mpo"] = (
            links_df["county_name"]
            .map(
                roadway_ps.roadway_value_lookups["mc_mpo_counties_dict"]
                .fillna(0)
                .astype(int)
            )
        )

        return links_df


    def roadway_standard_to_met_council_network(roadway_net: ModelRoadwayNetwork) -> Collection[GeoDataFrame, GeoDataFrame, List]:
        """
        Rename and format roadway attributes to be consistent with what metcouncil's model is expecting.

        Args:
            roadway_net: 

        Returns:
            tuple (model_links_df,nodes_df, shapes_df, list of steps completed) where list of steps completed is 
            a set of link_geometry_complete, geography_complete, field_name_complete, and types_compelte. 
        """

        WranglerLogger.info(
            "Transforming the standard roadway network into the format and variables that metcouncil's model is expecting"
        )

        """
        Verify inputs
        """

        output_epsg = output_epsg if output_epsg else roadway_net.parameters.file_ps.output_epsg

        """
        Start actual process
        """
        model_links_df, nodes_df, shapes_df = roadway_net.create_managed_lane_network()

        model_links_df = roadway_net.calculate_centroidconnect(model_links_df)
        model_links_df = roadway_net.calculate_distance(model_links_df,overwrite=True)

        roadway_net.coerce_network_types()

        roadway_net.calculate_centroidconnect()
        roadway_net.create_calculated_variables()
        
        roadway_net.coerce_types()
        roadway_net.split_properties_by_time_period_and_category()
        

            self.model_links_df = self.calculate_distance(self.model_links_df)

            self.model_links_df = self.coerce_types(self.model_links_df)
            self.model_nodes_df = self.coerce_types(self.model_nodes_df)

            self.model_links_df = self.split_properties_by_time_period_and_category(self.model_links_df)

            self.model_links_df = self.replace_geometry_with_true_shape(self.model_links_df)

        roadway_net.model_metcouncil_df = roadway_net.links_df.copy()
        roadway_net.model_metcouncil_df = roadway_net.nodes_df.copy()

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
        self.nodes_model_df.rename(columns={"model_node_id": "N"}, inplace=True)


        self.ready_for_model = True
