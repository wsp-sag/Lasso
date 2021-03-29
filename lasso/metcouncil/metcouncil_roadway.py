from typing import Mapping, Any

from pandas import DataFrame
from geopandas import GeoDataFrame

from network_wrangler import update_df

from ..parameters import Parameters, RoadwayNetworkModelParameters
from ..model_roadway import ModelRoadwayNetwork
from ..logger import WranglerLogger

from .defaults import MC_DEFAULT_PARAMS


class MetCouncilRoadwayNetwork(ModelRoadwayNetwork):
    @staticmethod
    def convert_model_roadway_net_to_metcouncil(
        model_roadway_network: ModelRoadwayNetwork,
    ) -> None:
        WranglerLogger("Converting ModelRoadwayNetwork to MetCouncil flavor.")
        model_roadway_network.__class__ = MetCouncilRoadwayNetwork

    @staticmethod
    def read(
        link_filename: str,
        node_filename: str,
        shape_filename: str,
        fast: bool = False,
        parameters: Parameters = None,
        parameters_dict: Mapping[str, Any] = {},
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
                If not specified, will use default MetCouncil parameters overridden by any relevant
                parameters in parameters_dict or in other, additional kwargs.
            parameters_dict: dictionary of parameter settings which override parameters instance.
                Defaults to {}.
        Returns:
            MetCouncilModelRoadwayNetwork
        """
        WranglerLogger.info("Reading as a MetCouncil Model Roadway Network")

        if not parameters:
            _params_dict = MC_DEFAULT_PARAMS
            _params_dict.update(parameters_dict)
            if kwargs:
                _params_dict.update(
                    {
                        k: v
                        for k, v in kwargs.items()
                        if k in Parameters.parameters_list()
                    }
                )

            WranglerLogger.debug(
                "[metcouncil.__init__] Initializing parameters with MetCouncil defaults."
            )
            # WranglerLogger.debug(f"[metcouncil.__init__.MC_DEFAULT_PARAMS] {_params_dict}")

            parameters = Parameters.initialize(base_params_dict=_params_dict)

        WranglerLogger.debug(
            "Using MetCouncilRoadwayNetwork parameters:      {}".format(parameters)
        )

        model_roadway_network = ModelRoadwayNetwork.read(
            link_filename,
            node_filename,
            shape_filename,
            fast=fast,
            parameters=parameters,
            **kwargs,
        )
        model_roadway_network.__class__ = MetCouncilRoadwayNetwork
        return model_roadway_network

    def calculate_number_of_lanes(
        self,
        links_df: DataFrame = None,
        network_variable="lanes",
        update_method: str = "update if found",
    ) -> DataFrame:

        """
        Computes the number of lanes using a heuristic defined in this method.

        Args:
            links_df: links dataframe to calculate number of lanes for. Defaults to self.links_df.
            network_variable: Name of the lanes variable
            update_method: update_method: update method to use in network_wrangler.update_df.
                One of "overwrite all",
                "update if found", or "update nan". Defaults to "update if found"

        Returns:
            GeoDataFrame of links_df
        """
        if links_df is None:
            links_df = self.links_df

        _lanes_value_lookup = self.parameters.roadway_network_ps.roadway_value_lookups[
            "lanes"
        ]
        _max_taz = self.parameters.roadway_network_ps.max_taz
        roadway_params = self.parameters.roadway_network_ps
        _centroidconnector_lanes = roadway_params.centroid_connector_properties["lanes"]

        msg = "Parameter set: {}\nMAX TAZ: {}\nCentroid Connector Lanes: {}".format(
            self.parameters.name, _max_taz, _centroidconnector_lanes
        )
        WranglerLogger.debug(msg)

        msg = "Calculating MetCouncil number of lanes.\n\
            Calculating # of lanes using roadway_value_lookups['lanes'] = {}//".format(
            _lanes_value_lookup
        )
        WranglerLogger.debug(msg)

        _update_df = _lanes_value_lookup.apply_mapping(links_df)
        _update_df = _update_df.fillna(0)

        msg = f"""[roadway_ps.roadway_value_lookups.lanes._mapping_df.value_counts()]:
            \n{_lanes_value_lookup._mapping_df.describe()}"""
        WranglerLogger.debug(msg)

        msg = f"""[MetcouncilRoadwayNetwork.calculate_number_of_lanes._update_df.value_counts()-1]:
            \n{_update_df.lanes.value_counts()}"""
        WranglerLogger.debug(msg)

        msg = f"""[MetcouncilRoadwayNetwork.calculate_number_of_lanes._update_df.columns -1]:
            \n{_update_df.columns}"""
        WranglerLogger.info(msg)

        def _set_lanes(x):
            if (x.A <= _max_taz) or (x.B <= _max_taz):
                return int(_centroidconnector_lanes)
            elif any([x.anoka, x.hennepin, x.carver, x.dakota, x.washington]) > 0:
                return int(max([x.anoka, x.hennepin, x.carver, x.dakota, x.washington]))
            elif max([x.widot, x.mndot]) > 0:
                return int(max([x.widot, x.mndot]))
            elif x.osm_min > 0:
                return int(x.osm_min)
            elif x.naive > 0:
                return int(x.naive)
            else:
                ValueError("Appropriate lanes criteria not found: {}".format(x))

        _update_df[network_variable] = _update_df.apply(lambda x: _set_lanes(x), axis=1)

        msg = f"""[MetcouncilRoadwayNetwork.calculate_number_of_lanes._update_df.value_counts()-2]:
        \n{_update_df.value_counts()}"""
        WranglerLogger.debug(msg)

        _output_links_df = update_df(
            links_df,
            _update_df,
            "model_link_id",
            update_fields=[network_variable],
            method=update_method,
        )

        msg = f"""[MetcouncilRoadwayNetwork.calculate_number_of_lanes._output_links_df.value_counts()]:
            \n{_update_df.lanes.value_counts()}"""
        WranglerLogger.debug(msg)

        WranglerLogger.info(
            f"Finished calculating number of lanes to: {network_variable}"
        )

        return _output_links_df

    def _set_final_assignment_group(self, x):
        """

        Args:
            x: row in link dataframe
        """

        _max_taz = self.parameters.roadway_network_ps.max_taz

        if (x.A <= _max_taz) or (x.B <= _max_taz):
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

    def _set_final_roadway_class(self, x):
        """

        Args:
            x: row in link dataframe
        """
        _max_taz = self.parameters.roadway_network_ps.max_taz

        if (x.A <= _max_taz) or (x.B <= _max_taz):
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

    def _calculate_mrcc_route_sys(
        self, links_df: GeoDataFrame, update_method: str = "update if found"
    ):
        """
        Get MRCC route_sys variable from shstGeometryID.
        1. shstGeometryId --> LINK_ID
        2. LINK_ID --> route_sys

        Args:
            links_df: links GeoDataFrame
            update_method: update method to use in network_wrangler.update_df. One of
                "overwrite all", "update if found", or "update nan".
                Defaults to "update if found"

        Returns:
            link_df with route_sys column added
        """

        roadway_ps = self.parameters.roadway_network_ps

        # 1. shstGeometryId --> LINK_ID
        # Expected columns shstReferenceId,shstGeometryId,pp_link_id (which is the LINK_ID),score
        _join_df = roadway_ps.roadway_value_lookups[
            "mrcc_shst_2_pp_link_id"
        ].apply_mapping(links_df)

        # LINK_ID --> route_sys
        # Expected columns  LINK_ID, route_sys
        _join_df = roadway_ps.roadway_value_lookups[
            "pp_link_id_2_route_sys"
        ].apply_mapping(_join_df)

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

        _join_df.drop_duplicates(subset=["model_link_id"], keep="last", inplace=True)

        links_route_sys_df = update_df(
            links_df,
            _join_df,
            "model_link_id",
            update_fields=["mrcc_route_sys"],
            method=update_method,
        )

        return links_route_sys_df

    def _calculate_widot_rdwy_ctgy(
        self, links_df: GeoDataFrame, update_method: str = "update if found"
    ):
        """
        Get WiDot rdwy ctgy from shstGeometryID.
        1. shstGeometryId --> LINK_ID
        2. LINK_ID --> rdwy_ctgy

        links_df: links GeoDataFrame
            update_method: update method to use in network_wrangler.update_df. One of
            "overwrite all", "update if found", or "update nan".
            Defaults to "update if found"

        Returns:
            link_df with rdwy_ctgy column added
        """
        roadway_ps = self.parameters.roadway_network_ps

        # 1. shstGeometryId --> LINK_ID
        # Expected columns shstReferenceId,shstGeometryId,pp_link_id (which is the LINK_ID),score
        _join_df = roadway_ps.roadway_value_lookups[
            "widot_shst_2_link_id"
        ].apply_mapping(links_df)

        # LINK_ID --> route_sys
        # Expected columns  LINK_ID, rdwy_ctgy
        _join_df = roadway_ps.roadway_value_lookups[
            "widot_id_2_rdwy_ctgy"
        ].apply_mapping(_join_df)

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

        _join_df.drop_duplicates(subset=["model_link_id"], keep="last", inplace=True)

        links_rdwy_ctgy_df = update_df(
            links_df,
            _join_df,
            "model_link_id",
            update_fields=["widot_rdwy_ctgy"],
            method=update_method,
        )

        return links_rdwy_ctgy_df

    def calculate_assign_group_and_roadway_class(
        self,
        links_df: GeoDataFrame = None,
        assign_group_variable_name="assign_group",
        road_class_variable_name="roadway_class",
        update_method="update if found",
    ):
        """
        Calculates assignment group and roadway class variables.

        Assignment Group is used in MetCouncil's traffic assignment to segment
        the volume/delay curves.
        Original source is from the MRCC data for the Minnesota: "route system"
        which is a roadway class
        For Wisconsin, it is from the Wisconsin DOT database, which has a variable
        called "roadway category"

        There is a crosswalk between the MRCC Route System and
        Wisconsin DOT --> Met Council Assignment group

        This method joins the network with mrcc and widot roadway data by shst
        js matcher returns

        Args:
            links_df: Links dataframe. If not set, defaults to self.links_df.
            assign_group_variable_name: Name of the variable assignment group should
                 be written to.  Default to "assign_group".
            road_class_variable_name: Name of the variable roadway class should be
                written to. Default to "roadway_class".
            update_method: update method to use in network_wrangler.update_df.
                One of "overwrite all", "update if found", or "update nan".
                Defaults to "update if found"
        Returns:
            RoadwayNetwork
        """
        if links_df is None:
            links_df = self.links_df

        roadway_ps = self.parameters.roadway_network_ps

        WranglerLogger.info(
            f"""Calculating Assignment Group and Roadway Class as network variables:
                '{assign_group_variable_name}' and
                '{road_class_variable_name}'"""
        )

        """
        Start actual process
        """
        # Get roadway category variables from ShSt spatial joins from MnDOT MRCC
        # network and WiDOT
        _links_update_df = self._calculate_mrcc_route_sys(links_df)
        _links_update_df = self._calculate_widot_rdwy_ctgy(_links_update_df)

        # Get initial assignment group and roadway class from lookup tables starting
        # with OSM and overlaying with MnDOT and then WiDOT

        _links_update_df = roadway_ps.roadway_value_lookups[
            "osm_roadway_assigngrp_mapping"
        ].apply_mapping(_links_update_df)
        _links_update_df = roadway_ps.roadway_value_lookups[
            "mrcc_roadway_assigngrp_mapping"
        ].apply_mapping(_links_update_df)
        _links_update_df = roadway_ps.roadway_value_lookups[
            "widot_roadway_assigngrp_mapping"
        ].apply_mapping(_links_update_df)

        # Apply more sophisticated rules for final variable calculation
        _links_update_df[assign_group_variable_name] = _links_update_df.apply(
            lambda x: self._set_final_assignment_group(x), axis=1
        )

        # msg = f"""_links_update_df.assign_group.value_counts 1:
        #    {_links_update_df.assign_group.value_counts()}"""
        # WranglerLogger.info(msg)

        _links_update_df[road_class_variable_name] = _links_update_df.apply(
            lambda x: self._set_final_roadway_class(x), axis=1
        )

        # msg = f"""_links_update_df.assign_group.value_counts 2:
        #     {_links_update_df.assign_group.value_counts()}"""
        # WranglerLogger.info(msg)

        # Update roadway class and assignment group variables in the roadway network
        _links_out_df = update_df(
            links_df,
            _links_update_df,
            "model_link_id",
            update_fields=[assign_group_variable_name, road_class_variable_name],
            method=update_method,
        )

        WranglerLogger.info(
            f"""Finished calculating assignment group variable {assign_group_variable_name}
            and roadway class variable {road_class_variable_name}"""
        )

        return _links_out_df

    def add_met_council_calculated_roadway_variables(
        self, links_df: GeoDataFrame
    ) -> None:
        """
        Adds calculated roadway variables to specified link dataframe.

        Args:
            link_df: specified link dataframe (model_links_df or links_df)
        """
        WranglerLogger.info("Creating metcouncil calculated roadway variables.")

        links_df = self.calculate_area_type(links_df)
        links_df = self.calculate_county_mpo(links_df)

        links_df = self.add_counts(links_df)
        links_df = self.update_distance(links_df, use_shapes=True)

        mc_variables_init_dict = {
            "count_year": 2017,
            "managed": 0,
            "ML_lanes": 0,
            "segment_id": 0,
            "HOV": 0,
        }

        links_df = self.add_placeholder_variables(
            links_df, variables_init_dict=mc_variables_init_dict
        )

        return links_df

    def calculate_area_type(self, links_df: GeoDataFrame) -> GeoDataFrame:
        """
        Add area type values to roadway network.
        Uses the RoadwayNetworkModelParameters:
        - roadway_overlays["area_type"]
        - roadway_value_lookups["area_type_codes_dict"]
        - roadway_value_lookups["mc_mpo_counties_dict"]

        Args:
            links_df: the input ModelRoadwayNetwork.

        Returns: ModelRoadwayNetwork with area type
        """
        roadway_ps = self.parameters.roadway_network_ps

        links_df = self.add_polygon_overlay_to_links(
            links_df, roadway_ps.roadway_overlays["area_type"], method="link centroid"
        )

        links_df = self.add_polygon_overlay_to_links(
            links_df,
            roadway_ps.roadway_overlays["downtown_area_type"],
            method="link centroid",
        )

        links_df["area_type"] = links_df["area_type_name"].map(
            roadway_ps.roadway_value_lookups["area_type_codes_dict"]
            .fillna(1)
            .astype(int)
        )

        return links_df

    def calculate_county_mpo(
        self, links_df: DataFrame, roadway_ps: RoadwayNetworkModelParameters = None
    ) -> DataFrame:
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
        if roadway_ps is None:
            roadway_ps = self.parameters.roadway_network_ps

        links_df = self.add_polygon_overlay_to_links(
            links_df, roadway_ps.roadway_overlays["counties"], method="link_centroid"
        )

        links_df["county"] = links_df["county_name"].map(
            roadway_ps.roadway_value_lookups["county_codes_dict"].fillna(10).astype(int)
        )

        links_df["mpo"] = links_df["county_name"].map(
            roadway_ps.roadway_value_lookups["mc_mpo_counties_dict"]
            .fillna(0)
            .astype(int)
        )

        return links_df

    def roadway_standard_to_met_council_network(self,) -> None:
        """
        Rename and format roadway attributes to be consistent with what metcouncil's
        model is expecting.

        Args:
            roadway_net:

        Returns:
            tuple (model_links_df,nodes_df, shapes_df, list of steps completed) where
                list of steps completed is a set of link_geometry_complete, geography_complete,
                field_name_complete, and types_compelte.
        """

        WranglerLogger.info(
            "Transforming the standard roadway network into the format and variables that \
            metcouncil's model is expecting"
        )

        """
        Start actual process
        """
        (
            self.model_links_df,
            self.nodes_df,
            self.shapes_df,
        ) = self.create_managed_lane_network()

        self.model_links_df = super().calculate_centroid_connectors(self.model_links_df)
        self.model_links_df = super().update_distance(
            self.model_links_df, use_shapes=True
        )

        self.model_links_df = self.add_met_council_calculated_roadway_variables(
            self.model_links_df
        )

        super().coerce_network_types()

        self.model_links_df = self.split_properties_by_time_period_and_category(
            self.model_links_df
        )

        self.model_links_df = self.replace_geometry_with_true_shape(self.model_links_df)

        self.nodes_df["X"] = self.nodes_df.geometry.apply(lambda g: g.x)
        self.nodes_df["Y"] = self.nodes_df.geometry.apply(lambda g: g.y)
        self.model_nodes_df = self.nodes_df.copy()

        # self.model_links_df.crs = "EPSG:4326"
        # self.nodes_df.crs = "EPSG:4326"
        WranglerLogger.info("Setting Coordinate Reference System to EPSG 26915")
        self.model_links_df = self.model_links_df.to_crs(epsg=26915)
        self.model_nodes_df = self.nodes_df.to_crs(epsg=26915)

        # CUBE expect node id to be N
        self.model_nodes_df = self.nodes_df.rename(
            columns={"model_node_id": "N"}, inplace=True
        )
