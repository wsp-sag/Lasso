import copy
import itertools
import os
from typing import Union, Mapping, Any, Collection, Dict

import geopandas as gpd

from geopandas import GeoDataFrame
from pandas import DataFrame

from network_wrangler import RoadwayNetwork

from .cube.cube_roadway import write_cube_hwy_net_script_network_from_ff_files
from .parameters import Parameters
from .logger import WranglerLogger
from .data import FieldMapping, PolygonOverlay, update_df
from .utils import fill_df_na, coerce_df_types, write_df_to_fixed_width, fill_df_cols


class ModelRoadwayNetwork(RoadwayNetwork):
    """
    Subclass of network_wrangler class :ref:`RoadwayNetwork <network_wrangler:RoadwayNetwork>`

    A representation of the physical roadway network and its properties.
    """

    CALCULATED_VALUES = ["area_type", "county", "centroidconnect"]

    def __init__(
        self,
        nodes: GeoDataFrame,
        links: DataFrame,
        shapes: GeoDataFrame,
        validate_schema: bool = True,
        parameters: Parameters = None,
        parameters_dict: dict = {},
        **kwargs,
    ):
        """
        Constructor

        Args:
            nodes: geodataframe of nodes
            links: dataframe of links
            shapes: geodataframe of shapes
            parameters: instance of Parameters.
            parameters_dict: dictionary of parameter settings (see Parameters class).
            ml_already_split: indicates if incoming network already has managed lanes split
                into separate links.
                if True, will copy links_df also to model_links_df. Defaults to False.
            split_properties: if True, will run split_properties_by_time_period_and_category.
                which is useful for creating project cards. Defaults to False.
            crs (int): coordinate reference system, ESPG number
            node_foreign_key (str):  variable linking the node table to the link table
            link_foreign_key (list): list of variable linking the link table to the node
                foreign key
            shape_foreign_key (str): variable linking the links table and shape table
            unique_link_key (str): variable used for linking link tables to each other
            unique_node_key (str): variable used for linking node tables to each other
            unique_link_ids (list): list of variables unique to each link
            unique_node_ids (list): list of variables unique to each node
            modes_to_network_link_variables (dict): Mapping of modes to link variables
                in the network
            modes_to_network_nodes_variables (dict): Mapping of modes to node variables
                in the network
            managed_lanes_node_id_scalar (int): Scalar values added to primary keys for
                nodes for corresponding managed lanes.
            managed_lanes_link_id_scalar (int): Scalar values added to primary keys for
                links for corresponding managed lanes.
            managed_lanes_required_attributes (list): attributes that must be specified
                in managed lane projects.
            keep_same_attributes_ml_and_gp (list): attributes to copy to managed lanes
                from parallel general purpose lanes.
        """
        if parameters:
            WranglerLogger.debug(
                "ModelRoadwayNetwork.__init__: using passed in Parameters instance."
            )
            self.parameters = parameters.update(update_dict=parameters_dict, **kwargs)

        else:
            WranglerLogger.debug(
                "ModelRoadwayNetwork.__init__: initializing Parameters instance with: \n{}".format(
                    parameters_dict
                )
            )
            self.parameters = Parameters.initialize(input_ps=parameters_dict, **kwargs)

        super().__init__(nodes, links, shapes, **self.parameters.as_dict())

        self.additional_initialization(**kwargs)

        # msg = "Used PARAMS\n"+'\n'.join(
        #       ['{}: {}'.format(k,v) for k,v in self.parameters.__dict__.items()]
        #   )
        # WranglerLogger.debug(msg)

    @staticmethod
    def read(
        link_filename: str,
        node_filename: str,
        shape_filename: str,
        fast: bool = False,
        ml_already_split: bool = False,
        split_properties: bool = False,
        parameters: Parameters = None,
        parameters_dict: dict = {},
        **kwargs,
    ):
        """
        Reads in links and nodes network standard. Default parameters will be overriden
            in following order:
         - instance properties from the roadway_network_object
         - parameters kwarg for this method
         - parameters_dict dictionary of parameters from this method call

        Args:
            link_filename: File path to link json.
            node_filename: File path to node geojson.
            shape_filename: File path to link true shape geojson
            fast: boolean that will skip validation to speed up read time.
            ml_already_split: indicates if incoming network already has managed lanes split
                into separate links.
                if True, will copy links_df also to model_links_df. Defaults to False.
            split_properties: indicates if network variables should be split by time and
                day and category upon being read in. Often necessary for turning log files
                into ProjectCards.  Defaults to False.
            parameters: instance of Parameters.
            parameters_dict: dictionary of parameter settings (see Parameters class).
                Overwrites settins in parameters.

        Returns:
            ModelRoadwayNetwork
        """

        if parameters:
            WranglerLogger.debug(
                "ModelRoadwayNetwork.read(): using passed in Parameters instance."
            )
            # WranglerLogger.debug("[.read().parameters] {}".format(parameters))
            _parameters = parameters.update(update_dict=parameters_dict, **kwargs)
            # WranglerLogger.debug("[.read()._parameters] {}".format(_parameters))
        else:
            WranglerLogger.debug(
                "ModelRoadwayNetwork.read(): initializing Parameters instance with: \n{}".format(
                    parameters_dict
                )
            )
            _parameters = Parameters.initialize(input_ps=parameters_dict, **kwargs)

        nodes_df, links_df, shapes_df = RoadwayNetwork.load_transform_network(
            node_filename=node_filename,
            link_filename=link_filename,
            shape_filename=shape_filename,
            validate_schema=not fast,
            **_parameters.as_dict(),
        )

        m_road_net = ModelRoadwayNetwork(
            nodes_df,
            links_df,
            shapes_df,
            validate_schema=not fast,
            ml_already_split=ml_already_split,
            split_properties=split_properties,
            parameters=_parameters,
        )

        return m_road_net

    def additional_initialization(
        self, ml_already_split: bool = False, split_properties: bool = False, **kwargs
    ):
        """
        Add additional variables which a *model*roadway network needs to have in
        addition to a RoadwayNetwork.

        Args:
            ml_already_split: indicates if incoming network already has managed lanes split
                into separate links.
                if True, will copy links_df also to model_links_df. Defaults to False.
            split_properties: if True, will run split_properties_by_time_period_and_category.
                which is useful for creating project cards. Defaults to False.
        """

        self.links_df = fill_df_cols(self.links_df, {"ML_lanes": 0})

        if split_properties:
            self.links_df = self.split_properties_by_time_period_and_category(
                self.links_df
            )

        self.model_links_df = None

        if ml_already_split:
            self.model_links_df = copy.deepcopy(self.links_df)

        self.coerce_network_types()

    @classmethod
    def from_RoadwayNetwork(
        cls,
        roadway_network_object: RoadwayNetwork,
        parameters: Parameters = None,
        parameters_dict: dict = {},
        ml_already_split: bool = False,
        split_properties: bool = False,
        **kwargs,
    ):
        """
        Converts a RoadwayNetwork object to a ModelRoadwayNetwork subclass.
        Default parameters will be overriden in following order:
         - instance properties from the roadway_network_object
         - parameters kwarg for this method
         - parameters_dict dictionary of parameters from this method call
         - kwargs in this method call

        Args:
            roadway_network_object: instance of a RoadwayNetwork from NetworkWrangler
            parameters: instance of Parameters.
            parameters_dict: dictionary of parameter settings (see Parameters class).
            ml_already_split: indicates if incoming network already has managed lanes
                split into separate links. if True, will copy links_df also to model_links_df.
                Defaults to False.
            split_properties: if True, will run split_properties_by_time_period_and_category.
                which is useful for creating project cards. Defaults to False.

        Returns: ModelRoadwayNetwork instance.
        """

        WranglerLogger.info(f"Converting RoadwayNetwork to {cls} flavor.")

        # Copy and change the class of the copy
        m_road_net = copy.deepcopy(roadway_network_object)
        m_road_net.__class__ = cls

        # Add additional variables which a *model*roadway network needs to have.
        if parameters:
            WranglerLogger.debug(
                "ModelRoadwayNetwork.from_RoadwayNetwork(): using passed in Parameters instance."
            )
            # WranglerLogger.debug("[.from_RoadwayNetwork().parameters] {}".format(parameters))
            _parameters = parameters.update(update_dict=parameters_dict, **kwargs)
            # WranglerLogger.debug("[.from_RoadwayNetwork()._parameters] {}".format(_parameters))
        else:
            WranglerLogger.debug(
                f"""[ModelRoadwayNetwork.from_RoadwayNetwork()]:
                    initializing Parameters instance with: \n
                    {parameters_dict}"""
            )
            _parameters = Parameters.initialize(input_ps=parameters_dict, **kwargs)

        m_road_net.parameters = _parameters

        m_road_net.additional_initialization(
            ml_already_split=ml_already_split,
            split_properties=split_properties,
            **kwargs,
        )

        return m_road_net

    def split_property(
        self,
        property_name: str,
        time_periods: Mapping = None,
        categories: Mapping = None,
    ):
        """
        Splits a link property by time period and categories.

        Args:
            property_name:
            time_periods:
            categories:

        Returns: None

        """
        if time_periods and categories:
            for time_suffix, category_suffix in itertools.product(
                list(time_periods.keys()), list(categories.keys())
            ):
                self.links_df[
                    property_name + "_" + category_suffix + "_" + time_suffix
                ] = self.get_property_by_time_period_and_group(
                    property_name,
                    category=categories[category_suffix],
                    time_period=time_periods[time_suffix],
                    default_return=0,
                )
        elif time_periods:
            for time_suffix, time_defs in time_periods.itmes():
                self.links_df[
                    property_name + "_" + time_suffix
                ] = self.get_property_by_time_period_and_group(
                    property_name,
                    category=None,
                    time_period=time_defs,
                    default_return=0,
                )
        else:
            raise ValueError(
                "Shoudn't have a category without a time period. Category: {}".format(
                    categories
                )
            )

    def split_properties_by_time_period_and_category(
        self, links_df: DataFrame, properties_to_split=None
    ) -> DataFrame:
        """
        Splits properties by time period, assuming a variable structure of

        Args:
            properties_to_split: dict
                dictionary of output variable prefix mapped to the source variable
                and what to stratify it by
                e.g.
                {
                    'trn_priority' : {
                        'v':'trn_priority',
                        'times_periods':{
                            "AM": ("6:00", "9:00"),
                            "PM": ("16:00", "19:00")
                        }
                    },
                    'ttime_assert' : {
                        'v':'ttime_assert',
                        'times_periods':{
                            "AM": ("6:00", "9:00"),
                            "PM": ("16:00", "19:00")
                        }
                    },
                    'lanes' : {
                        'v':'lanes',
                        'times_periods':{
                            "AM": ("6:00", "9:00"),
                            "PM": ("16:00", "19:00")
                        }
                    },
                    'ML_lanes' : {
                        'v':'ML_lanes',
                        'times_periods':{
                            "AM": ("6:00", "9:00"),
                            "PM": ("16:00", "19:00")
                        }
                    },
                    'price' : {
                        'v':'price',
                        'times_periods':{
                            "AM": ("6:00", "9:00"),
                            "PM": ("16:00", "19:00")}
                        },
                        'categories': {
                            "sov": ["sov", "default"],
                            "hov2": ["hov2", "default", "sov"]
                        }
                    },
                    'access' : {
                        'v':'access',
                        'times_periods':{
                            "AM": ("6:00", "9:00"),
                            "PM": ("16:00", "19:00")
                        }
                    },
                }

        """

        if properties_to_split is None:
            properties_to_split = self.parameters.roadway_network_ps.properties_to_split

        for out_var, params in properties_to_split.items():
            msg = "Splitting {} with params: {}".format(out_var, params)
            # print(msg)
            WranglerLogger.debug(msg)

            if params["v"] not in links_df.columns:
                WranglerLogger.warning(
                    "Specified variable to split: {} not in network variables: {}.".format(
                        params["v"], str(links_df.columns)
                    )
                )
            elif params.get("time_periods") and params.get("categories"):
                for time_suffix, category_suffix in itertools.product(
                    list(params["time_periods"].keys()),
                    list(params["categories"].keys()),
                ):
                    links_df[
                        out_var + "_" + category_suffix + "_" + time_suffix
                    ] = self.get_property_by_time_period_and_group(
                        params["v"],
                        category=params["categories"][category_suffix],
                        time_period=params["time_periods"][time_suffix],
                    )
            elif params.get("time_periods"):
                for time_suffix, time_spans in params["time_periods"].items():
                    links_df[
                        out_var + "_" + time_suffix
                    ] = self.get_property_by_time_period_and_group(
                        params["v"], category=None, time_period=time_spans
                    )
            else:
                raise NotImplementedError(
                    "Shoudn't have a category without a time period: {}".format(params)
                )
            return links_df

    def add_calculated_variables(self, links_df: GeoDataFrame) -> GeoDataFrame:
        """
        Adds additional calculated variables to the network.

        Steps:
        1. add counts stored in parameters.roadway_network_ps.counts []
        2. updates distance

        Args:
            links_df: links GeoDataFrame to add calculated variables to.

        Returns: links  GeoDataFrame with calculated variables.
        """
        links_df = self.add_counts(links_df)
        links_df = self.update_distance(links_df, use_shapes=True, inplace=False)
        return links_df

    def create_model_network(self) -> None:
        """
        Creates basic model network and stores as self.model_links_df and self.model_nodes_df.

        Steps:
         - calculates centroid connectors based on parameters.roadway_network_ps.max_tax
         - adds calculated variables like counts, distance
         - creates a managed lane network based on fields with prefix ML_ as
            self.model_links_df
         - coerces the types of links and nodes based on
            parameters.roadway_network_ps.field_types
         - splits the model_link properties by time of day and category as
            specified in parameters
         - replaces stick geometry with true shape
        """
        self.model_links_df = self.calculate_centroid_connectors(links_df=self.links_df)
        self.model_links_df = self.add_calculated_variables(self.model_links_df)

        (
            self.model_links_df,
            self.nodes_df,
            self.shapes_df,
        ) = RoadwayNetwork.create_managed_lane_network(self)

        self.coerce_network_types()

        self.model_links_df = self.split_properties_by_time_period_and_category(
            self.model_links_df
        )

        self.model_links_df = self.replace_geometry_with_true_shape(self.model_links_df)

    def is_ready_for_model(self) -> bool:
        """
        Checks if self.model_links_df and self.nodes_df are ready for writing out.

        Raises:
            AssertionError if not ready.
        """
        ##TODO
        pass

    def replace_geometry_with_true_shape(self, links_df: GeoDataFrame) -> GeoDataFrame:
        """
        Replace the geometry field of a links GeoDataFrame with true shape, if found.
        """

        links_df = update_df(
            links_df,
            self.shapes_df[["shape_id", "geometry"]],
            self.parameters.file_ps.shape_foreign_key,
            update_fields=["geometry"],
            method="update if found",
        )

        return links_df

    def add_polygon_overlay_to_links(
        self,
        links_df: GeoDataFrame,
        polygon_overlay: PolygonOverlay,
        method: str = "link_centroid",
        field_mapping: dict = None,
        fill_values_dict: Dict[str, Any] = None,
        update_method: str = None,
    ):
        """
        Adds or updates roadway network link variables from a PolygonOverlay.

        Args:
            links_df: links GeoDataFrame
            polygon_overlay: PolygonOverlay dataclass
            method: string indicating the join type for the link.  "link centroid" is
                only one currently implemented.
            field_mapping: if added, overwrites PolygonOverlay class's field mapping
            fill_values_dict: if specified, fills span of the overlay with this value
            update_method: update method to use in network_wrangler.update_df.
                One of "overwrite all",
                "update if found", or "update nan". If added, overwrites PolygonOverlay class's
                update_method, which defaults to "update if found" if not set.
        """

        if method not in ["link centroid"]:
            raise NotImplementedError(
                "{} not an implemented method in add_polygon_overlay_to_links()."
            )

        field_mapping = (
            polygon_overlay.field_mapping if field_mapping is None else field_mapping
        )
        update_method = (
            polygon_overlay.update_method if update_method is None else update_method
        )
        fill_values_dict = (
            polygon_overlay.fill_values_dict
            if fill_values_dict is None
            else fill_values_dict
        )

        WranglerLogger.debug(
            "Adding geographic overlay variables {} or fill variables {} from {}".format(
                field_mapping, fill_values_dict, polygon_overlay.input_filename
            )
        )
        WranglerLogger.info("LINKS_DF.CRS: {}".format(links_df.crs.to_epsg()))
        polygon_overlay_gdf = polygon_overlay.gdf.to_crs(epsg=links_df.crs.to_epsg())

        if fill_values_dict:
            polygon_overlay_gdf = fill_df_cols(polygon_overlay_gdf, fill_values_dict,)
            field_mapping = {k: k for k in fill_values_dict.keys()}

        if method == "link centroid":
            _link_centroids_gdf = links_df.copy()
            _link_centroids_gdf["geometry"] = _link_centroids_gdf["geometry"].centroid
            _link_centroids_gdf = _link_centroids_gdf[
                [self.unique_link_key, "geometry"]
            ]

            polygon_overlay_gdf.rename(columns=field_mapping, inplace=True)

            _update_gdf = gpd.sjoin(
                _link_centroids_gdf[[self.unique_link_key, "geometry"]],
                polygon_overlay_gdf,
                how="left",
                op="intersects",
            )

        _output_links_df = update_df(
            links_df,
            _update_gdf,
            self.unique_link_key,
            update_fields=list(field_mapping.values()),
            method=update_method,
        )

        return _output_links_df

    def add_counts(
        self,
        links_df: GeoDataFrame,
        split_counts_by_tod: bool = True,
        count_tod_split_fields: Mapping = None,
        time_period_vol_split: Mapping = None,
    ) -> GeoDataFrame:

        """
        Adds counts stored in self.parameters.roadway_network_ps.counts dictionary.

        Args:
            links_df: roadway network links to add counts to
            split_counts_by_tod: if set to True, will use time_period_vol_split and
                count_tod_split_fields
                to split counts by time of day. Defaults to True.
            time_period_vol_split: Mapping of time of day abbreviations and portions of volume to
                assign to each of them. e.g. {"AM": 0.25, "PM": 0.30}.
                If not specified, will default to
                parameters.roadway_network_ps.time_period_vol_split
            count_tod_split_fields: Mapping of fields to split counts and the prefix to
                use for the resulting fields. e.g. {"count_daily":"count_"}.  If not specified,
                will default to parameters.roadway_network_ps.count_tod_split_fields.

        Returns:
            GeoDataFrame storing ModelRoadwayNetwork links with count variables added.
        """

        WranglerLogger.info("Adding Counts")

        for (
            _count_name,
            _count_value_lookup,
        ) in self.parameters.roadway_network_ps.counts.items():
            links_df = _count_value_lookup.apply_mapping(links_df)

        if split_counts_by_tod:
            WranglerLogger.debug("Splitting counts by time of day.")
            if not count_tod_split_fields:
                count_tod_split_fields = (
                    self.parameters.roadway_network_ps.count_tod_split_fields
                )
            if not time_period_vol_split:
                time_period_vol_split = (
                    self.parameters.roadway_network_ps.time_period_vol_split
                )

            for count_to_split_field, prefix in count_tod_split_fields.items():
                for time_period_abbr, volume_portion in time_period_vol_split.items():
                    links_df[prefix + time_period_abbr] = (
                        volume_portion * links_df[count_to_split_field]
                    )

        WranglerLogger.debug("Finished adding counts variables.")

        return links_df

    def calculate_centroid_connectors(
        self,
        links_df: DataFrame = None,
        max_taz: int = None,
        centroid_connector_properties: dict = {},
        overwrite: bool = False,
    ) -> None:
        """
        Calculates indicator and other variables for centroid connectors.

        Args:
            max_taz: the max TAZ number in the network. Overrides
                self.parameters.roadway_network_ps.max_taz.
            centroid_connector_properties: if
            overwrite: True if overwriting existing properties in network, otherwise updates
                the dataframe.  Default to False.
        """
        _centroid_connector_properties = {}
        _centroid_connector_properties.update(
            self.parameters.roadway_network_ps.centroid_connector_properties
        )
        # self.parameters.roadway_network_ps.centroid_connector_properties =
        # {"centroidconnect": 1, "lanes": 1}
        _centroid_connector_prop_list = [
            {"property": k, "set": v} for k, v in _centroid_connector_properties.items()
        ]

        if links_df is None:
            links_df = self.links_df
        max_taz = max_taz if max_taz else self.parameters.roadway_network_ps.max_taz

        if not max_taz:
            msg = "No highest_TAZ number specified in method variable or in parameters"
            WranglerLogger.error(msg)
            raise ValueError(msg)

        WranglerLogger.debug(
            "Calculating Centroid Connectors using\n\
            - Highest TAZ number: {}\n\
            - Property Dictionary: {}".format(
                max_taz, _centroid_connector_properties
            )
        )

        """
        Start actual process
        """

        self.apply_roadway_feature_change(
            link_idx=self.links_df.index[
                (self.links_df["A"] <= max_taz) | (self.links_df["B"] <= max_taz)
            ],
            properties=_centroid_connector_prop_list,
            links_df=links_df,
        )

        return links_df

    def coerce_network_types(self, type_lookup: Mapping = None) -> None:
        """
        Coerce types for network (i.e. links and nodes) for columns specified in
            self.parameters.roadway_network_ps.field_type
            and overriden by the type_lookup dictionary specified as a keyword.

        Args:
            type_lookup: a dictionary mapping field names to types of str, int, or float.
                If not specified, will use roadway_net.parameters.roadway_network_ps.field_type

        """

        _type_lookup = copy.deepcopy(self.parameters.roadway_network_ps.field_type)

        if type_lookup:
            _type_lookup.update(type_lookup)

        WranglerLogger.debug("Coercing types based on:\n {}".format(_type_lookup))

        _df_list = ["nodes_df", "links_df", "shapes_df", "model_links_df"]
        for _df_name in _df_list:
            if self.__dict__.get(_df_name, None) is None:
                continue

            self.__dict__[_df_name] = coerce_df_types(
                self.__dict__[_df_name], type_lookup=_type_lookup
            )

            WranglerLogger.debug(
                "Updated types for {}:\n {}".format(
                    _df_name, self.__dict__[_df_name].dtypes
                )
            )

    def fill_na(self) -> None:
        """
        Fill na values with zeros and "" for links_df, nodes_df, shapes_df, and model_links_df
            (if they exist) based on types in self.parameters.roadway_network_ps.field_type
        """

        WranglerLogger.debug("Filling nan for network dfs from network wrangler")

        _df_list = ["nodes_df", "links_df", "shapes_df", "model_links_df"]
        for _df_name in _df_list:

            if not self.__dict__[_df_name]:
                continue

            self.__dict__[_df_name] = fill_df_na(
                self.__dict__[_df_name],
                type_lookup=self.parameters.roadway_network_ps.field_type,
            )

    def rename_fields_for_dbf(
        self,
        input_df: DataFrame,
        net_to_dbf_field_crosswalk: Union[dict, str, FieldMapping] = None,
        output_fields: list = None,
    ) -> DataFrame:
        """
        Rename attributes for DBF/SHP, make sure length within 10 chars.

        Args:
            input_df (dataframe): Network standard DataFrame.
            net_to_dbf_field_crosswalk_dict: File path to variable name crosswalk from
                network standard to DBF names.
            output_fields (list): List of strings for DBF variables.
        """
        WranglerLogger.info("Renaming variables so that they are DBF-safe")

        """
        Verify inputs
        """
        if net_to_dbf_field_crosswalk is None:
            _net_to_dbf_field_crosswalk = (
                self.parameters.roadway_network_ps.roadway_field_mappings.net_to_dbf
            )
        elif isinstance(net_to_dbf_field_crosswalk, FieldMapping):
            _net_to_dbf_field_crosswalk = net_to_dbf_field_crosswalk.field_mapping
        elif isinstance(net_to_dbf_field_crosswalk, dict):
            _net_to_dbf_field_crosswalk = net_to_dbf_field_crosswalk
        elif isinstance(net_to_dbf_field_crosswalk, str):
            _net_to_dbf_field_crosswalk = FieldMapping(
                input_filename=net_to_dbf_field_crosswalk
            ).field_mapping
        else:
            raise ValueError(
                "Unrecognized type for net_to_dbf_field_crosswalk: {}".format(
                    net_to_dbf_field_crosswalk
                )
            )

        WranglerLogger.debug(
            "DBF Field Mapping: {}".format(_net_to_dbf_field_crosswalk)
        )

        output_fields = (
            output_fields
            if output_fields
            else self.parameters.roadway_network_ps.output_fields
        )

        _output_fields = [c for c in input_df.columns if c in output_fields]

        """
        Start actual process
        """

        output_df = input_df[_output_fields].rename(columns=_net_to_dbf_field_crosswalk)

        WranglerLogger.debug("DBF Variables: {}".format(output_df.columns))

        return output_df

    def write_roadway_as_shp(
        self,
        links_df: GeoDataFrame,
        nodes_df: GeoDataFrame,
        node_output_fields: Collection[str] = None,
        link_output_fields: Collection[str] = None,
        data_to_csv: bool = True,
        output_directory: str = None,
        output_prefix: str = None,
        output_basename_links: str = None,
        output_basename_nodes: str = None,
        overwrite_existing_output: bool = False,
    ) -> None:

        """Write out shapefile of model network.

        Args:
            links_df (GeoDataFrame, optional): The links file to be output.
                If not specified, will default to self.model_links_df.
            nodes_df (GeoDataFrame, optional): The modes file to be output.
                If not specified, will default to self.nodes_df.
            node_output_fields (Collection[str], optional): List of strings for node
                output variables. Defaults to parameters.roadway_network_ps.output_fields.
            link_output_fields (Collection[str], optional): List of strings for link
                output variables. Defaults to parameters.roadway_network_ps.output_fields.
            data_to_csv (bool, optional): If True, will export most of link and node data to
                a csv of the same name/location as the shapefile (with a .csv ending). with
                the excpetion
                of following fields
                  - Links: ["A", "B", "shape_id", "geometry"]
                  - Nodes: ["N", "x", "y", "geometry"]
                Defaults to True.
            output_directory (str, optional): If set, will combine with output_link_shp and
                output_node_shp
                to form output directory. Defaults to parameters.file_ps.output_directory,
                which defaults to "".
            output_prefix (str, optional): prefix to add to output files. Helpful for identifying
                a scenario.
                Defaults to parameters.file_ps.output_prefix, which defaults to "".
            output_basename_links (str, optional): Combined with the output_director,
                output_prefix, and
                the appropriate filetype suffix for the link output filenames.
                Defaults to parameters.file_ps.output_basename_links, which defaults to
                "links_out".
            output_basename_nodes (str, optional): Combined with the output_director,
                output_prefix, and the appropriate filetype suffix for the node output
                filenames. Defaults to parameters.file_ps.output_basename_nodes,
                which defaults to "links_out".
            overwrite_existing_output (bool, optional): if True, will not ask about overwriting
                existing output.
                Defaults to False.
        """
        if not output_directory:
            output_directory = self.parameters.file_ps.output_directory
        if not os.path.exists(output_directory):
            raise ValueError(
                "output_directory {} is specified, but doesn't exist.".format(
                    output_directory
                )
            )

        if not output_prefix:
            output_prefix = self.parameters.file_ps.output_prefix
        if not output_basename_links:
            output_basename_links = self.parameters.file_ps.output_basename_links
        if not output_basename_nodes:
            output_basename_nodes = self.parameters.file_ps.output_basename_nodes

        _outfile_links = os.path.join(
            output_directory, output_prefix + output_basename_links
        )
        _outfile_nodes = os.path.join(
            output_directory, output_prefix + output_basename_nodes
        )

        suffix = [".dbf", ".shp"]
        if data_to_csv:
            suffix.append(".csv")

        ### Check filenames
        if not overwrite_existing_output:
            for f, suf in itertools.product([_outfile_links, _outfile_nodes], suffix):
                if os.path.exists(f + suf):
                    overwrite = input(
                        f"File: {f + suf} already exists.Overwrite?\
                            Y = yes,\
                            I = ignore and overwrite all\n"
                    )
                    if overwrite.lower() == "y":
                        continue
                    if overwrite.lower() == "i":
                        break
                    else:
                        raise ValueError(
                            f"""
                            Stopped execution because user input declined to overwrite file:
                            {f + suf}
                            """
                        )

        WranglerLogger.info(
            "Writing Network to shapefiles: \n   -links {}\n   -lnodes {}".format(
                _outfile_links + ".shp", _outfile_nodes + ".shp"
            )
        )

        ### Check if ready to be output for model. Warn, but don't force it.
        if not self.is_ready_for_model():
            WranglerLogger.error(
                """Not ready for the model yet, should run self.create_model_network()
                or similar first."""
            )

        ### Determine output dataframe for each file type
        if links_df is None:
            links_df = self.model_links_df
        if nodes_df is None:
            nodes_df = self.nodes_df

        ### Determine output fields for each file type
        if not node_output_fields:
            node_output_fields = self.parameters.roadway_network_ps.output_fields
        if not link_output_fields:
            link_output_fields = self.parameters.roadway_network_ps.output_fields

        _base_fields_nodes = self.parameters.roadway_network_ps.required_fields_nodes
        _base_fields_links = self.parameters.roadway_network_ps.required_fields_links

        # Check that bare minimum fields are there and error if not.
        _missing_base_col_nodes = [
            c for c in _base_fields_nodes if c not in nodes_df.columns
        ]
        _missing_base_col_links = [
            c for c in _base_fields_links if c not in links_df.columns
        ]

        if _missing_base_col_nodes:
            WranglerLogger.error(
                f"""Missing required fields from nodes file needed
                to export to shapefile: {_missing_base_col_nodes}"""
            )
        if _missing_base_col_links:
            WranglerLogger.error(
                f"""Missing required fields from links file needed to export
                to shapefile: {_missing_base_col_links}"""
            )
        if _missing_base_col_nodes or _missing_base_col_links:
            raise ValueError(
                f"""Missing required fields in order to export to shapefile:
                {_missing_base_col_nodes + _missing_base_col_links}"""
            )

        # determine which fields go to which files
        if data_to_csv:
            _dbf_fields_nodes = _base_fields_nodes
            _dbf_fields_links = _base_fields_links
            _csv_fields_nodes = [f for f in node_output_fields if f in nodes_df.columns]
            _csv_fields_links = [f for f in link_output_fields if f in links_df.columns]
        else:
            _dbf_fields_nodes = [f for f in node_output_fields if f in nodes_df.columns]
            _dbf_fields_links = [f for f in link_output_fields if f in links_df.columns]

        """
        Start Process
        """
        WranglerLogger.debug("Renaming Node and Link Variables for DBF")
        nodes_dbf_df = self.rename_fields_for_dbf(
            nodes_df, output_fields=_dbf_fields_nodes
        )
        links_dbf_df = self.rename_fields_for_dbf(
            links_df, output_fields=_dbf_fields_links
        )

        WranglerLogger.info(
            "Writing Shapefiles:\n - {}\n - {}".format(
                _outfile_links + ".shp", _outfile_nodes + ".shp"
            )
        )
        nodes_dbf_df.to_file(_outfile_links + ".shp")
        links_dbf_df.to_file(_outfile_nodes + ".shp")

        if data_to_csv:
            links_df[_csv_fields_links].to_csv(_outfile_links + ".csv", index=False)
            nodes_df[_csv_fields_nodes].to_csv(_outfile_nodes + ".csv", index=False)

    def write_roadway_as_fixedwidth(
        self,
        links_df: DataFrame,
        nodes_df: DataFrame,
        node_output_fields: Collection[str] = None,
        link_output_fields: Collection[str] = None,
        output_directory: str = None,
        output_prefix: str = None,
        output_basename_links: str = None,
        output_basename_nodes: str = None,
        overwrite_existing_output: bool = False,
        build_script_type: str = None,
    ) -> None:
        """Writes out fixed width files, headers, and

        This function does:
        1. write out link and node fixed width data files for cube.
        2. write out header and width correspondence.
        3. write out build script with header and width specification based
            on format specified.

        Args:
            links_df (GeoDataFrame, optional): The links file to be output. If not specified,
                will default to self.model_links_df.
            nodes_df (GeoDataFrame, optional): The modes file to be output. If not specified,
                will default to self.nodes_df.
            node_output_fields (Collection[str], optional): List of strings for node
                output variables. Defaults to parameters.roadway_network_ps.output_fields.
            link_output_fields (Collection[str], optional): List of strings for link
                output variables. Defaults to parameters.roadway_network_ps.output_fields.
            output_directory (str, optional): If set, will combine with output_link_shp and
                output_node_shp to form output directory. Defaults to
                parameters.file_ps.output_directory, which defaults to "".
            output_prefix (str, optional): prefix to add to output files. Helpful for
                identifying a scenario.
                Defaults to parameters.file_ps.output_prefix, which defaults to "".
            output_basename_links (str, optional): Combined with the output_director,
                output_prefix, and the appropriate filetype suffix for the
                link output filenames. Defaults to parameters.file_ps.output_basename_links,
                which defaults to  "links_out".
            output_basename_nodes (str, optional): Combined with the output_director,
                output_prefix, and
                the appropriate filetype suffix for the node output filenames.
                Defaults to parameters.file_ps.output_basename_nodes, which defaults to
                "links_out".
            overwrite_existing_output (bool, optional): if True, will not ask about overwriting
                existing output. Defaults to False.
            build_script_type (str, optional): If specified, will output a script to the output
                directory which will rebuild the network. Should be one of ["CUBE_HWYNET"].
                Defaults to None.
        """
        if not output_directory:
            output_directory = self.parameters.file_ps.output_directory
        if not os.path.exists(output_directory):
            raise ValueError(
                "output_directory {} is specified, but doesn't exist.".format(
                    output_directory
                )
            )

        if not output_prefix:
            output_prefix = self.parameters.file_ps.output_prefix
        if not output_basename_links:
            output_basename_links = self.parameters.file_ps.output_basename_links
        if not output_basename_nodes:
            output_basename_nodes = self.parameters.file_ps.output_basename_nodes

        FF_SUFFIX = ".txt"
        FF_HEADER_SUFFIX = "_FF_Header.txt"

        _outfile_links = os.path.join(
            output_directory, output_prefix + output_basename_links + FF_SUFFIX
        )
        _outfile_nodes = os.path.join(
            output_directory, output_prefix + output_basename_nodes + FF_SUFFIX
        )
        _outfile_links_header = os.path.join(
            output_directory, output_prefix + output_basename_links + FF_HEADER_SUFFIX
        )
        _outfile_nodes_header = os.path.join(
            output_directory, output_prefix + output_basename_links + FF_HEADER_SUFFIX
        )

        _outfiles = [
            _outfile_links,
            _outfile_nodes,
            _outfile_links_header,
            _outfile_nodes_header,
        ]

        # specify script names for each avail. script type
        BUILD_SCRIPT_TYPES_SUFFIX = {"CUBE_HWYNET": "_build_cube_hwynet.s"}

        if build_script_type:
            _outfile_build_script = os.path.join(
                output_directory,
                output_prefix + BUILD_SCRIPT_TYPES_SUFFIX[build_script_type],
            )
            _outfiles.append(_outfile_build_script)

        ### Check filenames
        if not overwrite_existing_output:
            for f in _outfiles:
                if os.path.exists(f):
                    overwrite = input(
                        f"""File: {f} already exists.
                        Overwrite?
                          Y = yes,
                          I = ignore and overwrite all\n"""
                    )
                    if overwrite.lower() == "y":
                        continue
                    if overwrite.lower() == "i":
                        break
                    else:
                        msg = f"Stopped execution because user input declined to overwrite file:\
                            {f}"
                        raise ValueError(msg)

        WranglerLogger.info(
            "Writing Network to fixed format files: \n{}".format(
                " - {}\n".join(_outfiles)
            )
        )

        ### Check if ready to be output for model. Warn, but don't force it.
        if not self.is_ready_for_model():
            WranglerLogger.error(
                "Not ready for the model yet, should run self.create_model_network()\
                 or similar first."
            )

        ### Determine output dataframe for each file type
        if links_df is None:
            links_df = self.model_links_df
        if nodes_df is None:
            nodes_df = self.nodes_df

        ### Determine output fields for each file type
        if not link_output_fields:
            link_output_fields = self.parameters.roadway_network_ps.output_fields
        if not node_output_fields:
            node_output_fields = self.parameters.roadway_network_ps.output_fields

        _output_fields_links = [f for f in node_output_fields if f in nodes_df.columns]
        _output_fields_nodes = [f for f in node_output_fields if f in nodes_df.columns]

        """
        Start Process
        """
        _link_header_df = write_df_to_fixed_width(
            df=links_df[_output_fields_links],
            data_outfile=_outfile_links,
            header_outfile=_outfile_links_header,
            overwrite=True,
        )

        _node_header_df = write_df_to_fixed_width(
            df=nodes_df[_output_fields_nodes],
            data_outfile=_outfile_nodes,
            header_outfile=_outfile_nodes_header,
            overwrite=True,
        )

        BUILD_SCRIPT_TYPES_FUNCTION_CALL = {
            "CUBE_HWYNET": write_cube_hwy_net_script_network_from_ff_files(
                _link_header_df,
                _node_header_df,
                script_outfile=_outfile_build_script,
                overwrite=True,
            )
        }

        if build_script_type:
            BUILD_SCRIPT_TYPES_FUNCTION_CALL[build_script_type]
