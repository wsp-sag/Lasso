import copy
import glob
import inspect
import itertools
import os
from typing import Optional, Union, Mapping, Any

import geopandas as gpd
import pandas as pd

from geopandas import GeoDataFrame
from pandas import DataFrame
import numpy as np

from network_wrangler import RoadwayNetwork
from .parameters import Parameters
from .logger import WranglerLogger
from .data import  PolygonOverlay


class ModelRoadwayNetwork(RoadwayNetwork):
    """
    Subclass of network_wrangler class :ref:`RoadwayNetwork <network_wrangler:RoadwayNetwork>`

    A representation of the physical roadway network and its properties.
    """

    CALCULATED_VALUES = [
        "area_type",
        "county",
        "centroidconnect",
    ]

    def __init__(
        self,
        nodes: GeoDataFrame,
        links: DataFrame,
        shapes: GeoDataFrame,
        parameters: Union[Parameters, dict] = {},
        added_variables_complete: bool = False,
        calculated_variables_complete: bool = False,
        managed_lanes_complete: bool = False,
        link_geometry_complete: bool = False,
        geography_complete: bool = False,
        field_names_complete: bool = False,
        types_complete: bool = False,
        ready_for_model: bool = False,
        **kwargs,
    ):
        """
        Constructor

        Args:
            nodes: geodataframe of nodes
            links: dataframe of links
            shapes: geodataframe of shapes
            parameters: dictionary of parameter settings (see Parameters class) or an instance of Parameters.
                If not specified, will use default parameters.  Any values in parameters will be overriden by a kwarg.
            added_variables_complete: boolean flag indicating all variables have been added 
                from external sources. Defaults to False.
            calculated_variables_complete: boolean flag indicating all additional variables that are calculated
                from internal data have been. Defaults to False.
            managed_lanes_complete: boolean flag indicating managed lane configurations such as creatting
                offset and dummy links have been complete. Defaults to False.
            link_geometry_complete: boolean flag indicating the link geometry is completed. Such as any
                operations adding complex shape geometry to links. Defaults to False.
            geography_complete: boolean flag indicating coordinate systems and projections 
                are consistent with output expectations. Defaults to False.
            field_names_complete: boolean flag indicating all field names are consistent with
                output expectations. Defaults to False.
            types_complete: boolean flag indicating all field types are consistent with
                output expectations. Defaults to False.
            ready_for_model: boolean flag indicating all fields are consistent with expectations
                for the model. Defaults to False.
            crs (int): coordinate reference system, ESPG number
            node_foreign_key (str):  variable linking the node table to the link table
            link_foreign_key (list): list of variable linking the link table to the node foreign key
            shape_foreign_key (str): variable linking the links table and shape table
            unique_link_ids (list): list of variables unique to each link
            unique_node_ids (list): list of variables unique to each node
            modes_to_network_link_variables (dict): Mapping of modes to link variables in the network
            modes_to_network_nodes_variables (dict): Mapping of modes to node variables in the network
            managed_lanes_node_id_scalar (int): Scalar values added to primary keys for nodes for
                corresponding managed lanes.
            managed_lanes_link_id_scalar (int): Scalar values added to primary keys for links for
                corresponding managed lanes.
            managed_lanes_required_attributes (list): attributes that must be specified in managed
                lane projects.
            keep_same_attributes_ml_and_gp (list): attributes to copy to managed lanes from parallel
                general purpose lanes.
        """

        #init_kwargs = {k:v for k,v in kwargs.items() if k in inspect.getfullargspec(super(ModelRoadwayNetwork,self).__init__).kwonlyargs}
        
        if isinstance(parameters, Parameters):
            self.parameters = parameters
        elif type(parameters) is dict:
            self.parameters = Parameters.initialize(parameters)
        else:
            msg = "parameters {} is of type {} - should be either Parameters instance or dictionary (including empty dictionary)".format(
                parameters, type(parameters)
            )
            WranglerLogger.error(msg)
            raise ValueError(msg)
        
        if kwargs:
            self.parameters.update(kwargs)

        super(ModelRoadwayNetwork,self).__init__(nodes, links, shapes, **self.parameters.as_dict())

        self.links_metcouncil_df = None
        self.nodes_metcouncil_df = None
        # self.shapes_metcouncil_df = None
        ##todo also write to file
        # WranglerLogger.debug("Used PARAMS\n", '\n'.join(['{}: {}'.format(k,v) for k,v in self.parameters.__dict__.items()]))

    @staticmethod
    def read(
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
            recalculate_calculated_variables (bool): calculates fields from spatial joins, etc.
            recalculate_distance (bool):  re-calculates distance.
            parameters: dictionary of parameter settings (see Parameters class) or an instance of Parameters. If not specified, will use default parameters.
            crs (int): coordinate reference system, ESPG number
            node_foreign_key (str):  variable linking the node table to the link table
            link_foreign_key (list): list of variable linking the link table to the node foreign key
            shape_foreign_key (str): variable linking the links table and shape table
            unique_link_ids (list): list of variables unique to each link
            unique_node_ids (list): list of variables unique to each node
            modes_to_network_link_variables (dict): Mapping of modes to link variables in the network
            modes_to_network_nodes_variables (dict): Mapping of modes to node variables in the network
            managed_lanes_node_id_scalar (int): Scalar values added to primary keys for nodes for
                corresponding managed lanes.
            managed_lanes_link_id_scalar (int): Scalar values added to primary keys for links for
                corresponding managed lanes.
            managed_lanes_required_attributes (list): attributes that must be specified in managed
                lane projects.
            keep_same_attributes_ml_and_gp (list): attributes to copy to managed lanes from parallel
                general purpose lanes.
        Returns:
            ModelRoadwayNetwork
        """


        nodes_df, links_df, shapes_df = RoadwayNetwork.load_transform_network(
            node_filename,
            link_filename,
            shape_filename,
            validate_schema=not fast,
            **kwargs,
        )

        m_road_net = ModelRoadwayNetwork(
            nodes_df,
            links_df,
            shapes_df,
            parameters=parameters,
            **kwargs,
        )

        m_road_net.fill_na()
        # this method is making period values as string "NaN", need to revise.
        m_road_net.split_properties_by_time_period_and_category()
        m_road_net.coerce_types()

        return m_road_net

    @staticmethod
    def from_RoadwayNetwork(
        roadway_network_object,
        parameters: Union[dict, Parameters] = {},
    ):
        """
        RoadwayNetwork to ModelRoadwayNetwork

        Args:
            roadway_network_object (RoadwayNetwork).
            parameters: dictionary of parameter settings (see Parameters class) or an instance of Parameters. If not specified, will use default parameters.

        Returns:
            ModelRoadwayNetwork
        """

        additional_params_dict = {
            k: v
            for k, v in roadway_network_object.__dict__.items()
            if k not in ["nodes_df", "links_df", "shapes_df", "parameters"]
        }

        return ModelRoadwayNetwork(
            roadway_network_object.nodes_df,
            roadway_network_object.links_df,
            roadway_network_object.shapes_df,
            parameters=parameters,
            **additional_params_dict,
        )

    def split_property(
        self, property_name: str, time_periods: Mapping = None, categories: Mapping = None
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
                list(time_periods.keys()), 
                list(categories.keys()),
            ):
                self.links_df[
                        property_name + "_" + category_suffix + "_" + time_suffix
                    ] = self.get_property_by_time_period_and_group(
                        property_name,
                        category=categories[category_suffix],
                        time_period=time_periods[time_suffix],
                        default_value=0
                    )
        elif time_periods:
            for time_suffix, time_defs in time_periods.itmes():
                self.links_df[
                    property_name + "_" + time_suffix
                ] = self.get_property_by_time_period_and_group(
                    property_name,
                    category=None,
                    time_period=time_defs,
                    default_return = 0,
                )
        else:
            raise ValueError(
                "Shoudn't have a category without a time period. Category: {}".format(categories)
            )

    def split_properties_by_time_period_and_category(self, properties_to_split=None):
        """
        Splits properties by time period, assuming a variable structure of

        Args:
            properties_to_split: dict
                dictionary of output variable prefix mapped to the source variable and what to stratify it by
                e.g.
                {
                    'trn_priority' : {'v':'trn_priority', 'times_periods':{"AM": ("6:00", "9:00"),"PM": ("16:00", "19:00")}},
                    'ttime_assert' : {'v':'ttime_assert', 'times_periods':{"AM": ("6:00", "9:00"),"PM": ("16:00", "19:00")}},
                    'lanes' : {'v':'lanes', 'times_periods':{"AM": ("6:00", "9:00"),"PM": ("16:00", "19:00")}},
                    'ML_lanes' : {'v':'ML_lanes', 'times_periods':{"AM": ("6:00", "9:00"),"PM": ("16:00", "19:00")}},
                    'price' : {'v':'price', 'times_periods':{"AM": ("6:00", "9:00"),"PM": ("16:00", "19:00")}},'categories': {"sov": ["sov", "default"],"hov2": ["hov2", "default", "sov"]}},
                    'access' : {'v':'access', 'times_periods':{"AM": ("6:00", "9:00"),"PM": ("16:00", "19:00")}},
                }

        """

        if properties_to_split == None:
            properties_to_split = self.parameters.roadway_network_ps.properties_to_split

        for out_var, params in properties_to_split.items():
            msg = "Splitting {} with params: {}".format(out_var, params)
            #print(msg)
            WranglerLogger.debug(msg)

            if params["v"] not in self.links_df.columns:
                WranglerLogger.warning(
                    "Specified variable to split: {} not in network variables: {}.".format(
                        params["v"], str(self.links_df.columns)
                    )
                )
            elif params.get("time_periods") and params.get("categories"):
                for time_suffix, category_suffix in itertools.product(
                    list(params["time_periods"].keys()), list(params["categories"].keys())
                ):
                    self.links_df[
                        out_var + "_" + category_suffix + "_" + time_suffix
                    ] = self.get_property_by_time_period_and_group(
                        params["v"],
                        category=params["categories"][category_suffix],
                        time_period=params["time_periods"][time_suffix],
                    )
            elif params.get("time_periods"):
                for time_suffix, time_spans in params["time_periods"].items():
                    self.links_df[
                        out_var + "_" + time_suffix
                    ] = self.get_property_by_time_period_and_group(
                        params["v"],
                        category=None,
                        time_period=time_spans,
                    )
            else:
                raise NotImplementedError(
                    "Shoudn't have a category without a time period: {}".format(params)
                )

    def add_polygon_overlay_to_links(
        self, 
        overlay_data: PolygonOverlay,
        method: str = "link_centroid",
        field_mapping: dict = None, 
        fill_value: Any = None,
        overwrite: bool =True
    ):
        """
        Adds or updates roadway network link variables from a PolygonOverlay.

        Args:
            overlay_data: PolygonOverlay dataclass
            method: string indicating the join type for the link.  "link centroid" is only one currently implemented. 
            field_mapping: if added, overwrites PolygonOverlay class's field mapping
            fill_value: if specified, fills span of the overlay with this value
            overwrite: boolean indicating if fields are updated if empty, or overwritten. Defaults to true. 
        """

        field_mapping = overlay_data.field_mapping if field_mapping is None else field_mapping 

        WranglerLogger.debug("Adding geographic overlay variables {} from {}".format(field_mapping,overlay_data.input_filename))

        

    def calculate_county(
        self,
        county_shape=None,
        county_shape_variable=None,
        network_variable="county",
        county_codes_dict=None,
        overwrite=False,
    ):
        """
        Calculates county variable.

        This uses the centroid of the geometry field to determine which county it should be labeled.
        This isn't perfect, but it much quicker than other methods.

        Args:
            county_shape (str): The File path to county geodatabase.
            county_shape_variable (str): The variable name of county in county geodadabase.
            network_variable (str): The variable name of county in network standard.  Default to "county".
            overwrite (Bool): True if overwriting existing county variable in network.  Default to False.

        Returns:
            None
        """
        if network_variable in self.links_df:
            if overwrite:
                WranglerLogger.info(
                    "Overwriting existing County Variable '{}' already in network".format(
                        network_variable
                    )
                )
            else:
                WranglerLogger.info(
                    "County Variable '{}' already in network. Returning without overwriting.".format(
                        network_variable
                    )
                )
                return

        """
        Verify inputs
        """

        county_shape = county_shape if county_shape else self.parameters.county_shape

        county_shape_variable = (
            county_shape_variable
            if county_shape_variable
            else self.parameters.county_variable_shp
        )

        WranglerLogger.info(
            "Adding roadway network variable for county using a spatial join with: {}".format(
                county_shape
            )
        )

        county_codes_dict = (
            county_codes_dict if county_codes_dict else self.parameters.county_code_dict
        )
        if not county_codes_dict:
            msg = "No county codes dictionary specified"
            WranglerLogger.error(msg)
            raise ValueError(msg)

        """
        Start actual process
        """

        centroids_gdf = self.links_df.copy()
        centroids_gdf["geometry"] = centroids_gdf["geometry"].centroid

        county_gdf = gpd.read_file(county_shape)
        county_gdf = county_gdf.to_crs(epsg=self.crs)
        joined_gdf = gpd.sjoin(centroids_gdf, county_gdf, how="left", op="intersects")

        joined_gdf[county_shape_variable] = (
            joined_gdf[county_shape_variable]
            .map(county_codes_dict)
            .fillna(10)
            .astype(int)
        )

        self.links_df[network_variable] = joined_gdf[county_shape_variable]

        WranglerLogger.info(
            "Finished Calculating county variable: {}".format(network_variable)
        )


    def add_variable_using_shst_reference(
        self,
        var_shst_csvdata=None,
        shst_csv_variable=None,
        network_variable=None,
        network_var_type=int,
        overwrite=False,
    ):
        """
        Join network links with source data, via SHST API node match result.

        Args:
            var_shst_csvdata (str): File path to SHST API return.
            shst_csv_variable (str): Variable name in the source data.
            network_variable (str): Name of the variable that should be written to.
            network_var_type : Variable type in the written network.
            overwrite (bool): True is overwriting existing variable. Default to False.

        Returns:
            None

        """
        WranglerLogger.info(
            "Adding Variable {} using Shared Streets Reference from {}".format(
                network_variable, var_shst_csvdata
            )
        )

        var_shst_df = pd.read_csv(var_shst_csvdata)

        if "shstReferenceId" not in var_shst_df.columns:
            msg = "'shstReferenceId' required but not found in {}".format(var_shst_csvdata)
            WranglerLogger.error(msg)
            raise ValueError(msg)

        if shst_csv_variable not in var_shst_df.columns:
            msg = "{} required but not found in {}".format(
                shst_csv_variable, var_shst_csvdata
            )
            WranglerLogger.error(msg)
            raise ValueError(msg)

        join_gdf = pd.merge(
            self.links_df,
            var_shst_df[["shstReferenceId", shst_csv_variable]],
            how="left",
            on="shstReferenceId",
        )

        join_gdf[shst_csv_variable].fillna(0, inplace=True)

        if network_variable in self.links_df.columns and not overwrite:
            join_gdf.loc[join_gdf[network_variable] > 0, network_variable] = join_gdf[
                shst_csv_variable
            ].astype(network_var_type)
        else:
            join_gdf[network_variable] = join_gdf[shst_csv_variable].astype(
                network_var_type
            )

        self.links_df[network_variable] = join_gdf[network_variable]

        WranglerLogger.info(
            "Added variable: {} using Shared Streets Reference".format(network_variable)
        )

    def add_counts(
        self,
        network_variable="AADT",
        mndot_count_shst_data=None,
        widot_count_shst_data=None,
        mndot_count_variable_shp=None,
        widot_count_variable_shp=None,
    ):

        """
        Adds count variable.

        join the network with count node data, via SHST API node match result

        Args:
            network_variable (str): Name of the variable that should be written to.  Default to "AADT".
            mndot_count_shst_data (str): File path to MNDOT count location SHST API node match result.
            widot_count_shst_data (str): File path to WIDOT count location SHST API node match result.
            mndot_count_variable_shp (str): File path to MNDOT count location geodatabase.
            widot_count_variable_shp (str): File path to WIDOT count location geodatabase.

        Returns:
            None
        """

        WranglerLogger.info("Adding Counts")

        """
        Verify inputs
        """

        mndot_count_shst_data = (
            mndot_count_shst_data
            if mndot_count_shst_data
            else self.parameters.mndot_count_shst_data
        )
        widot_count_shst_data = (
            widot_count_shst_data
            if widot_count_shst_data
            else self.parameters.widot_count_shst_data
        )
        mndot_count_variable_shp = (
            mndot_count_variable_shp
            if mndot_count_variable_shp
            else self.parameters.mndot_count_variable_shp
        )
        widot_count_variable_shp = (
            widot_count_variable_shp
            if widot_count_variable_shp
            else self.parameters.widot_count_variable_shp
        )

        for varname, var in {
            "mndot_count_shst_data": mndot_count_shst_data,
            "widot_count_shst_data": widot_count_shst_data,
        }.items():
            if not var:
                msg = "'{}' not found in method or lasso parameters.".format(varname)
                WranglerLogger.error(msg)
                raise ValueError(msg)
            if not os.path.exists(var):
                msg = "{}' not found at following location: {}.".format(varname, var)
                WranglerLogger.error(msg)
                raise ValueError(msg)

        for varname, var in {
            "mndot_count_variable_shp": mndot_count_variable_shp,
            "widot_count_variable_shp": widot_count_variable_shp,
        }.items():
            if not var:
                msg = "'{}' not found in method or lasso parameters.".format(varname)
                WranglerLogger.error(msg)
                raise ValueError(msg)

        """
        Start actual process
        """
        WranglerLogger.debug(
            "Adding MNDOT Counts using \n- shst file: {}\n- shp file: {}\n- as network variable: {}".format(
                mndot_count_shst_data, mndot_count_variable_shp, network_variable
            )
        )
        # Add Minnesota Counts
        self.add_variable_using_shst_reference(
            var_shst_csvdata=mndot_count_shst_data,
            shst_csv_variable=mndot_count_variable_shp,
            network_variable=network_variable,
            network_var_type=int,
            overwrite=True,
        )
        WranglerLogger.debug(
            "Adding WiDot Counts using \n- shst file: {}\n- shp file: {}\n- as network variable: {}".format(
                widot_count_shst_data, widot_count_variable_shp, network_variable
            )
        )
        # Add Wisconsin Counts, but don't overwrite Minnesota
        self.add_variable_using_shst_reference(
            var_shst_csvdata=widot_count_shst_data,
            shst_csv_variable=widot_count_variable_shp,
            network_variable=network_variable,
            network_var_type=int,
            overwrite=False,
        )

        self.links_df["count_AM"] = self.links_df[network_variable] / 4
        self.links_df["count_MD"] = self.links_df[network_variable] / 4
        self.links_df["count_PM"] = self.links_df[network_variable] / 4
        self.links_df["count_NT"] = self.links_df[network_variable] / 4

        self.links_df["count_daily"] = self.links_df[network_variable]
        self.links_df["count_year"] = 2017

        WranglerLogger.info(
            "Finished adding counts variable: {}".format(network_variable)
        )

    @staticmethod
    def read_match_result(path):
        """
        Reads the shst geojson match returns.

        Returns shst dataframe.

        Reading lots of same type of file and concatenating them into a single DataFrame.

        Args:
            path (str): File path to SHST match results.

        Returns:
            geodataframe: geopandas geodataframe

        ##todo
        not sure why we need, but should be in utilities not this class
        """
        refId_gdf = DataFrame()
        refid_file = glob.glob(path)
        for i in refid_file:
            new = gpd.read_file(i)
            refId_gdf = pd.concat([refId_gdf, new], ignore_index=True, sort=False)
        return refId_gdf

    @staticmethod
    def get_attribute(
        links_df,
        join_key,  # either "shstReferenceId", or "shstGeometryId", tests showed the latter gave better coverage
        source_shst_ref_df,  # source shst refId
        source_gdf,  # source dataframe
        field_name,  # , # targetted attribute from source
    ):
        """
        Gets attribute from source data using SHST match result.

        Args:
            links_df (dataframe): The network dataframe that new attribute should be written to.
            join_key (str): SHST ID variable name used to join source data with network dataframe.
            source_shst_ref_df (str): File path to source data SHST match result.
            source_gdf (str): File path to source data.
            field_name (str): Name of the attribute to get from source data.

        Returns:
            None
        """
        # join based on shared streets geometry ID
        # pp_link_id is shared streets match return
        # source_ink_id is mrcc
        WranglerLogger.debug(
            "source ShSt rename_variables_for_dbf columns\n{}".format(
                source_shst_ref_df.columns
            )
        )
        WranglerLogger.debug("source gdf columns\n{}".format(source_gdf.columns))
        # end up with OSM network with the MRCC Link ID
        # could also do with route_sys...would that be quicker?
        join_refId_df = pd.merge(
            links_df,
            source_shst_ref_df[[join_key, "pp_link_id", "score"]].rename(
                columns={"pp_link_id": "source_link_id", "score": "source_score"}
            ),
            how="left",
            on=join_key,
        )

        # joined with MRCC dataframe to get route_sys

        join_refId_df = pd.merge(
            join_refId_df,
            source_gdf[["LINK_ID", field_name]].rename(
                columns={"LINK_ID": "source_link_id"}
            ),
            how="left",
            on="source_link_id",
        )

        # drop duplicated records with same field value

        join_refId_df.drop_duplicates(
            subset=["model_link_id", "shstReferenceId", field_name], inplace=True
        )

        # more than one match, take the best score

        join_refId_df.sort_values(
            by=["model_link_id", "source_score"],
            ascending=True,
            na_position="first",
            inplace=True,
        )

        join_refId_df.drop_duplicates(
            subset=["model_link_id"], keep="last", inplace=True
        )

        # self.links_df[field_name] = join_refId_df[field_name]

        return join_refId_df[links_df.columns.tolist() + [field_name, "source_link_id"]]

    def calculate_hov(
        self,
        network_variable="HOV",
        as_integer=True,
        overwrite=False,
    ):
        """
        Calculates hov variable.

        Args:
            network_variable (str): Variable that should be written to in the network. Default to "HOV"
            as_integer (bool): If True, will convert true/false to 1/0s.  Defauly to True.
            overwrite (Bool): True if overwriting existing county variable in network.  Default to False.

        Returns:
            None
        """

        if network_variable in self.links_df:
            if overwrite:
                WranglerLogger.info(
                    "Overwriting existing hov Variable '{}' already in network".format(
                        network_variable
                    )
                )
            else:
                WranglerLogger.info(
                    "hov Variable '{}' already in network. Returning without overwriting.".format(
                        network_variable
                    )
                )
                return

        WranglerLogger.info(
            "Calculating hov and adding as roadway network variable: {}".format(
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
        self.links_df[network_variable] = 0

        self.links_df.loc[
            (self.links_df["assign_group"] == 8) | (self.links_df["access"] == "hov"),
            network_variable,
        ] = 100

        if as_integer:
            self.links_df[network_variable] = self.links_df[network_variable].astype(
                int
            )
        WranglerLogger.info(
            "Finished calculating hov variable: {}".format(network_variable)
        )

    def create_ML_variable(
        self,
        network_variable="ML_lanes",
        overwrite=False,
    ):
        """
        Created ML lanes placeholder for project to write out ML changes

        ML lanes default to 0, ML info comes from cube LOG file and store in project cards

        Args:
            overwrite (Bool): True if overwriting existing variable in network.  Default to False.

        Returns:
            None
        """
        if network_variable in self.links_df:
            if overwrite:
                WranglerLogger.info(
                    "Overwriting existing ML Variable '{}' already in network".format(
                        network_variable
                    )
                )
                self.links_df[network_variable] = int(0)
            else:
                WranglerLogger.info(
                    "ML Variable '{}' already in network. Returning without overwriting.".format(
                        network_variable
                    )
                )
                return

        """
        Verify inputs
        """

        WranglerLogger.info(
            "Finished creating ML lanes variable: {}".format(network_variable)
        )

    def create_hov_corridor_variable(
        self,
        network_variable="segment_id",
        overwrite=False,
    ):
        """
        Created hov corridor placeholder for project to write out corridor changes

        hov corridor id default to 0, its info comes from cube LOG file and store in project cards

        Args:
            overwrite (Bool): True if overwriting existing variable in network.  Default to False.

        Returns:
            None
        """
        if network_variable in self.links_df:
            if overwrite:
                WranglerLogger.info(
                    "Overwriting existing hov corridor Variable '{}' already in network".format(
                        network_variable
                    )
                )
            else:
                WranglerLogger.info(
                    "Hov corridor Variable '{}' already in network. Returning without overwriting.".format(
                        network_variable
                    )
                )
                return

        """
        Verify inputs
        """

        self.links_df[network_variable] = int(0)

        WranglerLogger.info(
            "Finished creating hov corridor variable: {}".format(network_variable)
        )

    def calculate_centroidconnect(
        self,
        max_taz: int = None,
        base_centroid_conenctor_properties: dict = {"centroidconnect": 1, "lanes": 1},
        additional_centroid_connector_properties: dict = {},
        overwrite: bool =False,
    ):
        """
        Calculates indicator and other variables for centroid connectors. 

        Args:
            max_taz: the max TAZ number in the network.
            centroid_connector_properties: if 
            overwrite: True if overwriting existing properties in network, otherwise updates the dataframe.  Default to False.
        Returns:
            RoadwayNetwork with updated centroid connector designations and properties. 
        """

        if overwrite:
            WranglerLogger.info(
                "Overwriting existing variables for centroid connectors '{}' already in network".format(
                    list(base_centroid_conenctor_properties.keys) + list(additional_centroid_connector_properties.keys),
                )
            )
        else:
            WranglerLogger.info(
                "Updating existing varibles for centroid connectors '{}'".format(
                    list(base_centroid_conenctor_properties.keys) + list(additional_centroid_connector_properties.keys),
                )
            )

        """
        Verify inputs
        """
        max_taz = (
            max_taz if max_taz else self.parameters.roadway_network_ps.max_taz
        )

        if not max_taz:
            msg = "No highest_TAZ number specified in method variable or in parameters"
            WranglerLogger.error(msg)
            raise ValueError(msg)


        property_dict = {}

        WranglerLogger.debug(
            "Calculating Centroid Connectors using\n\
            - Highest TAZ number: {}\n\
            - Property Dictionary: {}".format(
                max_taz,
                property_dict,
            )
        )

        """
        Start actual process
        """
        ##tODO
        

        self.links_df.apply_roadway_feature_change(
            link_idx = self.links_df.index[
                (self.links_df["A"] <= max_taz) \
                | (self.links_df["B"] <= max_taz)
            ],
            properties = property_dict,
        )

        WranglerLogger.info(
            "Finished calculating centroid connector variable: {}".format(
                network_variable,
            )
        )

    

    def coerce_types(self, type_lookup: dict = None):
        """
        Coerce types for links and nodes for columns specified in self.parameters.roadway_network_ps.field_type
            and overriden by the type_lookup dictionary specified as a keyword.

        Args:
            type_lookup: a dictionary mapping field names to types of str, int, or float

        Returns: None
        """
        print("FIELD TYPE: ",self.parameters.roadway_network_ps.field_type)

        _type_lookup = copy.deepcopy(self.parameters.roadway_network_ps.field_type)

        if type_lookup:
            _type_lookup.update(type_lookup)

        WranglerLogger.debug("Coercing types based on:\n {}".format(_type_lookup))

        for c in list(self.links_df.columns):
            if _type_lookup.get(c):
                self.links_df[c] = self.links_df[c].astype(_type_lookup[c])
            else:
                WranglerLogger.debug("Link column {} not found in type lookup, leaving as type: {}".format(c,self.links_df[c].dtype ))
        
        for c in list(self.nodes_df.columns):
            if _type_lookup.get(c):
                self.nodes_df[c] = self.nodes_df[c].astype(_type_lookup[c])
            else:
                WranglerLogger.debug("Node column {} not found in type lookup, leaving as type: {}".format(c,self.nodes_df[c].dtype ))

        self.fill_na()

        WranglerLogger.debug("Link types now:\n {}\nNode types now:\n {}\n".format(self.links_df.dtypes, self.nodes_df.dtypes))

    def fill_na(self):
        """
        Fill na values with zeros and "" based on if they are numeric columns or strings. Right now is looking for
        NA based on: np.nan, "", float("nan"), "NaN"
        """

        WranglerLogger.info("Filling nan for network from network wrangler")

        numeric_col = [c for c,t in self.parameters.roadway_network_ps.field_type.items() if t in [int,float]]

        numeric_link_cols = [c for c in list(self.links_df.columns) if c in numeric_col]
        non_numeric_link_cols = [c for c in list(self.links_df.columns) if c not in numeric_col]

        numeric_node_cols = [c for c in list(self.nodes_df.columns) if c in numeric_col]
        non_numeric_node_cols = [c for c in list(self.nodes_df.columns) if c not in numeric_col]

        non_standard_nan = [np.nan, "", float("nan"), "NaN"] 

        self.links_df[numeric_link_cols] = self.links_df[numeric_link_cols].replace(non_standard_nan,0).fillna(0)
        self.links_df[non_numeric_link_cols] = self.links_df[non_numeric_link_cols].fillna("")

        self.nodes_df[numeric_node_cols] = self.nodes_df[numeric_node_cols].replace(non_standard_nan,0).fillna(0) 
        self.nodes_df[non_numeric_node_cols] = self.nodes_df[non_numeric_node_cols].fillna("")

    
    def rename_variables_for_dbf(
        self,
        input_df,
        variable_crosswalk: str = None,
        output_variables: list = None,
        convert_geometry_to_xy=False,
    ):
        """
        Rename attributes for DBF/SHP, make sure length within 10 chars.

        Args:
            input_df (dataframe): Network standard DataFrame.
            variable_crosswalk (str): File path to variable name crosswalk from network standard to DBF names.
            output_variables (list): List of strings for DBF variables.
            convert_geometry_to_xy (bool): True if converting node geometry to X/Y

        Returns:
            dataframe

        """
        WranglerLogger.info("Renaming variables so that they are DBF-safe")

        """
        Verify inputs
        """

        variable_crosswalk = (
            variable_crosswalk
            if variable_crosswalk
            else self.parameters.net_to_dbf_crosswalk
        )

        output_variables = (
            output_variables if output_variables else self.parameters.output_variables
        )

        """
        Start actual process
        """

        crosswalk_df = pd.read_csv(variable_crosswalk)
        WranglerLogger.debug(
            "Variable crosswalk: {} \n {}".format(variable_crosswalk, crosswalk_df)
        )
        net_to_dbf_dict = dict(zip(crosswalk_df["net"], crosswalk_df["dbf"]))

        dbf_name_list = []

        dbf_df = copy.deepcopy(input_df)

        # only write out variables that we specify
        # if variable is specified in the crosswalk, rename it to that variable
        for c in dbf_df.columns:
            if c in output_variables:
                try:
                    dbf_df.rename(columns={c: net_to_dbf_dict[c]}, inplace=True)
                    dbf_name_list += [net_to_dbf_dict[c]]
                except:
                    dbf_name_list += [c]

        if "geometry" in dbf_df.columns:
            if str(dbf_df["geometry"].iloc[0].geom_type) == "Point":
                dbf_df["X"] = dbf_df.geometry.apply(lambda g: g.x)
                dbf_df["Y"] = dbf_df.geometry.apply(lambda g: g.y)
                dbf_name_list += ["X", "Y"]

        WranglerLogger.debug("DBF Variables: {}".format(",".join(dbf_name_list)))

        return dbf_df[dbf_name_list]

    def write_roadway_as_shp(
        self,
        node_output_variables: list = None,
        link_output_variables: list = None,
        data_to_csv: bool = True,
        data_to_dbf: bool = False,
        output_link_shp: str = None,
        output_node_shp: str = None,
        output_link_csv: str = None,
        output_node_csv: str = None,
    ):
        """
        Write out dbf/shp for cube.  Write out csv in addition to shp with full length variable names.

        Args:
            node_output_variables (list): List of strings for node output variables.
            link_output_variables (list): List of strings for link output variables.
            data_to_csv (bool): True if write network in csv format.
            data_to_dbf (bool): True if write network in dbf/shp format.
            output_link_shp (str): File path to output link dbf/shp.
            output_node_shp (str): File path to output node dbf/shp.
            output_link_csv (str): File path to output link csv.
            output_node_csv (str): File path to output node csv.

        Returns:
            None
        """

        WranglerLogger.info("Writing Network as Shapefile")
        WranglerLogger.debug(
            "Output Variables: \n - {}".format(
                "\n - ".join(self.parameters.output_variables)
            )
        )

        """
        Verify inputs
        """

        if self.nodes_metcouncil_df is None:
            self.roadway_standard_to_met_council_network()

        WranglerLogger.debug(
            "Network Link Variables: \n - {}".format(
                "\n - ".join(self.links_metcouncil_df.columns)
            )
        )
        WranglerLogger.debug(
            "Network Node Variables: \n - {}".format(
                "\n - ".join(self.nodes_metcouncil_df.columns)
            )
        )

        link_output_variables = (
            link_output_variables
            if link_output_variables
            else [
                c
                for c in self.links_metcouncil_df.columns
                if c in self.parameters.output_variables
            ]
        )

        node_output_variables = (
            node_output_variables
            if node_output_variables
            else [
                c
                for c in self.nodes_metcouncil_df.columns
                if c in self.parameters.output_variables
            ]
        )

        # unless specified that all the data goes to the DBF, only output A and B
        dbf_link_output_variables = (
            link_output_variables if data_to_dbf else ["A", "B", "shape_id", "geometry"]
        )

        output_link_shp = (
            output_link_shp if output_link_shp else self.parameters.output_link_shp
        )

        output_node_shp = (
            output_node_shp if output_node_shp else self.parameters.output_node_shp
        )

        output_link_csv = (
            output_link_csv if output_link_csv else self.parameters.output_link_csv
        )

        output_node_csv = (
            output_node_csv if output_node_csv else self.parameters.output_node_csv
        )

        """
        Start Process
        """

        WranglerLogger.info("Renaming DBF Node Variables")
        nodes_dbf_df = self.rename_variables_for_dbf(
            self.nodes_metcouncil_df, output_variables=node_output_variables
        )
        WranglerLogger.info("Renaming DBF Link Variables")
        links_dbf_df = self.rename_variables_for_dbf(
            self.links_metcouncil_df, output_variables=dbf_link_output_variables
        )

        links_dbf_df = gpd.GeoDataFrame(links_dbf_df, geometry=links_dbf_df["geometry"])

        WranglerLogger.info("Writing Node Shapes:\n - {}".format(output_node_shp))
        nodes_dbf_df.to_file(output_node_shp)
        WranglerLogger.info("Writing Link Shapes:\n - {}".format(output_link_shp))
        links_dbf_df.to_file(output_link_shp)

        if data_to_csv:
            WranglerLogger.info(
                "Writing Network Data to CSVs:\n - {}\n - {}".format(
                    output_link_csv, output_node_csv
                )
            )
            self.links_metcouncil_df[link_output_variables].to_csv(
                output_link_csv, index=False
            )
            self.nodes_metcouncil_df[node_output_variables].to_csv(
                output_node_csv, index=False
            )

    # this should be moved to util
    @staticmethod
    def dataframe_to_fixed_width(df):
        """
        Convert dataframe to fixed width format, geometry column will not be transformed.

        Args:
            df (pandas DataFrame).

        Returns:
            pandas dataframe:  dataframe with fixed width for each column.
            dict: dictionary with columns names as keys, column width as values.
        """
        WranglerLogger.info("Starting fixed width conversion")

        # get the max length for each variable column
        max_width_dict = dict(
            [
                (v, df[v].apply(lambda r: len(str(r)) if r != None else 0).max())
                for v in df.columns.values
                if v != "geometry"
            ]
        )

        fw_df = df.drop("geometry", axis=1).copy()
        for c in fw_df.columns:
            fw_df[c] = fw_df[c].apply(lambda x: str(x))
            fw_df["pad"] = fw_df[c].apply(lambda x: " " * (max_width_dict[c] - len(x)))
            fw_df[c] = fw_df.apply(lambda x: x["pad"] + x[c], axis=1)

        return fw_df, max_width_dict

    def write_roadway_as_fixedwidth(
        self,
        node_output_variables: list = None,
        link_output_variables: list = None,
        output_link_txt: str = None,
        output_node_txt: str = None,
        output_link_header_width_txt: str = None,
        output_node_header_width_txt: str = None,
        output_cube_network_script: str = None,
        drive_only: bool = False,
    ):
        """
        Writes out fixed width file.

        This function does:
        1. write out link and node fixed width data files for cube.
        2. write out header and width correspondence.
        3. write out cube network building script with header and width specification.

        Args:
            node_output_variables (list): list of node variable names.
            link_output_variables (list): list of link variable names.
            output_link_txt (str): File path to output link database.
            output_node_txt (str): File path to output node database.
            output_link_header_width_txt (str): File path to link column width records.
            output_node_header_width_txt (str): File path to node column width records.
            output_cube_network_script (str): File path to CUBE network building script.
            drive_only (bool): If True, only writes drive nodes and links

        Returns:
            None

        """

        """
        Verify inputs
        """

        if self.nodes_metcouncil_df is None:
            self.roadway_standard_to_met_council_network()

        WranglerLogger.debug(
            "Network Link Variables: \n - {}".format(
                "\n - ".join(self.links_metcouncil_df.columns)
            )
        )
        WranglerLogger.debug(
            "Network Node Variables: \n - {}".format(
                "\n - ".join(self.nodes_metcouncil_df.columns)
            )
        )

        link_output_variables = (
            link_output_variables
            if link_output_variables
            else [
                c
                for c in self.links_metcouncil_df.columns
                if c in self.parameters.output_variables
            ]
        )

        node_output_variables = (
            node_output_variables
            if node_output_variables
            else [
                c
                for c in self.nodes_metcouncil_df.columns
                if c in self.parameters.output_variables
            ]
        )

        output_link_txt = (
            output_link_txt if output_link_txt else self.parameters.output_link_txt
        )

        output_node_txt = (
            output_node_txt if output_node_txt else self.parameters.output_node_txt
        )

        output_link_header_width_txt = (
            output_link_header_width_txt
            if output_link_header_width_txt
            else self.parameters.output_link_header_width_txt
        )

        output_node_header_width_txt = (
            output_node_header_width_txt
            if output_node_header_width_txt
            else self.parameters.output_node_header_width_txt
        )

        output_cube_network_script = (
            output_cube_network_script
            if output_cube_network_script
            else self.parameters.output_cube_network_script
        )

        """
        Start Process
        """
        link_ff_df, link_max_width_dict = self.dataframe_to_fixed_width(
            self.links_metcouncil_df[link_output_variables]
        )

        if drive_only:
            link_ff_df = link_ff_df.loc[link_ff_df["drive_access"] == 1]

        WranglerLogger.info("Writing out link database")

        link_ff_df.to_csv(output_link_txt, sep=";", index=False, header=False)

        # write out header and width correspondence
        WranglerLogger.info("Writing out link header and width ----")
        link_max_width_df = DataFrame(
            list(link_max_width_dict.items()), columns=["header", "width"]
        )
        link_max_width_df.to_csv(output_link_header_width_txt, index=False)

        node_ff_df, node_max_width_dict = self.dataframe_to_fixed_width(
            self.nodes_metcouncil_df[node_output_variables]
        )
        WranglerLogger.info("Writing out node database")

        if drive_only:
            node_ff_df = node_ff_df.loc[node_ff_df["drive_node"] == 1]

        node_ff_df.to_csv(output_node_txt, sep=";", index=False, header=False)

        # write out header and width correspondence
        WranglerLogger.info("Writing out node header and width")
        node_max_width_df = DataFrame(
            list(node_max_width_dict.items()), columns=["header", "width"]
        )
        node_max_width_df.to_csv(output_node_header_width_txt, index=False)

        # write out cube script
        s = 'RUN PGM = NETWORK MSG = "Read in network from fixed width file" \n'
        s += "FILEI LINKI[1] = %LINK_DATA_PATH%,"
        start_pos = 1
        for i in range(len(link_max_width_df)):
            s += " VAR=" + link_max_width_df.header.iloc[i]

            if (
                self.links_metcouncil_df.dtypes.loc[link_max_width_df.header.iloc[i]]
                == "O"
            ):
                s += "(C" + str(link_max_width_df.width.iloc[i]) + ")"

            s += (
                ", BEG="
                + str(start_pos)
                + ", LEN="
                + str(link_max_width_df.width.iloc[i])
                + ","
            )

            start_pos += link_max_width_df.width.iloc[i] + 1

        s = s[:-1]
        s += "\n"
        s += "FILEI NODEI[1] = %NODE_DATA_PATH%,"
        start_pos = 1
        for i in range(len(node_max_width_df)):
            s += " VAR=" + node_max_width_df.header.iloc[i]

            if (
                self.nodes_metcouncil_df.dtypes.loc[node_max_width_df.header.iloc[i]]
                == "O"
            ):
                s += "(C" + str(node_max_width_df.width.iloc[i]) + ")"

            s += (
                ", BEG="
                + str(start_pos)
                + ", LEN="
                + str(node_max_width_df.width.iloc[i])
                + ","
            )

            start_pos += node_max_width_df.width.iloc[i] + 1

        s = s[:-1]
        s += "\n"
        s += 'FILEO NETO = "%SCENARIO_DIR%/complete_network.net" \n\n    ZONES = %zones% \n\n'
        s += "ROADWAY = LTRIM(TRIM(ROADWAY)) \n"
        s += "NAME = LTRIM(TRIM(NAME)) \n"
        s += "\n \nENDRUN"

        with open(output_cube_network_script, "w") as f:
            f.write(s)
