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
from .logger import WranglerLogger


class ModelRoadwayNetwork(RoadwayNetwork):
    """
    Subclass of network_wrangler class :ref:`RoadwayNetwork <network_wrangler:RoadwayNetwork>`

    A representation of the physical roadway network and its properties.
    """

    def __init__(
        self, nodes: GeoDataFrame, links: DataFrame, shapes: GeoDataFrame, parameters={}
    ):
        """
        Constructor
        """
        super().__init__(nodes, links, shapes)

        if type(parameters) is dict:
            self.parameters = Parameters(**parameters)
        else:
            self.parameters = Parameters(**parameters.__dict__)

        self.links_mtc_df = None
        self.nodes_mtc_df = None
        self.shapes_mtc_df = None
        ##todo also write to file
        # WranglerLogger.debug("Used PARAMS\n", '\n'.join(['{}: {}'.format(k,v) for k,v in self.parameters.__dict__.items()]))

    @staticmethod
    def read(
        link_file: str,
        node_file: str,
        shape_file: str,
        fast: bool = False,
        parameters={},
    ):
        """
        Reads in links and nodes network standard.

        Args:
            link_file (str): File path to link json.
            node_file (str): File path to node geojson.
            shape_file (str): File path to link true shape geojson
            fast (bool): boolean that will skip validation to speed up read time.
            parameters : parameters for lasso.

        Returns:
            ModelRoadwayNetwork
        """
        # road_net =  super().read(link_file, node_file, shape_file, fast=fast)
        road_net = RoadwayNetwork.read(link_file, node_file, shape_file, fast=fast)

        m_road_net = ModelRoadwayNetwork(
            road_net.nodes_df,
            road_net.links_df,
            road_net.shapes_df,
            parameters=parameters,
        )

        return m_road_net

    @staticmethod
    def from_RoadwayNetwork(roadway_network_object, parameters={}):
        """
        RoadwayNetwork to ModelRoadwayNetwork

        Args:
            roadway_network_object (RoadwayNetwork).
            parameters : parameters for lasso.

        Returns:
            ModelRoadwayNetwork
        """
        return ModelRoadwayNetwork(
            roadway_network_object.nodes_df,
            roadway_network_object.links_df,
            roadway_network_object.shapes_df,
            parameters=parameters,
        )

    def split_properties_by_time_period_and_category(self, properties_to_split=None):
        """
        Splits properties by time period, assuming a variable structure of

        Args:
            properties_to_split: dict
                dictionary of output variable prefix mapped to the source variable and what to stratify it by
                e.g.
                {
                    'lanes' : {'v':'lanes', 'times_periods':{"AM": ("6:00", "10:00"),"PM": ("15:00", "19:00")}},
                    'ML_lanes' : {'v':'ML_lanes', 'times_periods':{"AM": ("6:00", "10:00"),"PM": ("15:00", "19:00")}},
                    'use' : {'v':'use', 'times_periods':{"AM": ("6:00", "10:00"),"PM": ("15:00", "19:00")}},
                }

        """
        import itertools

        if properties_to_split == None:
            properties_to_split = self.parameters.properties_to_split

        for out_var, params in properties_to_split.items():
            if params["v"] not in self.links_df.columns:
                WranglerLogger.warning(
                    "Specified variable to split: {} not in network variables: {}. Returning 0.".format(
                        params["v"], str(self.links_df.columns)
                    )
                )
                if params.get("time_periods") and params.get("categories"):

                    for time_suffix, category_suffix in itertools.product(
                        params["time_periods"], params["categories"]
                    ):
                        self.links_df[
                            out_var + "_" + time_suffix + "_" + category_suffix
                        ] = 0
                elif params.get("time_periods"):
                    for time_suffix in params["time_periods"]:
                        self.links_df[out_var + "_" + time_suffix] = 0
            elif params.get("time_periods") and params.get("categories"):
                for time_suffix, category_suffix in itertools.product(
                    params["time_periods"], params["categories"]
                ):
                    self.links_df[
                        out_var + "_" + category_suffix + "_" + time_suffix
                    ] = self.get_property_by_time_period_and_group(
                        params["v"],
                        category=params["categories"][category_suffix],
                        time_period=params["time_periods"][time_suffix],
                    )
            elif params.get("time_periods"):
                for time_suffix in params["time_periods"]:
                    self.links_df[
                        out_var + "_" + time_suffix
                    ] = self.get_property_by_time_period_and_group(
                        params["v"],
                        category=None,
                        time_period=params["time_periods"][time_suffix],
                    )
            else:
                raise ValueError(
                    "Shoudn't have a category without a time period: {}".format(params)
                )

    def create_calculated_variables(self):
        """
        Creates calculated roadway variables.

        Args:
            None
        """
        WranglerLogger.info("Creating calculated roadway variables.")
        self.calculate_facility_type()
        self.create_ML_variable()
        self.create_use_variable()

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
            msg = "'shstReferenceId' required but not found in {}".format(var_shst_data)
            WranglerLogger.error(msg)
            raise ValueError(msg)

        if shst_csv_variable not in var_shst_df.columns:
            msg = "{} required but not found in {}".format(
                shst_csv_variable, var_shst_data
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
            join_gdf.loc[join_gdf[network_variable] > 0][network_variable] = join_gdf[
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
            subset = ["model_link_id", "shstReferenceId", field_name],
            inplace = True
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

        return join_refId_df[links_df.columns.tolist() + [field_name]]

    def calculate_use(
        self,
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
        self.links_df[network_variable] = int(1)

        if as_integer:
            self.links_df[network_variable] = self.links_df[network_variable].astype(
                int
            )
        WranglerLogger.info(
            "Finished calculating hov variable: {}".format(
                network_variable
            )
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

        self.links_df[network_variable] = int(0)

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

    def calculate_distance(
        self, network_variable="distance", centroidconnect_only=True, overwrite=False
    ):
        """
        calculate link distance in miles

        Args:
            centroidconnect_only (Bool):  True if calculating distance for centroidconnectors only.  Default to True.
            overwrite (Bool): True if overwriting existing variable in network.  Default to False.

        Returns:
            None

        """

        if network_variable in self.links_df:
            if overwrite:
                WranglerLogger.info(
                    "Overwriting existing distance Variable '{}' already in network".format(
                        network_variable
                    )
                )
            else:
                WranglerLogger.info(
                    "Distance Variable '{}' already in network. Returning without overwriting.".format(
                        network_variable
                    )
                )
                return

        """
        Verify inputs
        """

        if "centroidconnect" not in self.links_df:
            msg = "No variable specified for centroid connector, calculating centroidconnect first"
            WranglerLogger.info(msg)
            self.calculate_centroidconnect()

        """
        Start actual process
        """

        temp_links_gdf = self.links_df.copy()
        temp_links_gdf.crs = "EPSG:4326"
        temp_links_gdf = temp_links_gdf.to_crs(epsg=26915)

        if centroidconnect_only:
            WranglerLogger.info(
                "Calculating {} for centroid connectors".format(network_variable)
            )
            temp_links_gdf[network_variable] = np.where(
                temp_links_gdf.centroidconnect == 1,
                temp_links_gdf.geometry.length / 1609.34,
                temp_links_gdf[network_variable],
            )
        else:
            WranglerLogger.info(
                "Calculating distance for all links".format(network_variable)
            )
            temp_links_gdf[network_variable] = temp_links_gdf.geometry.length / 1609.34

        self.links_df[network_variable] = temp_links_gdf[network_variable]

    def convert_int(self):
        """
        convert integer columns
        """

        WranglerLogger.info(
            "Converting variable type to mtc standard"
        )

        int_col_names = self.parameters.int_col
        int_col_names.remove("lanes")

        for c in list(self.links_df.columns):
            if c in int_col_names:
                try:
                    self.links_df[c] = self.links_df[c].astype(int)
                except:
                    self.links_df[c] = self.links_df[c].astype(float)
                    self.links_df[c] = self.links_df[c].astype(int)

        for c in list(self.nodes_df.columns):
            if c in int_col_names:
                self.nodes_df[c] = self.nodes_df[c].astype(int)

    def fill_na(self):
        """
        Fill na values from create_managed_lane_network()
        """

        WranglerLogger.info(
            "Filling nan for network from network wrangler"
        )

        num_col = self.parameters.int_col + self.parameters.float_col

        for x in list(self.links_df.columns):
            if x in num_col:
                self.links_df[x].fillna(0, inplace = True)
                self.links_df[x] = self.links_df[x].apply(lambda k:
                    0 if k in [np.nan, '', float('nan'), 'NaN'] else k)

            else:
                self.links_df[x].fillna("", inplace = True)

        for x in list(self.nodes_df.columns):
            if x in num_col:
                self.nodes_df[x].fillna(0, inplace = True)
            else:
                self.nodes_df[x].fillna("", inplace = True)

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
            variable_crosswalk if variable_crosswalk else self.parameters.net_to_dbf_crosswalk
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
            if str(dbf_df["geometry"].geom_type[0]) == "Point":
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

        if self.nodes_mtc_df is None:
            self.roadway_standard_to_met_council_network()

        WranglerLogger.debug(
            "Network Link Variables: \n - {}".format(
                "\n - ".join(self.links_mtc_df.columns)
            )
        )
        WranglerLogger.debug(
            "Network Node Variables: \n - {}".format(
                "\n - ".join(self.nodes_mtc_df.columns)
            )
        )

        link_output_variables = (
            link_output_variables
            if link_output_variables
            else [
                c
                for c in self.links_mtc_df.columns
                if c in self.parameters.output_variables
            ]
        )

        node_output_variables = (
            node_output_variables
            if node_output_variables
            else [
                c
                for c in self.nodes_mtc_df.columns
                if c in self.parameters.output_variables
            ]
        )

        # unless specified that all the data goes to the DBF, only output A and B
        dbf_link_output_variables = (
            dbf_link_output_variables if data_to_dbf else ["A", "B", "geometry"]
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
            self.nodes_mtc_df, output_variables=node_output_variables
        )
        WranglerLogger.info("Renaming DBF Link Variables")
        links_dbf_df = self.rename_variables_for_dbf(
            self.links_mtc_df, output_variables=dbf_link_output_variables
        )

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
            self.links_mtc_df[link_output_variables].to_csv(
                output_link_csv, index=False
            )
            self.nodes_mtc_df[node_output_variables].to_csv(
                output_node_csv, index=False
            )

    # this should be moved to util
    @staticmethod
    def dataframe_to_fixed_with(df):
        """
        Convert dataframe to fixed width format, geometry column will not be transformed.

        Args:
            df (pandas DataFrame).

        Returns:
            pandas dataframe:  dataframe with fixed width for each column.
            dict: dictionary with columns names as keys, column width as values.
        """
        WranglerLogger.info("Starting fixed width convertion")

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
            fw_df[c] = fw_df.apply(lambda x: x.pad + x[c], axis=1)

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
        link_ff_df, link_max_width_dict = self.dataframe_to_fixed_with(
            self.links_metcouncil_df[link_output_variables]
        )

        if drive_only:
            link_ff_df = link_ff_df.loc[link_ff_df['drive_access'] == 1]

        WranglerLogger.info("Writing out link database")

        link_ff_df.to_csv(output_link_txt, sep=";", index=False, header=False)

        # write out header and width correspondence
        WranglerLogger.info("Writing out link header and width ----")
        link_max_width_df = DataFrame(
            list(link_max_width_dict.items()), columns=["header", "width"]
        )
        link_max_width_df.to_csv(output_link_header_width_txt, index=False)

        node_ff_df, node_max_width_dict = self.dataframe_to_fixed_with(
            self.nodes_metcouncil_df[node_output_variables]
        )
        WranglerLogger.info("Writing out node database")

        if drive_only:
            node_ff_df = node_ff_df.loc[node_ff_df['drive_node'] == 1]

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
        s += 'ROADWAY = LTRIM(TRIM(ROADWAY)) \n'
        s += 'NAME = LTRIM(TRIM(NAME)) \n'
        s += '\n \nENDRUN'

        with open(output_cube_network_script, "w") as f:
            f.write(s)
