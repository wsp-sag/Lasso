import os
import glob
from typing import Collection, Any, Mapping, Union

import pandas as pd
from pandas import DataFrame

from lark import Lark, Transformer, v_args

from network_wrangler import TransitNetwork, WranglerLogger
from ..model_transit import ModelTransit, StdToModelAdapter, ModelToStdAdapter
from ..parameters import Parameters


MODEL_TO_STD_PROP_MAP = {
    "LONGNAME": "route_long_name",
    "NAME": "trip_id",
    "N": "shape_model_node_id",
}

STD_TO_MODEL_PROP_MAP = {
    "route_long_name": "LONGNAME",
    "shape_model_node_id": "N",
    "shape_pt_sequence": "order",
}

MODEL_TO_STD_PROP_TRANS = {
    ("FREQ", "headway_secs"): lambda x: x * 60,
    ("HEADWAY", "headway_secs"): lambda x: x * 60,
}

STD_TO_MODEL_PROP_TRANS = {
    ("headway_secs", "FREQ"): lambda x: int(x / 60),
    ("headway_secs", "HEADWAY"): lambda x: int(x / 60),
}

TRNBUILD_ROUTE_PROPERTIES = [
    "NAME",
    "LONGNAME",
    "HEADWAY",
    "MODE",
    "ONEWAY",
    "NODES",
]

PT_ROUTE_PROPERTIES = [
    "NAME",
    "LONGNAME",
    "FREQ",
    "MODE",
    "ONEWAY",
    "OPERATOR",
    "NODES",
]

REQUIRED_ROUTE_PROPERTIES = {
    "TRNBUILD": ["HEADWAY", "NAME", "MODE", "NODES"],
    "PT": ["FREQ", "NAME", "MODE", "NODES"],
}

MODEL_ROUTE_PROPERTIES = {
    "TRNBUILD": TRNBUILD_ROUTE_PROPERTIES,
    "PT": PT_ROUTE_PROPERTIES,
}

REQUIRED_NODE_PROPERTIES = {
    "TRNBUILD": [],
    "PT": [],
}

MODEL_NODE_PROPERTIES = ["NNTIME"]

# Property used for defining which time period exists for each route
TP_PROPERTY = {
    "TRNBUILD": "HEADWAY",
    "PT": "FREQ",
}

ROUTE_ID_PROP = "NAME"
NODE_ID_PROP = "N"

# Properties which vary by time period
TIME_VARYING_TRANSIT_PROPERTIES = {"HEADWAY", "FREQ"}

DEFAULT_CUBE_TRANSIT_PROGRAM = "TRNBUILD"


class CubeTransit(ModelTransit):
    """Subclass of :py:class:`ModelTransit` which has special overriding
    methods for "special cube things".

    .. highlight:: python
    Typical usage example:
    ::
        tnet = CubeTransit.from_source(GTFS_DIR)
        tnet.write_cube(outpath="cube_from_gtfs.lin")

    """

    @classmethod
    def from_source(cls, source: Any, **kwargs):
        """Creates a CubeTransit subclass of `ModelTransit` from source data.

        Args:
            source (Any): source of transit network data to create object from. One of:
                (1) an existing network wrangler TransitNetwork object
                (2) a cube line file (source ends in ".lin", is a string with "line name",
                    or source_type is one of "cube", "trnbuild", "pt")
                (3) gtfs feed (source_type one of "std", "standard", "gtfs" or
                    minimum gtfs files found in source )

        Returns:
            CubeTransit: Subclass of `ModelTransit` with
        """
        tnet = cls(**kwargs)
        tnet.add_source(source, **kwargs)
        return tnet

    def __init__(
        self,
        route_properties_df: DataFrame = None,
        shapes_df: DataFrame = None,
        cube_transit_program: str = DEFAULT_CUBE_TRANSIT_PROGRAM,
        parameters: Parameters = None,
        parameters_dict: Mapping[str, Any] = {},
        tp_property: str = None,
        route_properties: Collection[str] = None,
        node_properties: Collection[str] = None,
        required_route_properties: Collection[str] = None,
        required_node_properties: Collection[str] = None,
        **kwargs,
    ):
        """Constructor for CubeModelTransit class.

        Args:
            route_properties_df (DataFrame, optional): Dataframe of route properties with one
                row for each route/line.  Time-varying fields are specified as additional
                horizontal fields. Defaults to None. Constructed from reading in source files.
            shapes_df (DataFrame, optional): Dataframe of route id fields with minimum values
                of "N","order","stop" and any required_node_properties. Defaults to None.
                Constructed from reading in source files.
            cube_transit_program (str, optional): One of TRNBUILD or PT. Defaults
                to `DEFAULT_CUBE_TRANSIT_PROGRAM`.
            parameters (`Parameters`, optional): Parameters instance. Defaults to `Parameters`()
                defaults.
            parameters_dict (Mapping[str, Any], optional): Dictionary which can add additional
                paramteters to the Parameters instance or to overwrite what is there.
                Defaults to {}.
            tp_property (str, optional): Transit route property which defines which time
                periods the route exists in. Defaults to `TP_PROPERTY`[cube_transit_program].
            route_properties (Collection[str], optional): Properties which are
                calculated and saved for route-level properties.
                Defaults to `MODEL_ROUTE_PROPERTIES`[cube_transit_program].
            node_properties (Collection[str], optional):  Properties which are
                calculated and saved for route nodes/shapes.
                Defaults to `MODEL_NODE_PROPERTIES`[cube_transit_program].
            required_route_properties (Collection[str], optional): Properties which are required
                for the route-level properties and will be always written out.
                Defaults to `REQUIRED_ROUTE_PROPERTIES`[cube_transit_program].
            required_node_properties (Collection[str], optional): Properties which are required
                for the route nodes/shapes and will be always written out.
                Defaults to `REQUIRED_NODE_PROPERTIES`[cube_transit_program].
        """

        self.cube_transit_program = cube_transit_program

        self.tp_property = TP_PROPERTY[cube_transit_program]
        if tp_property:
            self.tp_property = tp_property

        self.route_properties = MODEL_ROUTE_PROPERTIES[cube_transit_program]
        if route_properties:
            self.route_properties = route_properties

        self.node_properties = MODEL_NODE_PROPERTIES
        if node_properties:
            self.node_properties = node_properties

        self.required_route_properties = REQUIRED_ROUTE_PROPERTIES[cube_transit_program]
        if required_route_properties:
            self.required_route_properties = required_route_properties

        self.required_node_properties = REQUIRED_NODE_PROPERTIES[cube_transit_program]
        if required_node_properties:
            self.required_node_properties = required_node_properties

        super().__init__(
            route_properties_df=None,
            shapes_df=None,
            route_id_prop=ROUTE_ID_PROP,
            node_id_prop=NODE_ID_PROP,
            model_type="cube",
            parameters=parameters,
            parameters_dict=parameters_dict,
            **kwargs,
        )

    def add_source(
        self, source: Union[str, TransitNetwork], source_type: str = "", **kwargs
    ):
        """Wrapper method to add transit to CubeTransitNetwork from either
        (1) an existing network wrangler TransitNetwork object
        (2) a cube line file (source ends in ".lin", is a string with "line name", or
            source_type ios one of "cube", "trnbuild", "pt")
        (3) gtfs feed (source_type one of "std", "standard", "gtfs" or
            minimum gtfs files found in source )

        Passes through additional kwargs to read methods:
        - :py:meth:`add_std_source`
        - :py:meth:`add_model_source`

        Args:
            source (Any): Either a network wrangler TransitNetwork object, a file path
                to a cube line file, a cube line file string, or a directory containing GTFS files.
            source_type (str, optional): If specified, will use it to determine how
                to read in source. Values which are used include: ["std", "standard", "gtfs"]
                    and ["cube", "trnbuild", "pt"] Defaults to "".

        Raises:
            ValueError: Raised if source is not a TransitNetwork object and
                isn't a valid file path.
            NotImplementedError: Raised if not a TransitNetwork object and minimum
                GTFS files not found.
        """
        if (type(source) == TransitNetwork) or (
            source_type.lower() in ["std", "standard", "gtfs"]
        ):
            self.add_std_source(source, **kwargs)
            return
        elif (source_type.lower() in ["cube", "trnbuild", "pt"]) or any(
            [x in source.lower() for x in ["line name", ".lin"]]
        ):
            self.add_model_source(source, **kwargs)
            return

        if not os.path.exists(source):
            msg = f"No path exists: {source}"
            raise ValueError(msg)

        GTFS_MIN_FILES = ["stops.txt", "trips.txt", "routes.txt"]
        gtfs_files_exist = all(
            [os.path.exists(os.path.join(source, f)) for f in GTFS_MIN_FILES]
        )

        if gtfs_files_exist:
            self.add_std_source(source, **kwargs)
        else:
            msg = f"""Not sure how to read source: {source} with source_types: {source_type}.
                Try specifying keyword: `source_type`."""
            raise NotImplementedError(msg)

    def add_std_source(
        self,
        standardtransit: Union[TransitNetwork, str],
        cube_transit_program: str = None,
        std_to_model_prop_map: Mapping[str, str] = None,
        std_to_model_prop_trans: Mapping[Collection[str], Any] = None,
        tp_property: str = None,
        node_properties: Collection[str] = None,
        route_properties: Collection[str] = None,
        parameters: Parameters = None,
    ):
        """Adds standard transit networks to CubeModelTransit object using
        :py:class:`StdToCubeAdapter` to translate.

        Args:
            standardtransit (Union[TransitNetwork, str]): Either a NetworkWrangler
                standard TransitNetwork object or a directory with GTFS files in it.
            cube_transit_program (str, optional): Specifies which "flavor" of cube transit
                the adapter should use. One of ["TRNBUILD","PT"]. Defaults to
                self.cube_transit_program which is fed by parameters.
            std_to_model_prop_map (Mapping[str, str], optional): Specifies key:values
                relating standard transit properties to model properties which don't need to
                be transformed...just renamed. Defaults to None.
            std_to_model_prop_trans (Mapping[Collection[str], Any], optional): Maps a tuple of
                (standard property, model property) to a transformation function that turns the
                standard property into the model property. Defaults to None.
            tp_property (str, optional): Property which specifies whether a transit route
                exists in various time periods.  Defaults to self.tp_property
                which defaults to "FREQ" for PT or "HEADWAY" for TRNBUILD.
            node_properties (Collection[str], optional): Specifies the model properties for nodes.
                Defaults to self.node_properties which defaults to [].
            route_properties (Collection[str], optional): Specifies the model properties for
                routes. Defaults to self.route_properties which defaults to ["NAME","LONGNAME",
                "MODE","ONEWAY","NODES"] for either PT or TRNBUILD and then ["FREQ" and "OPERATOR"]
                for PT and ["HEADWAY"] for TRNBUILD.
            parameters (Parameters, optional): parameters object, which is used in the
                `StdToCubeAdapter`. Defaults to self.parameters.
        """
        if cube_transit_program is None:
            cube_transit_program = self.cube_transit_program
        if tp_property is None:
            tp_property = self.tp_property
        if node_properties is None:
            node_properties = self.node_properties
        if route_properties is None:
            route_properties = self.route_properties
        if parameters is None:
            parameters = self.parameters

        if type(standardtransit) is str:
            standardtransit = TransitNetwork.read(standardtransit)

        _adapter = StdToCubeAdapter(
            standardtransit,
            cube_transit_program=cube_transit_program,
            std_to_model_prop_map=std_to_model_prop_map,
            std_to_model_prop_trans=std_to_model_prop_trans,
            tp_property=tp_property,
            node_properties=node_properties,
            route_properties=route_properties,
            parameters=parameters,
        )

        model_route_properties_by_time_df, model_nodes_df = _adapter.transform()
        new_routes = (
            model_route_properties_by_time_df[self.route_id_prop].unique().tolist()
        )

        super().add_source(
            route_properties_by_time_df=model_route_properties_by_time_df,
            route_shapes_df=model_nodes_df,
            new_routes=new_routes,
            source_list=["standard transit object"],
        )

    def add_model_source(self, transit_source: str, cube_transit_program: str = None):
        """Reads a .lin file and adds it to the object instance and the dataframes held in it.

        Checks to make sure route ids (usually NAME) are unique.

        Args:
            transit_source: a string or the filename of the cube line file to be parsed.
            cube_transit_program: can explicitly state either PT or TRNBULD; if not specified,
                will try and discern from the source.

        """
        if not cube_transit_program:
            cube_transit_program = self.cube_transit_program
        (
            route_properties_df,
            route_shapes_df,
            source_list,
            cube_transit_program,
        ) = CubeTransitReader(cube_transit_program).read(transit_source)

        if cube_transit_program:
            self.cube_transit_program = cube_transit_program
        new_routes = route_properties_df[self.route_id_prop].unique().tolist()
        WranglerLogger.debug(f"Adding routes: {new_routes} \nfrom: {source_list}")

        super().add_source(
            route_properties_df=route_properties_df,
            route_shapes_df=route_shapes_df,
            new_routes=new_routes,
            source_list=source_list,
        )

        return self

    def write_cube(
        self,
        outpath,
        route_properties: Collection[str] = None,
        node_properties: Collection[str] = None,
    ):
        """Writes out the object as a cube line file using `CubeTransitWriter.write_dfs`

        Args:
            outpath (str): location and filename for the output line files.
            route_properties (Collection[str], optional): List of route properties to include
                in line file output. Defaults to self.route_properties.
            node_properties (Collection[str], optional): List of node properties to include
                in line file output. Defaults to self.node_properties.
        """

        if not route_properties:
            route_properties = [
                p
                for p in self.route_properties
                if p in self.route_properties_by_time_df.columns
            ]

        if not node_properties:
            node_properties = [
                p for p in self.node_properties if p in self.shapes_df.columns
            ]

        outfilename = CubeTransitWriter.write_dfs(
            self._route_properties_by_time_df,
            self.shapes_df,
            outpath=outpath,
            route_properties=route_properties,
            node_properties=node_properties,
            cube_transit_program=self.cube_transit_program,
        )
        WranglerLogger.info(f"Wrote to {outfilename}")

    def _melt_routes_by_time_period(
        self, transit_properties_df: DataFrame,
    ) -> DataFrame:
        """Translates a wide dataframe with fields for time-dependent route properties
        for each time period, to a long dataframe with a row for every route/time period
        combination.

        Wrapper method for :py:method:`ModelTransit._melt_routes_by_time_period`.

        Uses:
            df_key = self.route_id_prop
            tp_property = self.tp_property

        Args:
            transit_properties_df (DataFrame): dataframe with a row for each route and
            time periods which are time-dependent.

        Returns:
            DataFrame: Long dataframew with a row for every route/time period combination.
        """

        df = super()._melt_routes_by_time_period(
            transit_properties_df,
            df_key=self.route_id_prop,
            tp_property=self.tp_property,
        )

        return df

    @staticmethod
    def base_prop_from_time_varying_prop(prop: str) -> str:
        """Returns the base property name from a composite field name which
        also contains the time period id.

        Example:  HEADWAY[1] ---> HEADWAY

        Args:
            property (str): Field name with time period embedded, i.e. FREQ[2]

        Returns:
            str: Base field name without time period id.
        """
        return prop.split("[")[0]

    @classmethod
    def get_tp_nums_from_model_properties(cls, properties_list: list):
        """
        Finds properties that are associated with time periods and the
        returns the numbers in them.

        Args:
            properties_list (list): list of all properties.

        Returns:
            list of strings of the time period numbers found
        """
        time_props = cls.time_varying_props_filter(properties_list)
        tp_num_list = list(
            set([cls.tp_num_from_time_varying_prop(p) for p in time_props])
        )

        return tp_num_list

    @staticmethod
    def tp_num_from_time_varying_prop(prop: str) -> int:
        """Returns the time period number from a composite field name.

        Example:  HEADWAY[1] ---> 1

        Args:
            property (str): Field name with time period embedded, i.e. FREQ[2]

        Returns:
            int: Transit time period ID.
        """
        return prop.split("[")[1][0]

    @staticmethod
    def time_varying_props_filter(properties_list: Collection) -> Collection:
        """Filters a collection of route properties into a collection of time-varying
        route properties.

        Args:
            properties_list (Collection): List of route properties, e.g. ["NAME",
                "HEADWAY[1]","HEADWAY[2]"]

        Returns:
            Collection: List of route properties which vary by time,
                e.g. ["HEADWAY[1]","HEADWAY[2]"]
        """
        time_properties = [p for p in properties_list if ("[" in p) and ("]" in p)]
        return time_properties

    @staticmethod
    def model_prop_from_base_prop_tp_num(basename: str, tp_num: int) -> str:
        """Creates a composite, time-varying route property name from a
        base name and a transit time period number.

        Args:
            basename (str): Base route property name, e.g. "HEADWAY"
            tp_num (int): Transit time period number, e.g. 2

        Returns:
            str: Composite, time-varying route property name, e.g. "HEADWAY[2]"
        """
        return f"{basename}[{tp_num}]"


class StdToCubeAdapter(StdToModelAdapter):
    """Object with methods to translate between a standard GTFS data in a
    :py:class:`TransitNetwork` instance and :py:class:`ModelTransit`.
    Subclass of :py:class:`StdToModelAdapter` with cube-specific methods.

    .. highlight:: python
    Typical usage example:
    ::
        _adapter = StdToCubeAdapter(
            standardtransit,
            cube_transit_program="PT",
            std_to_model_prop_map={
                "route_long_name": "LONGNAME",
                "shape_model_node_id": "N",
                "shape_pt_sequence": "order",
                },
            std_to_model_prop_trans={
                 ("HEADWAY", "headway_secs"): lambda x: x * 60,
            },
            tp_property="HEADWAY",
            node_properties=[],
            route_properties=["HEADWAY","NAME","OPERATOR"],
            parameters=parameters,
        )

        model_route_properties_by_time_df, model_nodes_df = _adapter.transform()
    """

    def __init__(
        self,
        standardtransit: TransitNetwork,
        cube_transit_program: str = DEFAULT_CUBE_TRANSIT_PROGRAM,
        parameters: Parameters = None,
        std_to_model_prop_map: Mapping[str, str] = None,
        std_to_model_prop_trans: Mapping[Collection[str], Any] = None,
        tp_property: str = None,
        node_properties: Collection[str] = None,
        route_properties: Collection[str] = None,
        **kwargs,
    ):
        """Constructor method for StdToCubeAdapter.

        Args:
            standardtransit (:py:class:`TransitNetwork`): Input standard transit instance.
            cube_transit_program (str, optional): TRNBUILD or PT. Defaults to
                DEFAULT_CUBE_TRANSIT_PROGRAM.
            parameters (py:class:`Parameters`): Parameters instance for doing translations
                between time periods. Defaults to none and then default parameters.
            std_to_model_prop_map (Mapping[str, str], optional): Dictionary mapping
                standard transit property to cube model property which just need to be renamed
                without any translation.
                Example:
                    {
                        "route_long_name": "LONGNAME",
                        "shape_model_node_id": "N",
                        "shape_pt_sequence": "order",
                    }.Defaults to :py:`STD_TO_MODEL_PROP_MAP`.
            std_to_model_prop_trans (Mapping[Collection[str], Any], optional): Maps a tuple of
                (standard property,model property) to a transformation function that turns the
                standard/gtfs property into the cube model property.
                Defaults to :py:`std_to_model_prop_trans`.
                Example: {} ("HEADWAY", "headway_secs"): lambda x: x * 60}
            tp_property (str, optional): transit route property which defines which time
                periods the route exists in. Defaults to :py:`TP_PROPERTY`[cube_transit_program].
            node_properties (Collection[str], optional):  Properties which are
                calculated and saved for route nodes/shapes.
                Defaults to `MODEL_NODE_PROPERTIES`[cube_transit_program].
            route_properties (Collection[str], optional): Properties which are
                calculated and saved for route-level properties.
                Defaults to `MODEL_ROUTE_PROPERTIES`[cube_transit_program].
        """
        self.__standardtransit = standardtransit
        self.feed = self.__standardtransit.feed
        self.cube_transit_program = cube_transit_program

        ###PARAMETERS
        self.std_to_model_prop_map = STD_TO_MODEL_PROP_MAP
        if std_to_model_prop_map:
            self.std_to_model_prop_map = std_to_model_prop_map

        self.std_to_model_prop_trans = STD_TO_MODEL_PROP_TRANS
        if std_to_model_prop_trans:
            self.std_to_model_prop_trans = std_to_model_prop_trans

        self.tp_property = TP_PROPERTY[cube_transit_program]
        if tp_property:
            self.tp_property = tp_property

        self.node_properties = MODEL_NODE_PROPERTIES
        if node_properties:
            self.node_properties = node_properties

        self.route_properties = MODEL_ROUTE_PROPERTIES[cube_transit_program]
        if route_properties:
            self.route_properties = route_properties

        if not Parameters:
            parameters = Parameters()
        self.parameters = parameters.update(kwargs)
        transit_ps = self.parameters.transit_network_ps

        self.gtfs_to_cube_operator_lookup = transit_ps.__dict__.get(
            "transit_value_lookups"
        ).get("gtfs_agency_id_to_cube_operator")
        self.gtfs_to_cube_mode_lookup = transit_ps.__dict__.get(
            "transit_value_lookups"
        ).get("gtfs_route_type_to_mode")
        self.tod_num_2_tod_abbr = (
            transit_ps.transit_network_model_to_general_network_time_period_abbr
        )

        self.time_period_abbr_to_time = (
            transit_ps.network_model_parameters.time_period_abbr_to_time
        )

    def calculate_model_tod_name_abbr(
        self, model_route_properties_df: pd.DataFrame
    ) -> pd.Series:
        """Calculates the time of day period abbreviation used by the transit agency based
        on the start time of the transit schedule.  Time of day periods are stored in:
        self.time_period_abbr_to_time.

        Args:
            model_route_properties_df (DataFrame): [description]

        Returns:
            pd.Series: [description]
        """
        from ..time_utils import time_sec_to_time_period

        tp_abbr_s = model_route_properties_df.start_time.apply(
            time_sec_to_time_period,
            time_period_abbr_to_time=self.time_period_abbr_to_time,
        )

        return tp_abbr_s

    def calculate_model_tod_num(
        self, model_route_properties_df: pd.DataFrame
    ) -> pd.Series:
        """Calculates the number used by Cube's transit software which is associated
        with the time period and used for fields like HEADWAY[1] where 1 is the model time
        period number (`tod_num`).

        Mapping is stored in parameter
        `transit_network_ps.transit_network_model_to_general_network_time_period_abbr`

        Args:
            model_route_properties_df (DataFrame): [description]

        Returns:
            pd.Series: `tod_num`, the number used by Cube's transit software which is associated
        with the time period and used for fields like HEADWAY[1] where 1 is the model time
        period number.
        """

        _tod_abbr_2_tod_num = {v: k for k, v in self.tod_num_2_tod_abbr.items()}

        if "tod_name" not in model_route_properties_df:
            model_route_properties_df["tod_name"] = self.calculate_model_tod_name_abbr(
                model_route_properties_df
            )

        tp_num_s = model_route_properties_df.tod_name.map(_tod_abbr_2_tod_num)

        return tp_num_s

    def calculate_model_mode(
        self, model_route_properties_df: pd.DataFrame
    ) -> pd.Series:
        """Assigns a model mode number by following logic.

        Uses GTFS route_type variable:
            https://developers.google.com/transit/gtfs/reference

        If `self.gtfs_to_cube_mode_lookup` exists, will
        update based on that mapping, otherwise returns gtfs route_type

        Args:
            model_route_properties_df ([type]): [description]

        Returns:
            pd.Series: [description]
        """

        if self.gtfs_to_cube_mode_lookup:
            return model_route_properties_df["route_type"].map(
                self.gtfs_to_cube_mode_lookup
            )
        else:
            return model_route_properties_df["route_type"]

    def calculate_model_operator(
        self, model_route_properties_df: pd.DataFrame
    ) -> pd.Series:
        """If `self.gtfs_to_cube_operator_lookup` exists, will
        update based on that mapping, otherwise returns agency_id.

        Args:
            model_route_properties_df ([type]): [description]

        Returns:
            pd.Series: [description]
        """

        if self.gtfs_to_cube_operator_lookup:
            return model_route_properties_df["agency_id"].map(
                self.gtfs_to_cube_operator_lookup
            )
        else:
            return model_route_properties_df["agency_id"]

    def transform_nodes(self) -> pd.DataFrame:
        """Transforms std node/shapes to model shapes.

        Identifies stops by finding them in stop_times for each trip.
        Replicates shapes for each trip to create a shape dataframe.

        Returns:
            pd.DataFrame: Dataframe for each route, node, visit order combination
                with following fields: "NAME","N", "stop","order" + fields specified
                in self.node_properties.
        """
        # msg = f"""self.feed.shapes:\n
        #    {self.feed.shapes[['shape_pt_sequence','shape_model_node_id']]}"""
        # WranglerLogger.debug(msg)
        # msg = f"self.feed.stop_times:\n {self.feed.stop_times[['stop_id','stop_sequence']]}"
        # WranglerLogger.debug(msg)

        # get model_node_id and shape_ids mapped to stoptimes
        _stop_times_df = pd.merge(
            self.feed.stop_times[["stop_id", "trip_id"]],
            self.feed.stops[["stop_id", "model_node_id"]],
            how="left",
            on="stop_id",
        )

        _stop_times_df = pd.merge(
            _stop_times_df[["model_node_id", "trip_id", "stop_id"]],
            self.feed.trips[["shape_id", "trip_id"]],
            how="left",
            on="trip_id",
        )

        WranglerLogger.debug(f"_stop_times_df:\n {_stop_times_df}")

        # if exists in _stop_times_df, then it is a stop. Otherwise...a shape point.
        _shape_nodes_df = pd.merge(
            _stop_times_df[["shape_id", "model_node_id", "stop_id"]],
            self.feed.shapes[["shape_id", "shape_model_node_id", "shape_pt_sequence"]],
            how="outer",
            right_on=["shape_id", "shape_model_node_id"],
            left_on=["shape_id", "model_node_id"],
            indicator=True,
        )

        _shape_nodes_df["stop"] = _shape_nodes_df._merge.map(
            {"right_only": False, "both": True}
        )

        _nodes_df = pd.merge(
            _shape_nodes_df[
                [
                    "shape_id",
                    "stop_id",
                    "shape_model_node_id",
                    "stop",
                    "shape_pt_sequence",
                ]
            ],
            self.feed.trips[["shape_id", "trip_id"]],
            how="left",
            on="shape_id",
        )

        for std_prop, model_prop in self.std_to_model_prop_trans.keys():
            if std_prop not in _nodes_df.columns:
                continue
            _nodes_df[model_prop] = _nodes_df[std_prop].apply(
                self.std_to_model_prop_trans[(std_prop, model_prop)]
            )

        _rename_node_map = {
            k: v
            for k, v in self.std_to_model_prop_map.items()
            if k in _nodes_df.columns
        }
        # WranglerLogger.debug(f"_rename_node_map: {_rename_node_map}")
        _nodes_df = _nodes_df.rename(columns=_rename_node_map)
        # WranglerLogger.debug(f"_nodes_df 2 :\n {_nodes_df}")

        # get name from self.model_routes_df
        _nodes_df = pd.merge(
            self.model_routes_df[["trip_id", "NAME"]],
            _nodes_df,
            how="outer",
            on="trip_id",
        )
        _nodes_df = _nodes_df.sort_values(by=["NAME", "order"])
        # WranglerLogger.debug(f"_nodes_df 3 :\n {_nodes_df}")
        WranglerLogger.debug(f"Number of Stops:{len(_nodes_df[_nodes_df.stop])}")
        _properties_list = ["N", "stop", "NAME", "order"] + [
            p for p in self.node_properties if p in _nodes_df.columns
        ]

        return _nodes_df[_properties_list]

    def transform_routes(self, feed=None) -> pd.DataFrame:
        """Transforms route properties from standard properties to
        Cube properties.

        Returns:
            DataFrame: (DataFrame): DataFrame of trips with cube-appropriate values for:
                - NAME
                - ONEWAY
                - OPERATOR
                - MODE
                - HEADWAY
                - tp_abbr
                - tp_num
        """
        if feed is None:
            feed = self.feed
        _routes_df = feed.trips.merge(feed.routes, how="left", on="route_id")
        _routes_df = _routes_df.merge(feed.frequencies, how="left", on="trip_id")

        # WranglerLogger.debug(f"_routes_df: {_routes_df}")
        for std_prop, model_prop in self.std_to_model_prop_trans.keys():
            if std_prop not in _routes_df.columns:
                continue
            _routes_df[model_prop] = _routes_df[std_prop].apply(
                self.std_to_model_prop_trans[(std_prop, model_prop)]
            )

        _rename_route_map = {
            k: v
            for k, v in self.std_to_model_prop_map.items()
            if k in _routes_df.columns
        }
        _routes_df = _routes_df.rename(columns=_rename_route_map)

        _routes_df["OPERATOR"] = self.calculate_model_operator(_routes_df)
        _routes_df["ONEWAY"] = "T"
        _routes_df["MODE"] = self.calculate_model_mode(_routes_df)
        _routes_df["tp_abbr"] = self.calculate_model_tod_name_abbr(_routes_df)
        _routes_df["tp_num"] = self.calculate_model_tod_num(_routes_df)
        _routes_df["NAME"] = self.calculate_route_name(_routes_df)

        self.model_routes_df = _routes_df

        return _routes_df


class CubeToStdAdapter(ModelToStdAdapter):
    """Object with methods to translate between a :py:class:`ModelTransit`
    instance and standard GTFS data in a :py:class:`TransitNetwork`.
    Subclass of :py:class:`ModelToStdAdapter` with cube-specific methods.

    WIP. INCOMPLETE.


    Args:
        ModelToStdAdapter ([type]): [description]

    .. highlight:: python
    Typical usage example:
    ::
        ##todo
    """

    def __init__(
        self,
        modeltransit: ModelTransit,
        cube_transit_program=None,
        model_to_std_prop_map: Mapping[str, str] = None,
        model_to_std_prop_trans: Mapping[Collection[str], Any] = None,
    ) -> None:
        """Constructor method for object to translate between a :py:class:`ModelTransit` instance
         and standard GTFS data in a :py:class:`TransitNetwork`. WIP. INCOMPLETE.

        Args:
            modeltransit (ModelTransit): Instance to be transformed.
            cube_transit_program ([type], optional): TRNBUILD or PT.
                Defaults to None.
            model_to_std_prop_map (Mapping[str, str], optional): Specifies key:values
                relating  model transit properties to standard transit properties/gtfs which
                don't need to be transformed...just renamed.  Defaults to None.
            model_to_std_prop_trans (Mapping[Collection[str], Any], optional):Maps a tuple of
                (model property,standard property) to a transformation function that turns the
                model property into the standard/gtfs property. Defaults to None.
        """

        if not model_to_std_prop_map:
            model_to_std_prop_map = MODEL_TO_STD_PROP_MAP
        if not model_to_std_prop_trans:
            model_to_std_prop_trans = MODEL_TO_STD_PROP_TRANS

        super().__init__(
            modeltransit,
            model_to_std_prop_map=model_to_std_prop_map,
            model_to_std_prop_trans=model_to_std_prop_trans,
        )

        self.cube_transit_program = modeltransit.cube_transit_program
        self.cube_transit_program = cube_transit_program


class CubeTransitReader:
    """Class with methods for reading cube lines as a file
    or a string and translating them into two dataframes:
        - route_properties_df: keyed by route NAME with a column for
            each route-level property
        - route_shapes_df: keyed by route NAME, N (node #), and order. Also
            has a boolean field for stop.

    .. highlight:: python
    Typical usage example:
    ::

        (
            route_properties_df,
            route_shapes_df,
            source_list,
            cube_transit_program
        ) = CubeTransitReader.read(
            "my_line_file.lin",
        )

    """

    def __init__(
        self, cube_transit_program: str, route_id_prop: str = "NAME",
    ):
        self.program = cube_transit_program
        self.route_id_prop = route_id_prop

    def validate_routes(self, route_properties_df: DataFrame) -> None:
        """Validates that routes in the route_properties_df have unique
        names using self.route_id_prop.

        Args:
            route_properties_df (DataFrame): Dataframe of routes read in.

        Raises:
            ValueError: Raised if duplicate name found.
        """
        _dupes = route_properties_df[route_properties_df.duplicated(subset=["NAME"])]
        if len(_dupes) > 0:
            msg = f"Invalid transit: following route names are duplicate:\
                {_dupes[self.route_id_prop ]}."
            raise ValueError(msg)

    @classmethod
    def read(cls, transit_source: str) -> Collection:
        """Read in and parse cube transit lines using `TRANSIT_LINE_FILE_GRAMMAR`
        and transform using `CubeTransformer` to a parsed data tree. Translate the
        data tree into route_properties_df and route_shapes_df.

        Args:
            transit_source (str): Cube line file location or a string representing
                a cube line string.

        Raises:
            ValueError: Raised if cannot understand the transit_source format.

        Returns:
            (route_properties_df, route_shapes_df, source_list, cube_transit_program)
            - route_properties_df has a line for each transit route read in and a field
                for each route-level property.
            - route_shapes_df has a line for each route/node/visit order as well as a
                boolean-field for 'stop' as well as any additional node-level properties.
            - source_list is a list of sources/filenames.
            - cube_transit_program is the parser's guess as to whether it is TRNBUILD or PT.
        """
        parser = Lark(TRANSIT_LINE_FILE_GRAMMAR, debug="debug", parser="lalr")

        source_list = []
        if "NAME=" in transit_source:
            WranglerLogger.debug("reading transit source as string")
            source_list.append("input_str")
            parse_tree = parser.parse(transit_source)
        elif os.path.isfile(transit_source):
            WranglerLogger.debug(f"reading transit source: {transit_source}")
            with open(transit_source) as file:
                source_list.append(transit_source)
                parse_tree = parser.parse(file.read())
        elif os.path.isdir(transit_source):
            for lin_file in glob.glob(os.path.join(transit_source, "*.LIN")):
                cls.read(lin_file)
            return
        else:
            msg = "{} not a valid transit line string, directory, or file"
            WranglerLogger.error(msg)
            raise ValueError(msg)

        WranglerLogger.debug("finished parsing cube line file")
        # WranglerLogger.debug("--Parse Tree--\n {}".format(parse_tree.pretty()))
        transformed_tree_data = CubeTransformer().transform(parse_tree)
        # WranglerLogger.debug("--Transformed Parse Tree--\n {}".format(transformed_tree_data))
        cube_transit_program = transformed_tree_data.get("program_type", None)

        _line_data = transformed_tree_data["lines"]

        # WranglerLogger.debug(f"CubeToStdAdapter.read._line_data : {_line_data}")

        route_properties_df = DataFrame(
            [v["line_properties"] for k, v in _line_data.items()]
        )
        msg = f"CubeToStdAdapter.read.route_properties_df: {route_properties_df[0:5]}"
        # WranglerLogger.debug(msg)
        cls.validate_routes(route_properties_df)

        # _key1 = list(_line_data.keys())[0]
        # msg = f"LINESHAPES:{_line_data[_key1]['line_shape']}"
        # WranglerLogger.debug(msg)

        route_shapes_df = DataFrame()
        for k, v in _line_data.items():
            _add_df = v["line_shape"]
            _add_df["NAME"] = k
            route_shapes_df = route_shapes_df.append(_add_df, ignore_index=True)

        msg = f"CubeToStdAdapter.read.route_shapes_df: {route_shapes_df[0:5]}"
        # WranglerLogger.debug(msg)
        return route_properties_df, route_shapes_df, source_list, cube_transit_program


class CubeTransitWriter:
    """
    Class with methods to write to cube transit line files.

    .. highlight:: python
    Typical usage example:
    ::
        CubeTransitWriter.write_dfs(
            route_properties_df,
            nodes_df,
            node_properties = [],
            route_properties = ["HEADWAY","NAME","OPERATOR","MODE"],
            cube_transit_program = "PT",
            outpath = "my_transit_routes.lin",
        )
    """

    @classmethod
    def write_std_transit(std_transit):
        raise NotImplementedError

    @classmethod
    def write_gtfs(feed):
        raise NotImplementedError

    @classmethod
    def write_dfs(
        cls,
        route_properties_df: DataFrame,
        nodes_df: DataFrame,
        node_properties: Collection[str] = None,
        route_properties: Collection[str] = None,
        cube_transit_program: str = DEFAULT_CUBE_TRANSIT_PROGRAM,
        outpath: str = "outtransit.lin",
    ) -> None:
        """Writes the gtfs feed as a cube line file after
        converting gtfs properties to model properties.

        Args:
            route_properties_df (DataFrame): Dataframe with route properties: one row for
                each cube transit line which will be written. For now, new cube transit
                lines are written for each route/time period combination.
            nodes_df (DataFrame): Shape nodes dataframe with fields NAME, N, stop
            node_properties (Collection[str], optional): Node properties which will
                be written out in addition to required. Defaults to None.
            route_properties (Collection[str], optional): Route properties which will
                be written out in addition to required. Defaults to None.
            cube_transit_program (str, optional): [description]. Defaults to
                `DEFAULT_CUBE_TRANSIT_PROGRAM`.
            outpath (str, optional): File location for output cube line file.
                Defaults to "outtransit.lin".

        Returns:
            [str]: outpath where the file was written (so you don't lose it!)
        """

        WranglerLogger.debug(f"nodes_df: \n{nodes_df}")
        if not node_properties:
            node_properties = REQUIRED_NODE_PROPERTIES[cube_transit_program]

        nodes_str_df = cls._nodes_df_to_cube_node_strings(
            nodes_df, properties=node_properties,
        )

        if not route_properties:
            route_properties = REQUIRED_ROUTE_PROPERTIES[cube_transit_program]

        route_properties = list(
            set(REQUIRED_ROUTE_PROPERTIES[cube_transit_program] + route_properties)
        )

        routes_s = route_properties_df.apply(
            cls._route_to_cube_str,
            cube_node_string_df=nodes_str_df,
            cube_transit_program=cube_transit_program,
            route_properties=route_properties,
            axis=1,
        )

        with open(outpath, "w") as f:
            f.write(f";;<<{cube_transit_program}>><<LINE>>;;")
            f.write("\n".join(routes_s.tolist()))

        return outpath

    @staticmethod
    def _cube_node_format(row, properties=[]):
        _stop_notation = "-" if not row["stop"] else ""
        _id = row["N"]

        if properties:
            _properties = "," + ",".join([f"{p}={row.p}" for p in properties])
        else:
            _properties = ""
        s = f"{_stop_notation}{_id}{_properties}"
        return s

    @classmethod
    def _nodes_df_to_cube_node_strings(
        cls,
        nodes_df: pd.DataFrame,
        properties: Collection[str] = [],
        id_field: str = "NAME",
    ) -> pd.DataFrame:
        """Creates a dataframe of cube node strings for each route as specified by the id_field.

        Args:
            nodes_df (pd.DataFrame): Shape nodes dataframe with fields NAME, N, stop
            properties (Collection[str], optional): Node properties to write out. Defaults to [].
            id_field (str, optional): [description]. Defaults to "NAME".

        Returns:
            pd.DataFrame: [ Dataframe with `NAME` and `agg_cube_node_str`
        """
        # WranglerLogger.debug(f"nodes_df:\n{nodes_df}")
        nodes_df["cube_node_str"] = nodes_df.apply(
            cls._cube_node_format, properties=properties, axis=1
        )
        WranglerLogger.debug(f"cube_node_str:\n{nodes_df['cube_node_str']}")

        def _agg_node_str(row_group):
            return ",\n".join(row_group.cube_node_str.tolist())

        _node_str_df = nodes_df.groupby(id_field).agg(_agg_node_str)

        _node_str_df = _node_str_df["cube_node_str"]
        return _node_str_df

    @staticmethod
    def _route_to_cube_str(
        row: pd.Series,
        cube_node_string_df: DataFrame,
        route_properties: Collection[str],
        cube_transit_program: str,
        time_varying_properties: Collection = TIME_VARYING_TRANSIT_PROPERTIES,
    ) -> str:
        """Creates a string representing the route in cube line file notation.

        Args:
            row (pd.Series): row of a DataFrame representing a cube-formatted trip,
                with the Attributes trip_id, shape_id, NAME, LONGNAME, tod, HEADWAY,
                MODE, ONEWAY, OPERATOR
            cube_node_string_df (DataFrame): dataframe with cube node shape
                representations keyed by NAME
            route_properties (Collection[str]): List of route-level properties to
                be written out
            cube_transit_program (str): TRNBUILD or PT
            time_varying_properties (Collection, optional): List of properties which
                need to be keyed by transit time period number, e.g. "HEADWAY".
                Defaults to TIME_VARYING_TRANSIT_PROPERTIES.

        Returns:
            [str]: string representation of route in cube line file notation
        """
        USE_QUOTES = ["NAME", "LONGNAME"]
        FIRST_PROP = "NAME"
        LAST_PROP = "NODES"
        route_properties.remove(FIRST_PROP)
        route_properties.insert(0, FIRST_PROP)
        route_properties.remove(LAST_PROP)
        route_properties.append(LAST_PROP)

        s = "\nLINE "
        for p in route_properties:
            if p == "NODES":
                # msg = f"{cube_node_string_df[row.trip_id].squeeze()}"
                # WranglerLogger.warning(msg)
                if row["NAME"] not in cube_node_string_df.index.values:
                    msg = f"Cannot find nodes for NAME: {row['NAME']}\
                        in cube_node_string_df! Adding as **NOT FOUND**"
                    s += "NODES= **NOT FOUND**\n"
                    WranglerLogger.warning(msg)
                    continue
                s += f"NODES={cube_node_string_df.loc[row['NAME']]}\n"
            elif pd.isna(row.get(p)):
                continue
            elif p in time_varying_properties:
                s += f"{p}[{row.tp_num}]={row[p]},\n"
            elif p in USE_QUOTES:
                s += f'{p}="{row[p]}",\n'
            else:
                s += f"{p}={row[p]},\n"

        return s


class CubeTransformer(Transformer):
    """A lark-parsing Transformer which transforms the parse-tree to
    a dictionary.

    .. highlight:: python
    Typical usage example:
    ::
        transformed_tree_data = CubeTransformer().transform(parse_tree)

    Attributes:
        line_order (int): a dynamic counter to hold the order of the nodes within
            a route shape
        lines_list (list): a list of the line names
    """

    def __init__(self):
        self.line_order = 0
        self.lines_list = []

    def lines(self, line):
        # WranglerLogger.debug("lines: \n {}".format(line))

        # This MUST be a tuple because it returns to start in the tree
        lines = {k: v for k, v in line}
        return ("lines", lines)

    @v_args(inline=True)
    def program_type_line(self, PROGRAM_TYPE, whitespace=None):
        # WranglerLogger.debug("program_type_line:{}".format(PROGRAM_TYPE))
        self.program_type = PROGRAM_TYPE.value

        # This MUST be a tuple because it returns to start  in the tree
        return ("program_type", PROGRAM_TYPE.value)

    @v_args(inline=True)
    def line(self, lin_attributes, nodes):
        # WranglerLogger.debug("line...attributes:\n  {}".format(lin_attributes))
        # WranglerLogger.debug("line...nodes:\n  {}".format(nodes))
        lin_name = lin_attributes["NAME"]

        self.line_order = 0
        # WranglerLogger.debug("parsing: {}".format(lin_name))

        return (lin_name, {"line_properties": lin_attributes, "line_shape": nodes})

    @v_args(inline=True)
    def lin_attributes(self, *lin_attr):
        lin_attr = {k: v for (k, v) in lin_attr}
        # WranglerLogger.debug("lin_attributes:  {}".format(lin_attr))
        return lin_attr

    @v_args(inline=True)
    def lin_attr(self, lin_attr_name, attr_value, SEMICOLON_COMMENT=None):
        # WranglerLogger.debug("lin_attr {}:  {}".format(lin_attr_name, attr_value))
        return lin_attr_name, attr_value

    def lin_attr_name(self, args):
        attr_name = args[0].value.upper()
        # WranglerLogger.debug(".......args {}".format(args))
        if attr_name in ["USERA", "FREQ", "HEADWAY"]:
            attr_name = attr_name + "[" + str(args[2]) + "]"
        return attr_name

    def attr_value(self, attr_value):
        try:
            return int(attr_value[0].value)
        except ValueError:
            return attr_value[0].value

    def nodes(self, lin_node):
        lin_node = DataFrame(lin_node)
        # WranglerLogger.debug("nodes:\n {}".format(lin_node))

        return lin_node

    @v_args(inline=True)
    def lin_node(self, NODE_NUM, SEMICOLON_COMMENT=None, *lin_nodeattr):
        self.line_order += 1
        n = int(NODE_NUM.value)
        return {"N": abs(n), "stop": n > 0, "order": self.line_order}

    start = dict


TRANSIT_LINE_FILE_GRAMMAR = r"""

start             : program_type_line? lines
WHITESPACE        : /[ \t\r\n]/+
STRING            : /("(?!"").*?(?<!\\)(\\\\)*?"|'(?!'').*?(?<!\\)(\\\\)*?')/i
SEMICOLON_COMMENT : /;[^\n]*/
BOOLEAN           : "T"i | "F"i
program_type_line : ";;<<" PROGRAM_TYPE ">><<LINE>>;;" WHITESPACE?
PROGRAM_TYPE      : "PT" | "TRNBUILD"

lines             : line*
line              : "LINE" lin_attributes nodes

lin_attributes    : lin_attr+
lin_attr          : lin_attr_name "=" attr_value "," SEMICOLON_COMMENT*
TIME_PERIOD       : "1".."5"
!lin_attr_name     : "allstops"i
                    | "color"i
                    | ("freq"i "[" TIME_PERIOD "]")
                    | ("headway"i "[" TIME_PERIOD "]")
                    | "mode"i
                    | "name"i
                    | "oneway"i
                    | "owner"i
                    | "runtime"i
                    | "timefac"i
                    | "xyspeed"i
                    | "longname"i
                    | "shortname"i
                    | ("usera"i TIME_PERIOD)
                    | ("usern2"i)
                    | "circular"i
                    | "vehicletype"i
                    | "operator"i
                    | "faresystem"i

attr_value        : BOOLEAN | STRING | SIGNED_INT

nodes             : lin_node+
lin_node          : ("N" | "NODES")? "="? NODE_NUM ","? SEMICOLON_COMMENT? lin_nodeattr*
NODE_NUM          : SIGNED_INT
lin_nodeattr      : lin_nodeattr_name "=" attr_value ","? SEMICOLON_COMMENT*
!lin_nodeattr_name : "access_c"i
                    | "access"i
                    | "delay"i
                    | "xyspeed"i
                    | "timefac"i
                    | "nntime"i
                    | "time"i

operator          : SEMICOLON_COMMENT* "OPERATOR" opmode_attr* SEMICOLON_COMMENT*
mode              : SEMICOLON_COMMENT* "MODE" opmode_attr* SEMICOLON_COMMENT*
opmode_attr       : ( (opmode_attr_name "=" attr_value) ","?  )
opmode_attr_name  : "number" | "name" | "longname"

%import common.SIGNED_INT
%import common.WS
%ignore WS

"""
