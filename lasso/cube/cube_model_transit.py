import copy
import os
import glob
import math
from typing import Collection, Any, Mapping, Union

import pandas as pd
from pandas import DataFrame

from lark import Lark, Transformer, v_args

from network_wrangler import TransitNetwork, WranglerLogger
from ..model_transit import (
    ModelTransit,
    StdToModelAdapter,
    ModelToStdAdapter,
    diff_shape,
)
from ..parameters import Parameters

from ..utils import select_df_from_matched_df, intersect_dfs


MODEL_TO_STD_PROP_TRANS = {
    ("HEADWAY", "headway_secs"): lambda x: int(x * 60),
    ("LONGNAME", "route_long_name"): lambda x: x,
    ("NAME", "route_id"): lambda x: x,
    ("N", "shape_model_node_id"): lambda x: x,
}

MODEL_TO_STD_PROP_MAP = {k: v for k, v in MODEL_TO_STD_PROP_TRANS.keys()}

STD_TO_MODEL_PROP_TRANS = {
    ("headway_secs", "HEADWAY"): lambda x: int(x / 60),
    ("route_long_name", "LONGNAME"): lambda x: x,
    ("shape_model_node_id", "N"): lambda x: x,
    ("shape_pt_sequence", "order"): lambda x: x,
}

STD_TO_MODEL_PROP_MAP = {k: v for k, v in STD_TO_MODEL_PROP_TRANS.keys()}

MODEL_ROUTE_ID_PROP = "NAME"
MODEL_NODE_ID_PROP = "N"

MODEL_REQUIRED_ROUTE_PROPS = ["HEADWAY", "MODE"]
PROJ_REQUIRED_ROUTE_PROPS = [
    MODEL_TO_STD_PROP_MAP.get(p, p) for p in MODEL_REQUIRED_ROUTE_PROPS
]

MODEL_ROUTE_PROPS = [
    "LONGNAME",
    "HEADWAY",
    "MODE",
    "ONEWAY",
    "OPERATOR",
]
PROJ_ROUTE_PROPS = [MODEL_TO_STD_PROP_MAP.get(p, p) for p in MODEL_ROUTE_PROPS]

MODEL_NODE_PROPS = ["NNTIME"]
PROJ_NODE_PROPS = [MODEL_TO_STD_PROP_MAP.get(p, p) for p in MODEL_NODE_PROPS]

MODEL_REQUIRED_NODE_PROPS = []
PROJ_REQUIRED_NODE_PROPS = [
    MODEL_TO_STD_PROP_MAP.get(p, p) for p in MODEL_REQUIRED_NODE_PROPS
]


# Property used for defining which time period exists for each route
MODEL_TP_PROP = "HEADWAY"
# Properties which vary by time period
MODEL_TIME_VARYING_TRANSIT_PROPS = ["HEADWAY"]

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
        tp_prop: str = MODEL_TP_PROP,
        route_props: Collection[str] = MODEL_ROUTE_PROPS,
        node_props: Collection[str] = MODEL_NODE_PROPS,
        required_route_props: Collection[str] = MODEL_REQUIRED_ROUTE_PROPS,
        required_node_props: Collection[str] = MODEL_REQUIRED_NODE_PROPS,
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
            tp_prop (str, optional): Transit route property which defines which time
                periods the route exists in. Defaults to `MODEL_TP_PROP`.
            route_props (Collection[str], optional): Properties which are
                calculated and saved for route-level properties.
                Defaults to `MODEL_ROUTE_PROPS`.
            node_props (Collection[str], optional):  Properties which are
                calculated and saved for route nodes/shapes.
                Defaults to `MODEL_NODE_PROPS`.
            required_route_props (Collection[str], optional): Properties which are required
                for the route-level properties and will be always written out.
                Defaults to `MODEL_REQUIRED_ROUTE_PROPS`.
            required_node_props (Collection[str], optional): Properties which are required
                for the route nodes/shapes and will be always written out.
                Defaults to `MODEL_REQUIRED_NODE_PROPS`.
        """

        self.cube_transit_program = cube_transit_program

        self.tp_prop = tp_prop

        self.route_props = route_props
        self.node_props = node_props

        self.required_route_props = required_route_props
        self.required_node_props = required_node_props

        super().__init__(
            route_properties_df=None,
            shapes_df=None,
            route_id_prop=MODEL_ROUTE_ID_PROP,
            node_id_prop=MODEL_NODE_ID_PROP,
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

        lin_files = glob.glob(os.path.join(source, "*.[Ll][Ii][Nn]"))

        if not (gtfs_files_exist or lin_files):
            msg = f"""Not sure how to read source: {source} with source_types: {source_type}.
                Try specifying keyword: `source_type`."""
            raise NotImplementedError(msg)

        if gtfs_files_exist:
            self.add_std_source(source, **kwargs)

        if lin_files:
            self.add_model_source(source, **kwargs)

    def add_std_source(
        self,
        standardtransit: Union[TransitNetwork, str],
        cube_transit_program: str = None,
        std_to_model_prop_map: Mapping[str, str] = None,
        std_to_model_prop_trans: Mapping[Collection[str], Any] = None,
        tp_prop: str = None,
        node_props: Collection[str] = None,
        route_props: Collection[str] = None,
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
            tp_prop (str, optional): Property which specifies whether a transit route
                exists in various time periods.  Defaults to self.tp_prop
                which defaults to "FREQ" for PT or "HEADWAY" for TRNBUILD.
            node_props (Collection[str], optional): Specifies the model properties for nodes.
                Defaults to self.node_props which defaults to [].
            route_props (Collection[str], optional): Specifies the model properties for
                routes. Defaults to self.route_props which defaults to ["NAME","LONGNAME",
                "MODE","ONEWAY","NODES"] for either PT or TRNBUILD and then ["FREQ" and "OPERATOR"]
                for PT and ["HEADWAY"] for TRNBUILD.
            parameters (Parameters, optional): parameters object, which is used in the
                `StdToCubeAdapter`. Defaults to self.parameters.
        """
        if cube_transit_program is None:
            cube_transit_program = self.cube_transit_program
        if tp_prop is None:
            tp_prop = self.tp_prop
        if node_props is None:
            node_prop = self.node_props
        if route_props is None:
            route_properties = self.route_props
        if parameters is None:
            parameters = self.parameters

        if type(standardtransit) is str:
            standardtransit = TransitNetwork.read(standardtransit)

        _adapter = StdToCubeAdapter(
            standardtransit,
            cube_transit_program=cube_transit_program,
            std_to_model_prop_map=std_to_model_prop_map,
            std_to_model_prop_trans=std_to_model_prop_trans,
            tp_prop=tp_prop,
            node_props=node_props,
            route_props=route_props,
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

        reader = CubeTransitReader(cube_transit_program)

        (
            route_properties_df,
            route_shapes_df,
            source_list,
            cube_transit_program,
        ) = reader.read(transit_source)

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
        self,
        transit_properties_df: DataFrame,
    ) -> DataFrame:
        """Translates a wide dataframe with fields for time-dependent route properties
        for each time period, to a long dataframe with a row for every route/time period
        combination.

        Wrapper method for :py:method:`ModelTransit._melt_routes_by_time_period`.

        Uses:
            df_key = self.route_id_prop
            tp_prop= self.tp_prop

        Args:
            transit_properties_df (DataFrame): dataframe with a row for each route and
            time periods which are time-dependent.

        Returns:
            DataFrame: Long dataframew with a row for every route/time period combination.
        """

        df = super()._melt_routes_by_time_period(
            transit_properties_df,
            df_key=self.route_id_prop,
            tp_prop=self.tp_prop,
        )

        return df

    @staticmethod
    def base_prop_from_time_varying_prop(prop: str) -> str:
        """Returns the base property name from a composite field name which
        also contains the time period id.

        Example:  HEADWAY[1] ---> HEADWAY

        Args:
            prop (str): Field name with time period embedded, i.e. FREQ[2]

        Returns:
            str: Base field name without time period id.
        """
        return prop.split("[")[0]

    @classmethod
    def get_tp_nums_from_model_props(cls, props_list: list):
        """
        Finds properties that are associated with time periods and the
        returns the numbers in them.

        Args:
            props_list (list): list of all properties.

        Returns:
            list of strings of the time period numbers found
        """
        time_props = cls.time_varying_props_filter(props_list)
        tp_num_list = list(
            set([cls.tp_num_from_time_varying_prop(p) for p in time_props])
        )

        return tp_num_list

    @staticmethod
    def tp_num_from_time_varying_prop(prop: str) -> int:
        """Returns the time period number from a composite field name.

        Example:  HEADWAY[1] ---> 1

        Args:
            prop (str): Field name with time period embedded, i.e. FREQ[2]

        Returns:
            int: Transit time period ID.
        """
        return prop.split("[")[1][0]

    @staticmethod
    def time_varying_props_filter(props_list: Collection) -> Collection:
        """Filters a collection of route properties into a collection of time-varying
        route properties.

        Args:
            props_list (Collection): List of route properties, e.g. ["NAME",
                "HEADWAY[1]","HEADWAY[2]"]

        Returns:
            Collection: List of route properties which vary by time,
                e.g. ["HEADWAY[1]","HEADWAY[2]"]
        """
        time_props = [p for p in props_list if ("[" in p) and ("]" in p)]
        return time_props

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
            tp_prop="HEADWAY",
            node_props=[],
            route_props=["HEADWAY","NAME","OPERATOR"],
            parameters=parameters,
        )

        model_route_props_by_time_df, model_nodes_df = _adapter.transform()
    """

    def __init__(
        self,
        standardtransit: TransitNetwork,
        cube_transit_program: str = DEFAULT_CUBE_TRANSIT_PROGRAM,
        parameters: Parameters = None,
        std_to_model_prop_trans: Mapping[
            Collection[str], Any
        ] = STD_TO_MODEL_PROP_TRANS,
        tp_prop: str = MODEL_TP_PROP,
        node_props: Collection[str] = MODEL_NODE_PROPS,
        route_props: Collection[str] = MODEL_ROUTE_PROPS,
        **kwargs,
    ):
        """Constructor method for StdToCubeAdapter.

        Args:
            standardtransit (:py:class:`TransitNetwork`): Input standard transit instance.
            cube_transit_program (str, optional): TRNBUILD or PT. Defaults to
                DEFAULT_CUBE_TRANSIT_PROGRAM.
            parameters (py:class:`Parameters`): Parameters instance for doing translations
                between time periods. Defaults to none and then default parameters.
            std_to_model_prop_trans (Mapping[Collection[str], Any], optional): Maps a tuple of
                (standard property,model property) to a transformation function that turns the
                standard/gtfs property into the cube model property.
                Defaults to :py:`std_to_model_prop_trans`.
                Example: {} ("HEADWAY", "headway_secs"): lambda x: x * 60}
            tp_prop (str, optional): transit route property which defines which time
                periods the route exists in. Defaults to :py:`MODEL_TP_PROP`.
            node_props (Collection[str], optional):  Properties which are
                calculated and saved for route nodes/shapes.
                Defaults to `MODEL_NODE_PROPS`.
            route_propes (Collection[str], optional): Properties which are
                calculated and saved for route-level properties.
                Defaults to `MODEL_ROUTE_PROPS`.
        """
        self.__standardtransit = standardtransit
        self.feed = self.__standardtransit.feed
        self.cube_transit_program = cube_transit_program

        ###PARAMETERS

        self.std_to_model_prop_trans = std_to_model_prop_trans

        self.tp_prop = tp_prop

        self.node_props = node_props
        self.route_props = route_props

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
        model_to_std_prop_trans: Mapping[
            Collection[str], Any
        ] = MODEL_TO_STD_PROP_TRANS,
        model_route_id_props=["NAME", "tp_num"],
    ) -> None:
        """Constructor method for object to translate between a :py:class:`ModelTransit` instance
         and standard GTFS data in a :py:class:`TransitNetwork`. WIP. INCOMPLETE.

        Args:
            modeltransit (ModelTransit): Instance to be transformed.
            cube_transit_program ([type], optional): TRNBUILD or PT.
                Defaults to None.
            model_to_std_prop_trans (Mapping[Collection[str], Any], optional):Maps a tuple of
                (model property,standard property) to a transformation function that turns the
                model property into the standard/gtfs property. Defaults to None.
        """

        super().__init__(
            modeltransit,
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
        self,
        cube_transit_program: str,
        route_id_prop: str = "NAME",
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
    def read(cls, transit_source: str, route_id_prop: str = "NAME") -> Collection:
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
            msg = "YES I'M A DIRECTORY - but not implemented"
            raise NotImplementedError(msg)
            for lin_file in glob.glob(os.path.join(transit_source, "*.LIN")):
                cls.read(lin_file)
            return
        else:
            msg = (
                f"{transit_source} not a valid transit line string, directory, or file"
            )
            WranglerLogger.error(msg)
            raise ValueError(msg)

        WranglerLogger.debug("finished parsing cube line file")
        # WranglerLogger.debug("--Parse Tree--\n {}".format(parse_tree.pretty()))
        transformed_tree_data = CubeTransformer().transform(parse_tree)
        # WranglerLogger.debug("--Transformed Parse Tree--\n {}".format(transformed_tree_data))
        cube_transit_program = transformed_tree_data.get("program_type", None)

        _line_data = transformed_tree_data["lines"]

        WranglerLogger.debug(f"CubeToStdAdapter.read._line_data : {_line_data}")

        route_properties_df = DataFrame(
            [v["line_properties"] for k, v in _line_data.items()]
        )

        # Need to convert numpy (default) to pandas data types which support missing values as NA
        print("BEFORE TYPES", route_properties_df.dtypes)
        print(route_properties_df)
        route_properties_df = route_properties_df.convert_dtypes(convert_string=False)
        print("AFTER TYPES", route_properties_df.dtypes)
        print(route_properties_df)
        msg = f"CubeToStdAdapter.read.route_properties_df: {route_properties_df[0:5]}"
        WranglerLogger.debug(msg)

        msg = "Validating Routes."
        WranglerLogger.info(msg)
        _reader = CubeTransitReader(
            cube_transit_program=cube_transit_program,
            route_id_prop=route_id_prop,
        )
        _reader.validate_routes(route_properties_df)

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
        node_props: Collection[str] = MODEL_NODE_PROPS,
        route_props: Collection[str] = MODEL_ROUTE_PROPS,
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
            node_props (Collection[str], optional): Node properties which will
                be written out in addition to required. Defaults to None.
            route_props (Collection[str], optional): Route properties which will
                be written out in addition to required. Defaults to None.
            cube_transit_program (str, optional): [description]. Defaults to
                `CUBE_TRANSIT_PROGRAM`.
            outpath (str, optional): File location for output cube line file.
                Defaults to "outtransit.lin".

        Returns:
            [str]: outpath where the file was written (so you don't lose it!)
        """

        WranglerLogger.debug(f"nodes_df: \n{nodes_df}")
        if not node_props:
            node_properties = MODEL_REQUIRED_NODE_PROPS

        nodes_str_df = cls._nodes_df_to_cube_node_strings(
            nodes_df,
            properties=node_properties,
        )
        route_props = list(set(MODEL_REQUIRED_ROUTE_PROPS + route_props))

        routes_s = route_properties_df.apply(
            cls._route_to_cube_str,
            cube_node_string_df=nodes_str_df,
            cube_transit_program=cube_transit_program,
            route_props=route_props,
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
        route_props: Collection[str],
        cube_transit_program: str,
        time_varying_props: Collection = MODEL_TIME_VARYING_TRANSIT_PROPS,
    ) -> str:
        """Creates a string representing the route in cube line file notation.

        Args:
            row (pd.Series): row of a DataFrame representing a cube-formatted trip,
                with the Attributes trip_id, shape_id, NAME, LONGNAME, tod, HEADWAY,
                MODE, ONEWAY, OPERATOR
            cube_node_string_df (DataFrame): dataframe with cube node shape
                representations keyed by NAME
            route_props (Collection[str]): List of route-level properties to
                be written out
            cube_transit_program (str): TRNBUILD or PT
            time_varying_props (Collection, optional): List of properties which
                need to be keyed by transit time period number, e.g. "HEADWAY".
                Defaults to TIME_VARYING_TRANSIT_PROPERTIES.

        Returns:
            [str]: string representation of route in cube line file notation
        """
        USE_QUOTES = ["NAME", "LONGNAME"]
        FIRST_PROPS = ["NAME"]
        LAST_PROPS = ["NODES"]

        ordered_route_props = [p for p in FIRST_PROPS if p in route_props]
        ordered_route_props += [
            p for p in route_props if p not in list(set(FIRST_PROPS + LAST_PROPS))
        ]
        ordered_route_props += [p for p in LAST_PROPS if p in route_props]

        s = "\nLINE "
        for p in ordered_route_props:
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
            elif p in time_varying_props:
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

    SIGNED_INT = int
    STRING = str
    BOOLEAN = str
    TIME_DEPENDENT_PROPERTIES = ["USERA", "FREQ", "HEADWAY"]

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
        # WranglerLogger.debug(f"lin_attr {lin_attr_name}:  {attr_value}")
        # WranglerLogger.debug(f"lin_attr type {lin_attr_name}:  {attr_value} - {type(attr_value)}")
        return lin_attr_name, attr_value

    def lin_attr_name(self, args):
        attr_name = args[0].upper()
        # WranglerLogger.debug("attr_name.......args {}".format(args))
        if attr_name in CubeTransformer.TIME_DEPENDENT_PROPERTIES:
            attr_name = attr_name + "[" + str(args[2]) + "]"
        return attr_name

    def attr_value(self, values):
        value = values[0]
        if type(value) == str:
            value = value.strip('"')
        # WranglerLogger.debug(f"attr_value: {value} type:{type(value)}")

        return value

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
BOOLEAN           : "T"|"F"
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

attr_value        : STRING | SIGNED_INT | BOOLEAN

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

%import common.SIGNED_NUMBER
%import common.SIGNED_INT
%import common.WS
%ignore WS

"""


def evaluate_route_shape_changes(
    base_t: ModelTransit,
    updated_t: ModelTransit,
    match_id: Union[Collection[str], str] = None,
    n_buffer_vals: int = 3,
) -> Mapping[str, Mapping]:
    """Compares all the shapes (or the subset specified in `route_list`) for two
    :py:class:`ModelTransit` objects and outputs a list of project card changes
    which would turn `base_t.shapes_df` --> `updated_t.shapes_df`.

    Args:
        base_t (ModelTransit): Updated :py:class:`ModelTransit` with `shapes_df`.
        updated_t (ModelTransit): Updated :py:class:`ModelTransit` to compare to `base_t`
        match_id (Union[Collection[str], str], optional): Field name or a collection
            of field names to use as the match, e.g. ["N","stop"]. Defaults to None.
            Defaults to [base_t.node_id_prop, "stop"]
        n_buffer_vals (int, optional): Number of values on either side to include in
            match. Defaults to 3.

    Returns:
        Mapping[str,Mapping]: Dictionary of shape changes formatted as a
            project card-change dictionary keyed by route_id

    """
    base_shapes_df = base_t.shapes_df.copy()
    updated_shapes_df = updated_t.shapes_df.copy()

    # WranglerLogger.debug(
    #    f"\nbase_shapes_df: \n{base_shapes_df}\
    #    \nupdated_shapes_df: \n {updated_shapes_df}"
    # )

    if not base_t.node_id_prop == updated_t.node_id_prop:
        msg = f"Base and updated node_id fields not same {base_t.node_id_prop} vs\
            {updated_t.node_id_prop} can't create comparison."
        raise ValueError(msg)

    if not base_t.route_id_prop == updated_t.route_id_prop:
        msg = f"Base and updated route_id fields not same {base_t.route_id_prop} vs\
            {updated_t.route_id_prop} can't create comparison."
        raise ValueError(msg)

    # set the fields which match up routes and routing on
    _route_id_prop = base_t.route_id_prop
    if not match_id:
        match_id = [base_t.node_id_prop, "stop"]

    # reduce down routing matching from a list of properties to a single field
    # make sure the fields exist...
    if type(match_id) == Collection and len(match_id) == 1:
        match_id = match_id[0]

    if type(match_id) != str:
        col_id = "_".join(match_id)
        base_shapes_df[col_id] = (
            base_shapes_df[match_id].to_records(index=False).tolist()
        )
        updated_shapes_df[col_id] = (
            updated_shapes_df[match_id].to_records(index=False).tolist()
        )
    else:
        err = f""
        if match_id not in base_shapes_df.columns:
            err += f"match_id: {match_id} not in base_shapes_df columns.\
                Available columns: {base_shapes_df.columns}."
        if match_id not in updated_shapes_df.columns:
            err += f"match_id: {match_id} not in updated_shapes_df columns.\
                Available columns: {updated_shapes_df.columns}."
        if err:
            raise ValueError(err)
    # select all routes which are common between the transit networks
    route_list = [i for i in updated_t.routes if i in base_t.routes]
    print("ROUTE_LISTB", route_list)
    # Compare route changes for each route in the list
    shape_changes_by_route_id_dict = {}
    for i in route_list:
        existing_r_df, change_r_df = diff_shape(
            base_shapes_df[base_shapes_df[_route_id_prop] == i],
            updated_shapes_df[updated_shapes_df[_route_id_prop] == i],
            match_id,
        )
        rt_change_dict = update_route_routing_change_dict(
            existing_r_df,
            change_r_df,
        )
        WranglerLogger.debug(f"rt_change: {rt_change_dict}")
        shape_changes_by_route_id_dict[i] = copy.deepcopy(rt_change_dict)

    WranglerLogger.info(f"Found {len(shape_changes_by_route_id_dict)} transit changes.")

    return shape_changes_by_route_id_dict


def evaluate_model_transit_differences(
    base_transit: ModelTransit,
    updated_transit: ModelTransit,
    route_prop_update_list: Collection[str] = MODEL_ROUTE_PROPS,
    node_prop_update_list: Collection[str] = MODEL_NODE_PROPS,
    new_route_prop_list: Collection[str] = MODEL_ROUTE_PROPS,
    new_node_prop_list: Collection[str] = MODEL_NODE_PROPS,
    absolute: bool = True,
    include_existing: bool = False,
    n_buffer_vals: int = 3,
) -> Collection[Mapping]:
    """Evaluates differences between :py:class:`ModelTransit` instances
    `base_transit` and `updated_transit` and outputs a list of project card changes
    which would turn `base_transit` --> `updated_transit`.
    1. Identifies what routes need to be updated, deleted, or added
    2. For routes being added or updated, identify if the time periods
        have changed or if there are multiples, and make duplicate lines if so
    3. Create project card dictionaries for each change.

    Args:
        base_transit (ModelTransit): an :py:class:`ModelTransit` instance for the base condition
        updated_transit (ModelTransit): an ModelTransit instance for the updated condition
        update_route_prop_list (Collection[str], Optional): list of properties
            to consider updates for, ignoring others.
            If not set, will default to MODEL_ROUTE_PROPS.
        update_node_prop_list (Collection[str], Optional): list of properties
            to consider updates for, ignoring others.
            If not set, will default to NODE_PROPS.
        new_route_prop_list (Collection[str], Optional): list of properties to add
            to new routes. If not set, will default to MODEL_ROUTE_PROPS.
        new_node_prop_list (Collection[str], Optional): list of properties to add
            to new routes. If not set, will default to MODEL_ROUTE_PROPS.
        absolute (Bool): indicating if should use the [False case]'existing'+'change' or
            [True case]'set' notation a project card. Defaults to True.
        include_existing (Bool): if set to True, will include 'existing' in project card.
            Defaults to False.
        n_buffer_vals (int): Number of values on either side to include in
            match for routes changes. Defaults to 3.

    Returns:
        A list of dictionaries containing project card changes
        required to evaluate the differences between the base_transit
        and updated_transit network instance.
    """

    # Project cards are coded with "wrangler standard" variables (which is mostly GTFS)
    # Use CubeToStdAdapter to translate the variable names from Model Transit
    # std_ids is False so that the route_id is NAME rather than split up. This facilitates
    # matching between cube lins

    _base_adapter = CubeToStdAdapter(base_transit)
    _build_adapter = CubeToStdAdapter(updated_transit)

    base_route_props_df = _base_adapter.transform_routes(std_ids=False)
    updated_route_props_df = _build_adapter.transform_routes(std_ids=False)

    _updated_prop_names = list(MODEL_TO_STD_PROP_TRANS.keys()) + list(
        MODEL_TO_STD_PROP_MAP.items()
    )
    # WranglerLogger.debug(f"Updating property names using: {_updated_prop_names}")
    for m, s in _updated_prop_names:
        if m in route_prop_update_list:
            route_prop_update_list.remove(m)
            route_prop_update_list.append(s)
        if m in new_route_prop_list:
            new_route_prop_list.remove(m)
            new_route_prop_list.append(s)

    # WranglerLogger.debug(f"route_prop_update_list: {route_prop_update_list}")
    # Identify which routes to delete, add, or update
    # Note that wrangler treats new time periods as new routes - thus STD_ROUTE_ID_PROPS
    # contains the route_id as well as start and end times.

    _base_routes = base_route_props_df[ModelToStdAdapter.STD_ROUTE_ID_PROPS].to_dict(
        orient="records"
    )
    _updated_routes = updated_route_props_df[
        ModelToStdAdapter.STD_ROUTE_ID_PROPS
    ].to_dict(orient="records")

    _routes_to_delete = [i for i in _base_routes if i not in _updated_routes]
    _routes_to_add = [i for i in _updated_routes if i not in _base_routes]
    _routes_to_update = [i for i in _updated_routes if i in _base_routes]

    # Initialize

    project_card_changes = []

    """
    Deletions
    """

    if _routes_to_delete:
        _del_rts_str = "\n  - " + "\n - ".join(map(str, _routes_to_delete))
        msg = f"Deleting {len(_routes_to_delete)} Route(s): {_del_rts_str}"
        WranglerLogger.debug(msg)

        _delete_changes = (
            base_route_props_df[
                base_route_props_df[ModelToStdAdapter.STD_ROUTE_ID_PROPS].isin(
                    DataFrame(_routes_to_delete).to_dict(orient="list")
                )
            ]
            .dropna(subset=ModelToStdAdapter.STD_ROUTE_ID_PROPS)
            .apply(delete_route_change_dict, axis=1)
            .tolist()
        )
        project_card_changes += _delete_changes

    """
    Additions
    """

    if _routes_to_add:
        WranglerLogger.debug(f"Adding Routes: {_routes_to_add}")

        _routes_to_add_props_df = select_df_from_matched_df(
            updated_route_props_df,
            pd.DataFrame(_routes_to_add, columns=ModelToStdAdapter.STD_ROUTE_ID_PROPS),
            compare_cols=ModelToStdAdapter.STD_ROUTE_ID_PROPS,
        )
        print("_routes_to_add_props_df/n", _routes_to_add_props_df)
        print(f"new_route_prop_list {new_route_prop_list}")
        _addition_changes = _routes_to_add_props_df.apply(
            new_transit_route_change_dict,
            shapes_df=updated_transit.shapes_df,
            route_prop_list=new_route_prop_list,
            node_prop_list=new_node_prop_list,
            axis=1,
        ).tolist()

        project_card_changes += _addition_changes

    """
    Evaluate Property Updates
    """
    print("UPDATE DF", updated_route_props_df.columns)

    if _routes_to_update:
        _base_df, _updated_df = intersect_dfs(
            base_route_props_df,
            updated_route_props_df,
            how_cols_join="right",
            select_cols=ModelToStdAdapter.STD_ROUTE_ID_PROPS,
            id_col=["route_id"],
        )
        msg = f"_base_df,_updated_df: { _base_df,_updated_df }"
        WranglerLogger.debug(msg)

        _compare_df = _base_df[route_prop_update_list].compare(
            _updated_df[route_prop_update_list],
            keep_shape=True,
        )

        # Since df.compare drops fields which aren't different,
        # re-add ID fields
        _id_fields = ModelToStdAdapter.STD_ROUTE_ID_PROPS
        _compare_df[[(i, "self") for i in _id_fields]] = updated_route_props_df[
            _id_fields
        ]

        # drop columns where there aren't any differences
        _compare_df = _compare_df.dropna(axis=1, how="all")

        _compare_df["property_changes"] = _compare_df.apply(
            update_route_prop_change_dict,
            absolute=absolute,
            include_existing=include_existing,
            ignore_fields=_id_fields,
            axis=1,
        )

        _compare_df.columns = _compare_df.columns.droplevel(level=1)
        _compare_df = _compare_df[_id_fields + ["property_changes"]]

        _routing_changes_by_route_id = evaluate_route_shape_changes(
            base_transit,
            updated_transit,
            n_buffer_vals=n_buffer_vals,
        )

        _compare_df["shape_changes"] = _compare_df[
            MODEL_TO_STD_PROP_MAP[MODEL_ROUTE_ID_PROP]
        ].map(_routing_changes_by_route_id)

        _update_changes = _compare_df.apply(update_project_card_dict, axis=1)

        project_card_changes += _update_changes.tolist()

    return project_card_changes


def new_transit_route_change_dict(
    route_row: Union[pd.Series, Mapping],
    shapes_df: DataFrame,
    route_prop_list: Collection[str] = MODEL_ROUTE_PROPS,
    node_prop_list: Collection[str] = MODEL_NODE_PROPS,
) -> Mapping:
    """Processes a row of a pandas dataframe or a dictionary with the fields:
    - name
    - direction_id
    - start_time
    - end_time
    - agency_id
    - routing
    - + all fields in route_prop_list

    Args:
        route_row (pd.Series): [description]
        shapes[]
    """
    route_row = route_row.dropna()
    print(f"route_prop_list {route_prop_list}")
    print(f"PROJ_REQUIRED_ROUTE_PROPS {PROJ_REQUIRED_ROUTE_PROPS}")
    # Make sure required properties are included
    route_prop_list = list(set(PROJ_REQUIRED_ROUTE_PROPS + route_prop_list))
    node_prop_list = list(set(PROJ_REQUIRED_NODE_PROPS + node_prop_list))

    # Flag properties which aren't in the dataframe
    _missing_route_props = [p for p in route_prop_list if p not in route_row]
    _missing_node_props = [p for p in node_prop_list if p not in shapes_df.columns]

    if _missing_route_props:
        route_prop_list = list(set(route_prop_list) - set(_missing_route_props))
        WranglerLogger.warning(
            f"Missing specified route properties: {_missing_route_props}"
        )

    if _missing_node_props:
        node_prop_list = list(set(node_prop_list) - set(_missing_node_props))
        WranglerLogger.warning(
            f"Missing specified node properties: {_missing_node_props}"
        )

    _rt_id_field = "NAME"
    if _rt_id_field not in route_row:
        _rt_id_field = MODEL_TO_STD_PROP_MAP["NAME"]

    route_shapes_df = shapes_df.loc[shapes_df["NAME"] == route_row[_rt_id_field]]

    _shapes_s = route_shapes_df.apply(
        _wrangler_node_format,
        ## TODO
        # properties = properties,
        axis=1,
    )
    print("SHAPES.", _shapes_s)
    routing_props = {
        "property": "routing",
        "set": route_shapes_df.apply(
            _wrangler_node_format,
            ## TODO
            # properties = properties,
            axis=1,
        ).tolist(),
    }

    transit_route_props = [
        {"property": p, "set": route_row[p]} for p in route_prop_list
    ]

    _facility_fields = [
        p for p in ModelToStdAdapter.PROJ_FACILITY_FIELDS if p in route_row
    ]

    _card_dict = {
        "category": "New Transit Service",
        "facility": {p: route_row[p] for p in _facility_fields},
        "properties": transit_route_props + [routing_props],
    }

    _add_list = [
        f"{p}: {route_row[p]}"
        for p in ModelToStdAdapter.PROJ_FACILITY_FIELDS
        if p in route_row
    ]
    _add_str = "\n - " + "\n - ".join(_add_list)
    msg = f"Adding transit route: { _add_str }"
    WranglerLogger.debug(msg)

    return _card_dict


def delete_route_change_dict(route_row: Union[pd.Series, Mapping]) -> Mapping:
    """
    Creates a project card change formatted dictionary for deleting a line.

    Args:
        route_row: row of df with line to be deleted or a dict with following attributes:
        - name
        - direction_id
        - start_time
        - end_time

    Returns:
        A project card change-formatted dictionary for the route deletion.
    """

    _facility_fields = [
        p for p in ModelToStdAdapter.PROJ_FACILITY_FIELDS if p in route_row
    ]

    _card_dict = {
        "category": "Delete Transit Service",
        "facility": {p: route_row[p] for p in _facility_fields},
    }

    WranglerLogger.debug(f"Deleting transit route {_card_dict['facility']}")

    return _card_dict


def update_project_card_dict(route_row: pd.Series) -> Mapping:
    """[summary]

    Args:
        route_row (pd.Series): [description]

    Returns: project card dictionary
    """

    _facility_fields = [
        p for p in ModelToStdAdapter.PROJ_FACILITY_FIELDS if p in route_row
    ]

    update_card_dict = {
        "category": "Update Transit Service",
        "facility": {p: route_row[p] for p in _facility_fields},
        "properties": route_row.property_changes + [route_row.shape_changes],
    }

    return update_card_dict


def update_route_prop_change_dict(
    compare_route_row: pd.Series,
    include_existing: bool = False,
    ignore_fields: Collection[str] = ["NAME"],
    absolute: bool = True,
) -> Collection[Mapping]:
    """[summary]

    Args:
        compare_route_row (pd.Series): row of df with transit route to be deleted or
            a dict with following attributes:
        include_existing (bool, optional): If set to True, will include 'existing'
            in project card.
        ignore_fields: list of fields to ignore comparisons of. For example, because they
            are identifiers. Defaults to ["NAME"].
        absolute: If set to false, will print change in project card from base value rather
            than absolute value. Defaults to True.

    Returns:
        Collection[Mapping[str]]: [description]
    """
    if not absolute:
        raise NotImplementedError

    compare_route_row = compare_route_row.iloc[
        ~compare_route_row.index.get_level_values(0).isin(ignore_fields)
    ]

    compare_route_row = compare_route_row.dropna()

    _properties_update_list = []
    for p in list(set(compare_route_row.index.get_level_values(0))):
        change_item = {}
        change_item["property"] = p
        change_item["set"] = compare_route_row[p, "other"]
        if include_existing:
            change_item["existing"] = compare_route_row[p, "self"]

        _properties_update_list.append(change_item)
    return _properties_update_list


def update_route_routing_change_dict(
    existing_routing_df: Collection[Any],
    set_routing_df: Collection[Any],
) -> Mapping[str, Any]:
    """Format route changes for project cards. Right now, this matches
        the formatting for cube nodes. Could change in future.

    Args:
        existing_routing (Collection[Any]): [description]
        set_routing (Collection[Any]): [description]
        match_id (Union[str,Collection[str]]): [description]

    Returns:
        Mapping[str,Any]: [description]
    """
    match_id = list(existing_routing_df.columns)
    if match_id != ["N", "stop"]:
        raise NotImplementedError(f"Expecting match_id ['N','stop']; got {match_id}")

    if list(existing_routing_df.columns) != list(set_routing_df.columns):
        raise (
            f"Columns for existing and set don't match.\n\
            Existing: {existing_routing_df.columns}\n\
            Set: {set_routing_df.columns}"
        )

    # for grouping
    existing_routing_df["NAME"] = "_"
    set_routing_df["NAME"] = "_"

    existing_list = existing_routing_df.apply(
        _wrangler_node_format,
        # properties = properties,
        axis=1,
    ).tolist()

    set_list = set_routing_df.apply(
        _wrangler_node_format,
        # properties = properties,
        axis=1,
    ).tolist()

    WranglerLogger.debug(
        f"Existing Str: {existing_list}\n\
        Set Str: {set_list}"
    )

    shape_change_dict = {
        "property": "routing",
        "existing": existing_list,
        "set": set_list,
    }
    return shape_change_dict


def _wrangler_node_format(row, properties=[]):
    _stop_notation = "-" if not row["stop"] else ""
    _id = row["N"]

    if properties:
        _properties = "," + ",".join([f"{p}={row.p}" for p in properties])
    else:
        _properties = ""
    s = f"{_stop_notation}{_id}{_properties}"
    return s
