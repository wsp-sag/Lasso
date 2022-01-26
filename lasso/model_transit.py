import copy
from typing import Any, Union, Collection, Mapping

from pandas import DataFrame

import pandas as pd

from network_wrangler import WranglerLogger

from . import project
from . import time_utils
from .parameters import Parameters


class ModelTransit:
    """Class for storing information about transit in model-specific terms.

    Has the capability to:

     - Parse cube line file properties and shapes into python dictionaries
     - Compare line files and represent changes as Project Card dictionaries

    .. highlight:: python

    Typical usage example:
    ::
        tn = ModelTransit.create_from_cube(CUBE_DIR)
        transit_change_list = tn.evaluate_differences(base_transit_network)

    Attributes:
        lines (list): list of strings representing unique line names in
            the cube network.
        line_properties (dict): dictionary of line properties keyed by line name. Property
            values are stored in a dictionary by property name. These
            properties are directly read from the cube line files and haven't
            been translated to standard transit values.
        shapes (dict): dictionary of shapes
            keyed by line name. Shapes stored as a pandas DataFrame of nodes
            with following columns:
              - 'node_id' (int): positive integer of node id
              - 'node' (int): node number, with negative indicating a non-stop
              - 'stop' (boolean): indicates if it is a stop
              - 'order' (int):  order within this shape
        program_type (str): Either PT or TRNBLD
        parameters (Parameters):
            Parameters instance that will be applied to this instance which
            includes information about time periods and variables.
        source_list (list):
            List of cube line file sources that have been read and added.
    """

    def __init__(
        self,
        route_properties_df: DataFrame = None,
        shapes_df: DataFrame = None,
        parameters: Parameters = None,
        parameters_dict: dict = None,
        model_type: str = None,
        tp_prop: str = None,
        node_id_prop: str = "node_id",
        route_id_prop: str = "route",
    ):
        """
        Constructor for ModelTransit
        std_transit_source:
        parameters: instance of Parameters
        parameters_dict: dictionary of parameter settings (see Parameters class)
        """
        msg = "Creating a new Cube Transit instance"
        WranglerLogger.debug(msg)

        self.source_list = []
        # self.shapes = {}

        self.route_id_prop = route_id_prop
        self.node_id_prop = node_id_prop

        self.route_properties_df = route_properties_df
        self.shapes_df = shapes_df

        self._routes = []
        self._route_properties_by_time_df = None

        # settings
        self.model_type = model_type

        if parameters is None:
            self.parameters = Parameters(**parameters_dict)
        else:
            self.parameters = parameters
            if parameters_dict:
                self.parameters.update(update_dict=parameters_dict)

    @property
    def routes(self):
        if self.route_properties_df is None:
            return []
        _r = self.route_properties_df[self.route_id_prop].unique().tolist()
        return _r

    @property
    def route_properties_by_time_df(self):
        if self._route_properties_by_time_df is None:
            self._route_properties_by_time_df = self._melt_routes_by_time_period(
                self.route_properties_df
            )
        return self._route_properties_by_time_df

    def properties_of_route(self, route_id) -> dict:
        """Given a route id, returns properties associated w ith it.

        Args:
            route_id: id of route corresponding to a value in self.route_id_prop

        Returns:
            Dict: key:val of column name: value for first instance of where route_id matches
        """
        rt_records = self.route_properties_df[
            self.route_properties_df[self.route_id_prop] == route_id
        ]
        d = rt_records.to_dict(orient="records")[0]
        return d

    def shapes_of_route(self, route_id) -> pd.DataFrame:
        """Given a route id, returns shape values associated w ith it.

        Args:
            route_id: id of route corresponding to a value in self.route_id_prop

        Returns:
            DataFrame of shape records for route
        """
        rt_records = self.shapes_df[self.shapes_df[self.route_id_prop] == route_id]
        return rt_records

    def add_source(
        self,
        route_properties_df: DataFrame = None,
        route_properties_by_time_df: DataFrame = None,
        route_shapes_df: DataFrame = None,
        new_routes: Collection[str] = [],
        source_list: Collection[str] = [],
    ):
        """Method to add transit to ModelTransitNetwork from either
        route_properties_df or route_properties_by_type_df as well as
        route_shapes_df. DOES NOT DO EXTENSIVE VALIDATION.

        ##TODO: add valdiation of route_shapes_df to make sure there
        is a shape for each route.

        Args:
            route_properties_df (DataFrame, optional): Table of routes with a column for each
                property by time of day (wide representation). Either route_properties_df or
                route_properties_by_time_df must be specified.
            route_properties_by_time_df (DataFrame, optional): Table with a row for each routes/
                time period combination with a column for each property (long representation).
                Either route_properties_df or route_properties_by_time_df must be specified.
                Defaults to None.
            route_shapes_df (DataFrame):  Table with a row for each route/node/order combination
                which also has a boolean field for "stop". Defaults to None.
            new_routes (Collection[str], optional): If specified, will only import the listed
                routes. Defaults to [].
            source_list: If specified, will add this list of strings to the source_list variable.

        Raises:
            ValueError: [description]
        """

        routes_by_time = []
        routes_not_time = []

        if route_properties_df is not None:
            routes_not_time = route_properties_df[self.route_id_prop].unique().tolist()
        if route_properties_by_time_df is not None:
            routes_by_time = (
                route_properties_by_time_df[self.route_id_prop].unique().tolist()
            )

        if not new_routes:
            new_routes = list(set(routes_by_time + routes_not_time))

        # Before adding lines, check to see if any are overlapping with existing in the network
        overlapping_lines = set(new_routes) & set(self.routes)

        if overlapping_lines:
            msg = f"Overlapping routes with existing: {overlapping_lines}"
            WranglerLogger.error(msg)
            raise ValueError(msg)

        existing_dfs = [
            "_route_properties_by_time_df",
            "route_properties_df",
            "shapes_df",
        ]

        new_dfs = [route_properties_by_time_df, route_properties_df, route_shapes_df]

        # Add data by updating if some already exists.
        for existing, new in zip(existing_dfs, new_dfs):
            # WranglerLogger.debug(f"Existing:{self.__dict__[existing]}\nNew:{new}")
            if new is None:
                continue
            if self.__dict__[existing] is None:
                self.__dict__[existing] = new
            else:
                self.__dict__[existing] = self.__dict__[existing].append(
                    new, ignore_index=True
                )

        # fix concurrency
        _add_to_not_time = list(set(routes_by_time) - set(routes_not_time))
        _add_to_time = list(set(routes_not_time) - set(routes_by_time))

        self.fix_concurrency_between_route_representations(
            add_to_route_props=_add_to_not_time,
            add_to_route_props_by_time=_add_to_time,
        )

        # add to source list
        self.source_list += source_list

    def fix_concurrency_between_route_representations(
        self,
        add_to_route_props: Collection[str] = None,
        add_to_route_props_by_time: Collection[str] = None,
    ) -> None:
        """Makes sure self.route_properties_df (wide) and self.route_properties_by_time_df
        (long) are concurrent with each other.

        Args:
            add_to_route_props (Collection[str], optional): List of route names which need
                to be added to self.route_properties_df from self.route_properties_by_time_df.
                Defaults to None.
            add_to_route_props_by_time (Collection[str], optional): List of route names which need
                to be added to self.route_properties_by_time_df from self.route_properties_df.
                Defaults to None.
        """
        msg = f"Adding routes {len(add_to_route_props)} to \
            self.route_props_df: {add_to_route_props}"
        WranglerLogger.debug(msg)
        if add_to_route_props:
            self.add_route_props_from_route_props_by_time(add_to_route_props)

        if add_to_route_props_by_time:
            self.add_route_props_by_time_from_unmelted(add_to_route_props_by_time)

    def add_route_props_from_route_props_by_time(self, routes: Collection[str]) -> None:
        """Method which adds selected routes to self.route_properties_df from
        self.route_properties_by_time_df.

        Args:
            routes (Collection[str]): List of route names which need to be added to
                self.route_properties_df from self.route_properties_by_time_df.
        """
        if self.route_properties_df is None:
            self.route_properties_df = self._route_properties_by_time_df
        else:
            new_route_props_df = self.route_properties_by_time_df[
                self.route_properties_by_time_df[self.route_id_prop].isin(routes)
            ]
            self.route_properties_df = self.route_properties_df.append(
                new_route_props_df,
                ignore_index=True,
            )

    def add_route_props_by_time_from_unmelted(self, routes: Collection[str]) -> None:
        """Method which adds selected routes to self.route_properties_by_time_df from
        self.route_properties_df.

        Args:
            routes (Collection[str]):  List of route names which need to be added to
                self.route_properties_by_time_df from self.route_properties_df.
        """
        msg = f"Adding {len(routes)} routes \
                to self.route_properties_by_time_df: \
                {routes}"
        WranglerLogger.debug(msg)

        _new_route_properties_by_time_df = self._melt_routes_by_time_period(
            self.route_properties_df[
                self.route_properties_df[self.route_id_prop].isin(routes)
            ]
        )
        if self._route_properties_by_time_df is None:
            WranglerLogger.debug("Creating new: self._route_properties_by_time_df")
            self._route_properties_by_time_df = _new_route_properties_by_time_df
        else:
            WranglerLogger.debug(
                "Adding to existing: self._route_properties_by_time_df"
            )
            self._route_properties_by_time_df = (
                self._route_properties_by_time_df.append(
                    _new_route_properties_by_time_df, ignore_index=True
                )
            )

    def _melt_routes_by_time_period(
        self,
        transit_properties_df: DataFrame,
        df_key: str = "NAME",
        tp_prop: str = "HEADWAY",
    ) -> DataFrame:
        """Go from wide to long DataFrame with additional rows for transit routes which
        span multiple time periods, noted by additional column `model_time_period_number`.
        For going from self._model_route_properties_df =>
            self._model_route_properties_by_time_period_df

        Args:
            transit_properties_df (DataFrame): Dataframe in "wide" format.
            df_key (str, optional): Key of the route dataframe. Defaults to "NAME".
            tp_prop (str, optional): Field name for property which specifies which
                time period a route is active for. Defaults to "HEADWAY".

        Returns:
            DataFrame: With additional rows for transit routes which span multiple time periods,
                noted by additional column `model_time_period_number`
        """
        # WranglerLogger.debug(f"transit_properties_df\n: {transit_properties_df}")
        # WranglerLogger.debug(f"tp_prop\n: {tp_prop}")
        _time_period_fields = [
            p
            for p in transit_properties_df.columns
            if self.base_prop_from_time_varying_prop(p) == tp_prop
        ]
        # WranglerLogger.debug(f"_time_period_fields: {_time_period_fields}")
        melted_properties_df = transit_properties_df.melt(
            id_vars=[df_key], value_vars=_time_period_fields
        )
        # WranglerLogger.debug(f"melted_properties:\n {melted_properties_df}")
        melted_properties_df = melted_properties_df.dropna(how="any")
        melted_properties_df["tp_num"] = melted_properties_df["variable"].apply(
            lambda x: self.tp_num_from_time_varying_prop(x)
        )

        # WranglerLogger.debug(f"melted_properties.dropna:\n {melted_properties_df}")

        melted_properties_df.drop(columns=["variable", "value"])

        melted_properties_df = melted_properties_df.merge(
            transit_properties_df, how="left", on=df_key
        )
        # WranglerLogger.debug(f"melted_properties_df.merge:\n {melted_properties_df}")
        _time_varying_fields = self.time_varying_props_filter(
            list(transit_properties_df.columns)
        )
        _time_varying_field_basenames = list(
            set(
                [self.base_prop_from_time_varying_prop(x) for x in _time_varying_fields]
            )
        )
        _drop_cols = ["variable", "value"]
        # Move time-varying fields which are like HEADWAY[1] into HEADWAY
        for basename in _time_varying_field_basenames:
            melted_properties_df[basename] = melted_properties_df.apply(
                lambda x: x[self.model_prop_from_base_prop_tp_num(basename, x.tp_num)],
                axis=1,
            )
            _drop_cols += [
                x
                for x in melted_properties_df.columns
                if (self.base_prop_from_time_varying_prop(x) == basename)
                and (x != basename)
            ]
        melted_properties_df = melted_properties_df.drop(columns=_drop_cols)

        transit_ps = self.parameters.transit_network_ps

        melted_properties_df["tp_abbr"] = melted_properties_df["tp_num"].map(
            transit_ps.transit_network_model_to_general_network_time_period_abbr
        )

        WranglerLogger.debug(f"melted_properties_df:\n{melted_properties_df}")
        return copy.deepcopy(melted_properties_df)


class StdToModelAdapter:
    """Object with methods to translate between a standard GTFS data in a
    :py:class:`TransitNetwork` instance and :py:class:`ModelTransit`.
    """

    def transform(self) -> Collection[DataFrame]:
        """Wrapper method to transform routes and nodes.

        Returns:
            Collection[DataFrame]: Returns tuple with transformed
                model_route_properties_by_time_df and model_nodes_df
        """
        model_route_properties_by_time_df = self.transform_routes()
        model_nodes_df = self.transform_nodes()

        return model_route_properties_by_time_df, model_nodes_df

    @staticmethod
    def calculate_route_name(
        route_properties_df,
        name_field_list: Collection[str] = [
            "agency_id",
            "route_id",
            "tp_abbr",
            "direction_id",
        ],
        delim: str = "_",
    ) -> pd.Series:
        """Calculates the route name based on name_field_list.

        Args:
            route_properties_df (pd.DataFrame): dataframe with fields in `name_field_list`
            name_field_list (Collection[str],Optional): list of fields to use for route
                name in order of use.
                Defaults to ["agency_id","route_id", "time_period", "direction_id"].
            delim (str, Optional): delimeter string for creating route name. Defaults to "_".

        Returns:
            Union[pd.Series,Mapping]: [description]
        """
        route_name_s = (
            route_properties_df[name_field_list].astype(str).agg(delim.join, axis=1)
        )

        return route_name_s


class ModelToStdAdapter:
    """Converts ModelTransit object to network wrangler standard df transit representation:
    - routes_df: route/trip properties for a time-of-day range
    - nodes_df: node/stop properties
    - shapes_df: shape properties
    """

    MODEL_ROUTE_ID_PROPS = ["NAME", "tp_num"]

    STD_ROUTE_ID_PROPS = [
        "route_id",
        "start_time",
        "end_time",
    ]

    DEFAULT_STD_ROUTE_PROPS = {"direction_id": 0, "agency_id": 1}

    STD_NAME_REGEX = r"_.*_.*_[01]"
    STD_NAME_PROPS = ["agency_id", "route_id", "time_period", "direction_id"]
    FIELDS_KEPT_FROM_STD_NAME = ["agency_id", "route_id", "direction_id"]

    PROJ_REQ_FACILITY_FIELDS = ["route_id", "start_time", "end_time"]
    PROJ_FACILITY_FIELDS = [
        "route_id",
        "agency_id",
        "direction_id",
        "trip_id",
        "start_time",
        "end_time",
    ]

    def __init__(
        self,
        modeltransit: ModelTransit,
        model_to_std_prop_trans: Mapping[Collection[str], Any] = None,
        model_route_id_props: Collection[str] = MODEL_ROUTE_ID_PROPS,
        std_route_id_props: Collection[str] = STD_ROUTE_ID_PROPS,
    ) -> None:

        self.__modeltransit = modeltransit

        self.model_routes_df = modeltransit._route_properties_by_time_df
        self.model_shapes_df = modeltransit.shapes_df

        _ps = modeltransit.parameters
        _transit_ps = _ps.transit_network_ps

        self.time_period_abbr_to_time = _ps.network_model_ps.time_period_abbr_to_time

        self.time_period_abbr_to_time = _ps.network_model_ps.time_period_abbr_to_time

        self.transit_to_network_time_periods = (
            _transit_ps.transit_network_model_to_general_network_time_period_abbr
        )

        self.model_to_std_prop_trans = model_to_std_prop_trans

    def transform(self) -> Collection[DataFrame]:
        std_routes_df = self.transform_routes()
        std_shapes_df = self.transform_shapes()
        std_nodes_df = None
        return std_routes_df, std_nodes_df, std_shapes_df

    def transform_shapes(self):

        _properties_to_transform = [
            k
            for k, v in self.model_to_std_prop_trans.items()
            if k[0] in self.model_shapes_df.columns
        ]

        _std_shapes_df = copy.deepcopy(self.model_shapes_df)

        for model_prop, standard_prop in _properties_to_transform:
            _std_shapes_df[standard_prop] = _std_shapes_df[standard_prop].apply(
                self.model_to_std_prop_trans[(model_prop, standard_prop)]
            )

        _std_shapes_df = _std_shapes_df.rename(self.model_to_std_prop_map)

        return _std_shapes_df

    def get_std_ids_from_name(
        self,
        routes_df: DataFrame,
    ) -> DataFrame:
        """Adds standard transit route identifiers:
            route_id
            agency_id
            direction_id

        by first seeing if should
        break apart route "NAME"; And then

        Args:
            routes_df: [description]

        Returns: routes dataframe with added rows.
        """

        std_route_name = len(
            routes_df[
                routes_df[self.model_to_std_prop_map["route_id"]].str.match(
                    ModelToStdAdapter.STD_NAME_REGEX
                )
                is True
            ]
        ) == len(routes_df)

        if not std_route_name:
            raise ValueError(
                f"Can't calculate standard route identifiers from\
                MODEL_ROUTE_ID_PROP: {self.model_to_std_prop_map['route_id']}\
                because it doens't fit the right pattern."
            )

        routes_df[ModelToStdAdapter.FIELDS_KEPT_FROM_STD_NAME] = routes_df[
            self.model_to_std_prop_map["route_id"]
        ].apply(
            ModelToStdAdapter.fields_from_std_route_name,
            name_field_list=ModelToStdAdapter.STD_NAME_PROPS,
            output_fields=ModelToStdAdapter.FIELDS_KEPT_FROM_STD_NAME,
            axis=1,
        )

        return routes_df

    @staticmethod
    def fields_from_std_route_name(
        line_name_s: pd.Series,
        name_field_list: Collection[str],
        output_fields: Collection[str],
        delim: str = "_",
    ) -> pd.DataFrame:
        """Unpacks route name into direction, route, agency, and time period info

        Args:
            line_name_s(pd.Series): series of line names i.e. "0_452-111_pk_1"
            name_field_list (Collection[str],Optional): list of fields to use for
                route name in order of use.
                Defaults to ["agency_id","route_id", "time_period", "direction_id"].
            output_fields: list of fields to output/
                Defaults to ["agency_id","route_id", "direction_id"].
            delim (str, Optional): delimeter string for creating route name.
                Defaults to "_".

        Returns: DataFrame with following fields:
            agency_id (str) : i.e. 0
            route_id (str): 452-111
            direction_id (str) : i.e. 1

        """
        line_name_s.dropna(inplace=True)
        split_name_df = line_name_s.str.strip('"').split(delim, n=1, expand=True)
        split_name_df.columns = name_field_list

        return split_name_df[output_fields]

    def calculate_start_end_time_HHMM(self, routes_df: DataFrame) -> DataFrame:
        """Adds fields to dataframe for start and end times based on
        :py:`self.time_period_abbr_to_time`.

        Args:
            routes_df (DataFrame): Routes dataframe with "tp_abbr".

        Returns:
            DataFrame: DataFrame with new fields "start_time" and "end_time"
        """
        _start_end_df = routes_df["tp_abbr"].map(self.time_period_abbr_to_time)
        routes_df[["start_time", "end_time"]] = _start_end_df.apply(pd.Series)
        return routes_df

    def get_timespan_from_transit_network_model_time_period(
        self, transit_time_periods: Collection[int]
    ) -> Collection[str]:
        """Calculate the start and end times from a list of transit network model time periods..

        Wrapper for time_utils.get_timespan_from_transit_network_model_time_period
        with object parameters.

        Args:
            transit_time_periods (Collection[int]): list of integers representing transit
            network model time periods to find time span for.

        Returns:
            Collection[str]: Tuple of start and end times in HH:MM:SS strings
        """
        (
            start_time,
            end_time,
        ) = time_utils.get_timespan_from_transit_network_model_time_period(
            transit_time_periods,
            time_period_abbr_to_time=self.time_period_abbr_to_time,
            transit_to_network_time_periods=self.transit_to_network_time_periods,
        )

        return start_time, end_time

    def transform_routes(self, std_ids: bool = True) -> DataFrame:
        """
        Converts model style properties in :py:`self.model_transit_routes_df` to
        standard properties.

        Args:
            std_ids: if True, will split name into route_id, direction_id, agency_id

        Returns:
            An updated dataframe with standard network property names and units.

        """

        _routes_df = self.model_routes_df

        for (m_prop, s_prop), trans in self.model_to_std_prop_trans.items():
            if m_prop not in _routes_df.columns:
                continue
            _routes_df[s_prop] = _routes_df[m_prop].apply(trans)
            _routes_df = _routes_df.drop(columns=[m_prop])

        _routes_df = self.calculate_start_end_time_HHMM(_routes_df)

        if std_ids:
            _routes_df = ModelToStdAdapter.get_std_ids_from_name(_routes_df)
            for k, v in ModelToStdAdapter.DEFAULT_STD_ROUTE_PROPS.items():
                if k not in _routes_df.columns:
                    _routes_df[k] = v
                elif _routes_df[k].dropna().empty:
                    _routes_df[k] = v

        return _routes_df


def diff_shape(
    shape_a: DataFrame,
    shape_b: DataFrame,
    match_id: Union[str, Collection[str]],
    n_buffer_vals: int = 3,
) -> Collection[DataFrame]:
    """Selects subset of shape_a and shape_b which encompasses any changes in the field(s) specified
    in match_id, plus a buffer as specified in n_buffer_vals.

    Args:
        shape_a (DataFrame): Base transit shape with row for each route/N/order combination as
            well as 'stop'.
        shape_b (DataFrame): Updated transit shape with row for each route/N/order combination as
            well as 'stop'.
        match_id (Union[str, Collection[str]]): Field name or a collection of field names to
            use as the match, e.g. ["N","stop"]. Defaults to None.
        n_buffer_vals (int, optional): Number of nodes on either side which are included in
            the output string of the match. Defaults to 3.

    Returns:
        Collection[DataFrame]: existing_df, set_df for subset of input shape_a and shape_b which
            should be specified in a route change project card – the different part as well as
            a buffer (n_buffer_vals) so it can be matched.
    """

    import difflib

    if type(match_id) != str:
        id_field = "_".join(match_id)
    else:
        id_field = match_id

    shape_a = shape_a.reset_index()
    shape_b = shape_b.reset_index()
    WranglerLogger.debug(f"[shape_a]: \n {shape_a}")
    WranglerLogger.debug(f"[shape_b]: \n {shape_b}")

    blocks = difflib.SequenceMatcher(
        a=shape_a[id_field],
        b=shape_b[id_field],
        autojunk=False,
    ).get_matching_blocks()
    """
    diff_df
    ix a  b  n
    0  0  0  1
    1  3  3  6
    2  9  9  0
    """
    diff_df = pd.DataFrame([{"a": x[0], "b": x[1], "n": x[2]} for x in blocks])
    WranglerLogger.debug(f"[diff_df].n_buffer_vals: {n_buffer_vals}")
    # WranglerLogger.debug(f"[diff_df]: \n {diff_df}")

    _first_change_a = -1
    _first_change_b = -1
    _first_overlap_a = diff_df.iloc[0]["a"]
    _first_overlap_b = diff_df.iloc[0]["b"]

    if _first_overlap_a == _first_overlap_b == 0:
        _first_change_a = diff_df.iloc[0]["n"]
        _first_change_b = diff_df.iloc[0]["n"]

    """
    WranglerLogger.debug(
        f"[diff_df]: \n   \
        _first_overlap_a: {_first_overlap_a}\n   \
        _first_overlap_b: {_first_overlap_b}\n    \
        _first_change_a: {_first_change_a}\n  \
        _first_change_b: {_first_change_b}\n"
    )
    """
    print("SHAPE_A\n", shape_a)

    print("SHAPE_B\n", shape_b)
    _last_change_a = len(shape_a) - 1
    _last_change_b = len(shape_b) - 1
    _last_overlap_a = diff_df.iloc[-2]["a"] + diff_df.iloc[-2]["n"]
    _last_overlap_b = diff_df.iloc[-2]["b"] + diff_df.iloc[-2]["n"]

    if diff_df.iloc[-1]["a"] == _last_overlap_a:
        _last_change_a = diff_df.iloc[-2]["a"] - 1

    if diff_df.iloc[-1]["b"] == _last_overlap_b:
        _last_change_b = diff_df.iloc[-2]["b"] - 1

    if _last_change_a < _first_change_a:
        _last_change_a = _first_change_a

    if _last_change_b < _first_change_b:
        _last_change_b = _first_change_b

    """
    WranglerLogger.debug(
        f"[diff_df]: \n   \
        _last_overlap_a: {_last_overlap_a}\n   \
        _last_overlap_b: {_last_overlap_b}\n    \
        _last_change_a: {_last_change_a}\n  \
        _last_change_b: {_last_change_b}\n"
    )
    """

    A_i = max(0, _first_change_a - n_buffer_vals + 1)
    A_j = min(len(shape_a), _last_change_a + n_buffer_vals)
    B_i = max(0, _first_change_b - n_buffer_vals + 1)
    B_j = min(len(shape_b), _last_change_b + n_buffer_vals)

    """
    WranglerLogger.debug(
        f"\
        [diff_df]:\n  \
        A_i: {A_i}\n  \
        A_j: {A_j}\n  \
        B_i: {B_i}\n  \
        B_j: {B_j}\n"
    )
    """

    existing_df = shape_a.iloc[A_i:A_j][match_id]
    set_df = shape_b.iloc[B_i:B_j][match_id]

    return existing_df, set_df
