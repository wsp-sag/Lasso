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
        tp_property: str = None,
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

    def add_source(
        self,
        route_properties_df: DataFrame = None,
        route_properties_by_time_df: DataFrame = None,
        route_shapes_df: DataFrame = None,
        new_routes: Collection[str] = [],
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
                new_route_props_df, ignore_index=True,
            )

    def add_route_props_by_time_from_unmelted(self, routes: Collection[str]) -> None:
        """Method which adds selected routes to self.route_properties_by_time_df from
        self.route_properties_df.

        Args:
            routes (Collection[str]):  List of route names which need to be added to
                self.route_properties_by_time_df from self.route_properties_df.
        """
        msg = f"Adding {len(routes)}routes \
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
            self._route_properties_by_time_df = self._route_properties_by_time_df.append(
                _new_route_properties_by_time_df, ignore_index=True
            )

    @staticmethod
    def fields_from_std_route_name(
        line_name_s: pd.Series,
        name_field_list: Collection[str] = [
            "agency_id",
            "route_id",
            "time_period",
            "direction_id",
        ],
        delim: str = "_",
    ) -> pd.DataFrame:
        """Unpacks route name into direction, route, agency, and time period info

        Args:
            line_name_s(pd.Series): series of line names i.e. "0_452-111_pk_1"
            name_field_list (Collection[str],Optional): list of fields to use for
                route name in order of use.
                Defaults to ["agency_id","route_id", "time_period", "direction_id"].
            delim (str, Optional): delimeter string for creating route name.
                Defaults to "_".

        Returns: DataFrame with following fields:
            agency_id (str) : i.e. 0
            route_id (str): 452-111
            time_period (str): i.e. pk
            direction_id (str) : i.e. 1

        """
        line_name_s.dropna(inplace=True)
        split_name_df = line_name_s.str.strip('"').split(delim, n=1, expand=True)
        split_name_df.columns = name_field_list

        return split_name_df[name_field_list]

    def _melt_routes_by_time_period(
        self,
        transit_properties_df: DataFrame,
        df_key: str = "NAME",
        tp_property: str = "HEADWAY",
    ) -> DataFrame:
        """Go from wide to long DataFrame with additional rows for transit routes which
        span multiple time periods, noted by additional column `model_time_period_number`.
        For going from self._model_route_properties_df =>
            self._model_route_properties_by_time_period_df

        Args:
            transit_properties_df (DataFrame): Dataframe in "wide" format.
            df_key (str, optional): Key of the route dataframe. Defaults to "NAME".
            tp_property (str, optional): Field name for property which specifies which
                time period a route is active for. Defaults to "HEADWAY".

        Returns:
            DataFrame: With additional rows for transit routes which span multiple time periods,
                noted by additional column `model_time_period_number`
        """

        _time_period_fields = [
            p
            for p in transit_properties_df.columns
            if self.base_prop_from_time_varying_prop(p) == tp_property
        ]
        # WranglerLogger.debug(f"_time_period_fields: {_time_period_fields}")
        # WranglerLogger.debug(f"transit_properties_df:\n {transit_properties_df}")
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

    def __init__(
        self,
        modeltransit: ModelTransit,
        model_to_std_prop_map: Mapping[str, str] = None,
        model_to_std_prop_trans: Mapping[Collection[str], Any] = None,
    ) -> None:

        self.__modeltransit = modeltransit

        self.model_routes_df = modeltransit._route_properties_by_time_df
        self.model_shapes_df = modeltransit.shapes_df

        self.time_period_abbr_to_time = (
            modeltransit.parameters.network_model_ps.time_period_abbr_to_time
        )
        self.transit_to_network_time_periods = (
            modeltransit.parameters.transit_network_ps.transit_to_network_time_periods
        )
        self.time_period_abbr_to_time = (
            modeltransit.parameters.transit_ps.network_model_parameters.time_period_abbr_to_time
        )

        self.model_to_std_prop_map = model_to_std_prop_map
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

    def calculate_start_end_time_HHMM(self, routes_df: DataFrame) -> DataFrame:
        """Adds fields to dataframe for start and end times based on
        :py:`self.time_period_abbr_to_time`.

        Args:
            routes_df (DataFrame): Routes dataframe with "tp_abbr".

        Returns:
            DataFrame: DataFrame with new fields "start_time_HHMM" and "end_time_HHMM"
        """
        _start_end_df = routes_df["tp_abbr"].map(self.time_period_abbr_to_time)
        routes_df[["start_time_HHMM", "end_time_HHMM"]] = _start_end_df.apply(pd.Series)
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

    def transform_routes(self) -> DataFrame:
        """
        Converts model style properties in :py:`self.model_transit_routes_df` to
        standard properties.

        Returns:
            An updated dataframe with standard network property names and units.

        """
        _routes_df = self.model_routes_df.rename(self.model_to_std_prop_map)

        _properties_to_transform = [
            k
            for k, v in self.model_to_std_prop_trans.items()
            if k[0] in self.model_routes_df.columns
        ]

        for model_prop, standard_prop in _properties_to_transform:
            _routes_df[standard_prop] = _routes_df[standard_prop].apply(
                self.model_to_std_prop_trans[(model_prop, standard_prop)]
            )

        return _routes_df


def _diff_shape(
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
        a=shape_a[id_field], b=shape_b[id_field], autojunk=False,
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


def evaluate_route_shape_changes(
    base_t: ModelTransit,
    updated_t: ModelTransit,
    match_id: Union[Collection[str], str] = None,
    route_list: Collection = None,
    n_buffer_vals: int = 3,
) -> Collection[Mapping]:
    """Compares all the shapes (or the subset specified in `route_list`) for two
    :py:class:`ModelTransit` objects and outputs a list of project card changes
    which would turn `base_t.shapes_df` --> `updated_t.shapes_df`.

    Args:
        base_t (ModelTransit): Updated :py:class:`ModelTransit` with `shapes_df`.
        updated_t (ModelTransit): Updated :py:class:`ModelTransit` to compare to `base_t`
        match_id (Union[Collection[str], str], optional): Field name or a collection
            of field names to use as the match, e.g. ["N","stop"]. Defaults to None.
            Defaults to [base_t.node_id_prop, "stop"]
        route_list (Collection, optional): List of routes to compare. If not provided,
            will evaluate changes between all routes common between `base_t` and `updated_t`.
        n_buffer_vals (int, optional): Number of values on either side to include in
            match. Defaults to 3.

    Returns:
        Collection[Mapping]: [List of shape changes formatted as a
            project card-change dictionary.

    """
    base_shapes_df = base_t.shapes_df.copy()
    updated_shapes_df = updated_t.shapes_df.copy()

    WranglerLogger.debug(
        f"\nbase_shapes_df: \n{base_shapes_df}\
        \nupdated_shapes_df: \n {updated_shapes_df}"
    )

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

    # if the routes to compare are not specified in route_list, select all
    # routes which are common between the transit networks
    if not route_list:
        route_list = [i for i in updated_t.routes if i in base_t.routes]

    # Compare route changes for each route in the list
    shape_change_list = []
    for i in route_list:
        existing_r_df, change_r_df = _diff_shape(
            base_shapes_df[base_shapes_df[_route_id_prop] == i],
            updated_shapes_df[updated_shapes_df[_route_id_prop] == i],
            match_id,
        )
        rt_change_dict = project.update_route_routing_change_dict(
            existing_r_df, change_r_df,
        )
        WranglerLogger.debug(f"rt_change: {rt_change_dict}")
        shape_change_list.append(copy.deepcopy(rt_change_dict))
        WranglerLogger.debug(f"shape_change_list:{shape_change_list}")

    WranglerLogger.info(f"Found {len(shape_change_list)} transit changes.")

    return shape_change_list


def evaluate_model_transit_differences(
    base_transit: ModelTransit,
    updated_transit: ModelTransit,
    route_property_update_list: Collection[str] = None,
    new_route_property_list: Collection[str] = None,
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
        route_property_update_list (Collection[str], Optional): list of properties
            to consider updates for, ignoring others.
            If not set, will default to all the fields in updated_transit.
        new_route_property_list (Collection[str], Optional): list of properties to add
            to new routes. If not set, will default to all the fields in updated_transit.
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

    lines_to_update = [i for i in updated_transit.lines if i in base_transit.lines]

    project_card_changes = []

    base_route_props_df = base_transit.std_route_properties_df
    updated_route_props_df = updated_transit.std_route_properties_df

    if not route_property_update_list:
        route_property_update_list = list(
            updated_transit.std_route_properties_df.columns
        )

    if not new_route_property_list:
        new_route_property_list = list(updated_transit.std_route_properties_df.columns)

    _base_routes = base_route_props_df["name"].tolist()
    _updated_routes = updated_route_props_df["name"].tolist()

    """
    Deletions
    """
    lines_to_delete = [i for i in _base_routes if i not in _updated_routes]
    _delete_changes = (
        base_route_props_df.isin({"NAME": lines_to_delete})
        .apply(project.delete_route_change_dict, axis=1)
        .tolist()
    )
    project_card_changes += _delete_changes

    """
    Additions
    """
    lines_to_add = [i for i in _updated_routes if i not in _base_routes]
    _addition_changes = (
        updated_route_props_df.isin({"NAME": lines_to_add})
        .apply(
            project.new_transit_route_change_dict,
            transit_route_property_list=new_route_property_list,
            shapes=updated_transit.shapes,
            axis=1,
        )
        .tolist()
    )
    project_card_changes += _addition_changes

    """
    Evaluate Property Updates
    """
    _compare_df = base_route_props_df[route_property_update_list].compare(
        updated_route_props_df[route_property_update_list]
    )
    _compare_df[
        "self", ["name", "start_time_HHMM", "end_time_HHMM"]
    ] = updated_route_props_df[["name", "start_time_HHMM", "end_time_HHMM"]]

    _updated_properties_changes = _compare_df.apply(
        project.update_route_prop_change_dict,
        absolute=absolute,
        include_existing=include_existing,
        axis=1,
    )
    project_card_changes += _updated_properties_changes

    _updated_shapes_changes = updated_transit.evaluate_route_shape_changes(
        base_transit,
        updated_transit,
        line_list=lines_to_update,
        n_buffer_vals=n_buffer_vals,
    )

    WranglerLogger.debug(f"_updated_shapes_changes:{_updated_shapes_changes}")
    project_card_changes += _updated_shapes_changes

    return project_card_changes
