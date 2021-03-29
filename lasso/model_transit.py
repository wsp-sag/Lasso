"""Transit-related classes to parse, compare, and write standard and cube transit files.

  Typical usage example:

    tn = CubeTransit.create_from_cube(CUBE_DIR)
    transit_change_list = tn.evaluate_differences(base_transit_network)

    cube_transit_net = StandardTransit.read_gtfs(BASE_TRANSIT_DIR)
    cube_transit_net.write_as_cube_lin(os.path.join(WRITE_DIR, "outfile.lin"))
"""
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
        _r = self.route_properties_df["NAME"].unique().tolist()
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
        source_list: Collection[str] = [],
    ):
        """[summary]

        Args:
            route_properties_df (DataFrame, optional): [description]. Defaults to None.
            route_properties_by_time_df (DataFrame, optional): [description]. Defaults to None.
            route_shapes_df (DataFrame, optional): [description]. Defaults to None.
            new_routes (Collection[str], optional): [description]. Defaults to [].
            source_list (Collection[str], optional): [description]. Defaults to [].

        Raises:
            ValueError: [description]
        """

        routes_by_time = []
        routes_not_time = []

        if route_properties_df is not None:
            routes_not_time = route_properties_df["NAME"].unique().tolist()
        if route_properties_by_time_df is not None:
            routes_by_time = route_properties_by_time_df["NAME"].unique().tolist()

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
            add_to_route_properties_df=_add_to_not_time,
            add_to_route_properties_by_time_df=_add_to_time,
        )

    def fix_concurrency_between_route_representations(
        self,
        add_to_route_properties_df: Collection[str] = None,
        add_to_route_properties_by_time_df: Collection[str] = None,
    ) -> None:
        """[summary]

        Args:
            add_to_route_properties_df (Collection[str], optional): [description].
                Defaults to None.
            add_to_route_properties_by_time_df (Collection[str], optional): [description].
                Defaults to None.

        Returns:
            [type]: [description]
        """
        msg = f"Adding routes to self.route_properties_df: {add_to_route_properties_df}"
        # WranglerLogger.debug(msg)
        if add_to_route_properties_df:
            if self.route_properties_df is None:
                self.route_properties_df = self._route_properties_by_time_df
            else:
                self.route_properties_df = self.route_properties_df.append(
                    self._route_properties_by_time_df.isin(
                        {"NAME", add_to_route_properties_df}
                    ),
                    ignore_index=True,
                )

        msg = f"Adding routes to self.route_properties_by_time_df: \
            {add_to_route_properties_by_time_df}"
        WranglerLogger.debug(msg)
        if add_to_route_properties_by_time_df:
            if self._route_properties_by_time_df is None:
                self._route_properties_by_time_df = self._melt_routes_by_time_period(
                    self.route_properties_df
                )
            else:
                _new_route_properties_by_time_df = self._melt_routes_by_time_period(
                    self.route_properties_df.isin(
                        {"NAME": add_to_route_properties_by_time_df}
                    )
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
        route_properties=True,
    ) -> DataFrame:
        """Go from wide to long DataFrame with additional rows for transit routes which
        span multiple time periods, noted by additional column `model_time_period_number`.
        For going from self._model_route_properties_df =>
            self._model_route_properties_by_time_period_df

        Args:
            transit_properties_df (DataFrame): Dataframe with route properties keyd by key.
            df_key = Defaults to "name".

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
        WranglerLogger.debug(f"melted_properties_df.merge:\n {melted_properties_df}")
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
        WranglerLogger.debug(f"melted_properties_df:\n{melted_properties_df}")

        transit_ps = self.parameters.transit_network_ps

        melted_properties_df["tp_abbr"] = melted_properties_df["tp_num"].map(
            transit_ps.transit_network_model_to_general_network_time_period_abbr
        )

        return melted_properties_df


class StdToModelAdapter:
    def transform(self) -> Collection[DataFrame]:
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
        """[summary]

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
        std_nodes_df = self.transform_routes()
        std_shapes_df = self.transform_routes()

        return std_routes_df, std_nodes_df, std_shapes_df

    def transform_nodes(self):
        ##TODO
        _nodes = self.model_shapes_df
        return _nodes

    def transform_shapes(self):
        ##TODO
        _shapes = self.model_shapes_df
        return _shapes

    def calculate_start_end_time_HHMM(self, routes_df):
        # Create dataframe with columns for start and end times
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
        Converts model style properties to standard properties.

        Args:
            model_transit_routes_df: dataframe of all routes with fields for route properties

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


def compare_route_shapes(
    base_shape_df: DataFrame, updated_shape_df: DataFrame
) -> Union[None, Mapping]:
    """[summary]

    Args:
        base_shape_df (DataFrame): [description]
            {"node_id": abs(n), "node": n, "stop": n > 0, "order": self.line_order}
        updated_shape_df (DataFrame): [description]

    Returns:
        Union[None,Mapping]: Returns a project card mapping for shape change.
            If no changes, will return None.
    """

    if base_shape_df == updated_shape_df:
        return None

    base_shape_df.before_node = base_shape_df.node.shift(periods=1)
    updated_shape_df.before_node = updated_shape_df.node.shift(periods=1)
    base_shape_df.after_node = base_shape_df.node.shift(periods=-1)
    updated_shape_df.after_node = updated_shape_df.node.shift(periods=-1)

    # options:
    # 1 - base shape extends farther on either end than updated_shape ==>
    # replace all with updated shape
    updated_first_n = base_shape_df.loc[0, "node"].squeeze()
    base_starts_before_update = (
        base_shape_df.loc[base_shape_df.node == updated_first_n, "order"].min() < 1
    )

    updated_last_n = updated_shape_df.loc[-1, "node"].squeeze()
    base_ends_after_update = (
        base_shape_df.loc[base_shape_df.node == updated_last_n, "order"].max()
        < base_shape_df["order"].max()
    )

    if base_starts_before_update or base_ends_after_update:
        existing_shape = base_shape_df.node.tolist()
        updated_shape = updated_shape_df.node.tolist()

    # 2 - updated shape extends farther on one or either end than base shape ==>
    # replace first change-->last change
    else:
        merged_df = updated_shape_df.merge(
            base_shape_df,
            how="left",
            on=["before_node", "node", "after_node"],
            indicator=True,
        )

        diffs = merged_df.loc[merged_df._merge == "left only"]

        diffs_start_base = diffs.loc[0, ["node", "order_y"]]
        diffs_start_update = diffs.loc[0, ["node", "order_x"]]

        diffs_end_base = diffs.loc[-1, ["node", "order_y"]]
        diffs_end_update = diffs.loc[-1, ["node", "order_x"]]

        existing_shape = base_shape_df.loc[
            (base_shape_df["order"] >= diffs_start_base["node", "order_y"])
            and (base_shape_df["order"] <= diffs_end_base["node", "order_y"]),
            "node",
        ].tolist()

        updated_shape = updated_shape_df.loc[
            (updated_shape_df["order"] >= diffs_start_update["node", "order_x"])
            and (updated_shape_df["order"] <= diffs_end_update["node", "order_x"]),
            "node",
        ].tolist()

    shape_change_dict = {
        "property": "routing",
        "existing": existing_shape,
        "set": updated_shape,
    }

    return shape_change_dict


def evaluate_route_shape_changes(
    base_transit: ModelTransit,
    updated_transit: ModelTransit,
    line_list: Collection = None,
) -> Collection[Mapping]:
    """
    Compares two route shapes and constructs returns list of changes
    suitable for a project card.

    Args:
        base_transit: ModelTransit,
        updated_transit: ModelTransit,

    Returns:
        List of shape changes formatted as a project card-change dictionary.

    """

    base_shapes = base_transit.shapes
    updated_shapes = updated_transit.shapes

    if base_shapes == updated_shapes:
        return []

    if not line_list:
        line_list = [i for i in updated_transit.lines if i in base_transit.lines]

    shape_change_list = [
        compare_route_shapes(base_shapes[r], updated_shapes[r]) for r in line_list
    ]

    return shape_change_list


def evaluate_model_transit_differences(
    base_transit: ModelTransit,
    updated_transit: ModelTransit,
    route_property_update_list: Collection[str] = None,
    new_route_property_list: Collection[str] = None,
    absolute: bool = True,
    include_existing: bool = False,
) -> Collection[Mapping]:
    """
    1. Identifies what routes need to be updated, deleted, or added
    2. For routes being added or updated, identify if the time periods
        have changed or if there are multiples, and make duplicate lines if so
    3. Create project card dictionaries for each change.

    Args:
        base_transit (CubeTransit): an ModelTransit instance for the base condition
        updated_transit (CubeTransit): an ModelTransit instance for the updated condition
        route_property_update_list (Collection[str], Optional): list of properties
            to consider updates for, ignoring others.
            If not set, will default to all the fields in updated_transit.
        new_route_property_list (Collection[str], Optional): list of properties to add
            to new routes. If not set, will default to all the fields in updated_transit.
        absolute (Bool): indicating if should use the [False case]'existing'+'change' or
            [True case]'set' notation a project card. Defaults to True.
        include_existing (Bool): if set to True, will include 'existing' in project card.
            Defaults to False.


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
        project.update_route_change_dict,
        absolute=absolute,
        include_existing=include_existing,
        axis=1,
    ).tolist()
    project_card_changes += _updated_properties_changes

    _updated_shapes_changes = updated_transit.evaluate_route_shape_changes(
        base_transit, updated_transit, line_list=lines_to_update
    )
    project_card_changes += _updated_shapes_changes

    return project_card_changes
