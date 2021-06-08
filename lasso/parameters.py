"""
Structures parameters as dataclasses with good defaults.

Parameters can be set by instantiating the classes at run-time.
"""

import os
from dataclasses import dataclass, field, is_dataclass
from typing import Any, List, Dict, Set, Mapping, Collection, Sequence

from .data import PolygonOverlay, ValueLookup
from .logger import WranglerLogger


def get_base_dir(lasso_base_dir=os.getcwd()):
    d = lasso_base_dir
    for _ in range(3):
        if "lasso" in os.listdir(d):
            WranglerLogger.info("Lasso base directory set as: {}".format(d))
            return d
        d = os.path.dirname(d)

    msg = f"""Cannot find Lasso base directory from {lasso_base_dir}, please input using
     keyword in parameters: `lasso_base_dir =` """
    WranglerLogger.error(msg)
    raise (ValueError(msg))


@dataclass
class NetworkModelParameters:
    """

    Attributes:
        time_period_abbr_to_names: Maps time period abbreviations to names.
        time_periods_to_time: Maps time period abbreviations used in
            Cube to time of days used on transit/gtfs and highway network standard.
        network_time_period_abbr: list of network time period abbreviations,
            e.g. ["AM","MD","PM","NT]. Defaults to being calculated as
            `list(self.time_period_abbr_to_names.keys())`.

    """

    time_period_abbr_to_names: Mapping[Any, Any] = field(
        default_factory=lambda: {
            "AM": "AM Peak",
            "MD": "Midday",
            "PM": "PM Peak",
            "NT": "Evening/Night",
        }
    )
    time_period_abbr_to_time: Mapping[Any, Sequence] = field(
        default_factory=lambda: {
            "AM": ("6:00", "9:00"),
            "MD": ("9:00", "16:00"),
            "PM": ("16:00", "19:00"),
            "NT": ("19:00", "6:00"),
        }
    )
    network_time_period_abbr: Collection[Any] = None

    def __post_init__(self):
        if not set(self.time_period_abbr_to_names.keys()) == set(
            self.time_period_abbr_to_time.keys()
        ):
            raise (
                ValueError(
                    f"""time period abbreviations don't match up: \n
                        _to_names:{self.time_period_abbr_to_names.keys()}\n
                        vs _to_time: {self.time_period_abbr_to_time.keys()}"""
                )
            )
        if not self.network_time_period_abbr:
            self.network_time_period_abbr = list(self.time_period_abbr_to_names.keys())

    def __str__(self):
        _header = "[NetworkModelParameters]"
        _hide_vars = []
        _data_dict = {k: v for k, v in self.__dict__.items() if k not in _hide_vars}
        _data_str = "\n-->".join(
            ["{}:\n   {}".format(k, v) for k, v in _data_dict.items()]
        )
        return "{}\n{}".format(_header, _data_str)


@dataclass
class RoadwayNetworkModelParameters:
    """
    Attributes:
        model_roadway_class: str
        network_model_parameters: NetworkModelParameters
        allowed_use_categories: set of categories that can be used to refine the roadway network
            network by use type. Defaults to sov", "hov2", "hov3", "trk","default".
        category_grouping: Maps demand category abbreviations used as variable suffices to a
            list of network categories they are allowed to use.
        properties_to_split_by_network_time_periods: list of properties to split by the network
            time period.
        properties_to_split_by_category_groupings: list of properties to split by the category
            groupings
        properties_to_split: Dictionary mapping variables in standard
            roadway network to categories and time periods that need to be
            split out in final model network to get variables like LANES_AM. If not
                explicitly provided, can be calculated from
                properties_to_split_by_network_time_periods and
                properties_to_split_by_category_groupings
            or a default will be used.
        max_taz: integer denoting maximum taz number which is used for identifying which
            network links act as centroid connectors.
        centroid_connector_properties: maps properties in the standard network format with
            values to assert upon centroid connectors.
            Defaults to {"centroidconnect":1, "lanes": 1}
        additional_centroid_connector_properties: adds additional properties to
            centroid connectors.
        field_type: maps important field types. (e.g., int, str, float )
        roadway_value_lookups: dictionary of ValueLookup data classes or dictionaries
        roadway_field_mappings: dictionary of FieldMapping data classes or dictionaries
        roadway_overlays: geographic overlays
        output_fields: lists fields to output in the roadway network.
        required_fields_links: list of fields that must be in link output.
            ["A", "B", "shape_id", "geometry"]
        required_fields_nodes: list of fields that must be in node output.
            Defaults to ["N", "X", "Y", "geometry"]
        counts: mapping of count names and count files to be added.
        time_period_vol_split: dictionary mapping time period abbreviations to
            basic assumptions about
            fractions of daily volumes associated with them.
        count_tod_split_fields: a mapping of fields to split counts for
            by time_period_vol_split mapping, and
            the prefix to use for the resulting fields.
        network_build_script_type: If specified, will output a script to the output
                directory which will rebuild the network in the. Should be one of ["CUBE_HWYNET"].
                Defaults to None.
        output_espg: int = 26915
    """

    network_model_parameters: NetworkModelParameters
    model_roadway_class: str = "lasso.model_roadway.ModelRoadwayNetwork"
    allowed_use_categories: Set[str] = field(
        default_factory=lambda: ["sov", "hov2", "hov3", "trk", "default"]
    )
    category_grouping: Mapping[str, Set] = field(
        default_factory=lambda: {
            "sov": ["sov", "default"],
            "hov2": ["hov2", "default", "sov"],
            "hov3": ["hov3", "hov2", "default", "sov"],
            "trk": ["trk", "sov", "default"],
        }
    )
    properties_to_split_by_network_time_periods: Collection[str] = field(
        default_factory=list
    )
    properties_to_split_by_category_groupings: Collection[str] = field(
        default_factory=list
    )
    properties_to_split: Mapping[str, Mapping] = None
    output_espg: int = None
    max_taz: int = None
    centroid_connector_properties: Mapping[str, Any] = field(
        default_factory=lambda: {"centroidconnect": 1, "lanes": 1}
    )
    additional_centroid_connector_properties: Mapping[str, Any] = field(
        default_factory=dict
    )
    field_type: Mapping[str, Any] = field(default_factory=dict)
    roadway_value_lookups: Mapping[str, Any] = field(default_factory=dict)
    roadway_field_mappings: Mapping[str, Any] = field(default_factory=dict)
    roadway_overlays: Mapping[str, Any] = field(default_factory=dict)
    output_fields: Collection[str] = field(default_factory=list)
    required_fields_links: Collection[str] = field(
        default_factory=lambda: ["A", "B", "shape_id", "geometry"]
    )
    required_fields_nodes: Collection[str] = field(
        default_factory=lambda: ["N", "X", "Y", "geometry"]
    )
    counts: Mapping[str, ValueLookup] = field(default_factory=dict)
    time_period_vol_split: Mapping[str, float] = field(default_factory=dict)
    count_tod_split_fields: Mapping[str, str] = field(default_factory=dict)
    network_build_script_type: str = None
    output_espg: int = 26915
    # def __update__(self):
    # add an override so nested variables are updated not overwritten

    def __post_init__(self):
        assert set(list(self.field_type.values())).issubset(set([int, str, float]))

        # more than one method of split properties is specified
        if self.properties_to_split and (
            self.properties_to_split_by_network_time_periods
            or self.properties_to_split_by_category_groupings
        ):
            WranglerLogger.warning(
                "Both properties_to_split and at least one of \
                    (properties_to_split_by_category_groupings or \
                    properties_to_split_by_category_groupings) provided. \
                    Will overwrite properties_to_split."
            )

        # create standard properties to split from lists
        if not self.properties_to_split:
            self.properties_to_split = {
                k: {"v": k}
                for k in set(
                    self.properties_to_split_by_network_time_periods
                    + self.properties_to_split_by_category_groupings
                )
            }
            for v in self.properties_to_split_by_network_time_periods:
                self.properties_to_split[v][
                    "time_periods"
                ] = self.network_model_parameters.time_period_abbr_to_time
            for v in self.properties_to_split_by_category_groupings:
                self.properties_to_split[v]["categories"] = self.category_grouping

        # if nothing specified, create a default
        if not self.properties_to_split:
            self.properties_to_split = {
                "lanes": {
                    "v": "lanes",
                    "time_periods": self.network_model_parameters.time_period_abbr_to_time,
                },
                "ML_lanes": {
                    "v": "ML_lanes",
                    "time_periods": self.network_model_parameters.time_period_abbr_to_time,
                },
                "price": {
                    "v": "price",
                    "time_periods": self.network_model_parameters.time_period_abbr_to_time,
                    "categories": self.category_grouping,
                },
                "access": {
                    "v": "access",
                    "time_periods": self.network_model_parameters.time_period_abbr_to_time,
                },
            }

    def __str__(self):
        _header = "[RoadwayNetworkModelParameters]"
        _hide_vars = []
        _data_dict = {k: v for k, v in self.__dict__.items() if k not in _hide_vars}
        _data_str = "\n-->".join(
            ["{}:\n   {}".format(k, v) for k, v in _data_dict.items()]
        )
        return "{}\n{}".format(_header, _data_str)


@dataclass
class TransitNetworkModelParameters:
    """

    Attributes:
        model_transit_class: str
        std_transit_class: str
        network_model_parameters: NetworkModelParameters instance
        transit_value_lookups: dictionary of lookups
        transit_network_model_to_general_network_time_period_abbr: Maps cube time period
                numbers used in
                transit line files to the time period abbreviations in ttime_period_abbr_to_time
                dictionary. Defaults to `{"1": "AM", "2": "MD"}`.
    """

    network_model_parameters: NetworkModelParameters
    model_transit_class: str = ".transit.ModelTransitNetwork"
    std_transit_class: str = "network_wrangler.TransitNetwork"
    transit_network_model_to_general_network_time_period_abbr: Mapping[
        Any, Any
    ] = field(default_factory=lambda: {"1": "AM", "2": "MD"})
    transit_value_lookups: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """
        Checks parameter compatibility and sets defaults.

        Checks that the following parameters are compatible:
         - `network_model_parameters.time_period_abbr_to_time` and
            `transit_network_model_to_general_network_time_period_abbr.values()`
        """
        if not set(
            self.transit_network_model_to_general_network_time_period_abbr.values()
        ).issubset(set(self.network_model_parameters.time_period_abbr_to_time.keys())):
            raise (
                ValueError(
                    f"""specified transit_to_network_time_periods:
                        {self.transit_network_model_to_general_network_time_period_abbr}
                        does not align with specified network time periods
                        {self.network_model_parameters.time_period_abbr_to_time.keys()}"""
                )
            )

    def __str__(self):
        _header = "[TransitNetworkModelParameters]"
        _hide_vars = []
        _data_dict = {k: v for k, v in self.__dict__.items() if k not in _hide_vars}
        _data_str = "\n-->".join(
            ["{}:\n   {}".format(k, v) for k, v in _data_dict.items()]
        )
        return "{}\n{}".format(_header, _data_str)


@dataclass
class DemandModelParameters:
    """A data class representing parameters which define how a model system
    is set up such as time of day, mode, and use categorizations.

    This class is instantiated with "good default" values if they aren't specified
    at runtime.

    Attributes:
        network_model_parameters: Instance of NetworkModelParameters
        demand_time_periods: list of time period abbreviations used by the demand model,
            e.g. `["pk","op"]`.
            If not set, will default to `network_to_demand_time_periods.values()`
        network_to_demand_time_periods: mapping of network model time period abbreviations
            to demand model time period abbreviations. Defaults to `{"AM": "pk", "MD": "op"}`.

    """

    network_model_parameters: NetworkModelParameters
    demand_time_periods: List[Any] = field(default_factory=list)
    network_to_demand_time_periods: Dict = field(
        default_factory=lambda: {"AM": "pk", "MD": "op"}
    )

    def __post_init__(self):
        if not set(self.network_to_demand_time_periods.keys()).issubset(
            set(self.network_model_parameters.time_period_abbr_to_time.keys())
        ):
            raise (
                ValueError(
                    f"""specified network_to_demand_time_periods:
                        {self.network_to_demand_time_periods}
                        does not align with specified network time periods
                        {self.network_model_parameters.time_period_abbr_to_time.keys()}"""
                )
            )
        if not self.demand_time_periods:
            self.demand_time_periods = set(self.network_to_demand_time_periods.values())

    def __str__(self):
        _header = "[DemandModelParameters]"
        _hide_vars = []
        _data_dict = {k: v for k, v in self.__dict__.items() if k not in _hide_vars}
        _data_str = "\n-->".join(
            ["{}:\n   {}".format(k, v) for k, v in _data_dict.items()]
        )
        return "{}\n{}".format(_header, _data_str)


@dataclass
class FileParameters:
    """
    FileSystem Parameters

    Attr:
        lasso_base_directory: Directory lasso is within.
        data_directory: Directory to look for data files like lookups, overlays, etc.
        value_lookups: dictionary of ValueLookup data classes
        scratch_directory: Directory location for temporary files.
        settings_location: Directory location for run-level settings. Defaults to
            examples/settings
        shape_foreign_key: join field between link_df/link.json and shape_df/shape.geojson
        output_directory: Directory location for output and log files.
        output_basename_links: Defaults to "links_out"
        output_basename_nodes: Defaults to "nodes_out"
        output_relative: bool = True: If set to true, will assume output files are
            relative filenames to the output directory on instantiation.
        output_espg: projection for any output geographic files to be written in defaults to 26915

    """

    lasso_base_directory: str = get_base_dir()
    settings_location: str = os.path.join(get_base_dir(), "examples", "settings")
    data_directory: str = None
    value_lookups: Mapping[str, ValueLookup] = field(default_factory=dict)
    scratch_directory: str = None
    shape_foreign_key: str = "id"
    output_directory: str = None
    output_prefix: str = ""
    output_basename_links: str = "links_out"
    output_basename_nodes: str = "nodes_out"

    def __post_init__(self):
        if not self.scratch_directory:
            self.scratch_directory = os.path.join(
                self.lasso_base_directory, "tests", "scratch"
            )

        if not self.output_directory:
            self.output_directory = self.scratch_directory

        if not os.path.exists(self.scratch_directory):
            os.mkdir(self.scratch_directory)

        if not os.path.exists(self.output_directory):
            os.mkdir(self.output_directory)

    def __str__(self):
        _header = "[FileParameters]"
        _hide_vars = []
        _data_dict = {k: v for k, v in self.__dict__.items() if k not in _hide_vars}
        _data_str = "\n-->".join(
            ["{}:\n   {}".format(k, v) for k, v in _data_dict.items()]
        )
        return "{}\n{}".format(_header, _data_str)


@dataclass
class Parameters:
    input_ps: Mapping[str, Any] = field(default_factory=dict)
    file_ps: FileParameters = None
    network_model_ps: NetworkModelParameters() = None
    transit_network_ps: TransitNetworkModelParameters = None
    roadway_network_ps: RoadwayNetworkModelParameters = None
    demand_model_ps: DemandModelParameters = None
    geographic_overlays: Collection[PolygonOverlay] = field(default_factory=list)
    name: str = "Class Default"

    def __str__(self):
        _header = "---Parameters instance---"
        _hide_vars = ["input_ps"]
        _data_dict = {k: v for k, v in self.__dict__.items() if k not in _hide_vars}
        _data_str = "\n-->".join(
            ["{}:\n   {}".format(k, v) for k, v in _data_dict.items()]
        )
        return "{}\n{}".format(_header, _data_str)

    @staticmethod
    def parameters_list():
        all_params = set(
            list(vars(DemandModelParameters).keys())
            + list(vars(TransitNetworkModelParameters).keys())
            + list(vars(RoadwayNetworkModelParameters).keys())
            + list(vars(NetworkModelParameters).keys())
            + list(vars(FileParameters).keys())
            + list(vars(Parameters).keys())
        )
        return all_params

    @staticmethod
    def keywords_into_dict_by_param_type(flat_dict: dict):
        """
        Sort the input parameters into which parameter types they belong to.

        Args:
            flat_dict: dictionary of keywords for parameter classes

        Returns: a nested dictionary by parameter type (e.g. "file", "network model", etc.)
        """
        if not flat_dict:
            WranglerLogger.debug(
                "No parameter keywords to sort into parameter type; will be returning\
                empty dictionaries."
            )

        nested_dict = {}
        nested_dict["file"] = {
            k: v
            for k, v in flat_dict.items()
            if k in FileParameters.__dataclass_fields__.keys()
        }
        nested_dict["network model"] = {
            k: v
            for k, v in flat_dict.items()
            if k in NetworkModelParameters.__dataclass_fields__.keys()
        }
        nested_dict["roadway network model"] = {
            k: v
            for k, v in flat_dict.items()
            if k in RoadwayNetworkModelParameters.__dataclass_fields__.keys()
        }
        nested_dict["transit network model"] = {
            k: v
            for k, v in flat_dict.items()
            if k in TransitNetworkModelParameters.__dataclass_fields__.keys()
        }
        nested_dict["demand model"] = {
            k: v
            for k, v in flat_dict.items()
            if k in DemandModelParameters.__dataclass_fields__.keys()
        }
        nested_dict["base"] = {
            k: v
            for k, v in flat_dict.items()
            if k in Parameters.__dataclass_fields__.keys()
        }

        return nested_dict

    def __post_init__(self):
        """
        Initializes parameter instances under an instance of the umbrella class Parameters().
        Overwrites class-level default parameters with parameters in self.input_ps.
        Warns if there are parameters specified in self.input_ps which aren't used in
            parameter classes.
        """
        WranglerLogger.debug("[Parameters.__post_init__()]")

        _input_ps_dict = Parameters.keywords_into_dict_by_param_type(self.input_ps)
        # WranglerLogger.debug(
        #    "-----POST INIT INPUT DICT BY TYPE------\n_input_ps_dict:\n{}".format(
        #        _input_ps_dict
        #    )
        # )
        # TODO this is very messy and verbose. Is there a way to make this more pythonic?

        # want to use matching network parameters, so delete any residual ones
        for _, v in _input_ps_dict.items():
            try:
                del v["network_model_parameters"]
            except KeyError:
                pass

        if _input_ps_dict.get("file"):
            self.file_ps = FileParameters(**_input_ps_dict.get("file"))
        else:
            self.file_ps = FileParameters()

        if _input_ps_dict.get("network model"):
            self.network_model_ps = NetworkModelParameters(
                **_input_ps_dict.get("network model")
            )
        else:
            self.network_model_ps = NetworkModelParameters()

        if _input_ps_dict.get("roadway network model"):
            self.roadway_network_ps = RoadwayNetworkModelParameters(
                self.network_model_ps, **_input_ps_dict.get("roadway network model")
            )
        else:
            self.roadway_network_ps = RoadwayNetworkModelParameters(
                self.network_model_ps
            )

        if _input_ps_dict.get("transit network model"):
            self.transit_network_ps = TransitNetworkModelParameters(
                self.network_model_ps, **_input_ps_dict.get("transit network model")
            )
        else:
            self.transit_network_ps = TransitNetworkModelParameters(
                self.network_model_ps
            )

        if _input_ps_dict.get("demand model"):
            self.demand_model_ps = DemandModelParameters(
                self.network_model_ps, **_input_ps_dict.get("demand model")
            )
        else:
            self.demand_model_ps = DemandModelParameters(self.network_model_ps)

        if _input_ps_dict.get("base"):
            self.__dict__.update(_input_ps_dict["base"])

        used_params = Parameters.parameters_list()

        if self.input_ps:
            unused_keys = list(set(list(self.input_ps.keys())) - set(used_params))

            if unused_keys:
                WranglerLogger.warning(
                    f"""[Parameters.__post_init__()] The following parameters were not used.
                        Check spelling.\n{unused_keys}"""
                )

    @staticmethod
    def initialize(base_params_dict: dict = {}, **kwargs):
        """Initializes a Parameters data class with base parameter dictionary which
        can be overwritten by including a keyword argument from any of the parameters
        classes.

        Args:
            base_params_dict: dictionary of parameters. Defaults to {}.
            kwarg: a keyword argument of any of the parameters classes in parameters.py
            which will be used to add-to or overwrite any defaults.

        Returns: Parameters data class with initialized parameters.
        """
        WranglerLogger.debug(
            "-----INITIALIZING PARAMETERS------\n...running Parameters.initialize()"
        )
        msg = "base parameter set: \n   -{}\n".format(
            "\n   - ".join(["{}: {}".format(k, v) for k, v in base_params_dict.items()])
        )
        # WranglerLogger.debug(msg)

        if kwargs:
            msg = "Updating default parameters with following kwargs: {}".format(kwargs)
            WranglerLogger.debug(msg)

            base_params_dict.update(kwargs)

            msg = "Updated parameter settings:\n   -{}\n".format(
                "\n   - ".join(
                    ["{}: {}".format(k, v) for k, v in base_params_dict.items()]
                )
            )
            WranglerLogger.debug(msg)

        p = Parameters(input_ps=base_params_dict)

        msg = "****Resulting Initialized Parameters*****\n  {}".format(p)
        # print(msg)
        # print("INITIAL FIELD TYPES PARAM: {}".format(p.roadway_network_ps.field_type))

        return p

    def update(self, update_dict={}, **kwargs):
        """
        Updates a parameter object overriding existing parameters.

        Args:
            update_dict:
            kwargs:

        Returns: updated Parameters instance
        """
        # print("1 - UPDATE_DICT {} kwargs {}".format(update_dict, kwargs))
        update_dict.update(kwargs)

        if not update_dict:
            WranglerLogger.warning(
                "Update called but nothing to update. Given: {}".format(update_dict)
            )
            return self

        _update_dict = Parameters.keywords_into_dict_by_param_type(update_dict)
        # print("2 - _UPDATE_DICT {}".format(_update_dict))

        self.file_ps.__dict__.update(_update_dict.get("file"))
        self.network_model_ps.__dict__.update(_update_dict.get("network model"))
        self.roadway_network_ps.__dict__.update(
            _update_dict.get("roadway network model")
        )
        self.transit_network_ps.__dict__.update(
            _update_dict.get("transit network model")
        )
        self.demand_model_ps.__dict__.update(_update_dict.get("demand model"))
        self.__dict__.update(_update_dict["base"])

        used_params = Parameters.parameters_list()

        unused_keys = list(set(list(kwargs)) - set(used_params))

        if unused_keys:
            WranglerLogger.warning(
                f"""[Parameters.update()]
                    The following parameters were not used.
                    Check spelling.\n
                    {unused_keys}"""
            )
        return self

    def as_dict(self):
        all_params = {}
        for k, v in self.__dict__.items():
            if is_dataclass(v):
                all_params.update(v.__dict__)
        return all_params
