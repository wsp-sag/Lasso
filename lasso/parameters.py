
"""
Structures parameters as dataclasses with good defaults. 

Parameters can be set by instantiating the classes at run-time.

Example:
    
"""

import os
from dataclasses import dataclass
from typing import Any, Union, List, String, Dict, Tuple, Set, Mapping, Collection, Sequence
from .data import GeographicOverlay,SharedStreetsOverlay,ValueLookup
from .logger import WranglerLogger


def get_base_dir(lasso_base_dir=os.getcwd()):
    d = lasso_base_dir
    for i in range(3):
        if "metcouncil_data" in os.listdir(d):
            WranglerLogger.info("Lasso base directory set as: {}".format(d))
            return d
        d = os.path.dirname(d)

    msg = "Cannot find Lasso base directory from {}, please input using keyword in parameters: `lasso_base_dir =` ".format(
        lasso_base_dir
    )
    WranglerLogger.error(msg)
    raise (ValueError(msg))

@dataclass
class NetworkModelParameters:
    """

    Attributes:
        time period_names: Maps time period abbreviations to names.
        time_periods_to_time: Maps time period abbreviations used in
            Cube to time of days used on transit/gtfs and highway network standard.

    """
    time_period_abbr_to_names: Mapping[Any,Any] = {
        "AM":"AM Peak",
        "MD":"Midday",
        "PM":"PM Peak",
        "NT":"Evening/Night",
    }
    time_period_abbr_to_time: Mapping[Any,Sequence] = {
        "AM": ("6:00", "9:00"),
        "MD": ("9:00", "16:00"),
        "PM": ("16:00", "19:00"),
        "NT": ("19:00", "6:00"),
    }

    def __post_init__(self):
        if not self.time_period_abbr_to_names.keys == self.time_period_abbr_to_time.keys:
            raise(ValueError("time period abbreviations don't match up"))
        


@dataclass
class RoadwayNetworkModelParameters:
    """

    Attributes: 
        network_model_parameters: NetworkModelParameters
        allowed_use_categories: set of categories that can be used to refine the roadway network 
            network by use type. Defaults to sov", "hov2", "hov3", "trk","default".
        category_grouping: Maps demand category abbreviations used as variable suffices to a list of
            network categories they are allowed to use.
        properties_to_split: Dictionary mapping variables in standard
            roadway network to categories and time periods that need to be
            split out in final model network to get variables like LANES_AM.
        max_taz: integer denoting maximum taz number which is used for identifying which
            network links act as centroid connectors.
        centroid_connector_properties: maps properties in the standard network format with 
            values to assert upon centroid connectors. Defaults to {"lanes": 1}
        output_fields: maps fields to output in the roadway network with transformations to 
            make upon output (e.g., int, str, float )
    """
    network_model_parameters: NetworkModelParameters
    allowed_use_categories: Set[String] = {
        "sov",
        "hov2",
        "hov3",
        "trk",
        "default",
    }
    category_grouping: Mapping[String:,Set] = {
        "sov": ["sov", "default"],
        "hov2": ["hov2", "default", "sov"],
        "hov3": ["hov3", "hov2", "default", "sov"],
        "truck": ["truck", "sov", "default"],
    }
    #categories 
    properties_to_split: Mapping[String,Mapping] = None
    output_espg: int = None
    max_taz: int = None
    centroid_connector_properties: Mapping[str,Any] = {"lanes": 1}
    output_fields: Collection[str] = []

    #def __update__(self):
    # add an override so nested variables are updated not overwritten

    def __post_init__(self):
        assert(output_fields.values() in [int,str,float])

        if not self.properties_to_split:
            self.properties_to_split = {
                "lanes": {"v": "lanes", "time_periods": self.network_model_parameters.time_period_to_time},
                "ML_lanes": {"v": "ML_lanes", "time_periods": self.network_model_parameters.time_period_to_time},
                "price": {
                    "v": "price",
                    "time_periods": self.network_model_parameters.time_period_to_time,
                    "categories": self.categories,
                },
                "access": {"v": "access", "time_periods": self.network_model_parameters.time_period_to_time},
            }


@dataclass
class TransitNetworkModelParameters:
    """

    Attributes:
        network_model_parameters: NetworkModelParameters
        transit_time_periods: 
        transit_to_network_time_periods: Maps cube time period numbers used in
                transit line files to the time period abbreviations in time_period_to_time
                dictionary.

    """
    network_model_parameters: NetworkModelParameters
    transit_to_network_time_periods: Mapping[Any, Any] = {
        "1": "AM", 
        "2": "MD"
    }
    #cube_time_periods 
    transit_time_periods: Set = None
    time_varying_properties: Set[String] = {"HEADWAY","FREQ"}
    time_period_properties_list: Set[String] = None
    

    def __post_init__(self):
        if not set(self.transit_to_network_time_periods.values()).issubset(
                    set(self.network_model_parameters.time_period_abbr_to_time.keys())
                    )
            raise(ValueError("specified transit_to_network_time_periods: {} does not align with specified network time periods {}".format(self.transit_to_network_time_periods,self.network_model_parameters.time_period_abbr_to_time.keys()) ))

        if not self.transit_time_periods:
            self.transit_time_periods = list(self.transit_to_network_time_periods.keys())

        if not self.time_period_properties_list:
            self.time_period_properties_list = [
                p + "[" + str(t) + "]"
                for p in self.time_varying_properties
                for t in self.transit_time_periods
            ]

@dataclass
class DemandModelParameters:
     """A data class representing parameters which define how a model system
    is set up such as time of day, mode, and use categorizations.

    This class is instantiated with "good default" values if they aren't specified 
    at runtime.

    Attributes:
        network_model_parameters
        demand_time_periods: list of time period abbreviations used by the demand model
        network_to_demand_time_periods: mapping of network model time period abbreviations 
            to demand model time period abbreviations

    """
    network_model_parameters: NetworkModelParameters
    demand_time_periods: List[Any:Any] = None
    network_to_demand_time_periods: Dict = {"AM": "pk", "MD": "op"}
    #cube_time_periods_name =
    #

    def __post_init__(self):
        if not set(self.network_to_demand_time_periods.keys()).issubset(
                    set(self.network_model_parameters.time_period_abbr_to_time.keys())
                    )
            raise(ValueError("specified network_to_demand_time_periods: {} does not align with specified network time periods {}".format(self.network_to_demand_time_periods,self.network_model_parameters.time_period_abbr_to_time.keys()) ))
        if not self.demand_time_periods:
            self.demand_time_periods = set(self.network_to_demand_time_periods.values())

@dataclass
class FileParameters:
    """
    FileSystem Parameters

    Attr:
        lasso_base_directory: Directory lasso is within. 
        data_directory: Directory to look for data files like lookups, overlays, etc. 
        scratch_directory: Directory location for temporary files.
        settings_location: Directory location for run-level settings. Defaults to examples/settings
        output_directory: Directory location for output and log files.
        output_link_csv_filename: Output csv filename containing links. 
            Defaults to be relative to output directory unless output_relative is False. Default is links.csv.
        output_node_csv_filename: Output csv filename containing nodes. 
            Defaults to be relative to output directory unless output_relative is False. Default is nodes.csv.
        output_link_txt_filename: Output fixed width text filename containing links. 
            Defaults to be relative t output directory unless output_relative is False. Default is links.txt.
        output_node_txt_filename: Output fixed width text filename containing nodes. 
            Defaults to be relative to output directory unless output_relative is False. Default is nodes.txt.
        output_link_fixed_width_header_filename: Output fixed width text filename containing headers 
            for links. 
           Defaults to be relative to output directory unless output_relative is False.
            Defaults to "links_header_width.txt".
        output_node_fixed_width_header_filename: Output fixed width text filename containing headers 
            for nodes. 
            Defaults to be relative to output directory unless output_relative is False. 
            "nodes_header_width.txt"
        output_cube_network_script_filename: Output cube script for processing fixed width files. 
            Defaults to be relative to output directory unless output_relative is False. 
            Defaults "make_complete_network_from_fixed_width_file.s"
        output_link_shp_filename: Output shapefile filename containing links. Default is links.shp. 
            Defaults to be relative to output directory unless output_relative is False. 
        output_node_shp_filename: Output shapefile filename containing nodes. Default is nodes.shp. 
            Defaults to be relative to output directory unless output_relative is False. 
        output_relative: bool = True: If set to true, will assume output files are relative filenames to 
            the output directory on instantiation. 
    
    """
    lasso_base_directory: str = get_base_dir()
    data_directory: str = None
    scratch_directory: str = None 
    settings_location: str = os.path.join(get_base_dir(), "examples", "settings")
    output_directory: str
    output_link_csv_filename: str = "links.csv"
    output_node_csv_filename: str = "nodes.csv"
    output_link_txt_filename: str = "links.txt"
    output_node_txt_filename: str = "nodes.txt"
    output_link_fixed_width_header_filename: str = "links_header_width.txt"
    output_node_fixed_width_header_filename: str = "nodes_header_width.txt"
    output_cube_network_script_filename: str = "make_complete_network_from_fixed_width_file.s"
    output_link_shp_filename: str = "links.shp"
    output_node_shp_filename: str = "nodes.shp"
    output_relative: bool = True
    
    __post_init__(self):
        if not self.scratch_directory:
            self.scratch_directory = os.path.join(self.base_directory, "tests", "scratch")

        if not self.output_directory:
            self.output_directory = self.scratch_directory

        if not os.path.exists(self.scratch_directory)
            os.mkdir(self.scratch_directory)

        if not os.path.exists(self.output_directory)
            os.mkdir(self.output_directory)
        
        if output_relative: 
            self.output_link_csv_filename = os.path.join(self.output_directory,self.output_link_csv_filename)
            self.output_node_csv_filename = os.path.join(self.output_directory,self.output_node_csv_filename)
            self.output_link_txt_filename = os.path.join(self.output_directory,self.output_link_txt_filename)
            self.output_node_txt_filename = os.path.join(self.output_directory,self.output_node_txt_filename)
            self.output_link_fixed_width_header_filename = 
                os.path.join(self.output_directory,self.output_link_fixed_width_header_filename)
            self.output_node_fixed_width_header_filename = 
                os.path.join(self.output_directory,self.output_node_fixed_width_header_filename)
            self.output_cube_network_script_filename = 
                os.path.join(self.output_directory,self. output_cube_network_script_filename)
            self.output_link_shp_filename = os.path.join(self.output_directory,self.output_link_shp_filename)
            self.output_node_shp_filename = os.path.join(self.output_directory,self.output_node_shp_filename)

        

@dataclass
class Parameters:
    file_ps: FileParameters = FileParameters()
    transit_network_ps: TransitNetworkModelParameters = TransitNetworkModelParameters()
    roadway_network_ps: RoadwayNetworkModelParameters = RoadwayNetworkModelParameters()
    demand_model_ps: DemandModelParameters = DemandModelParameters()
    shared_streets_overlays: Collection[SharedStreetsOverlay] = []
    geographic_overlays: Collection[GeographicOverlay] = []
