import os
from .logger import WranglerLogger

from pyproj import CRS

def get_base_dir(lasso_base_dir = os.getcwd()):
    d = lasso_base_dir
    for i in range(3):
        if 'mtc_data' in os.listdir(d):
            WranglerLogger.info("Lasso base directory set as: {}".format(d))
            return d
        d = os.path.dirname(d)

    msg = "Cannot find Lasso base directory from {}, please input using keyword in parameters: `lasso_base_dir =` ".format(lasso_base_dir)
    WranglerLogger.error(msg)
    raise(ValueError(msg))


class Parameters:
    """A class representing all the parameters defining the networks
    including time of day, categories, etc.

    Parameters can be set at runtime by initializing a parameters instance
    with a keyword argument setting the attribute.  Parameters that are
    not explicitly set will use default parameters listed in this class.
    .. highlight:: python

    Attr:
        time_period_to_time (dict): Maps time period abbreviations used in
            Cube to time of days used on gtfs and highway network standard
            Default:
            ::
                {
                    "EA": ("3:00", "6:00"),
                    "AM": ("6:00, "10:00"),
                    "MD": ("10:00", "15:00"),
                    "PM": ("15:00", "19:00"),
                    "EV": ("19:00", "3:00"),
                }
        cube_time_periods (dict):  Maps cube time period numbers used in
            transit line files to the time period abbreviations in time_period_to_time
            dictionary.
            Default:
            ::
                {"1": "EA", "2": "AM", "3": "MD", "4": "PM", "5": "EV"}
        categories (dict): Maps demand category abbreviations to a list of
            network categories they are allowed to use.
            Default:
            ::
                {
                    # suffix, source (in order of search)
                    "sov": ["sov", "default"],
                    "hov2": ["hov2", "default", "sov"],
                    "hov3": ["hov3", "hov2", "default", "sov"],
                    "truck": ["trk", "sov", "default"],
                }
        properties_to_split (dict): Dictionary mapping variables in standard
            roadway network to categories and time periods that need to be
            split out in final model network to get variables like LANES_AM.
            Default:
            ::
                {
                    "lanes": {
                        "v": "lanes",
                        "time_periods": self.time_periods_to_time
                    },
                    "ML_lanes": {
                        "v": "ML_lanes",
                        "time_periods": self.time_periods_to_time
                    },
                    "use": {
                        "v": "use",
                        "time_periods": self.time_periods_to_time
                    },
                }
        output_variables (list): list of variables to output in final model
            network.
            Default:
            ::
                [
                    "model_link_id",
                    "link_id",
                    "A",
                    "B",
                    "shstGeometryId",
                    "distance",
                    "roadway",
                    "name",
                    "roadway_class",
                    "bike_access",
                    "walk_access",
                    "drive_access",
                    "truck_access",
                    "trn_priority_EA",
                    "trn_priority_AM",
                    "trn_priority_MD",
                    "trn_priority_PM",
                    "trn_priority_EV",
                    "ttime_assert_EA",
                    "ttime_assert_AM",
                    "ttime_assert_MD",
                    "ttime_assert_PM",
                    "ttime_assert_EV",
                    "lanes_EA",
                    "lanes_AM",
                    "lanes_MD",
                    "lanes_PM",
                    "lanes_EV",
                    "price_sov_EA",
                    "price_hov2_EA",
                    "price_hov3_EA",
                    "price_truck_EA",
                    "price_sov_AM",
                    "price_hov2_AM",
                    "price_hov3_AM",
                    "price_truck_AM",
                    "price_sov_MD",
                    "price_hov2_MD",
                    "price_hov3_MD",
                    "price_truck_MD",
                    "price_sov_PM",
                    "price_hov2_PM",
                    "price_hov3_PM",
                    "price_truck_PM",
                    "price_sov_EV",
                    "price_hov2_EV",
                    "price_hov3_EV",
                    "price_truck_EV",
                    "roadway_class_idx",
                    "facility_type",
                    "county",
                    "centroidconnect",
                    "model_node_id",
                    "N",
                    "osm_node_id",
                    "bike_node",
                    "transit_node",
                    "walk_node",
                    "drive_node",
                    "geometry",
                    "X",
                    "Y",
                    "ML_lanes_EA",
                    "ML_lanes_AM",
                    "ML_lanes_MD",
                    "ML_lanes_PM",
                    "ML_lanes_EV",
                    "segment_id",
                    "managed",
                    "bus_only",
                    "rail_only"
                ]
        osm_facility_type_dict (dict): Mapping between OSM Roadway variable
            and facility type. Default:
            ::
                "lookups/osm_highway_facility_type_crosswalk.csv"
        legacy_tm2_attributes (str): CSV file of link attributes by
            shStReferenceId from Legacy TM2 network. Default:
            ::
                "lookups/legacy_tm2_attributes.csv"
        osm_lanes_attributes (str): CSV file of number of lanes by shStReferenceId
            from OSM. Default:
            ::
                "lookups/osm_lanes_attributes.csv"
        tam_tm2_attributes (str): CSV file of link attributes by
            shStReferenceId from TAM TM2 network. Default:
            ::
                "lookups/tam_tm2_attributes.csv"
        tom_tom_attributes (str): CSV file of link attributes by
            shStReferenceId from TomTom network. Default:
            ::
                "lookups/tomtom_attributes.csv"
        sfcta_attributes (str): CSV file of link attributes by
            shStReferenceId from SFCTA network. Default:
            ::
                "lookups/sfcta_attributes.csv"
        output_epsg (int): EPSG type of geographic projection for output
            shapefiles. Default:
            ::
                102646
        output_link_shp (str): Output shapefile for roadway links. Default:
            ::
                r"tests/scratch/links.shp"
        output_node_shp (str):  Output shapefile for roadway nodes. Default:
            ::
                r"tests/scratch/nodes.shp"
        output_link_csv (str):  Output csv for roadway links. Default:
            ::
                r"tests/scratch/links.csv"
        output_node_csv (str): Output csv for roadway nodes. Default:
            ::
                r"tests/scratch/nodes.csv"
        output_link_txt (str): Output fixed format txt for roadway links. Default:
            ::
                r"tests/scratch/links.txt"
        output_node_txt (str): Output fixed format txt for roadway nodes. Default:
            ::
                r"tests/scratch/nodes.txt"
        output_link_header_width_txt (str): Header for txt roadway links. Default:
            ::
                r"tests/scratch/links_header_width.txt"
        output_node_header_width_txt (str): Header for txt for roadway Nodes. Default:
            ::
                r"tests/scratch/nodes_header_width.txt"
        output_cube_network_script (str): Cube script for importing
            fixed-format roadway network. Default:
            ::
                r"tests/scratch/make_complete_network_from_fixed_width_file.s



    """

    def __init__(self, **kwargs):
        """
        Time period and category  splitting info
        """

        if 'time_periods_to_time' in kwargs:
            self.time_periods_to_time = kwargs.get("time_periods_to_time")
        else:
            self.time_period_to_time = {
                "EA": ("3:00", "6:00"),
                "AM": ("6:00", "10:00"),
                "MD": ("10:00", "15:00"),
                "PM": ("15:00", "19:00"),
                "EV": ("19:00", "3:00"),
            }

        self.cube_time_periods = {
            "1": "EA",
            "2": "AM",
            "3": "MD",
            "4": "PM",
            "5": "EV",
        }

        if 'categories' in kwargs:
            self.categories = kwargs.get("categories")
        else:
            self.categories = {
                # suffix, source (in order of search)
                "sov": ["sov", "default"],
                "hov2": ["hov2", "default", "sov"],
                "hov3": ["hov3", "hov2", "default", "sov"],
                "truck": ["trk", "sov", "default"],
            }

        # prefix, source variable, categories
        self.properties_to_split = {
            "lanes": {
                "v": "lanes",
                "time_periods": self.time_period_to_time,
            },
            "ML_lanes": {
                "v": "ML_lanes",
                "time_periods": self.time_period_to_time,
            },
            "useclass": {
                "v": "useclass",
                "time_periods": self.time_period_to_time,
            },
        }

        """
        Details for calculating the county based on the centroid of the link.
        The NAME varible should be the name of a field in shapefile.
        """
        if 'lasso_base_dir' in kwargs:
            self.base_dir = get_base_dir(lasso_base_dir = kwargs.get("lasso_base_dir"))
        else:
            self.base_dir = get_base_dir()

        if 'data_file_location' in kwargs:
            self.data_file_location =  kwargs.get("data_file_location")
        else:
            self.data_file_location = os.path.join(self.base_dir,  "mtc_data")

        if 'settings_location' in kwargs:
            self.settings_location = kwargs.get("settings_location")
        else:
            self.settings_location = os.path.join(self.base_dir , "examples", "settings")

        if  'scratch_location' in kwargs:
            self.scratch_location = kwargs.get("scratch_location")
        else:
            self.scratch_location = os.path.join(self.base_dir , "tests", "scratch")

        ### COUNTIES

        self.county_shape = os.path.join(
            self.data_file_location, "county", "county.shp"
        )
        self.county_variable_shp = "NAME"

        self.county_code_dict = {
            'San Francisco':1,
            'San Mateo':2,
            'Santa Clara':3,
            'Alameda':4,
            'Contra Costa':5,
            'Solano':6,
            'Napa':7,
            'Sonoma':8,
            'Marin':9,
            }

        self.mpo_counties = [
            1,
            3,
            4,
            5,
            6,
            7,
            8,
            9
        ]

        self.osm_facility_type_dict = os.path.join(
            self.data_file_location, "lookups", "osm_highway_facility_type_crosswalk.csv"
        )

        self.osm_lanes_attributes = os.path.join(
            self.data_file_location, "lookups", "osm_lanes_attributes.csv"
        )

        self.legacy_tm2_attributes = os.path.join(
            self.data_file_location, "lookups", "legacy_tm2_attributes.csv"
        )

        self.tam_tm2_attributes = os.path.join(
            self.data_file_location, "lookups", "tam_tm2_attributes.csv"
        )

        self.sfcta_attributes = os.path.join(
            self.data_file_location, "lookups", "sfcta_attributes.csv"
        )

        self.tomtom_attributes = os.path.join(
            self.data_file_location, "lookups", "tomtom_attributes.csv"
        )

        self.pems_attributes = os.path.join(
            self.data_file_location, "lookups", "pems_attributes.csv"
        )

        self.centroid_file = os.path.join(
            self.data_file_location, "centroid", "centroid_node.pickle"
        )

        self.centroid_connector_link_file = os.path.join(
            self.data_file_location, "centroid", "cc_link.pickle"
        )

        self.centroid_connector_shape_file = os.path.join(
            self.data_file_location, "centroid", "cc_shape.pickle"
        )

        self.net_to_dbf_crosswalk = os.path.join(
            self.settings_location, "net_to_dbf.csv"
        )

        self.log_to_net_crosswalk = os.path.join(self.settings_location, "log_to_net.csv")

        self.output_variables = [
            "model_link_id",
            "link_id",
            "A",
            "B",
            "shstGeometryId",
            "distance",
            "roadway",
            "name",
            "bike_access",
            "walk_access",
            "drive_access",
            "truck_access",
            "lanes_EA",
            "lanes_AM",
            "lanes_MD",
            "lanes_PM",
            "lanes_EV",
            "county",
            "model_node_id",
            "N",
            "osm_node_id",
            "geometry",
            "X",
            "Y",
            "ML_lanes_EA",
            "ML_lanes_AM",
            "ML_lanes_MD",
            "ML_lanes_PM",
            "ML_lanes_EV",
            "segment_id",
            "managed",
            "bus_only",
            "rail_only",
            "assignable",
            "cntype",
            "useclass_AM",
            "useclass_MD",
            "useclass_PM",
            "useclass_EV",
            "useclass_EA",
            "transit",
            "tollbooth",
            "tollseg",
            "ft",
            "tap_drive",
            "tollbooth",
            "tollseg",
        ]

        self.output_link_shp = os.path.join(self.scratch_location, "links.shp")
        self.output_node_shp = os.path.join(self.scratch_location, "nodes.shp")
        self.output_link_csv = os.path.join(self.scratch_location, "links.csv")
        self.output_node_csv = os.path.join(self.scratch_location, "nodes.csv")
        self.output_link_txt = os.path.join(self.scratch_location, "links.txt")
        self.output_node_txt = os.path.join(self.scratch_location, "nodes.txt")
        self.output_link_header_width_txt = os.path.join(
            self.scratch_location, "links_header_width.txt"
        )
        self.output_node_header_width_txt = os.path.join(
            self.scratch_location, "nodes_header_width.txt"
        )
        self.output_cube_network_script = os.path.join(
            self.scratch_location, "make_complete_network_from_fixed_width_file.s"
        )
        self.output_dir = os.path.join(self.scratch_location)
        self.output_proj = CRS("ESRI:102646")

        """
        Create all the possible headway variable combinations based on the cube time periods setting
        """
        self.time_period_properties_list = [
            p + "[" + str(t) + "]"
            for p in ["HEADWAY", "FREQ"]
            for t in self.cube_time_periods.keys()
        ]

        self.int_col = [
            "model_link_id",
            "model_node_id",
            "A",
            "B",
            #"county",
            "drive_access",
            "walk_access",
            "bike_access",
            "truck_access",
            "ML_lanes_AM",
            "ML_lanes_MD",
            "ML_lanes_PM",
            "ML_lanes_EV",
            "ML_lanes_EA",
            "segment_id",
            "managed",
            "bus_only",
            "rail_only",
            "ft",
            "lanes_AM",
            "lanes_MD",
            "lanes_PM",
            "lanes_EA",
            "lanes_EV",
            "useclass_AM",
            "useclass_EA",
            "useclass_MD",
            "useclass_PM",
            "useclass_EV",
            "tollseg",
            "tollbooth"
        ]

        self.float_col = [
            "distance",
            "price",
            "X",
            "Y"
        ]

        self.__dict__.update(kwargs)
