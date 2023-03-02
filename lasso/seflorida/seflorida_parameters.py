import os
from ..logger import WranglerLogger
from ..parameters import Parameters

from pyproj import CRS


def get_base_dir(lasso_base_dir=os.getcwd()):
    d = lasso_base_dir

    for _ in range(3):
        if "lasso" in os.listdir(d):
            WranglerLogger.info("Lasso base directory set as: {}".format(d))
            return d
        d = os.path.dirname(d)

    msg = f"""Cannot find Lasso base directory from {lasso_base_dir}, 
        please input using keyword in parameters: `lasso_base_dir =` """

    WranglerLogger.error(msg)
    raise (ValueError(msg))


class SEFloridaParameters(Parameters):
    """ Inherit from Lasso `Parameters` class, with parameter settings specific
    to the SERPM model.
    """

    def __init__(self, **kwargs):
        """
        Time period and category  splitting info
        """
        super().__init__()

        if "time_periods_to_time" in kwargs:
            self.time_periods_to_time = kwargs.get("time_periods_to_time")
        else:
            self.time_period_to_time = {
                "AM": ("6:00", "9:00"),
                "MD": ("9:00", "15:00"),
                "PM": ("15:00", "19:00"),
                "EV": ("19:00", "22:00"),
                "EA": ("22:00", "6:00"),
            }

        self.cube_time_periods = {
            "1": "EA",
            "2": "AM",
            "3": "MD",
            "4": "PM",
            "5": "EV",
        }

        if "categories" in kwargs:
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
        }

        """
        Details for calculating the county based on the centroid of the link.
        The NAME varible should be the name of a field in shapefile.
        """
        if "lasso_base_dir" in kwargs:
            self.base_dir = get_base_dir(lasso_base_dir=kwargs.get("lasso_base_dir"))
        else:
            self.base_dir = get_base_dir()

        if "data_file_location" in kwargs:
            self.data_file_location = kwargs.get("data_file_location")
        else:
            self.data_file_location = os.path.join(self.base_dir, "lasso", "seflorida", "data")

        if "lasso_base_dir" in kwargs:
            self.base_dir = get_base_dir(lasso_base_dir=kwargs.get("lasso_base_dir"))
        else:
            self.base_dir = get_base_dir()
        """
        if "data_file_location" in kwargs:
            self.data_file_location = kwargs.get("data_file_location")
        else:
            self.data_file_location = os.path.join(self.base_dir, "metcouncil_data")
        """

        # --------
        if "settings_location" in kwargs:
            self.settings_location = kwargs.get("settings_location")
        else:
            self.settings_location = os.path.join(self.base_dir, "examples", "settings")

        if "scratch_location" in kwargs:
            self.scratch_location = kwargs.get("scratch_location")
        else:
            self.scratch_location = os.path.join(self.base_dir, "tests", "scratch")

        self.county_code_dict = {"Broward": 1, "Miami-Dade": 2, "Palm Beach": 3}

        # SERPM
        self.osm_facility_type_dict = os.path.join(
            self.data_file_location,
            "lookups",
            "osm_highway_facility_type_crosswalk.csv",
        )

        self.osm_lanes_attributes = os.path.join(
            self.data_file_location, "lookups", "osm_lanes_attributes.csv"
        )

        self.legacy_serpm8_attributes = os.path.join(
            self.data_file_location, "lookups", "legacy_serpm8_attributes.csv"
        )

        self.navteq_attributes = os.path.join(
            self.data_file_location, "lookups", "navteq_attributes.csv"
        )

        self.fdot_attributes = os.path.join(
            self.data_file_location, "lookups", "fdot_attributes.csv"
        )

        self.county_attributes = os.path.join(
            self.data_file_location, "lookups", "county_attributes.csv"
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

        self.net_to_dbf_crosswalk = os.path.join(self.settings_location, "net_to_dbf.csv")

        self.log_to_net_crosswalk = os.path.join(
            self.data_file_location, "lookups", "log_to_net.csv"
        )

        self.mode_crosswalk_file = os.path.join(
            self.data_file_location, "lookups", "gtfs_to_serpm_mode_crosswalk.csv"
        )

        self.output_variables = [
            "model_link_id",
            "link_id",
            "A",
            "B",
            "shstGeometryId",
            "distance",
            "roadway",
            # "name",
            "ftype",
            # "maxspeed",
            "segid",
            "twoway",
            "location",
            "tmode",
            "tspeed",
            # "shape_id",
            # "distance",
            # "roadway",
            # "name",
            # "roadway_class",
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
            # "ML_lanes_EA",
            # "ML_lanes_AM",
            # "ML_lanes_MD",
            # "ML_lanes_PM",
            # "ML_lanes_EV",
            # "segment_id",
            "managed",
            "bus_only",
            "rail_only",
            # "assignable",
            # "cntype",
            # "useclass_AM",
            # "useclass_MD",
            # "useclass_PM",
            # "useclass_EV",
            # "useclass_EA",
            # "transit",
            # "tollbooth",
            # "tollseg",
            # "ft",
            # "tap_drive",
            # "tollbooth",
            # "tollseg",
            "farezone",
            # "tap_id",
            # "bike_facility",
            "CARTOLL",
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
        self.output_proj = CRS.from_epsg(4326)

        self.fare_matrix_output_variables = [
            "faresystem",
            "origin_farezone",
            "destination_farezone",
            "price",
        ]

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
            # "county",
            # "lanes",
            "lanes_AM",
            "lanes_MD",
            "lanes_PM",
            "lanes_NT",
            "roadway_class",
            "assign_group",
            "area_type",
            "trn_priority",
            "AADT",
            "count_AM",
            "count_MD",
            "count_PM",
            "count_NT",
            "count_daily",
            "centroidconnect",
            "bike_facility",
            "drive_access",
            "walk_access",
            "bike_access",
            "truck_access",
            "ML_lanes_AM",
            "ML_lanes_MD",
            "ML_lanes_PM",
            "ML_lanes_EV",
            "ML_lanes_EA",
            ###
            # MC
            "drive_node",
            "walk_node",
            "bike_node",
            "transit_node",
            # "ML_lanes",
            "ML_lanes_AM",
            "ML_lanes_MD",
            "ML_lanes_PM",
            "ML_lanes_NT",
            "segment_id",
            "managed",
            "bus_only",
            "rail_only",
            "ftype",
            "assignable",
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
            "tollbooth",
            "farezone",
            "tap_id",
        ]

        self.float_col = [
            "distance",
            "price",
            "X",
            "Y" "mrcc_id",
        ]

        self.float_col = ["distance", "ttime_assert", "price", "X", "Y"]

        self.string_col = [
            "osm_node_id",
            "name",
            "roadway",
            "shstGeometryId",
            "access_AM",
            "access_MD",
            "access_PM",
            "access_NT",
            "ROUTE_SYS",
        ]

        self.__dict__.update(kwargs)
