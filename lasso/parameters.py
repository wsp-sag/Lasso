import os
from .logger import WranglerLogger

def get_base_dir(init_d = os.getcwd()):
    d = init_d
    for i in range(3):
        if 'metcouncil_data' in os.listdir(d):
            WranglerLogger.info("Lasso base directory set as: {}".format(d))
            return d
        d = os.path.dirname(d)

    msg = "Cannot find Lasso base directory from {}, please input using keyword in parameters: `lasso_dir =` ".format(init_d)
    WranglerLogger.error(msg)
    raise(ValueError(msg))


class Parameters:
    """A class representing all the parameters defining the networks
    including time of day, categories, etc.

    Parameters can be set at runtime by initializing a parameters instance
    with a keyword argument setting the attribute.  Parameters that are
    not explicitly set will use default parameters listed in this class.
    .. highlight:: python
    ##TODO potentially split this between several classes.

    Attr:
        time_period_to_time (dict): Maps time period abbreviations used in
            Cube to time of days used on gtfs and highway network standard
            Default:
            ::
                {
                    "AM": ("6:00", "9:00"),
                    "MD": ("9:00", "16:00"),
                    "PM": ("16:00", "19:00"),
                    "NT": ("19:00", "6:00"),
                }
        cube_time_periods (dict):  Maps cube time period numbers used in
            transit line files to the time period abbreviations in time_period_to_time
            dictionary.
            Default:
            ::
                {"1": "AM", "2": "MD"}
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
                    "trn_priority": {
                        "v": "trn_priority",
                        "time_periods": DEFAULT_TIME_PERIOD_TO_TIME,
                    },
                    "ttime_assert": {
                        "v": "ttime_assert",
                        "time_periods": DEFAULT_TIME_PERIOD_TO_TIME,
                    },
                    "lanes": {"v": "lanes", "time_periods": DEFAULT_TIME_PERIOD_TO_TIME},
                    "price": {
                        "v": "price",
                        "time_periods": DEFAULT_TIME_PERIOD_TO_TIME,
                        "categories": DEFAULT_CATEGORIES,
                    },
                    "access": {"v": "access", "time_periods": DEFAULT_TIME_PERIOD_TO_TIME},
                }
        county_shape (str): File location of shapefile defining counties.
            Default:
            ::
                r"metcouncil_data/county/cb_2017_us_county_5m.shp"

        county_variable_shp (str): Property defining the county n ame in
            the county_shape file.
            Default:
            ::
                NAME
        mpo_counties (list): list of county names within MPO boundary.
            Default:
            ::
                [
                    "ANOKA",
                    "DAKOTA",
                    "HENNEPIN",
                    "RAMSEY",
                    "SCOTT",
                    "WASHINGTON",
                    "CARVER",
                ]

        taz_shape (str):
            Default:
            ::
                r"metcouncil_data/TAZ/TAZOfficialWCurrentForecasts.shp"
        taz_data (str):
            Default:
            ::
                ??
        highest_taz_number (int): highest TAZ number in order to define
            centroid connectors.
            Default:
            ::
                3100
        output_variables (list): list of variables to output in final model
            network.
            Default:
            ::
                [
                    "model_link_id",
                    "A",
                    "B",
                    "shstGeometryId",
                    "distance",
                    "roadway",
                    "name",
                    "roadway_class",
                    "bike_access",
                    "transit_access",
                    "walk_access",
                    "drive_access",
                    "truck_access",
                    "trn_priority_AM",
                    "trn_priority_MD",
                    "trn_priority_PM",
                    "trn_priority_NT",
                    "ttime_assert_AM",
                    "ttime_assert_MD",
                    "ttime_assert_PM",
                    "ttime_assert_NT",
                    "lanes_AM",
                    "lanes_MD",
                    "lanes_PM",
                    "lanes_NT",
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
                    "price_sov_NT",
                    "price_hov2_NT",
                    "price_hov3_NT",
                    "price_truck_NT",
                    "roadway_class_idx",
                    "assign_group",
                    "access_AM",
                    "access_MD",
                    "access_PM",
                    "access_NT",
                    "mpo",
                    "area_type",
                    "county",
                    "centroidconnect",
                    "AADT",
                    "count_year",
                    "count_AM",
                    "count_MD",
                    "count_PM",
                    "count_NT",
                    "count_daily",
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
                ]
        area_type_shape (str):   Location of shapefile defining area type.
            Default:
            ::
                r"metcouncil_data/area_type/ThriveMSP2040CommunityDesignation.shp"
        area_type_variable_shp (str): property in area_type_shape with area
            type in it.
            Default:
            ::
                "COMDES2040"
        area_type_code_dict (dict): Mapping of the area_type_variable_shp to
            the area type code used in the MetCouncil cube network.
            Default:
            ::
                {
                    23: 4,  # urban center
                    24: 3,
                    25: 2,
                    35: 2,
                    36: 1,
                    41: 1,
                    51: 1,
                    52: 1,
                    53: 1,
                    60: 1,
                }
        mrcc_roadway_class_shape (str): Shapefile of MRCC links with a property
            associated with roadway class. Default:
            ::
                r"metcouncil_data/mrcc/trans_mrcc_centerlines.shp"
        mrcc_roadway_class_variable_shp (str): The property in mrcc_roadway_class_shp
            associated with roadway class. Default:
            ::
                "ROUTE_SYS"
        widot_roadway_class_shape (str): Shapefile of Wisconsin links with a property
            associated with roadway class. Default:
            ::
                r"metcouncil_data/Wisconsin_Lanes_Counts_Median/WISLR.shp"
        widot_roadway_class_variable_shp (str):  The property in widot_roadway_class_shape
            associated with roadway class.Default:
            ::
                "RDWY_CTGY_"
        mndot_count_shape (str):  Shapefile of MnDOT links with a property
            associated with counts. Default:
            ::
                r"metcouncil_data/count_mn/AADT_2017_Count_Locations.shp"
        mndot_count_variable_shp (str): The property in mndot_count_shape
            associated with counts. Default:
            ::
                "AADT_mn"
        widot_count_shape (str): Shapefile of Wisconsin DOT links with a property
            associated with counts. Default:Default:
            ::
                r"metcouncil_data/Wisconsin_Lanes_Counts_Median/TRADAS_(counts).shp"
        widot_count_variable_shp (str): The property in widot_count_shape
            associated with counts. Default:
            ::
                "AADT_wi"
        mrcc_shst_data (str): MnDOT MRCC to Shared Streets crosswalk. Default:
            ::
                r"metcouncil_data/mrcc/mrcc.out.matched.csv"
        widot_shst_data (str): WisconsinDOT to Shared Streets crosswalk.Default:
            ::
                r"metcouncil_data/Wisconsin_Lanes_Counts_Median/widot.out.matched.geojson"
        mndot_count_shst_data (str): MetCouncil count data with ShST Default:
            ::
                r"metcouncil_data/count_mn/mn_count_ShSt_API_match.csv"
        widot_count_shst_data (str): WisconsinDOT count data with ShST Default:
            ::
                r"metcouncil_data/Wisconsin_Lanes_Counts_Median/wi_count_ShSt_API_match.csv",
        mrcc_assgngrp_dict (str): Mapping beetween MRCC ROUTE_SYS variable
            and assignment group. Default:
            ::
                "lookups/mrcc_route_sys_asgngrp_crosswalk.csv"
        widot_assgngrp_dict (dict): Mapping beetween Wisconsin DOT RDWY_CTGY_
            variable and assignment group. Default:
            ::
                "lookups/widot_ctgy_asgngrp_crosswalk.csv"
        osm_assgngrp_dict (dict): Mapping between OSM Roadway variable
            and assignment group. Default:
            ::
                "lookups/osm_highway_asgngrp_crosswalk.csv"
        roadway_class_dict (str):  Mapping between assignment group and
            roadway class. Default:
            ::
                "lookups/asgngrp_rc_num_crosswalk.csv"
        output_epsg (int): EPSG type of geographic projection for output
            shapefiles. Default:
            ::
                26915
        net_to_dbf (str): Lookup of network variables to DBF compliant
            lengths. Default:
            ::
                "examples/settings/net_to_dbf.csv"
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
        output_link_header_width_csv (str): Header for csv roadway links. Default:
            ::
                r"tests/scratch/links_header_width.csv"
        output_node_header_width_csv (str): Header for csv for roadway Nodes. Default:
            ::
                r"tests/scratch/nodes_header_width.csv"
        output_cube_network_script (str): Cube script for importing
            fixed-format roadway network. Default:
            ::
                r"tests/scratch/make_complete_network_from_fixed_width_file.s



    """

    """
    Time period and category  splitting info
    """
    DEFAULT_TIME_PERIOD_TO_TIME = {
        "AM": ("6:00", "9:00"),  ##TODO FILL IN with real numbers
        "MD": ("9:00", "16:00"),
        "PM": ("16:00", "19:00"),
        "NT": ("19:00", "6:00"),
    }

    route_type_bus_mode_dict = {"Urb Loc": 5, "Sub Loc": 6, "Express": 7}

    route_type_mode_dict = {0: 8, 2: 9}

    DEFAULT_CUBE_TIME_PERIODS = {"1": "AM", "2": "MD"}

    DEFAULT_CATEGORIES = {
        # suffix, source (in order of search)
        "sov": ["sov", "default"],
        "hov2": ["hov2", "default", "sov"],
        "hov3": ["hov3", "hov2", "default", "sov"],
        "truck": ["trk", "sov", "default"],
    }

    # prefix, source variable, categories
    DEFAULT_PROPERTIES_TO_SPLIT = {
        "trn_priority": {
            "v": "trn_priority",
            "time_periods": DEFAULT_TIME_PERIOD_TO_TIME,
        },
        "ttime_assert": {
            "v": "ttime_assert",
            "time_periods": DEFAULT_TIME_PERIOD_TO_TIME,
        },
        "lanes": {"v": "lanes", "time_periods": DEFAULT_TIME_PERIOD_TO_TIME},
        "price": {
            "v": "price",
            "time_periods": DEFAULT_TIME_PERIOD_TO_TIME,
            "categories": DEFAULT_CATEGORIES,
        },
        "access": {"v": "access", "time_periods": DEFAULT_TIME_PERIOD_TO_TIME},
    }

    """
    Details for calculating the county based on the centroid of the link.
    The COUNTY_VARIABLE should be the name of a field in shapefile.
    """

    BASE_DIR = get_base_dir()
    DATA_FILE_LOCATION = os.path.join(BASE_DIR,  "metcouncil_data")

    SETTINGS_LOCATION = os.path.join(BASE_DIR , "examples", "settings")

    SCRATCH_LOCATION = os.path.join(BASE_DIR , "tests", "scratch")

    WranglerLogger.info("Data File Location set as : {}".format(DATA_FILE_LOCATION))

    DEFAULT_COUNTY_SHAPE = os.path.join(
        DATA_FILE_LOCATION, "county", "cb_2017_us_county_5m.shp"
    )
    DEFAULT_COUNTY_VARIABLE_SHP = "NAME"

    DEFAULT_MPO_COUNTIES = [
        "ANOKA",
        "DAKOTA",
        "HENNEPIN",
        "RAMSEY",
        "SCOTT",
        "WASHINGTON",
        "CARVER",
    ]

    DEFAULT_TAZ_SHAPE = os.path.join(
        DATA_FILE_LOCATION, "TAZ", "TAZOfficialWCurrentForecasts.shp"
    )
    DEFAULT_TAZ_DATA = None
    DEFAULT_HIGHEST_TAZ_NUMBER = 3100

    DEFAULT_AREA_TYPE_SHAPE = os.path.join(
        DATA_FILE_LOCATION, "area_type", "ThriveMSP2040CommunityDesignation.shp"
    )
    DEFAULT_AREA_TYPE_VARIABLE_SHP = "COMDES2040"
    # area type map from raw data to model category

    # source https://metrocouncil.org/Planning/Publications-And-Resources/Thrive-MSP-2040-Plan-(1)/7_ThriveMSP2040_LandUsePoliciesbyCD.aspx
    # urban center
    # urban
    # suburban
    # suburban edge
    # emerging suburban edge
    # rural center
    # diversified rural
    # rural residential
    # agricultural
    DEFAULT_AREA_TYPE_CODE_DICT = {
        23: 4,  # urban center
        24: 3,
        25: 2,
        35: 2,
        36: 1,
        41: 1,
        51: 1,
        52: 1,
        53: 1,
        60: 1,
    }

    DEFAULT_OSM_ASSGNGRP_DICT = os.path.join(
        DATA_FILE_LOCATION, "lookups", "osm_highway_asgngrp_crosswalk.csv"
    )

    DEFAULT_MRCC_ROADWAY_CLASS_SHAPE = os.path.join(
        DATA_FILE_LOCATION, "mrcc", "trans_mrcc_centerlines.shp"
    )

    DEFAULT_MRCC_ROADWAY_CLASS_VARIABLE_SHP = "ROUTE_SYS"

    DEFAULT_MRCC_ASSGNGRP_DICT = os.path.join(
        DATA_FILE_LOCATION, "lookups", "mrcc_route_sys_asgngrp_crosswalk.csv"
    )

    DEFAULT_MRCC_SHST_DATA = os.path.join(
        DATA_FILE_LOCATION, "mrcc", "mrcc.out.matched.csv"
    )

    DEFAULT_WIDOT_ROADWAY_CLASS_SHAPE = os.path.join(
        DATA_FILE_LOCATION, "Wisconsin_Lanes_Counts_Median", "WISLR.shp"
    )

    DEFAULT_WIDOT_ROADWAY_CLASS_VARIABLE_SHP = "RDWY_CTGY_"

    DEFAULT_WIDOT_ASSGNGRP_DICT = os.path.join(
        DATA_FILE_LOCATION, "lookups", "widot_ctgy_asgngrp_crosswalk.csv"
    )

    DEFAULT_WIDOT_SHST_DATA = os.path.join(
        DATA_FILE_LOCATION, "Wisconsin_Lanes_Counts_Median", "widot.out.matched.geojson"
    )

    DEFAULT_ROADWAY_CLASS_DICT = os.path.join(
        DATA_FILE_LOCATION, "lookups", "asgngrp_rc_num_crosswalk.csv"
    )

    DEFAULT_MNDOT_COUNT_SHAPE = os.path.join(
        DATA_FILE_LOCATION, "count_mn", "AADT_2017_Count_Locations.shp"
    )

    DEFAULT_MNDOT_COUNT_SHST_DATA = os.path.join(
        DATA_FILE_LOCATION, "count_mn", "mn_count_ShSt_API_match.csv"
    )

    DEFAULT_MNDOT_COUNT_VARIABLE_SHP = "AADT_mn"

    DEFAULT_WIDOT_COUNT_SHAPE = os.path.join(
        DATA_FILE_LOCATION, "Wisconsin_Lanes_Counts_Median", "TRADAS_(counts).shp"
    )

    DEFAULT_WIDOT_COUNT_SHST_DATA = os.path.join(
        DATA_FILE_LOCATION,
        "Wisconsin_Lanes_Counts_Median",
        "wi_count_ShSt_API_match.csv",
    )

    DEFAULT_WIDOT_COUNT_VARIABLE_SHP = "AADT_wi"

    DEFAULT_NET_TO_DBF_CROSSWALK = os.path.join(SETTINGS_LOCATION, "net_to_dbf.csv")

    DEFAULT_OUTPUT_VARIABLES = [
        "model_link_id",
        "A",
        "B",
        "shstGeometryId",
        "distance",
        "roadway",
        "name",
        "roadway_class",
        "bike_access",
        "transit_access",
        "walk_access",
        "drive_access",
        "truck_access",
        "trn_priority_AM",
        "trn_priority_MD",
        "trn_priority_PM",
        "trn_priority_NT",
        "ttime_assert_AM",
        "ttime_assert_MD",
        "ttime_assert_PM",
        "ttime_assert_NT",
        "lanes_AM",
        "lanes_MD",
        "lanes_PM",
        "lanes_NT",
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
        "price_sov_NT",
        "price_hov2_NT",
        "price_hov3_NT",
        "price_truck_NT",
        "roadway_class_idx",
        "assign_group",
        "access_AM",
        "access_MD",
        "access_PM",
        "access_NT",
        "mpo",
        "area_type",
        "county",
        "centroidconnect",
        #'mrcc_id',
        "AADT",
        "count_year",
        "count_AM",
        "count_MD",
        "count_PM",
        "count_NT",
        "count_daily",
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
    ]

    DEFAULT_OUTPUT_LINK_SHP = os.path.join(SCRATCH_LOCATION, "links.shp")

    DEFAULT_OUTPUT_NODE_SHP = os.path.join(SCRATCH_LOCATION, "nodes.shp")

    DEFAULT_OUTPUT_LINK_CSV = os.path.join(SCRATCH_LOCATION, "links.csv")

    DEFAULT_OUTPUT_NODE_CSV = os.path.join(SCRATCH_LOCATION, "nodes.csv")

    DEFAULT_OUTPUT_LINK_TXT = os.path.join(SCRATCH_LOCATION, "links.txt")

    DEFAULT_OUTPUT_NODE_TXT = os.path.join(SCRATCH_LOCATION, "nodes.txt")

    DEFAULT_OUTPUT_LINK_HEADER_WIDTH_CSV = os.path.join(
        SCRATCH_LOCATION, "links_header_width.csv"
    )

    DEFAULT_OUTPUT_NODE_HEADER_WIDTH_CSV = os.path.join(
        SCRATCH_LOCATION, "nodes_header_width.csv"
    )

    DEFAULT_OUTPUT_CUBE_NETWORK_SCRIPT = os.path.join(
        SCRATCH_LOCATION, "make_complete_network_from_fixed_width_file.s"
    )

    DEFAULT_OUTPUT_DIR = os.path.join(SCRATCH_LOCATION)

    DEFAULT_OUTPUT_EPSG = 26915

    def __init__(self, **kwargs):

        self.__dict__.update(kwargs)

        if "time_period_to_time" not in kwargs:
            self.time_period_to_time = Parameters.DEFAULT_TIME_PERIOD_TO_TIME
        if "cube_time_periods" not in kwargs:
            self.cube_time_periods = Parameters.DEFAULT_CUBE_TIME_PERIODS
        if "categories" not in kwargs:
            self.categories = Parameters.DEFAULT_CATEGORIES
        if "properties_to_split" not in kwargs:
            self.properties_to_split = Parameters.DEFAULT_PROPERTIES_TO_SPLIT
        if "county_shape" not in kwargs:
            self.county_shape = Parameters.DEFAULT_COUNTY_SHAPE
        if "county_variable_shp" not in kwargs:
            self.county_variable_shp = Parameters.DEFAULT_COUNTY_VARIABLE_SHP
        if "mpo_counties" not in kwargs:
            self.mpo_counties = Parameters.DEFAULT_MPO_COUNTIES
        if "taz_shape" not in kwargs:
            self.taz_shape = Parameters.DEFAULT_TAZ_SHAPE
        if "taz_data" not in kwargs:
            self.taz_data = Parameters.DEFAULT_TAZ_DATA
        if "highest_taz_number" not in kwargs:
            self.highest_taz_number = Parameters.DEFAULT_HIGHEST_TAZ_NUMBER
        if "output_variables" not in kwargs:
            self.output_variables = Parameters.DEFAULT_OUTPUT_VARIABLES
        if "area_type_shape" not in kwargs:
            self.area_type_shape = Parameters.DEFAULT_AREA_TYPE_SHAPE
        if "mrcc_roadway_class_shape" not in kwargs:
            self.mrcc_roadway_class_shape = Parameters.DEFAULT_MRCC_ROADWAY_CLASS_SHAPE
        if "widot_roadway_class_shape" not in kwargs:
            self.widot_roadway_class_shape = (
                Parameters.DEFAULT_WIDOT_ROADWAY_CLASS_SHAPE
            )
        if "mndot_count_shape" not in kwargs:
            self.mndot_count_shape = Parameters.DEFAULT_MNDOT_COUNT_SHAPE
        if "widot_count_shape" not in kwargs:
            self.widot_count_shape = Parameters.DEFAULT_WIDOT_COUNT_SHAPE
        if "area_type_code_dict" not in kwargs:
            self.area_type_code_dict = Parameters.DEFAULT_AREA_TYPE_CODE_DICT
        if "mrcc_shst_data" not in kwargs:
            self.mrcc_shst_data = Parameters.DEFAULT_MRCC_SHST_DATA
        if "widot_shst_data" not in kwargs:
            self.widot_shst_data = Parameters.DEFAULT_WIDOT_SHST_DATA
        if "mndot_count_shst_data" not in kwargs:
            self.mndot_count_shst_data = Parameters.DEFAULT_MNDOT_COUNT_SHST_DATA
        if "widot_count_shst_data" not in kwargs:
            self.widot_count_shst_data = Parameters.DEFAULT_WIDOT_COUNT_SHST_DATA
        if "mrcc_assgngrp_dict" not in kwargs:
            self.mrcc_assgngrp_dict = Parameters.DEFAULT_MRCC_ASSGNGRP_DICT
        if "widot_assgngrp_dict" not in kwargs:
            self.widot_assgngrp_dict = Parameters.DEFAULT_WIDOT_ASSGNGRP_DICT
        if "osm_assgngrp_dict" not in kwargs:
            self.osm_assgngrp_dict = Parameters.DEFAULT_OSM_ASSGNGRP_DICT
        if "roadway_class_dict" not in kwargs:
            self.roadway_class_dict = Parameters.DEFAULT_ROADWAY_CLASS_DICT
        if "area_type_variable_shp" not in kwargs:
            self.area_type_variable_shp = Parameters.DEFAULT_AREA_TYPE_VARIABLE_SHP
        if "mrcc_roadway_class_variable_shp" not in kwargs:
            self.mrcc_roadway_class_variable_shp = (
                Parameters.DEFAULT_MRCC_ROADWAY_CLASS_VARIABLE_SHP
            )
        if "widot_roadway_class_variable_shp" not in kwargs:
            self.widot_roadway_class_variable_shp = (
                Parameters.DEFAULT_WIDOT_ROADWAY_CLASS_VARIABLE_SHP
            )
        if "mndot_count_variable_shp" not in kwargs:
            self.mndot_count_variable_shp = Parameters.DEFAULT_MNDOT_COUNT_VARIABLE_SHP
        if "widot_count_variable_shp" not in kwargs:
            self.widot_count_variable_shp = Parameters.DEFAULT_WIDOT_COUNT_VARIABLE_SHP
        if "net_to_dbf" not in kwargs:
            self.net_to_dbf = Parameters.DEFAULT_NET_TO_DBF_CROSSWALK
        if "output_link_shp" not in kwargs:
            self.output_link_shp = Parameters.DEFAULT_OUTPUT_LINK_SHP
        if "output_node_shp" not in kwargs:
            self.output_node_shp = Parameters.DEFAULT_OUTPUT_NODE_SHP
        if "output_link_csv" not in kwargs:
            self.output_link_csv = Parameters.DEFAULT_OUTPUT_LINK_CSV
        if "output_node_csv" not in kwargs:
            self.output_node_csv = Parameters.DEFAULT_OUTPUT_NODE_CSV
        if "output_epsg" not in kwargs:
            self.output_epsg = Parameters.DEFAULT_OUTPUT_EPSG
        if "output_link_txt" not in kwargs:
            self.output_link_txt = Parameters.DEFAULT_OUTPUT_LINK_TXT
        if "output_node_txt" not in kwargs:
            self.output_node_txt = Parameters.DEFAULT_OUTPUT_NODE_TXT
        if "output_link_header_width_csv" not in kwargs:
            self.output_link_header_width_csv = (
                Parameters.DEFAULT_OUTPUT_LINK_HEADER_WIDTH_CSV
            )
        if "output_node_header_width_csv" not in kwargs:
            self.output_node_header_width_csv = (
                Parameters.DEFAULT_OUTPUT_NODE_HEADER_WIDTH_CSV
            )
        if "output_cube_network_script" not in kwargs:
            self.output_cube_network_script = (
                Parameters.DEFAULT_OUTPUT_CUBE_NETWORK_SCRIPT
            )
        if "output_dir" not in kwargs:
            self.output_dir = Parameters.DEFAULT_OUTPUT_DIR
        """
        Create all the possible headway variable combinations based on the cube time periods setting
        """
        self.time_period_properties_list = [
            p + "[" + str(t) + "]"
            for p in ["HEADWAY", "FREQ"]
            for t in self.cube_time_periods.keys()
        ]
