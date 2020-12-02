"""

"""

import os
############################
# FILE
############################

MC_DATA_DIRECTORY = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), "data",
)

MC_FILE_PS = {"data_directory": MC_DATA_DIRECTORY}

############################
# NETWORK
############################

MC_TIME_PERIOD_NAMES = {
        "AM": "AM Peak",
        "MD": "Midday",
        "PM": "PM Peak",
    }

MC_TIME_PERIODS_TO_TIME = { 
        "AM": ("6:00", "9:00"),
        "MD": ("9:00", "16:00"),
        "PM": ("16:00", "19:00"),
        "NT": ("19:00", "6:00")
    }

MC_NETORK_MODEL_PS = {
    "time_period_names": MC_TIME_PERIOD_NAMES,
    "time_periods_to_time": MC_TIME_PERIODS_TO_TIME,
}

############################
# ROADWAY NETWORK
############################

MC_HIGHEST_TAZ = 3100

MC_CATEGORY_GROUPING = {
    "sov": ["sov", "default"],
    "hov2": ["hov2", "default", "sov"],
    "hov3": ["hov3", "hov2", "default", "sov"],
    "truck": ["trk", "sov", "default"],
}

MC_PROPERTIES_TO_SPLIT = {
        "trn_priority": {
            "v": "trn_priority",
            "time_periods": {},
        },
        "ttime_assert": {
            "v": "ttime_assert",
            "time_periods": self.time_period_to_time,
        },
        "lanes": {
            "v": "lanes", 
            "time_periods": self.time_period_to_time},
        "ML_lanes": {
            "v": "ML_lanes", 
            "time_periods": self.time_period_to_time},
        "price": {
            "v": "price",
            "time_periods": self.time_period_to_time,
            "categories": self.categories,
        },
        "access": {
            "v": "access", 
            "time_periods": self.time_period_to_time},
    }

MC_ROADWAY_OUTPUT_ESPG = 26915

MC_ROADWAY_OUTPUT_FIELDS = {
    "model_link_id":int,
    "link_id":int,
    "A":int,
    "B":int,
    "shstGeometryId":str,
    "shape_id":str,
    "distance":float,
    "roadway":str,
    "name":str,
    "roadway_class":int,
    "assign_group": int,
    "bike_access": int,
    "walk_access": int,
    "drive_access": int,
    "truck_access": int,
    "trn_priority_AM": int,
    "trn_priority_MD": int,
    "trn_priority_PM": int,
    "trn_priority_NT": int,
    "ttime_assert_AM":float,
    "ttime_assert_MD":float,
    "ttime_assert_PM":float,
    "ttime_assert_NT":float,
    "lanes_AM":int,
    "lanes_MD":int,
    "lanes_PM":int,
    "lanes_NT":int,
    "price_sov_AM":float,
    "price_hov2_AM":float,
    "price_hov3_AM":float,
    "price_truck_AM":float,
    "price_sov_MD":float,
    "price_hov2_MD":float,
    "price_hov3_MD":float,
    "price_truck_MD":float,
    "price_sov_PM":float,
    "price_hov2_PM":float,
    "price_hov3_PM":float,
    "price_truck_PM":float,
    "price_sov_NT":float,
    "price_hov2_NT":float,
    "price_hov3_NT":float,
    "price_truck_NT":float,
    "roadway_class_idx":int,
    "access_AM":str,
    "access_MD":str,
    "access_PM":str,
    "access_NT":str,
    "mpo":int,
    "area_type":int,
    "county":int,
    "centroidconnect": int,
    'mrcc_id': int,
    "AADT": int,
    "count_year": int,
    "count_AM": int,
    "count_MD": int,
    "count_PM": int,
    "count_NT": int,
    "count_daily": int,
    "model_node_id",
    "N": int,
    "osm_node_id":str,
    "bike_node": int,
    "transit_node": int,
    "walk_node": int,
    "drive_node": int,
    "geometry":str,,
    "X":float,
    "Y":float,
    "ML_lanes_AM": int,
    "ML_lanes_MD": int,
    "ML_lanes_PM": int,
    "ML_lanes_NT": int,
    "segment_id": int,
    "managed": int,
    "bus_only": int,
    "rail_only": int,
    "bike_facility": int,
    "ROUTE_SYS":str,  # mrcc functional class
}

MC_ROADWAY_NETORK_MODEL_PS = {
    "highest_taz": MC_HIGHEST_TAZ,
    "category_grouping": MC_CATEGORY_GROUPING,
    "properties_to_split": MC_PROPERTIES_TO_SPLIT,
    "roadway_output_espg": MC_ROADWAY_OUTPUT_ESPG,
    "roadway_output_fields": MC_ROADWAY_OUTPUT_FIELDS
}

############################
# TRANSIT NETWORK
############################

MC_TRANSIT_TO_NETWORK_TIME_PERIODS = {
        "1": "AM", 
        "2": "MD"
    }

MC_TRANSIT_NETORK_MODEL_PS = {
    "transit_to_network_time_eriods": MC_TRANSIT_TO_NETWORK_TIME_PERIODS,
}

############################
# DEMAND MODEL
############################

MC_NETWORK_TO_DEMAND_TIME_PERIODS = {"AM": "pk", "MD": "op"}

MC_DEMAND_MODEL_PS = {
    "network_to_demand_time_periods": MC_NETWORK_TO_DEMAND_TIME_PERIODS,
}

############################
# OVERLAY DATA
############################

### COUNTIES

MC_COUNTY_SHAPEFILE = os.path.join(MC_DATA_DIRECTORY, "county", "cb_2017_us_county_5m.shp")
MC_COUNTY_VARIABLE_MAP= {"NAME":"NAME"}

MC_COUNTY_CODE_DICT = {
    "Anoka": 1,
    "Carver": 2,
    "Dakota": 3,
    "Hennepin": 4,
    "Ramsey": 5,
    "Scott": 6,
    "Washington": 7,
    "external": 10,
    "Chisago": 11,
    "Goodhue": 12,
    "Isanti": 13,
    "Le Sueur": 14,
    "McLeod": 15,
    "Pierce": 16,
    "Polk": 17,
    "Rice": 18,
    "Sherburne": 19,
    "Sibley": 20,
    "St. Croix": 21,
    "Wright": 22,
}

MC_MPO_COUNTIES = [
    "ANOKA",
    "DAKOTA",
    "HENNEPIN",
    "RAMSEY",
    "SCOTT",
    "WASHINGTON",
    "CARVER",
]

### TAZS

MC_TAZ_SHAPEFILE = os.path.join(
        MC_DATA_DIRECTORY, "TAZ", "TAZOfficialWCurrentForecasts.shp"
    )

MC_TAZ_VARIABLE_MAP = {"TAZ":"TAZ"}

### AREATYPE

MC_AREATYPE_SHAPEFILE = os.path.join(
            MC_DATA_DIRECTORY,
            "area_type",
            "ThriveMSP2040CommunityDesignation.shp",
        )

MC_AREA_TYPE_VARIABLE_MAP = {"COMDES2040":"area_type"}        
        
        
#area_type_code_dict (dict): Mapping of the area_type_variable_shp to
#The area type code used in the MetCouncil cube network.
# source https://metrocouncil.org/Planning/Publications-And-Resources/Thrive-MSP-2040-Plan-(1)/7_ThriveMSP2040_LandUsePoliciesbyCD.aspx
MC_AREA_TYPE_CODE_MAP = {
                    23: 4, # urban center
                    24: 3, # urban center
                    25: 2, # urban
                    35: 2, # suburban
                    36: 1, # suburban edge
                    41: 1, # emerging suburban edge
                    51: 1, # rural center
                    52: 1, # diversified rural
                    53: 1, # rural residential
                    60: 1, # agricultural
                }
MC_DOWNTOWN_AREATYPE_SHAPE = os.path.join(
            MC_DATA_DIRECTORY,
            "area_type",
            "downtownzones_TAZ.shp",
        )

MC_AREA_TYPE_VARIABLE_MAP = {"?":"area_type"}

MC_DOWNTOWN_AREA_TYPE_CODE_MAP = {"?":5} 
#downtown_area_type_shape (str): Location of shapefile defining downtown area type.

### COUNTS

#    MNDOT
MC_MNDOT_COUNTS_SHAPEFILE = os.path.join(
    MC_DATA_DIRECTORY, "count_mn", "AADT_2017_Count_Locations.shp",
    )

MC_MNDOT_COUNTS_SHST_MATCH = os.path.join(
    MC_DATA_DIRECTORY, "count_mn", "mn_count_ShSt_API_match.csv"
    )

MC_MNDOT_COUNTS_SHST_VARIABLE_MAP = {"AADT_mn":"AADT"}

#    WIDOT
MC_WIDOT_COUNTS_SHAPEFILE = os.path.join(
    MC_DATA_DIRECTORY, "Wisconsin_Lanes_Counts_Median", "TRADAS_(counts).shp",
    )

MC_WIDOT_COUNTS_SHST_MATCH = os.path.join(
    MC_DATA_DIRECTORY, "Wisconsin_Lanes_Counts_Median", "wi_count_ShSt_API_match.csv"
    )

MC_MNDOT_COUNTS_SHST_VARIABLE_MAP = {"AADT_wi":"AADT"}


### ROADWAY_CLASS




#   MRCC/MNDOT

MC_MRCC_SHAPEFILE = os.path.join(
    MC_DATA_DIRECTORY,, "mrcc", "trans_mrcc_centerlines.shp"
)

MC_MRCC_GEOGRAPHIC_OVERLAY = GeographicOverlay:
    shapefile_filename = MC_MRCC_SHAPEFILE,
    added_id = "LINK_ID",
)


        self.mrcc_roadway_class_variable_shp = 

        self.mrcc_assgngrp_dict = os.path.join(
            MC_DATA_DIRECTORY,, "lookups", "mrcc_route_sys_asgngrp_crosswalk.csv"
        )

        self.mrcc_shst_data = os.path.join(
            MC_DATA_DIRECTORY, "mrcc", "mrcc.out.matched.csv"
        )

#   WIDOT
MC_WIDOT_SHAPEFILE = os.path.join(
    MC_DATA_DIRECTORY, "Wisconsin_Lanes_Counts_Median", "WISLR.shp"
)

        self.widot_assgngrp_dict = os.path.join(
            MC_DATA_DIRECTORY,, "lookups", "widot_ctgy_asgngrp_crosswalk.csv"
        )

        self.widot_shst_data = os.path.join(
            MC_DATA_DIRECTORY,
            "Wisconsin_Lanes_Counts_Median",
            "widot.out.matched.geojson",
        )

############################
# LOOKUPS
############################


#   OSM
MC_OSM_ROADWAY_ASSIGNGRP_MAPPING = ValueMapping(
    input_csv_filename = os.path.join(
            MC_DATA_DIRECTORY, "lookups", "osm_highway_asgngrp_crosswalk.csv"
        ),
    input_csv_has_header = True,
    input_csv_key_field = 'roadway',
    target_df_key_field = 'roadway',
    field_mapping = { # csv_field, output/target_field
        "assign_group": "assignment_group_osm",
        "roadway_class": "roadway_class_osm",
    },
    overwrite = True,
)

MC_MRCC_ROADWAY_ASSIGNGRP_MAPPING = ValueMapping(
    input_csv_filename = os.path.join(
            MC_DATA_DIRECTORY, "lookups", "mrcc_route_sys_asgngrp_crosswalk.csv"
        ),
    input_csv_has_header = True,
    input_csv_key_field = 'ROUTE_SYS',
    target_df_key_field = 'ROUTE_SYS',
    field_mapping = { # csv_field, output/target_field
        "assign_group": "assignment_group_mrcc",
        "roadway_class": "roadway_class_mrcc",
    },
    overwrite = True,
)

MC_WIDOT_ROADWAY_ASSIGNGRP_MAPPING = ValueMapping(
    input_csv_filename = os.path.join(
            MC_DATA_DIRECTORY, "lookups", "widot_ctgy_asgngrp_crosswalk.csv"
        ),
    input_csv_has_header = True,
    input_csv_key_field = 'RDWY_CTGY_',
    target_df_key_field = 'RDWY_CTGY_',
    field_mapping = { # csv_field, output/target_field
        "assign_group": "assignment_group_widot",
        "roadway_class": "roadway_class_widot",
    },
    overwrite = True,
)

### LANES

MC_LANES_LOOKUP_CSV = os.path.join(
    MC_DATA_DIRECTORY, "lookups", "lanes.csv",
    )

### OUTPUT ASSISTANCE

MC_NET_DBF_CROSSWALK_CSV = os.path.join(
    MC_SETTINGS_DIRECTORY, "net_to_dbf.csv"
    )

MC_NET_DBF_CROSSWALK = FieldRename(
    input_csv_filename = MC_NET_DBF_CROSSWALK_CSV,
    input_csv_has_header = True,
    input_csv_fields = ("net","dbf"),
)

MC_LOG_TO_NET_CROSSWALK_CSV = os.path.join(
    MC_SETTINGS_DIRECTORY, "log_to_net.csv"


MC_LOG_TO_NET_CROSSWALK = FieldRename(
    input_csv_filename = MC_LOG_TO_NET_CROSSWALK_CSV,
    input_csv_has_header = True,
    input_csv_fields = ("log","net"),
)

### TRANSIT

MC_ROUTE_TYPE_BUS_MODE_LOOKUP = {
    "Urb Loc": 5, 
    "Sub Loc": 6, 
    "Express": 7,
}

MC_ROUTE_TYPE_MODE_LOOKUP = {
    0: 8, 
    2: 9,
}