"""

"""

import os
import copy

from ..data import GeographicOverlay, FieldMapping, ValueLookup


_MC_DEFAULT_PARAMS = {}

_MC_DEFAULT_PARAMS["name"] = "MetCouncil Defaults"

# Add nested dicts...

_MC_DEFAULT_PARAMS["roadway_value_lookups"]={}
_MC_DEFAULT_PARAMS["roadway_overlays"] = {}
_MC_DEFAULT_PARAMS["lookups"]={}
############################
# FILE
############################

_MC_DEFAULT_PARAMS["data_directory"] = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    "data",
)

_MC_DEFAULT_PARAMS["shape_foreign_key"] = "shape_id"

############################
# NETWORK
############################

_MC_DEFAULT_PARAMS["time period_abbr_to_names"] = {
    "AM": "AM Peak",
    "MD": "Midday",
    "PM": "PM Peak",
}

_MC_DEFAULT_PARAMS["time_period_abbr_to_time"] = {
    "AM": ("6:00", "9:00"),
    "MD": ("9:00", "16:00"),
    "PM": ("16:00", "19:00"),
    "NT": ("19:00", "6:00"),
}

############################
# ROADWAY NETWORK
############################

_MC_DEFAULT_PARAMS["highest_taz"] = 3100

_MC_DEFAULT_PARAMS["category_grouping"] = {
    "sov": ["sov", "default"],
    "hov2": ["hov2", "default", "sov"],
    "hov3": ["hov3", "hov2", "default", "sov"],
    "truck": ["trk", "sov", "default"],
}


_MC_DEFAULT_PARAMS["properties_to_split_by_network_time_periods"] = [
    "trn_priority",
    "ttime_assert",
    "lanes",
    "ML_lanes",
    "price",
    "access",
]

_MC_DEFAULT_PARAMS["properties_to_split_by_category_groupings"] = [
    "price",
]

_MC_DEFAULT_PARAMS["roadway_output_espg"] = 26915

_MC_DEFAULT_PARAMS["field_type"] = {
    "model_link_id" : int, 
    "link_id" : int, 
    "A" : int, 
    "B" : int, 
    "shstGeometryId": str,
    "shape_id": str,
    "distance": float,
    "roadway": str,
    "name": str,
    "roadway_class" : int, 
    "assign_group" : int, 
    "bike_access" : int, 
    "walk_access" : int, 
    "drive_access" : int, 
    "truck_access" : int, 
    "trn_priority_AM" : int, 
    "trn_priority_MD" : int, 
    "trn_priority_PM" : int, 
    "trn_priority_NT": int, 
    "ttime_assert_AM": float,
    "ttime_assert_MD": float,
    "ttime_assert_PM": float,
    "ttime_assert_NT": float,
    "lanes_AM" : int, 
    "lanes_MD": int,
    "lanes_PM": int,
    "lanes_NT": int,
    "price_sov_AM": float,
    "price_hov2_AM": float,
    "price_hov3_AM": float,
    "price_truck_AM": float,
    "price_sov_MD": float,
    "price_hov2_MD": float,
    "price_hov3_MD": float,
    "price_truck_MD": float,
    "price_sov_PM": float,
    "price_hov2_PM": float,
    "price_hov3_PM": float,
    "price_truck_PM": float,
    "price_sov_NT": float,
    "price_hov2_NT": float,
    "price_hov3_NT": float,
    "price_truck_NT": float,
    "roadway_class_idx": int,
    "access_AM": str,
    "access_MD": str,
    "access_PM": str,
    "access_NT": str,
    "mpo": int,
    "area_type": int,
    "county": int,
    "centroidconnect": int,
    "mrcc_id": int,
    "AADT": int,
    "count_year": int,
    "count_AM": int,
    "count_MD": int,
    "count_PM": int,
    "count_NT": int,
    "count_daily": int,
    "model_node_id": str,
    "N": int,
    "osm_node_id": str,
    "bike_node": int,
    "transit_node": int,
    "walk_node": int,
    "drive_node": int,
    "geometry": str,
    "X": float,
    "Y": float,
    "ML_lanes_AM": int,
    "ML_lanes_MD": int,
    "ML_lanes_PM": int,
    "ML_lanes_NT": int,
    "segment_id": int,
    "managed": int,
    "bus_only": int,
    "rail_only": int,
    "bike_facility": int,
    "ROUTE_SYS": str,  # mrcc functional class
}

_MC_DEFAULT_PARAMS["output_fields"] = [
    "model_link_id",
    "link_id",
    "A",
    "B",
    "shstGeometryId",
    "shape_id",
    "distance",
    "roadway",
    "name",
    "roadway_class",
    "assign_group",
    "bike_access", 
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
    "access_AM",
    "access_MD",
    "access_PM",
    "access_NT",
    "mpo", 
    "area_type" ,
    "county", 
    "centroidconnect", 
    "mrcc_id", 
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
    "ML_lanes_AM", 
    "ML_lanes_MD", 
    "ML_lanes_PM", 
    "ML_lanes_NT", 
    "segment_id", 
    "managed", 
    "bus_only", 
    "rail_only", 
    "bike_facility",
    "ROUTE_SYS",
]

############################
# TRANSIT NETWORK
############################

_MC_DEFAULT_PARAMS["transit_to_network_time_periods"] = {"1": "AM", "2": "MD"}

############################
# DEMAND MODEL
############################

_MC_DEFAULT_PARAMS["network_to_demand_time_periods"] = {"AM": "pk", "MD": "op"}

############################
# OVERLAY DATA
############################

### COUNTIES


MC_COUNTY_SHAPEFILE = os.path.join(
    _MC_DEFAULT_PARAMS["data_directory"], "county", "cb_2017_us_county_5m.shp"
)

_MC_DEFAULT_PARAMS["roadway_overlays"]["counties"] = PolygonOverlay(
    input_filename = MC_COUNTY_SHAPEFILE,
    field_mapping = {"NAME": "NAME"},
)

_MC_DEFAULT_PARAMS["roadway_value_lookups"]["mc_county_code_dict"] = {
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

_MC_DEFAULT_PARAMS["roadway_value_lookups"]["mc_mpo_counties_dict"]= {
    "Anoka":1,
    "Dakota":1,
    "Hennipin":1,
    "Ramsey":1,
    "Scott":1,
    "Washington":1,
    "Carver":1,
}

### TAZS



MC_TAZ_SHAPEFILE = os.path.join(
    _MC_DEFAULT_PARAMS["data_directory"], "TAZ", "TAZOfficialWCurrentForecasts.shp"
)

_MC_DEFAULT_PARAMS["roadway_overlays"]["tazs"] = PolygonOverlay(
    input_filename = MC_TAZ_SHAPEFILE,
    field_mapping = {"TAZ": "TAZ"},
)


### AREATYPE

MC_AREATYPE_SHAPEFILE = os.path.join(
    _MC_DEFAULT_PARAMS["data_directory"],
    "area_type",
    "ThriveMSP2040CommunityDesignation.shp",
)

_MC_DEFAULT_PARAMS["roadway_overlays"]["area_type"] = PolygonOverlay(
    input_filename = MC_AREATYPE_SHAPEFILE,
    field_mapping = {"COMDES2040": "area_type"},
)

MC_DOWNTOWN_AREATYPE_SHAPEFILE = os.path.join(
    _MC_DEFAULT_PARAMS["data_directory"],
    "area_type",
    "downtownzones_TAZ.shp",
)

_MC_DEFAULT_PARAMS["roadway_overlays"]["downtown_area_type"] = PolygonOverlay(
    input_filename = MC_DOWNTOWN_AREATYPE_SHAPEFILE,
)

# area_type_code_dict (dict): Mapping of the area_type_variable_shp to
# The area type code used in the MetCouncil cube network.
# source https://metrocouncil.org/Planning/Publications-And-Resources/Thrive-MSP-2040-Plan-(1)/7_ThriveMSP2040_LandUsePoliciesbyCD.aspx
MC_AREA_TYPE_CODE_MAP = {
    "downtown": 5 # downtown
    23: 4,  # urban center
    24: 3,  # urban center
    25: 2,  # urban
    35: 2,  # suburban
    36: 1,  # suburban edge
    41: 1,  # emerging suburban edge
    51: 1,  # rural center
    52: 1,  # diversified rural
    53: 1,  # rural residential
    60: 1,  # agricultural
}


### COUNTS

#    MNDOT
MC_MNDOT_COUNTS_SHAPEFILE = os.path.join(
    _MC_DEFAULT_PARAMS["data_directory"],
    "count_mn",
    "AADT_2017_Count_Locations.shp",
)

MC_MNDOT_COUNTS_SHST_MATCH = os.path.join(
    _MC_DEFAULT_PARAMS["data_directory"], "count_mn", "mn_count_ShSt_API_match.csv"
)

MC_MNDOT_COUNTS_SHST_VARIABLE_MAP = {"AADT_mn": "AADT"}

#    WIDOT
MC_WIDOT_COUNTS_SHAPEFILE = os.path.join(
    _MC_DEFAULT_PARAMS["data_directory"],
    "Wisconsin_Lanes_Counts_Median",
    "TRADAS_(counts).shp",
)

MC_WIDOT_COUNTS_SHST_MATCH = os.path.join(
    _MC_DEFAULT_PARAMS["data_directory"],
    "Wisconsin_Lanes_Counts_Median",
    "wi_count_ShSt_API_match.csv",
)

MC_MNDOT_COUNTS_SHST_VARIABLE_MAP = {"AADT_wi": "AADT"}


### ROADWAY_CLASS

#   MRCC/MNDOT

MC_MRCC_SHAPEFILE = os.path.join(
    _MC_DEFAULT_PARAMS["data_directory"], "mrcc", "trans_mrcc_centerlines.shp"
)

MC_MRCC_GEOGRAPHIC_OVERLAY = GeographicOverlay(
    shapefile_filename=MC_MRCC_SHAPEFILE,
    added_id="LINK_ID",
)

# Expected columns for MRCC_SHST_MATCH_CSV:
#     shstReferenceId,shstGeometryId,pp_link_id (which is the LINK_ID),score
MRCC_SHST_MATCH_CSV = os.path.join(
    _MC_DEFAULT_PARAMS["data_directory"], "mrcc", "mrcc.out.matched.csv"
)
_MC_DEFAULT_PARAMS["roadway_value_lookups"]["mrcc_shst_2_pp_link_id"] = ValueLookup(
    input_csv_filename = MRCC_SHST_MATCH_CSV,
    input_csv_has_header = True,
    input_key_field = "shstGeometryId",
    target_df_key_field = "shstGeometryId",
    field_mapping = {"pp_link_id": "mrcc_link_id", "score": "mrcc_score"},
)

# Expected columns for MRCC_SHST_MATCH_CSV:
#     LINK_ID, route_sys
MRCC_ROUTESYS_MATCH_DBF = os.path.join(
    _MC_DEFAULT_PARAMS["data_directory"], "mrcc", "trans_mrcc_centerlines.dbf"
)
_MC_DEFAULT_PARAMS["roadway_value_lookups"]["pp_link_id_2_route_sys"] = ValueLookup(
    input_dbf_filename = MRCC_ROUTESYS_MATCH_DBF,
    input_key_field = "LINK_ID",
    target_df_key_field = "mrcc_link_id",
    field_mapping = {"ROUTE_SYS": "mrcc_route_sys"},
)

#   WIDOT
WIDOT_SHST_MATCH_GEOJSON = os.path.join(
    _MC_DEFAULT_PARAMS["data_directory"], "WiscDOT", "widot.out.matched.geojson"
)

_MC_DEFAULT_PARAMS["roadway_value_lookups"]["widot_shst_2_link_id"] = ValueLookup(
    input_geojson_filename = WIDOT_SHST_MATCH_GEOJSON,
    input_key_field = "shstGeometryId",
    target_df_key_field = "shstGeometryId",
    field_mapping = {"pp_link_id": "widot_link_id", "score": "widot_score"},
)

MC_WIDOT_DBF = os.path.join(
    _MC_DEFAULT_PARAMS["data_directory"], "WiscDOT", "WISLR.dbf"
)

try:
    _MC_DEFAULT_PARAMS["roadway_value_lookups"]["widot_id_2_rdwy_ctgy"] = ValueLookup(
        input_dbf_filename = MC_WIDOT_DBF,
        input_key_field = "LINK_ID",
        target_df_key_field = "widot_link_id",
        field_mapping = {"RDWY_CTGY_": "widot_rdwy_ctgy"},
    )
except:
    from .metcouncil import add_id_field_to_shapefile
    outfile_filename = add_id_field_to_shapefile(MC_WIDOT_DBF.replace(".dbf",".shp"),"LINK_ID")

    _MC_DEFAULT_PARAMS["roadway_value_lookups"]["widot_id_2_rdwy_ctgy"] = ValueLookup(
        input_dbf_filename = outfile_filename.replace(".shp",".dbf"),
        input_key_field = "LINK_ID",
        target_df_key_field = "widot_link_id",
        field_mapping = {"RDWY_CTGY_": "widot_rdwy_ctgy"},
    )


############################
# LOOKUPS
###########################

#   OSM
_MC_DEFAULT_PARAMS["roadway_value_lookups"]["osm_roadway_assigngrp_mapping"] = ValueLookup(
    input_csv_filename=os.path.join(
        _MC_DEFAULT_PARAMS["data_directory"],
        "lookups",
        "osm_highway_asgngrp_crosswalk.csv",
    ),
    input_csv_has_header=True,
    input_key_field="roadway",
    target_df_key_field="roadway",
    field_mapping={  # csv_field, output/target_field
        "assign_group": "assignment_group_osm",
        "roadway_class": "roadway_class_osm",
    },
    overwrite=True,
)

_MC_DEFAULT_PARAMS["roadway_value_lookups"]["mrcc_roadway_assigngrp_mapping"] = ValueLookup(
    input_csv_filename=os.path.join(
        _MC_DEFAULT_PARAMS["data_directory"],
        "lookups",
        "mrcc_route_sys_asgngrp_crosswalk.csv",
    ),
    input_csv_has_header=True,
    input_key_field="ROUTE_SYS",
    target_df_key_field="mrcc_route_sys",
    field_mapping={  # csv_field, output/target_field
        "assign_group": "assignment_group_mrcc",
        "roadway_class": "roadway_class_mrcc",
    },
    overwrite=True,
)

_MC_DEFAULT_PARAMS["roadway_value_lookups"]["widot_roadway_assigngrp_mapping"] = ValueLookup(
    input_csv_filename=os.path.join(
        _MC_DEFAULT_PARAMS["data_directory"],
        "lookups",
        "widot_ctgy_asgngrp_crosswalk.csv",
    ),
    input_csv_has_header=True,
    input_key_field="RDWY_CTGY_",
    target_df_key_field="widot_rdwy_ctgy",
    field_mapping={  # csv_field, output/target_field
        "assign_group": "assignment_group_widot",
        "roadway_class": "roadway_class_widot",
    },
    overwrite=True,
)

### LANES

MC_LANES_LOOKUP_CSV = os.path.join(
    _MC_DEFAULT_PARAMS["data_directory"],
    "lookups",
    "lanes.csv",
)

_MC_DEFAULT_PARAMS["roadway_value_lookups"]["lanes"] = ValueLookup(
    MC_LANES_LOOKUP_CSV,
    input_csv_has_header = True,
    input_key_field = "model_link_id",
    target_df_key_field = "model_link_id",
)

### OUTPUT ASSISTANCE

MC_NET_DBF_CROSSWALK_CSV = os.path.join(
    _MC_DEFAULT_PARAMS["data_directory"], "lookups", "net_to_dbf.csv"
)

MC_NET_DBF_CROSSWALK = FieldMapping(
    input_csv_filename=MC_NET_DBF_CROSSWALK_CSV,
    input_csv_has_header=True,
    input_csv_fields=("net", "dbf"),
)

MC_LOG_TO_NET_CROSSWALK_CSV = os.path.join(
    _MC_DEFAULT_PARAMS["data_directory"], "lookups", "log_to_net.csv"
)


MC_LOG_TO_NET_CROSSWALK = FieldMapping(
    input_csv_filename=MC_LOG_TO_NET_CROSSWALK_CSV,
    input_csv_has_header=True,
    input_csv_fields=("log", "net"),
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


MC_DEFAULT_PARAMS =  copy.deepcopy(_MC_DEFAULT_PARAMS)
