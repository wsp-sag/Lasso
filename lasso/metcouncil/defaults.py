"""

"""

import os
import copy

from ..data import PolygonOverlay, FieldMapping, ValueLookup

_MC_DEFAULT_PARAMS = {}

# Add a name for identification purposes
_MC_DEFAULT_PARAMS["name"] = "MetCouncil Defaults"

# Add nested dicts
_MC_DEFAULT_PARAMS["roadway_value_lookups"] = {}
_MC_DEFAULT_PARAMS["transit_value_lookups"] = {}
_MC_DEFAULT_PARAMS["roadway_field_mappings"] = {}
_MC_DEFAULT_PARAMS["counts"] = {}
_MC_DEFAULT_PARAMS["roadway_overlays"] = {}
_MC_DEFAULT_PARAMS["lookups"] = {}
############################
# FILE
############################

_MC_DEFAULT_PARAMS["data_directory"] = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), "data"
)

_MC_DEFAULT_PARAMS["shape_foreign_key"] = "shape_id"

############################
# NETWORK
############################

_MC_DEFAULT_PARAMS["time_period_abbr_to_names"] = {
    "AM": "AM Peak",
    "MD": "Midday",
    "PM": "PM Peak",
    "NT": "Night",
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

_MC_DEFAULT_PARAMS[
    "model_roadway_class"
] = "metcouncil.metcouncil_roadway.MetCouncilRoadwayNetwork"

_MC_DEFAULT_PARAMS["network_build_script_type"] = "CUBE_HWYNET"

_MC_DEFAULT_PARAMS["max_taz"] = 3100

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

_MC_DEFAULT_PARAMS["properties_to_split_by_category_groupings"] = ["price"]

_MC_DEFAULT_PARAMS["roadway_output_espg"] = 26915

_MC_DEFAULT_PARAMS["field_type"] = {
    "model_link_id": int,
    "link_id": int,
    "A": int,
    "B": int,
    "shstGeometryId": str,
    "shape_id": str,
    "distance": float,
    "roadway": str,
    "name": str,
    "roadway_class": int,
    "assign_group": int,
    "bike_access": int,
    "walk_access": int,
    "drive_access": int,
    "truck_access": int,
    "trn_priority_AM": int,
    "trn_priority_MD": int,
    "trn_priority_PM": int,
    "trn_priority_NT": int,
    "ttime_assert_AM": float,
    "ttime_assert_MD": float,
    "ttime_assert_PM": float,
    "ttime_assert_NT": float,
    "lanes_AM": int,
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
    "ROUTE_SYS": str,
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
    "area_type",
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

_MC_DEFAULT_PARAMS["transit_class"] = ".metcouncil.metcouncil_transit.MetCouncilTransit"

_MC_DEFAULT_PARAMS["transit_to_network_time_periods"] = {"1": "AM", "2": "MD"}

_MC_DEFAULT_PARAMS["transit_value_lookups"]["gtfs_agency_id_to_cube_operator"] = {
    "0": 3,
    "1": 3,
    "2": 3,
    "3": 4,
    "4": 2,
    "5": 5,
    "6": 8,
    "7": 1,
    "8": 1,
    "9": 10,
    "10": 3,
    "11": 9,
    "12": 3,
    "13": 4,
    "14": 4,
    "15": 3,
}

_MC_DEFAULT_PARAMS["transit_value_lookups"]["route_type_to_bus_mode"] = {
    "Urb Loc": 5,
    "Sub Loc": 6,
    "Express": 7,
}

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
    input_filename=MC_COUNTY_SHAPEFILE, field_mapping={"NAME": "county_name"}
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

_MC_DEFAULT_PARAMS["roadway_value_lookups"]["mc_mpo_counties_dict"] = {
    "Anoka": 1,
    "Dakota": 1,
    "Hennipin": 1,
    "Ramsey": 1,
    "Scott": 1,
    "Washington": 1,
    "Carver": 1,
}

### TAZS


MC_TAZ_SHAPEFILE = os.path.join(
    _MC_DEFAULT_PARAMS["data_directory"], "TAZ", "TAZOfficialWCurrentForecasts.shp"
)

_MC_DEFAULT_PARAMS["roadway_overlays"]["tazs"] = PolygonOverlay(
    input_filename=MC_TAZ_SHAPEFILE, field_mapping={"TAZ": "TAZ"}
)


### AREATYPE

MC_AREATYPE_SHAPEFILE = os.path.join(
    _MC_DEFAULT_PARAMS["data_directory"],
    "area_type",
    "ThriveMSP2040CommunityDesignation.shp",
)

_MC_DEFAULT_PARAMS["roadway_overlays"]["area_type"] = PolygonOverlay(
    input_filename=MC_AREATYPE_SHAPEFILE, field_mapping={"COMDES2040": "area_type_name"}
)

MC_DOWNTOWN_AREATYPE_SHAPEFILE = os.path.join(
    _MC_DEFAULT_PARAMS["data_directory"], "area_type", "downtownzones_TAZ.shp"
)

_MC_DEFAULT_PARAMS["roadway_overlays"]["downtown_area_type"] = PolygonOverlay(
    input_filename=MC_DOWNTOWN_AREATYPE_SHAPEFILE,
    fill_values_dict={"area_type_name": "downtown"},
)

# area_type_code_dict (dict): Mapping of the area_type_variable_shp to
# The area type code used in the MetCouncil cube network.
#
# noqa: E501 source https://metrocouncil.org/Planning/Publications-And-Resources/Thrive-MSP-2040-Plan-(1)/7_ThriveMSP2040_LandUsePoliciesbyCD.aspx

_MC_DEFAULT_PARAMS["roadway_value_lookups"]["area_type_codes_dict"] = {
    "downtown": 5,  # downtown
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

MC_MNDOT_COUNTS_SHST_MATCH = os.path.join(
    _MC_DEFAULT_PARAMS["data_directory"], "count_mn", "mn_count_ShSt_API_match.csv"
)

_MC_DEFAULT_PARAMS["counts"]["mn_2017_counts"] = ValueLookup(
    input_filename=MC_MNDOT_COUNTS_SHST_MATCH,
    input_csv_has_header=True,
    input_key_field="shstReferenceId",
    target_df_key_field="shstReferenceId",
    field_mapping={"AADT_mn": "count_daily"},
)


MC_WIDOT_COUNTS_SHST_MATCH = os.path.join(
    _MC_DEFAULT_PARAMS["data_directory"], "WiscDOT", "wi_count_ShSt_API_match.csv"
)

_MC_DEFAULT_PARAMS["counts"]["wi_2017_counts"] = ValueLookup(
    input_filename=MC_WIDOT_COUNTS_SHST_MATCH,
    input_csv_has_header=True,
    input_key_field="shstReferenceId",
    target_df_key_field="shstReferenceId",
    field_mapping={"AADT_wi": "count_daily"},
)

_MC_DEFAULT_PARAMS["time_period_vol_split"] = {
    "AM": 0.25,
    "MD": 0.25,
    "PM": 0.25,
    "NT": 0.25,
}

_MC_DEFAULT_PARAMS["count_tod_split_fields"] = {"count_daily": "count_"}


### ROADWAY_CLASS

#   MRCC/MNDOT

MC_MRCC_SHAPEFILE = os.path.join(
    _MC_DEFAULT_PARAMS["data_directory"], "mrcc", "trans_mrcc_centerlines.shp"
)

MC_MRCC_GEOGRAPHIC_OVERLAY = PolygonOverlay(
    input_filename=MC_MRCC_SHAPEFILE, added_id="LINK_ID"
)

# Expected columns for MRCC_SHST_MATCH_CSV:
#     shstReferenceId,shstGeometryId,pp_link_id (which is the LINK_ID),score
MRCC_SHST_MATCH_CSV = os.path.join(
    _MC_DEFAULT_PARAMS["data_directory"], "mrcc", "mrcc.out.matched.csv"
)
_MC_DEFAULT_PARAMS["roadway_value_lookups"]["mrcc_shst_2_pp_link_id"] = ValueLookup(
    input_filename=MRCC_SHST_MATCH_CSV,
    input_csv_has_header=True,
    input_key_field="shstGeometryId",
    target_df_key_field="shstGeometryId",
    field_mapping={"pp_link_id": "mrcc_link_id", "score": "mrcc_score"},
)

# Expected columns for MRCC_SHST_MATCH_CSV:
#     LINK_ID, route_sys
MRCC_ROUTESYS_MATCH_DBF = os.path.join(
    _MC_DEFAULT_PARAMS["data_directory"], "mrcc", "trans_mrcc_centerlines.dbf"
)
_MC_DEFAULT_PARAMS["roadway_value_lookups"]["pp_link_id_2_route_sys"] = ValueLookup(
    input_filename=MRCC_ROUTESYS_MATCH_DBF,
    input_key_field="LINK_ID",
    target_df_key_field="mrcc_link_id",
    field_mapping={"ROUTE_SYS": "mrcc_route_sys"},
    assert_types={"ROUTE_SYS": str, "mrcc_route_sys": str},
)

#   WIDOT
WIDOT_SHST_MATCH_GEOJSON = os.path.join(
    _MC_DEFAULT_PARAMS["data_directory"], "WiscDOT", "widot.out.matched.geojson"
)

_MC_DEFAULT_PARAMS["roadway_value_lookups"]["widot_shst_2_link_id"] = ValueLookup(
    input_filename=WIDOT_SHST_MATCH_GEOJSON,
    input_key_field="shstGeometryId",
    target_df_key_field="shstGeometryId",
    field_mapping={"pp_link_id": "widot_link_id", "score": "widot_score"},
)

MC_WIDOT_DBF = os.path.join(
    _MC_DEFAULT_PARAMS["data_directory"], "WiscDOT", "WISLR_with_id.dbf"
)

ID_FLAG = "_with_id"
NO_ID_SHP = os.path.exists(MC_WIDOT_DBF.replace(".dbf", ID_FLAG + ".shp"))

if not os.path.exists(MC_WIDOT_DBF):
    if not os.path.exists(NO_ID_SHP):
        raise (ValueError("File not found for MC_WI_DBF: {}".format(MC_WIDOT_DBF)))

    from .metcouncil import add_id_field_to_shapefile

    add_id_field_to_shapefile(
        NO_ID_SHP,
        outfile_filename=MC_WIDOT_DBF.replace(".dbf", ".shp"),
        fieldname="LINK_ID",
    )

_MC_DEFAULT_PARAMS["roadway_value_lookups"]["widot_id_2_rdwy_ctgy"] = ValueLookup(
    input_filename=MC_WIDOT_DBF,
    input_key_field="LINK_ID",
    target_df_key_field="widot_link_id",
    field_mapping={"RDWY_CTGY_": "widot_rdwy_ctgy"},
)


############################
# LOOKUPS
###########################

#   OSM
_MC_DEFAULT_PARAMS["roadway_value_lookups"][
    "osm_roadway_assigngrp_mapping"
] = ValueLookup(
    input_filename=os.path.join(
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
)

_MC_DEFAULT_PARAMS["roadway_value_lookups"][
    "mrcc_roadway_assigngrp_mapping"
] = ValueLookup(
    input_filename=os.path.join(
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
    assert_types={"ROUTE_SYS": str},
)

_MC_DEFAULT_PARAMS["roadway_value_lookups"][
    "widot_roadway_assigngrp_mapping"
] = ValueLookup(
    input_filename=os.path.join(
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
)

### LANES ###

MC_LANES_LOOKUP_CSV = os.path.join(
    _MC_DEFAULT_PARAMS["data_directory"], "lookups", "lanes.csv"
)

_MC_DEFAULT_PARAMS["roadway_value_lookups"]["lanes"] = ValueLookup(
    MC_LANES_LOOKUP_CSV,
    input_csv_has_header=True,
    input_key_field="model_link_id",
    field_mapping={
        "anoka": "anoka",
        "hennepin": "hennepin",
        "carver": "carver",
        "dakota": "dakota",
        "washington": "washington",
        "widot": "widot",
        "mndot": "mndot",
        "osm_min": "osm_min",
        "naive": "naive",
    },
    target_df_key_field="model_link_id",
)

### OUTPUT ASSISTANCE


MC_NET_DBF_CROSSWALK_CSV = os.path.join(
    _MC_DEFAULT_PARAMS["data_directory"], "lookups", "net_to_dbf.csv"
)

_MC_DEFAULT_PARAMS["roadway_field_mappings"]["net_to_dbf"] = FieldMapping(
    input_filename=MC_NET_DBF_CROSSWALK_CSV,
    input_csv_has_header=True,
    input_csv_fields=("net", "dbf"),
)


MC_LOG_TO_NET_CROSSWALK_CSV = os.path.join(
    _MC_DEFAULT_PARAMS["data_directory"], "lookups", "log_to_net.csv"
)

_MC_DEFAULT_PARAMS["roadway_field_mappings"]["log_to_net"] = FieldMapping(
    input_filename=MC_LOG_TO_NET_CROSSWALK_CSV,
    input_csv_has_header=True,
    input_csv_fields=("log", "net"),
)

### TRANSIT

MC_ROUTE_TYPE_BUS_MODE_LOOKUP = {"Urb Loc": 5, "Sub Loc": 6, "Express": 7}

MC_ROUTE_TYPE_MODE_LOOKUP = {0: 8, 2: 9}

MC_DEFAULT_PARAMS = copy.deepcopy(_MC_DEFAULT_PARAMS)
