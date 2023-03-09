import os
import math

import pandas as pd
import geopandas as gpd
from scipy.spatial import cKDTree
from pyproj import CRS
from shapely.geometry import Point, LineString

from .seflorida_parameters import SEFloridaParameters
from ..logger import WranglerLogger
from ..time_utils import hhmmss_to_datetime, secs_to_datetime

Parameters = SEFloridaParameters


class SEFloridaTransit(object):
    def __init__(self, feed, parameters):
        self.feed = feed

        if type(parameters) is dict:
            self.parameters = Parameters(**parameters)
        elif isinstance(parameters, Parameters):
            self.parameters = Parameters(**parameters.__dict__)
        else:
            msg = "Parameters should be a dict or instance of Parameters: found {} which is of type:{}".format(
                parameters, type(parameters)
            )
            WranglerLogger.error(msg)
            raise ValueError(msg)

    def time_to_cube_time_period(
        self, start_time_secs: int, as_str: bool = True, verbose: bool = False
    ):
        """
        Converts seconds from midnight to the cube time period.

        Args:
            start_time_secs: start time for transit trip in seconds
                from midnight
            as_str: if True, returns the time period as a string,
                otherwise returns a numeric time period

        Returns:
            this_tp_num: if as_str is False, returns the numeric
                time period
            this_tp: if as_str is True, returns the Cube time period
                name abbreviation
        """

        # set initial time as the time that spans midnight
        start_time_dt = secs_to_datetime(start_time_secs)

        # set initial time as the time that spans midnight
        this_tp = "NA"
        for tp_name, _times in self.parameters.time_period_to_time.items():
            _start_time, _end_time = _times
            _dt_start_time = hhmmss_to_datetime(_start_time)
            _dt_end_time = hhmmss_to_datetime(_end_time)
            if _dt_start_time > _dt_end_time:
                this_tp = tp_name
                break

        for tp_name, _times in self.parameters.time_period_to_time.items():
            _start_time, _end_time = _times
            _dt_start_time = hhmmss_to_datetime(_start_time)
            if start_time_dt >= _dt_start_time:
                this_time = _dt_start_time
                this_tp = tp_name

        WranglerLogger.debug(
            "Finding Cube Time Period from Start Time: \
                \n  - start_time_sec: {} \
                \n  - start_time_dt: {} \
                \n  - this_tp: {}".format(
                start_time_secs, start_time_dt, this_tp
            )
        )

        if verbose:
            WranglerLogger.debug(
                "Finding Cube Time Period from Start Time: \
                \n  - start_time_sec: {} \
                \n  - start_time_dt: {} \
                \n  - this_tp: {}".format(
                    start_time_secs, start_time_dt, this_tp
                )
            )

        if as_str:
            return this_tp

        name_to_num = {v: k for k, v in self.parameters.cube_time_periods.items()}
        this_tp_num = name_to_num.get(this_tp)

        if not this_tp_num:
            msg = "Cannot find time period number in {} for time period name: {}".format(
                name_to_num, this_tp
            )
            WranglerLogger.error(msg)
            raise ValueError(msg)

        return this_tp_num

    def shape_gtfs_to_cube(self, row, add_nntime=False):
        """
        Creates a list of nodes that for the route in appropriate
        cube format.

        Args:
            row: DataFrame row with both shape_id and trip_id

        Returns: a string representation of the node list
            for a route in cube format.

        """
        agency_raw_name = row.agency_raw_name
        shape_id = row.shape_id
        trip_id = row.trip_id
        trip_stop_times_df = self.feed.stop_times.copy()

        if "agency_raw_name" in trip_stop_times_df.columns:
            trip_stop_times_df.drop("agency_raw_name", axis=1, inplace=True)

        trip_stop_times_df = pd.merge(
            trip_stop_times_df,
            self.feed.trips[["trip_id", "agency_raw_name"]],
            how="left",
            on="trip_id",
        )
        trip_stop_times_df = trip_stop_times_df[
            (trip_stop_times_df.trip_id == row.trip_id)
            & (trip_stop_times_df.agency_raw_name == agency_raw_name)
        ]

        trip_node_df = self.feed.shapes.copy()
        if "agency_raw_name" in trip_node_df.columns:
            trip_node_df.drop("agency_raw_name", axis=1, inplace=True)

        trip_node_df = pd.merge(
            trip_node_df,
            self.feed.trips[["shape_id", "agency_raw_name"]].drop_duplicates(),
            how="left",
            on=["shape_id"],
        )

        trip_node_df = trip_node_df[
            (trip_node_df.shape_id == shape_id) & (trip_node_df.agency_raw_name == agency_raw_name)
        ]
        trip_node_df.sort_values(by=["shape_pt_sequence"], inplace=True)

        trip_stop_times_df = pd.merge(
            trip_stop_times_df,
            self.feed.stops,
            how="left",
            on=["agency_raw_name", "trip_id", "stop_id"],
        )

        try:
            trip_stop_times_df["model_node_id"] = (
                trip_stop_times_df["model_node_id"].astype(float).astype(int)
            )
            stop_node_id_list = trip_stop_times_df["model_node_id"].tolist()
            trip_node_list = trip_node_df["shape_model_node_id"].astype(float).astype(int).tolist()
        except:
            stop_node_id_list = trip_stop_times_df["model_node_id"].tolist()
            trip_node_list = trip_node_df["shape_model_node_id"].tolist()

        trip_stop_times_df.sort_values(by=["stop_sequence"], inplace=True)
        # sometimes GTFS `stop_sequence` does not start with 1, e.g. SFMTA light rails
        trip_stop_times_df["internal_stop_sequence"] = range(1, 1 + len(trip_stop_times_df))
        # sometimes GTFS `departure_time` is not recorded for every stop, e.g. VTA light rails
        trip_stop_times_df["departure_time"].fillna(method="ffill", inplace=True)
        trip_stop_times_df["departure_time"].fillna(0, inplace=True)
        trip_stop_times_df["NNTIME"] = trip_stop_times_df["departure_time"].diff() / 60
        # CUBE NNTIME takes 2 decimals
        trip_stop_times_df["NNTIME"] = trip_stop_times_df["NNTIME"].round(2)
        trip_stop_times_df["NNTIME"].fillna(-1, inplace=True)

        trip_stop_times_df["ACCESS"] = 0

        # node list
        node_list_str = ""
        stop_seq = 0
        for nodeIdx in range(len(trip_node_list)):
            if trip_node_list[nodeIdx] in stop_node_id_list:
                # in case a route stops at a stop more than once, e.g. circular route
                stop_seq += 1

                if (add_nntime) & (stop_seq > 1):
                    if (
                        len(
                            trip_stop_times_df[
                                trip_stop_times_df["model_node_id"] == trip_node_list[nodeIdx]
                            ]
                        )
                        > 1
                    ):
                        nntime_v = trip_stop_times_df.loc[
                            (trip_stop_times_df["model_node_id"] == trip_node_list[nodeIdx])
                            & (trip_stop_times_df["internal_stop_sequence"] == stop_seq),
                            "NNTIME",
                        ].iloc[0]
                    else:
                        nntime_v = trip_stop_times_df.loc[
                            (trip_stop_times_df["model_node_id"] == trip_node_list[nodeIdx]),
                            "NNTIME",
                        ].iloc[0]

                    if nntime_v > 0:
                        nntime = ", NNTIME=%s" % (nntime_v)
                    else:
                        nntime = ""
                else:
                    nntime = ""

                access_v = trip_stop_times_df.loc[
                    (trip_stop_times_df["model_node_id"] == trip_node_list[nodeIdx]),
                    "ACCESS",
                ].iloc[0]
                if access_v > 0:
                    access = ", ACCESS=%s" % (access_v)
                else:
                    access = ""

                node_list_str += "\n %s%s%s" % (trip_node_list[nodeIdx], nntime, access)
                if nodeIdx < (len(trip_node_list) - 1):
                    node_list_str += ","
                    if ((add_nntime) & (stop_seq > 1) & (len(nntime) > 0)) | (len(access) > 0):
                        node_list_str += " N="
            else:
                node_list_str += "\n -%s" % (trip_node_list[nodeIdx])
                if nodeIdx < (len(trip_node_list) - 1):
                    node_list_str += ","

        # remove NNTIME = 0
        node_list_str = node_list_str.replace(" NNTIME=0.0, N=", "")
        node_list_str = node_list_str.replace(" NNTIME=0.0,", "")

        return node_list_str

    def route_properties_gtfs_to_cube(self, parameters=None):
        """
        Prepare gtfs for cube lin file.
        Does the following operations:
        1. Combines route, frequency, trip, and shape information
        2. Converts time of day to time periods
        3. Calculates cube route name from gtfs route name and properties
        4. Assigns a cube-appropriate mode number
        5. Assigns a cube-appropriate operator number
        Returns:
            trip_df (DataFrame): DataFrame of trips with cube-appropriate values for:
                - NAME
                - ONEWAY
                - OPERATOR
                - MODE
                - HEADWAY
        """
        WranglerLogger.info("Converting GTFS Standard Properties to SERPM's Cube Standard")

        trip_df = self.feed.trips.copy()

        mode_crosswalk = pd.read_csv(parameters.mode_crosswalk_file)

        """
        Add information from: routes, frequencies, and routetype to trips_df
        """

        trip_df = pd.merge(
            trip_df,
            self.feed.routes.drop(columns=["agency_raw_name"]),
            how="left",
            on="route_id",
        )

        trip_df = pd.merge(trip_df, self.feed.frequencies, how="left", on="trip_id")

        trip_df["tod"] = trip_df.start_time.apply(self.time_to_cube_time_period, as_str=False)
        trip_df["tod_name"] = trip_df.start_time.apply(self.time_to_cube_time_period)

        # add shape_id to name when N most common pattern is used for routes*tod*direction
        trip_df["shp_id"] = trip_df.groupby(["route_id", "tod", "direction_id"]).cumcount()
        trip_df["shp_id"] = trip_df["shp_id"].astype(str)
        trip_df["shp_id"] = "s" + trip_df["shp_id"]

        trip_df["route_short_name"] = (
            trip_df["route_short_name"]
            .str.replace("-", "_")
            .str.replace(" ", ".")
            .str.replace(",", "_")
            .str.slice(stop=50)
        )

        trip_df["route_long_name"] = (
            trip_df["route_long_name"].str.replace(",", "_").str.slice(stop=50)
        )

        trip_df["LONGNAME"] = trip_df["route_long_name"]
        trip_df["HEADWAY"] = (trip_df["headway_secs"] / 60).astype(int)

        trip_df = pd.merge(
            trip_df,
            self.feed.agency[["agency_raw_name", "agency_id"]],
            how="left",
            on=["agency_raw_name", "agency_id"],
        )

        trip_df = pd.merge(
            trip_df,
            mode_crosswalk,
            how="left",
            on=["agency_raw_name", "operator_code", "route_type"],
        )

        trip_df["SERPM_mode"].fillna(0, inplace=True)
        trip_df["SERPM_mode"] = trip_df["SERPM_mode"].astype(int)

        trip_df["ONEWAY"] = "T"

        trip_df["agency_id"].fillna("", inplace=True)

        trip_df["NAME"] = trip_df.apply(
            lambda x: str(x.SERPM_operator)
            + "_"
            + str(x.route_id)
            + "_"
            + x.tod_name
            + "_"
            + "d"
            + str(int(x.direction_id))
            + "_s"
            + x.shape_id,
            axis=1,
        )

        trip_df["NAME"] = trip_df["NAME"].str.slice(stop=28)

        print("line name:")
        print(trip_df[["NAME"]].head())

        return trip_df

    def cube_format(self, row):
        """
        Creates a string represnting the route in cube line file notation.
        Args:
            row: row of a DataFrame representing a cube-formatted trip, with the Attributes
                trip_id, shape_id, NAME, LONGNAME, tod, HEADWAY, MODE, ONEWAY, OPERATOR
        Returns:
            string representation of route in cube line file notation
        """

        s = '\nLINE NAME="{}",'.format(row.NAME)
        s += '\n LONGNAME="{}",'.format(str(row.LONGNAME).replace('"', ""))
        s += '\n USERA1="%s",' % (row.agency_id if row.agency_id != "" else row.agency_raw_name)
        s += "\n HEADWAY[{}]={},".format(row.tod, row.HEADWAY)
        s += "\n MODE={},".format(row.SERPM_mode)
        s += "\n ONEWAY={},".format(row.ONEWAY)
        s += "\n OPERATOR={},".format(
            int(row.SERPM_operator) if ~math.isnan(row.SERPM_operator) else 99
        )
        s += '\n SHORTNAME="%s",' % (row.route_short_name,)
        add_nntime = False
        s += "\n N={}".format(self.shape_gtfs_to_cube(row, add_nntime))

        return s

    def write_as_cube_lin(self, parameters=None, outpath: str = None):
        """
        Writes the gtfs feed as a cube line file after
        converting gtfs properties to MetCouncil cube properties.
        Args:
            outpath: File location for output cube line file.
        """
        if not outpath:
            outpath = os.path.join(parameters.scratch_location, "outtransit.lin")

        trip_cube_df = self.route_properties_gtfs_to_cube(parameters)

        trip_cube_df[["SERPM_operator"]] = trip_cube_df[["SERPM_operator"]].fillna(99)

        trip_cube_df = trip_cube_df.fillna("")
        trip_cube_df["LIN"] = trip_cube_df.apply(lambda x: self.cube_format(x), axis=1)

        l = trip_cube_df["LIN"].tolist()
        l = [";;<<PT>><<LINE>>;;"] + l

        with open(outpath, "w") as f:
            f.write("\n".join(l))
