from typing import Any, Dict, Optional
import glob
import os

import geopandas as gpd
import numpy as np
import pandas as pd
import partridge as ptg

from network_wrangler import TransitNetwork

from .TransitNetwork import TransitNetworkLasso
from .Logger import WranglerLogger


class CubeTransit(object):
    def __init__(
        self,
        cube_transit_network: Optional[TransitNetworkLasso] = None,
        gtfs_feed: Optional[str] = None,
    ):
        """
        """
        ## TODO Sijia
        self.cube_transit_network = cube_transit_network
        self.diff_dict = Dict[str, Any]

        self.gtfs_feed = ptg.load_feed(gtfs_feed) if gtfs_feed else None

        self.lines = [";;<<PT>><<LINE>>;;"]

        pass

    @staticmethod
    def create_cubetransit(
        cube_transit_dir: Optional[str] = None,
        cube_transit_file: Optional[str] = None,
        gtfs_feed_dir: Optional[str] = None,
    ):
        if (
            sum(
                x is not None
                for x in [cube_transit_dir, cube_transit_file, gtfs_feed_dir]
            )
            > 1
        ):
            msg = "cube_transit_dir takes only one of cube_transit_dir, cube_transit_file, and gtfs_feed_dir but more than one input."
            WranglerLogger.error(msg)
            raise ValueError(msg)

        transit_net = TransitNetworkLasso("CHAMP", 1.0)

        if cube_transit_file:
            transit_net.mergeFile(cube_transit_file)
        elif cube_transit_dir:
            for cube_transit_file in glob.glob(os.path.join(cube_transit_dir, "*.lin")):
                transit_net.mergeFile(cube_transit_file)
        else:
            msg = "Creating cube network with GTFS files not yet supported"
            WranglerLogger.error(msg)
            raise NotImplemented(msg)
            # feed = TransitNetwork.read(feed_path = gtfs_feed_dir)

        cube_transit_net = CubeTransit(
            cube_transit_network=transit_net, gtfs_feed=gtfs_feed_dir,
        )

        return cube_transit_net

    def evaluate_differences(self, base_transit):
        """

        Parameters
        -----------

        Returns
        -------

        """

        # loop thru every record in new .lin
        transit_change_list = []
        """
        should be like this eventually
        time_enum = {"pk": {"start_time" : "06:00:00",
                            "end_time" : "09:00:00"},
                     "op" : {"start_time" : "09:00:00",
                            "end_time" : "15:00:00"}}
        """

        WranglerLogger.info("Evaluating differences between base and build transit")

        time_enum = {
            "pk": list(["06:00:00", "09:00:00"]),
            "op": list(["09:00:00", "15:00:00"]),
        }

        ##TODO compare name lists to find new lines and deleted lines

        ## calls method to create new line when applicable as a line object and then append

        ##todo make this a pandas merge to make it all vector operations
        for line in self.cube_transit_network.lines[1:]:
            _name = line.name
            for line_base in base_transit.cube_transit_network.lines[1:]:
                if line_base.name == _name:
                    ## TODO also evaluate differences in stops and route shapes
                    shape_change_list = CubeTransit.evaluate_route_shape_changes(
                        line, line_base
                    )
                    ## Might be useful to review: https://github.com/wsp-sag/network_wrangler/blob/master/network_wrangler/TransitNetwork.py#L411
                    properties_list = CubeTransit.evaluate_route_property_changes(
                        line, line_base
                    )
                    WranglerLogger.debug("Properties List: {}".format(properties_list))
                    if len(properties_list) > 0:
                        if _name[-3:-1] == "pk":
                            time = ["06:00:00", "09:00:00"]
                        else:
                            time = ["09:00:00", "15:00:00"]
                        card_dict = {
                            "category": "Transit Service Property Change",
                            "facility": {
                                "route_id": _name.split("_")[1],
                                "direction_id": int(_name[-1]),
                                "time": time
                                # "start_time" : time_enum[_name[-3:-1]]["start_time"],
                                # "end_time" : time_enum[_name[-3:-1]]["end_time"]
                            },
                            "properties": properties_list,
                        }
                        WranglerLogger.debug("Card_Dict: {}".format(card_dict))
                        transit_change_list.append(card_dict)
                else:
                    continue
        print(transit_change_list)
        return transit_change_list

    def evaluate_route_shape_changes(line_build, line_base):
        ##TODO Sijia
        pass

    def evaluate_route_property_changes(line_build, line_base):
        properties_list = []
        if line_build.getFreq() != line_base.getFreq():
            _headway_diff = line_build.getFreq() - line_base.getFreq()
            properties_list.append(
                {"property": "headway_secs", "change": _headway_diff * 60}
            )
        # if tn_build.
        return properties_list

    @staticmethod
    def gtfs_to_cube(self):
        """
        prepare gtfs for cube lin file
        """
        mode_dict = {0: 8, 2: 9}
        bus_mode_dict = {"Urb Loc": 5, "Sub Loc": 6, "Express": 7}
        metro_operator_dict = {
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
        # mvta_operator_dict = {'1':4, '2':3, '3':3}

        bus_routetype_gdf = gpd.read_file(
            "Z:/Data/Users/Sijia/Met_Council/GIS/shp_trans_transit_routes/TransitRoutes.shp"
        )
        routetype_df = bus_routetype_gdf.copy()

        shape_df = self.gtfs_feed.shapes.copy()
        trip_df = self.gtfs_feed.trips.copy()

        trip_df = pd.merge(trip_df, self.gtfs_feed.routes, how="left", on="route_id")
        trip_df = pd.merge(
            trip_df, self.gtfs_feed.frequencies, how="left", on="trip_id"
        )
        trip_df = pd.merge(
            trip_df,
            routetype_df[["route", "routetype"]],
            how="left",
            left_on="route_short_name",
            right_on="route",
        )

        trip_df["tod"] = np.where(trip_df.start_time == "06:00:00", "pk", "op")

        trip_df["NAME"] = trip_df.apply(
            lambda x: x.agency_id
            + "_"
            + x.route_id
            + "_"
            + x.route_short_name
            + "_"
            + x.tod
            + str(x.direction_id),
            axis=1,
        )

        trip_df["LONGNAME"] = trip_df["route_long_name"]
        trip_df["HEADWAY"] = (trip_df["headway_secs"] / 60).astype(int)
        trip_df["MODE"] = np.where(
            trip_df.route_type == 3,
            trip_df["routetype"].map(bus_mode_dict),
            trip_df["route_type"].map(mode_dict),
        )
        trip_df["MODE"].fillna(5, inplace=True)
        trip_df["MODE"] = trip_df["MODE"].astype(int)

        trip_df["ONEWAY"] = "T"

        trip_df["OPERATOR"] = trip_df["agency_id"].map(metro_operator_dict)

        return trip_df

    def cube_format(self, x):
        """
        formatter for cube .lin file
        """
        trip_stop_times_df = self.gtfs_feed.stop_times.copy()
        trip_stop_times_df = trip_stop_times_df[trip_stop_times_df.trip_id == x.trip_id]

        trip_node_df = self.gtfs_feed.shapes.copy()

        trip_node_df = trip_node_df[trip_node_df.shape_id == x.shape_id]

        trip_stop_times_df = pd.merge(
            trip_stop_times_df, self.gtfs_feed.stops, how="left", on="stop_id"
        )

        # stop_id_list = trip_stop_times_df['stop_id'].tolist()
        stop_node_id_list = trip_stop_times_df["model_node_id"].tolist()

        trip_node_list = trip_node_df["shape_model_node_id"].tolist()

        s = '\nLINE NAME="%s",' % (x.NAME,)

        # line attribtes
        s += '\n LONGNAME="%s",' % (x.LONGNAME,)
        if x.tod == "pk":
            s += "\n HEADWAY[1]=%s," % (x.HEADWAY,)
        else:
            s += "\n HEADWAY[2]=%s," % (x.HEADWAY,)
        s += "\n MODE=%s," % (x.MODE,)
        s += "\n ONEWAY=%s," % (x.ONEWAY,)
        s += "\n OPERATOR=%s," % (x.OPERATOR,)
        s += "\nNODES="

        # node list
        for nodeIdx in range(len(trip_node_list)):
            if trip_node_list[nodeIdx] in stop_node_id_list:
                s += "\n %s" % (trip_node_list[nodeIdx])
                if nodeIdx < (len(trip_node_list) - 1):
                    s += ","
            else:
                s += "\n -%s" % (trip_node_list[nodeIdx])
                if nodeIdx < (len(trip_node_list) - 1):
                    s += ","

        self.lines.append(s)

    def write_cube_transit(self, outpath):
        """
        write out to .lin file
        """
        trip_cube_df = CubeTransit.gtfs_to_cube(self)

        trip_cube_df.apply(lambda x: CubeTransit.cube_format(self, x), axis=1)

        with open(outpath, "w") as f:
            f.write("\n".join(map(str, self.lines)))
