import json
import os
import re
from typing import Any, Dict, Optional

import pandas as pd
from pandas import DataFrame

from network_wrangler import ProjectCard
from network_wrangler import RoadwayNetwork

from .transit import CubeTransit, StandardTransit
from .Logger import WranglerLogger


class Project(object):
    """
    A single or set of changes to the roadway or transit system.
    """

    DEFAULT_PROJECT_NAME = "USER TO define"

    STATIC_VALUES = [
        "model_link_id",
        "area_type",
        "county",
        "asgngrp",
        "centroidconnect",
    ]

    def __init__(
        self,
        roadway_changes: Optional[DataFrame] = None,
        transit_changes: Optional[CubeTransit] = None,
        base_roadway_network: Optional[RoadwayNetwork] = None,
        base_transit_network: Optional[CubeTransit] = None,
        build_transit_network: Optional[CubeTransit] = None,
        project_name: Optional[str] = "",
        evaluate: bool = False,
    ):
        """

        """
        self.card_data = Dict[str, Dict[str, Any]]

        self.roadway_changes = roadway_changes
        self.base_roadway_network = base_roadway_network
        self.base_transit_network = base_transit_network
        self.build_transit_network = build_transit_network
        self.transit_changes = transit_changes
        self.project_name = (
            project_name if project_name else Project.DEFAULT_PROJECT_NAME
        )

        if base_roadway_network != None:
            self.determine_roadway_network_changes_compatability()

        if evaluate:
            self.evaluate_changes()

    def write_project_card(self, filename):
        """

        Parameters
        -----------
        filename
        Returns
        -------
        """
        ProjectCard(self.card_data).write(filename)
        WranglerLogger.info("Wrote project card to: {}".format(filename))

    @staticmethod
    def create_project(
        roadway_log_file: Optional[str] = None,
        base_roadway_dir: Optional[str] = None,
        base_transit_source: Optional[str] = None,
        build_transit_source: Optional[str] = None,
        roadway_changes: Optional[DataFrame] = None,
        transit_changes: Optional[CubeTransit] = None,
        base_roadway_network: Optional[RoadwayNetwork] = None,
        base_transit_network: Optional[CubeTransit] = None,
        build_transit_network: Optional[CubeTransit] = None,
        project_name=None,
    ):
        """

        Parameters
        -----------
        roadway_log_file : str
            filename for consuming logfile

        Returns
        -------
        Project object
        """

        if base_transit_source:
            base_transit_network = CubeTransit.create_from_cube(base_transit_source)
            WranglerLogger.debug("base nw lines: {}".format(base_transit_network.lines))
        else:
            msg = "No base transit network."
            WranglerLogger.info(msg)
            base_transit_network = None

        if build_transit_source and transit_changes:
            msg = "Method takes only one of 'build_transit_source' and 'transit_changes' but both given"
            WranglerLogger.error(msg)
            raise ValueError(msg)
        if build_transit_source:
            WranglerLogger.debug("build")
            build_transit_network = CubeTransit.create_from_cube(build_transit_source)
            WranglerLogger.debug(
                "build nw lines: {}".format(build_transit_network.lines)
            )
        else:
            msg = "No transit changes given or processed."
            WranglerLogger.info(msg)
            transit_changes = None

        if roadway_log_file and roadway_changes:
            msg = "Method takes only one of 'roadway_log_file' and 'roadway_changes' but both given"
            WranglerLogger.error(msg)
            raise ValueError(msg)
        if roadway_log_file:
            roadway_changes = Project.read_logfile(roadway_log_file)
        else:
            msg = "No roadway changes given or processed."
            WranglerLogger.info(msg)
            roadway_changes = pd.DataFrame({})

        if base_roadway_network and base_roadway_dir:
            msg = "Method takes only one of 'base_roadway_network' and 'base_roadway_dir' but both given"
            WranglerLogger.error(msg)
            raise ValueError(msg)
        if base_roadway_dir:
            base_roadway_network = RoadwayNetwork.read(
                os.path.join(base_roadway_dir, "link.json"),
                os.path.join(base_roadway_dir, "node.geojson"),
                os.path.join(base_roadway_dir, "shape.geojson"),
                True,
            )
        else:
            msg = "No base roadway network."
            WranglerLogger.info(msg)
            base_roadway_network = None

        project = Project(
            roadway_changes=roadway_changes,
            transit_changes=transit_changes,
            base_roadway_network=base_roadway_network,
            base_transit_network=base_transit_network,
            build_transit_network=build_transit_network,
            evaluate=True,
            project_name=project_name,
        )

        return project

    @staticmethod
    def read_logfile(logfilename: str) -> DataFrame:
        """
        Reads a Cube log file and returns a dataframe of roadway_changes
        Parameters
        -----------
        logfilename : str
            filename for consuming logfile

        Returns
        -------
        """
        WranglerLogger.info("Reading logfile: {}".format(logfilename))
        with open(logfilename) as f:
            content = f.readlines()

        # (content[0].startswith("HighwayLayerLogX")):
        if not content[0].startswith("HighwayLayerLogX"):
            WranglerLogger.info("Returning an empty dataframe")
            return DataFrame()

        NodeLines = [x.strip() for x in content if x.startswith("N")]

        LinkLines = [x.strip() for x in content if x.startswith("L")]

        linkcol_names = ["OBJECT", "OPERATION", "GROUP"] + LinkLines[0].split(",")[1:]

        nodecol_names = ["OBJECT", "OPERATION", "GROUP"] + NodeLines[0].split(",")[1:]

        link_df = DataFrame(
            data=[re.split(",|;", x) for x in LinkLines[1:]], columns=linkcol_names
        )

        node_df = DataFrame(
            data=[re.split(",|;", x) for x in NodeLines[1:]], columns=nodecol_names
        )

        log_df = pd.concat([link_df, node_df], ignore_index=True, sort=False)
        WranglerLogger.info(
            "Processed {} Node lines and {} Link lines".format(
                link_df.shape[0], node_df.shape[0]
            )
        )

        return log_df

    def determine_roadway_network_changes_compatability(self):
        """
        Checks to see that any links or nodes that change exist in base roadway network.
        """
        WranglerLogger.info(
            "Evaluating compatibility between roadway network changes and base network. Not evaluating deletions."
        )

        link_changes_df = self.roadway_changes[
            (self.roadway_changes.OBJECT == "L")
            & (self.roadway_changes.OPERATION == "C")
        ]
        link_merge_df = pd.merge(
            link_changes_df[["A", "B"]].astype(str),
            self.base_roadway_network.links_df[["A", "B", "model_link_id"]].astype(str),
            how="left",
            on=["A", "B"],
        )
        missing_links = link_merge_df.loc[link_merge_df["model_link_id"].isna()]
        if missing_links.shape[0]:
            msg = "Network missing the following AB links:\n{}".format(missing_links)
            WranglerLogger.error(msg)
            raise ValueError(msg)

        node_changes_df = self.roadway_changes[
            (self.roadway_changes.OBJECT == "N")
            & (self.roadway_changes.OPERATION == "C")
        ]
        node_merge_df = pd.merge(
            node_changes_df[["model_node_id"]],
            self.base_roadway_network.nodes_df[["model_node_id", "geometry"]],
            how="left",
            on=["model_node_id"],
        )
        missing_nodes = node_merge_df.loc[node_merge_df["geometry"].isna()]
        if missing_nodes.shape[0]:
            msg = "Network missing the following nodes:\n{}".format(missing_nodes)
            WranglerLogger.error(msg)
            raise ValueError(msg)

    def evaluate_changes(self):
        """
        Determines which changes should be evaluated.
        """
        highway_change_list = []
        transit_change_list = []

        WranglerLogger.info("Evaluating project changes.")

        if not self.roadway_changes.empty:
            highway_change_list = self.add_highway_changes()

        if (self.transit_changes is not None) or (
            self.base_transit_network is not None
            and self.build_transit_network is not None
        ):
            transit_change_list = self.add_transit_changes()

        self.card_data = {
            "project": self.project_name,
            "changes": transit_change_list + highway_change_list,
        }

    def add_transit_changes(self):
        """
        Evaluates changes between base and build transit objects and
        adds entries into the self.card_data dictionary.
        """

        transit_change_list = self.build_transit_network.evaluate_differences(
            self.base_transit_network
        )

        return transit_change_list

    def add_highway_changes(self, limit_variables_to_existing_network=False):
        """
        Evaluates changes from the log file based on the base highway object and
        adds entries into the self.card_data dictionary.
        """

        ## if worth it, could also add some functionality  to network wrangler itself.
        node_changes_df = self.roadway_changes[
            self.roadway_changes.OBJECT == "N"
        ].copy()

        link_changes_df = self.roadway_changes[
            self.roadway_changes.OBJECT == "L"
        ].copy()

        def _final_op(x):
            if x.OPERATION_history[-1] == "D":
                if "A" in x.OPERATION_history[:-1]:
                    return "N"
                else:
                    return "D"
            elif x.OPERATION_history[-1] == "A":
                if "D" in x.OPERATION_history[:-1]:
                    return "C"
                else:
                    return "A"
            else:
                if "A" in x.OPERATION_history[:-1]:
                    return "A"
                else:
                    return "C"

        def _consolidate_actions(log, base, key_list):
            log_df = log.copy()
            # will be changed if to allow new variables being added/changed that are not in base network
            changeable_col = [x for x in log_df.columns if x in base.columns]

            for x in changeable_col:
                log_df[x] = log_df[x].astype(base[x].dtype)

            action_history_df = (
                log_df.groupby(key_list)["OPERATION"]
                .agg(lambda x: x.tolist())
                .rename("OPERATION_history")
                .reset_index()
            )

            log_df = pd.merge(log_df, action_history_df, on=key_list, how="left")
            log_df.drop_duplicates(subset=key_list, keep="last", inplace=True)
            log_df["OPERATION_final"] = log_df.apply(lambda x: _final_op(x), axis=1)
            return log_df[changeable_col + ["OPERATION_final"]]

        if len(link_changes_df) != 0:
            link_changes_df = _consolidate_actions(
                link_changes_df, self.base_roadway_network.links_df, ["A", "B"]
            )

        if len(node_changes_df) != 0:
            node_changes_df = _consolidate_actions(
                node_changes_df, self.base_roadway_network.nodes_df, ["model_node_id"]
            )

            # print error message for node change and node deletion
            if (
                len(node_changes_df[node_changes_df.OPERATION_final.isin(["C", "D"])])
                > 0
            ):
                msg = "NODE changes and deletions are not allowed!"
                WranglerLogger.error(msg)
                raise ValueError(msg)
            node_add_df = node_changes_df[node_changes_df.OPERATION_final == "A"]
        else:
            node_add_df = pd.DataFrame()

        # process deletions
        WranglerLogger.debug("Processing link deletions")

        cube_delete_df = link_changes_df[link_changes_df.OPERATION_final == "D"]
        if cube_delete_df.shape[1] > 0:
            links_to_delete = cube_delete_df["model_link_id"].tolist()
            delete_link_dict = {
                "category": "Roadway Deletion",
                "links": {"model_link_id": links_to_delete},
            }
            WranglerLogger.debug("{} Links Deleted.".format(len(links_to_delete)))
        else:
            delete_link_dict = None
            WranglerLogger.debug("No link deletions processed")

        # process additions
        WranglerLogger.debug("Processing link additions")
        cube_add_df = link_changes_df[link_changes_df.OPERATION_final == "A"]
        if cube_add_df.shape[1] > 0:
            if limit_variables_to_existing_network:
                add_col = [
                    c
                    for c in cube_add_df.columns
                    if c in self.base_roadway_network.links_df.columns
                ]
            else:
                add_col = cube_add_df.columns
                # can leave out "OPERATION_final" from writing out, is there a reason to write it out?

            add_link_properties = cube_add_df[add_col].to_dict("records")

            # WranglerLogger.debug("Add Link Properties: {}".format(add_link_properties))
            WranglerLogger.debug("{} Links Added".format(len(add_link_properties)))

            add_link_dict = {"category": "New Roadway", "links": add_link_properties}
        else:
            WranglerLogger.debug("No link additions processed")
            add_link_dict = {}

        if len(node_add_df):
            add_nodes_dict_list = node_add_df.drop(["OPERATION_final"], axis=1).to_dict(
                "records"
            )
            WranglerLogger.debug("{} Nodes Added".format(len(add_nodes_dict_list)))
            add_link_dict["nodes"] = add_nodes_dict_list
        else:
            WranglerLogger.debug("No Nodes Added")
            node_dict_list = None

        # process changes
        WranglerLogger.debug("Processing changes")

        changeable_col = [
            x
            for x in link_changes_df.columns
            if x in self.base_roadway_network.links_df.columns
        ]

        change_link_dict_df = pd.DataFrame()

        for index, change_row in link_changes_df[
            link_changes_df.OPERATION_final == "C"
        ].iterrows():

            base_df = self.base_roadway_network.links_df[
                (self.base_roadway_network.links_df["A"] == change_row.A)
                & (self.base_roadway_network.links_df["B"] == change_row.B)
            ]

            if not base_df.shape[0]:
                msg = "No match found in network for AB combination: ({},{}). Incompatible base network.".format(
                    change_row.A, change_row.B
                )
                WranglerLogger.error(msg)
                raise ValueError(msg)

            if base_df.shape[0] > 1:
                WranglerLogger.warning(
                    "Found more than one match in base network for AB combination: ({},{}). Selecting first one to operate on but AB should be unique to network.".format(
                        row.A, row.B
                    )
                )
            base_row = base_df.iloc[0]

            out_col = []
            for col in changeable_col:
                # if it is the same as before, or a static value, don't process as a change
                if (str(change_row[col]) == base_row[col].astype(str)) | (
                    col in Project.STATIC_VALUES
                ):
                    continue
                # only look at distance if it has significantly changed
                if col == "distance":
                    if (
                        abs(
                            (change_row[col] - base_row[col].astype(float))
                            / base_row[col].astype(float)
                        )
                        > 0.01
                    ):
                        out_col.append(col)
                    else:
                        continue
                else:
                    out_col.append(col)

            property_dict_list = []
            for c in out_col:
                property_dict = {}
                property_dict["property"] = c
                property_dict["existing"] = base_row[c]
                property_dict["set"] = change_row[c]
                property_dict_list.append(property_dict)
            # WranglerLogger.debug("property_dict_list: {}".format(property_dict_list))
            # WranglerLogger.debug("base_df.model_link_id: {}".format(base_row['model_link_id']))
            card_df = pd.DataFrame(
                {
                    "properties": pd.Series([property_dict_list]),
                    "model_link_id": pd.Series(base_row["model_link_id"]),
                }
            )
            # WranglerLogger.debug('card_df: {}'.format(card_df))
            change_link_dict_df = pd.concat(
                [change_link_dict_df, card_df], ignore_index=True, sort=False
            )

        change_link_dict_df["properties"] = change_link_dict_df["properties"].astype(
            str
        )
        # WranglerLogger.debug('change_link_dict_df 1: {}'.format(change_link_dict_df))
        change_link_dict_df = (
            change_link_dict_df.groupby("properties")[["model_link_id"]]
            .agg(lambda x: list(x))
            .reset_index()
        )
        # WranglerLogger.debug('change_link_dict_df 2: {}'.format(change_link_dict_df))
        change_link_dict_df["facility"] = change_link_dict_df.apply(
            lambda x: {"link": {"model_link_id": x.model_link_id}}, axis=1
        )

        # WranglerLogger.debug('change_link_dict_df 3: {}'.format(change_link_dict_df))
        change_link_dict_df["properties"] = change_link_dict_df["properties"].apply(
            lambda x: json.loads(x.replace("'", '"'))
        )

        change_link_dict_list = change_link_dict_df[["facility", "properties"]].to_dict(
            "record"
        )

        for change in change_link_dict_list:
            change["category"] = "Roadway Attribute Change"

        WranglerLogger.debug("{} Changes Processed".format(len(change_link_dict_list)))

        highway_change_list = list(
            filter(None, [delete_link_dict] + [add_link_dict] + change_link_dict_list)
        )

        return highway_change_list
