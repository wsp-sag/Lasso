import re
from typing import Any, Dict, Optional

import pandas as pd
from pandas import DataFrame
import json

from network_wrangler import RoadwayNetwork
from network_wrangler import ProjectCard
import os


from .transit import CubeTransit

class Project(object):
    def __init__(
        self,
        roadway_changes: Optional[DataFrame] = None,
        transit_changes: Optional[CubeTransit] = None,
        base_roadway_network: Optional[RoadwayNetwork] = None,
        base_transit_network: Optional[CubeTransit] = None,
        evaluate =  False
        ):
        """

        """
        self.card_data = Dict[str, Dict[str, Any]]

        self.roadway_changes = roadway_changes
        self.base_roadway_network = base_roadway_network
        self.base_transit_network = base_transit_network
        self.transit_changes = transit_changes

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

    @staticmethod
    def create_project(
        roadway_log_file: Optional[str] = None,
        base_roadway_dir: Optional[str] = None,
        base_transit_dir: Optional[str] = None,
        build_transit_dir: Optional[str] = None,
        roadway_changes: Optional[DataFrame] = None,
        transit_changes: Optional[CubeTransit] = None,
        base_roadway_network: Optional[RoadwayNetwork] = None,
        base_transit_network: Optional[CubeTransit] = None):
        """

        Parameters
        -----------
        roadway_log_file : str
            filename for consuming logfile

        Returns
        -------
        Project object
        """

        if build_transit_dir and transit_changes:
            raise("only need one base roadway file")
        if build_transit_dir:
            transit_changes = CubeTransit(build_transit_dir)
        else:
            transit_changes = pd.DataFrame({})

        if roadway_log_file and roadway_changes:
            raise("only need one roadway changes file")
        if roadway_log_file:
            roadway_changes = Project.read_logfile(roadway_log_file)
        else:
            roadway_changes = pd.DataFrame({})

        if base_roadway_network and base_roadway_dir:
            raise("only need one base roadway file")
        if base_roadway_dir:
            base_roadway_network = RoadwayNetwork.read(os.path.join(base_roadway_dir,"link.json"),
                                                    os.path.join(base_roadway_dir,"node.geojson"),
                                                    os.path.join(base_roadway_dir,"shape.geojson"),
                                                    True)
        else:
            base_roadway_network = None

        if base_transit_dir and base_transit_network:
            raise("only need one base roadway file")
        if base_transit_dir:
            base_transit_network = TransitNetwork(base_transit_dir)
        else:
            base_transit_network = None

        project = Project(
            roadway_changes = roadway_changes,
            transit_changes = transit_changes,
            base_roadway_network = base_roadway_network,
            base_transit_network = base_transit_network,
            evaluate = True
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
        ##TODO Sijia
        with open(logfilename) as f:
            content = f.readlines()

        # (content[0].startswith("HighwayLayerLogX")):
        try:
            if(content[0].startswith("HighwayLayerLogX")):

                NodeLines = [x.strip() for x in content if x.startswith("N")]

                LinkLines = [x.strip() for x in content if x.startswith("L")]

                linkcol_names = ["OBJECT", "OPERATION", "GROUP"] + LinkLines[0].split(",")[1:]

                nodecol_names = ["OBJECT", "OPERATION", "GROUP"] + NodeLines[0].split(",")[1:]

                link_df = DataFrame(data = [re.split(",|;", x) for x in LinkLines[1:]],
                                    columns = linkcol_names)

                node_df = DataFrame(data = [re.split(",|;", x) for x in NodeLines[1:]],
                                    columns = nodecol_names)

                log_df = pd.concat([link_df, node_df], ignore_index = True, sort = False)

                return log_df

            else:

                return DataFrame()

        except:

            #pass
            return DataFrame()


    def evaluate_changes(self):
        """
        Determines which changes should be evaluated.
        """

        if not self.roadway_changes.empty:
            self.add_highway_changes()

        if not self.transit_changes.empty:
            self.add_transit_changes()


    def add_transit_changes(self):
        """
        Evaluates changes between base and build transit objects and
        adds entries into the self.card_data dictionary.
        """
        ## TODO Sijia
        ## should do comparisons in transit.py

        pass

    def add_highway_changes(self):
        """
        Evaluates changes from the log file based on the base highway object and
        adds entries into the self.card_data dictionary.
        """
        ## TODO Sijia
        ## if worth it, could also add some functionality  to network wrangler itself.
        base_links_df = self.base_roadway_network.links_df
        base_links_df.drop("area", axis = 1, inplace = True)

        base_nodes_df = self.base_roadway_network.nodes_df

        base_nodes_df.rename(columns = {"x" : "X",
                                        "y" : "Y"},
                                inplace = True)

        changes_df = self.roadway_changes

        node_changes_df = changes_df[changes_df.OBJECT == "N"].copy()

        link_changes_df = changes_df[changes_df.OBJECT == "L"].copy()

        link_changes_df.rename(columns = {"ISDRIVELIN" : "isDriveLink",
                                          "ISWALKLINK" : "isWalkLink",
                                          "ISBIKELINK" : "isBikeLink",
                                          "ISTRANLINK" : "isTranLink",
                                          "ISTRANACCE" : "isTranAcce"},
                                inplace = True)
        node_changes_df.rename(columns = {"ISDRIVENOD" : "isDriveNode",
                                          "N" :"travelModelId",
                                          "ISWALKNODE" : "isWalkNode",
                                          "ISTRANNODE" : "isTranNode",
                                          "ISBIKENODE" : "isBikeNode"},
                                inplace = True)

        def final_op(x):
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

        def consolidate_actions(log, base, key_list):
            log_df = log.copy()
            #log_df.columns = [x.upper() for x in log_df.columns]

            #base.columns = [x.upper() for x in base.columns]

            changeable_col = [x for x in log_df.columns if x in base.columns]

            for x in changeable_col:
                log_df[x] = log_df[x].astype(base[x].dtype)

            action_history_df = log_df.groupby(key_list)["OPERATION"].agg(lambda x: x.tolist()).rename("OPERATION_history").reset_index()

            log_df = pd.merge(log_df,
                                action_history_df,
                                on = key_list,
                                how = "left")
            log_df.drop_duplicates(subset = key_list,
                                    keep = "last",
                                    inplace = True)
            log_df['OPERATION_final'] = log_df.apply(lambda x: final_op(x),
                                                axis = 1)
            return log_df[changeable_col + ['OPERATION_final']]

        if len(link_changes_df) != 0:
            link_changes_df = consolidate_actions(link_changes_df,
                                                base_links_df,
                                                ['A', 'B'])


        if len(node_changes_df) != 0:
            node_changes_df = consolidate_actions(node_changes_df,
                                                    base_nodes_df,
                                                    ['travelModelId'])

            # print error message for node change and node deletion
            if len(node_changes_df[node_changes_df.OPERATION_final.isin(["C", "D"])]) > 0:
                print("\n ERROR: NODE changes and deletions are not allowed! \n")
            node_add_df = node_changes_df[node_changes_df.OPERATION_final == "A"]
        else:
            node_add_df = pd.DataFrame()

        # process deletions
        try:
            cube_delete_df = link_changes_df[link_changes_df.OPERATION_final == "D"]
            delete_link_df = cube_delete_df.copy()

            _links_dict = {"LINK_ID" : delete_link_df["LINK_ID"].tolist()}

            delete_link_dict = {"category" : "Roadway Deletion",
                                "links" : _links_dict}

            if len(delete_link_df) == 0:
                delete_link_dict = None
        except:
            delete_link_dict = None

        # process additions
        try:
            cube_add_df = link_changes_df[link_changes_df.OPERATION_final == "A"]

            add_col = [x for x in cube_add_df.columns if x in base_links_df.columns]
            add_link_dict_df = cube_add_df.copy()

            def prop_dict(x):
                d = {}
                for c in add_col:
                    d[c] = x[c]
                return d

            add_link_dict_df["properties"] = add_link_dict_df.apply(prop_dict,
                                                                        axis = 1)

            add_link_dict = {"category" : "New Roadway",
                            "links" : add_link_dict_df['properties'].tolist()}
        except:
            add_link_dict = None

        try:
            if len(node_add_df):
                node_dict_list = node_add_df.drop(['OPERATION_final'], axis = 1).to_dict("record")
            add_link_dict["nodes"] = node_dict_list
        except:
            node_dict_list = None

        # process changes
        try:
            changeable_col = [x for x in link_changes_df.columns if x in base_links_df.columns]

            cube_change_df = link_changes_df[link_changes_df.OPERATION_final == "C"]

            change_link_dict_df = pd.DataFrame()

            for i in cube_change_df.index:
                change_df = cube_change_df.loc[i]

                base_df = base_links_df[(base_links_df["A"] == change_df.A) &
                                        (base_links_df["B"] == change_df.B)].iloc[0]

                out_col = []
                for x in changeable_col:
                    if (change_df[x] == base_df[x]) | (x in ["AREA", "COUNTY", "ASGNGRP", "CENTROID"]):
                        continue
                    else:
                        out_col.append(x)

                property_dict_list = []
                for x in out_col:
                    property_dict = {}
                    property_dict["property"] = x
                    property_dict["existing"] = base_df[x]
                    property_dict["set"] = change_df[x]
                    property_dict_list.append(property_dict)

                card_df = pd.DataFrame({"properties":pd.Series([property_dict_list]),
                                    "A":pd.Series(base_df.A),
                                    "B":pd.Series(base_df.B),
                                    "LINK_ID":pd.Series(base_df.LINK_ID)})

                change_link_dict_df = pd.concat([change_link_dict_df,
                                            card_df],
                                            ignore_index = True,
                                            sort = False)

            change_link_dict_df["properties"] = change_link_dict_df["properties"].astype(str)
            change_link_dict_df = change_link_dict_df.groupby("properties")[["A", "B", "LINK_ID"]].agg(lambda x:list(x)).reset_index()

            change_link_dict_df["facility"] = change_link_dict_df.apply(lambda x: {"A" : {"travelModelId" : x.A},
                                                                                "B" : {"travelModelId" : x.B},
                                                                                "link" : {"LINK_ID" : x.LINK_ID}},
                                                                    axis = 1)

            change_link_dict_df["properties"] = change_link_dict_df["properties"].apply(lambda x: json.loads(x.replace("'", "\"")))

            change_link_dict_list = change_link_dict_df[["facility", "properties"]].to_dict("record")

            for change in change_link_dict_list:
                change["category"] = "Roadway Attribute Change"

        except:
            change_link_dict_list = []

        card_dict = {"project":"TO DO User Define",
                    "changes": list(filter(None, [delete_link_dict] + [add_link_dict] + change_link_dict_list))}

        self.card_data = card_dict

        pass
