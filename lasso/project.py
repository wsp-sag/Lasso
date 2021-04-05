import json
import os
from typing import Any, Dict, Optional, Union, List, Mapping, Collection

import pandas as pd
from pandas import DataFrame
import geopandas as gpd

from network_wrangler import ProjectCard
from network_wrangler import RoadwayNetwork

from .model_transit import ModelTransit
from .logger import WranglerLogger
from .parameters import Parameters
from .model_roadway import ModelRoadwayNetwork
from .utils import column_name_to_parts
from cube.cube_model_transit import CubeTransitWriter


class Project(object):
    """A single or set of changes to the roadway or transit system.

    Compares a base and a build transit network or a base and build
    highway network and produces project cards.

    .. highlight:: python

    Typical usage example:
    ::
        test_project = Project.create_project(
            base_transit_source=os.path.join(CUBE_DIR, "transit.LIN"),
            build_transit_source=os.path.join(CUBE_DIR, "transit_route_shape_change"),
        )
        test_project.evaluate_changes()
        test_project.write_project_card(
            os.path.join(SCRATCH_DIR, "t_transit_shape_test.yml")
        )

    Attributes:
        DEFAULT_PROJECT_NAME: a class-level constant that defines what
            the project name will be if none is set.
        STATIC_VALUES: a class-level constant which defines values that
            are not evaluated when assessing changes.
        card_data (dict):  {"project": <project_name>, "changes": <list of change dicts>}
        roadway_changes (DataFrame):  pandas dataframe of CUBE roadway changes.
        transit_changes (ModelTransit):
        base_roadway_network (RoadwayNetwork):
        base_transit_network (ModelTransit):
        build_transit_network (ModelTransit):
        project_name (str): name of the project, set to DEFAULT_PROJECT_NAME if not provided
        parameters: an  instance of the Parameters class which sets a bunch of parameters
    """

    DEFAULT_PROJECT_NAME = "USER TO define"

    STATIC_VALUES = [
        "model_link_id",
        "area_type",
        "county",
        # "assign_group",
        "centroidconnect",
    ]
    CALCULATED_VALUES = ["area_type", "county", "assign_group", "centroidconnect"]

    def __init__(
        self,
        roadway_changes: Optional[DataFrame] = None,
        transit_changes: Optional[ModelTransit] = None,
        base_roadway_network: Optional[RoadwayNetwork] = None,
        base_transit_network: Optional[ModelTransit] = None,
        build_transit_network: Optional[ModelTransit] = None,
        project_name: Optional[str] = "",
        evaluate: Optional[bool] = False,
        parameters: Parameters = None,
        parameters_dict: dict = {},
        **kwargs,
    ):
        """
        ProjectCard constructor.

        args:
            roadway_changes: dataframe of roadway changes read from a log file
            transit_changes:
            base_roadway_network: RoadwayNetwork instance for base case
            base_transit_network: ModelTransit instance for base transit network
            build_transit_network: ModelTransit instance for build transit network
            project_name: name of the project
            evaluate: defaults to false, but if true, will create card data
            parameters: dictionary of parameter settings (see Parameters class) or
                an instance of Parameters. If not specified, will use default parameters.

        returns: instance of ProjectCard
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

        if parameters:
            WranglerLogger.debug(
                "Project.__init__(): using passed in Parameters instance."
            )
            # WranglerLogger.debug("[.read().parameters] {}".format(parameters))
            self.parameters = parameters.update(update_dict=parameters_dict, **kwargs)
            # WranglerLogger.debug("[.read()._parameters] {}".format(_parameters))
        else:
            WranglerLogger.debug(
                "Project.__init__(): initializing Parameters instance with: \n{}".format(
                    parameters_dict
                )
            )
            self.parameters = Parameters.initialize(input_ps=parameters_dict, **kwargs)

        if base_roadway_network is not None:
            self.determine_roadway_network_changes_compatability(
                self.base_roadway_network, self.roadway_changes, self.parameters
            )

        if evaluate:
            self.evaluate_changes()

    def write_project_card(self, filename: str = None):
        """
        Writes project cards.

        Args:
            filename (str): File path to output .yml

        Returns:
            None
        """
        ProjectCard(self.card_data).write(out_filename=filename)

    @staticmethod
    def create_project(
        roadway_log_file: Union[str, List[str], None] = None,
        roadway_shp_file: Optional[str] = None,
        roadway_csv_file: Optional[str] = None,
        base_roadway_dir: Optional[str] = None,
        base_transit_source: Optional[str] = None,
        build_transit_source: Optional[str] = None,
        roadway_changes: Optional[DataFrame] = None,
        transit_changes: Optional[ModelTransit] = None,
        base_roadway_network: Optional[RoadwayNetwork] = None,
        base_transit_network: Optional[ModelTransit] = None,
        build_transit_network: Optional[ModelTransit] = None,
        project_name: Optional[str] = None,
        recalculate_calculated_variables: Optional[bool] = False,
        recalculate_distance: Optional[bool] = False,
        parameters_dict: Optional[dict] = {},
        parameters: Optional[Parameters] = None,
        **kwargs,
    ):
        """
        Constructor for a Project instance.

        Args:
            roadway_log_file: File path to consuming logfile or a list of logfile paths.
            roadway_shp_file: File path to consuming shape file for roadway changes.
            roadway_csv_file: File path to consuming csv file for roadway changes.
            base_roadway_dir: Folder path to base roadway network.
            base_transit_source: Folder path to base transit network or cube line file string.
            base_transit_file: File path to base transit network.
            build_transit_source: Folder path to build transit network or cube line file string.
            build_transit_file: File path to build transit network.
            roadway_changes: pandas dataframe of CUBE roadway changes.
            transit_changes: build transit changes.
            base_roadway_network: Base roadway network object.
            base_transit_network: Base transit network object.
            build_transit_network: Build transit network object.
            project_name:  If not provided, will default to the roadway_log_file filename if
                provided (or the first filename if a list is provided)
            recalculate_calculated_variables: if reading in a base network, if this is true it
                will recalculate variables such as area type, etc. This only needs to be true
                if you are creating project cards that are changing the calculated variables.
            recalculate_distance:  recalculate the distance variable. This only needs to be
                true if you are creating project cards that change the distance.
            parameters: dictionary of parameters
            crs (int): coordinate reference system, ESPG number
            node_foreign_key (str):  variable linking the node table to the link table
            link_foreign_key (list): list of variable linking the link table to the node
                foreign key
            shape_foreign_key (str): variable linking the links table and shape table
            unique_link_ids (list): list of variables unique to each link
            unique_node_ids (list): list of variables unique to each node
            modes_to_network_link_variables (dict): Mapping of modes to link variables in
                the network
            modes_to_network_nodes_variables (dict): Mapping of modes to node variables
                in the network
            managed_lanes_node_id_scalar (int): Scalar values added to primary keys for nodes for
                corresponding managed lanes.
            managed_lanes_link_id_scalar (int): Scalar values added to primary keys for links for
                corresponding managed lanes.
            managed_lanes_required_attributes (list): attributes that must be specified in managed
                lane projects.
            keep_same_attributes_ml_and_gp (list): attributes to copy to managed lanes from
                parallel general purpose lanes.

        Returns:
            A Project instance.
        """
        if parameters:
            WranglerLogger.debug(
                "create_project(): using passed in Parameters instance."
            )
            # WranglerLogger.debug("[.read().parameters] {}".format(parameters))
            _parameters = parameters.update(update_dict=parameters_dict, **kwargs)
            # WranglerLogger.debug("[.read()._parameters] {}".format(_parameters))
        else:
            WranglerLogger.debug(
                "create_project(): initializing Parameters instance with: \n{}".format(
                    parameters_dict
                )
            )
            _parameters = Parameters.initialize(input_ps=parameters_dict, **kwargs)

        if base_transit_source and base_transit_network:
            msg = "Method takes only one of 'base_transit_source' and 'base_transit_network'\
                but both given"
            WranglerLogger.error(msg)
            raise ValueError(msg)
        if base_transit_source:
            base_transit_network = ModelTransit.create_from_source(base_transit_source)
            WranglerLogger.debug(
                "Base network has {} lines".format(len(base_transit_network.lines))
            )
            if len(base_transit_network.lines) <= 10:
                WranglerLogger.debug(
                    "Base network lines: {}".format(
                        "\n - ".join(base_transit_network.lines)
                    )
                )
        elif base_transit_network:
            pass
        else:
            msg = "No base transit network."
            WranglerLogger.info(msg)
            base_transit_network = None

        if build_transit_source and transit_changes:
            msg = "Method takes only one of 'build_transit_source'\
                and 'transit_changes' but both given"
            WranglerLogger.error(msg)
            raise ValueError(msg)
        if build_transit_source:
            WranglerLogger.debug("build")
            __import__(_parameters.transit_network_ps.model_transit_class)
            transit_params = _parameters.transit_network_ps
            build_transit_network = transit_params.model_transit_class.create_from_source(
                build_transit_source
            )
            WranglerLogger.debug(
                "Build network has {} lines".format(len(build_transit_network.lines))
            )
            if len(build_transit_network.lines) <= 10:
                WranglerLogger.debug(
                    "Build network lines: {}".format(
                        "\n - ".join(build_transit_network.lines)
                    )
                )
        elif transit_changes:
            pass
        else:
            msg = "No transit changes given or processed."
            WranglerLogger.info(msg)
            transit_changes = None

        if roadway_log_file and roadway_changes:
            msg = "Method takes only one of 'roadway_log_file' and 'roadway_changes'\
                but both given"
            WranglerLogger.error(msg)
            raise ValueError(msg)
        if roadway_shp_file and roadway_changes:
            msg = "Method takes only one of 'roadway_shp_file' and 'roadway_changes'\
                but both given"
            WranglerLogger.error(msg)
            raise ValueError(msg)
        if roadway_csv_file and roadway_changes:
            msg = "Method takes only one of 'roadway_csv_file' and 'roadway_changes'\
                but both given"
            WranglerLogger.error(msg)
            raise ValueError(msg)
        if roadway_log_file and roadway_csv_file:
            msg = "Method takes only one of 'roadway_log_file' and 'roadway_csv_file'\
                but both given"
            WranglerLogger.error(msg)
            raise ValueError(msg)
        if roadway_shp_file and roadway_csv_file:
            msg = "Method takes only one of 'roadway_shp_file' and 'roadway_csv_file'\
                but both given"
            WranglerLogger.error(msg)
            raise ValueError(msg)
        if roadway_log_file and roadway_shp_file:
            msg = "Method takes only one of 'roadway_log_file' and 'roadway_shp_file'\
                but both given"
            WranglerLogger.error(msg)
            raise ValueError(msg)
        if roadway_log_file and not project_name:
            if type(roadway_log_file) == list:
                project_name = os.path.splitext(os.path.basename(roadway_log_file[0]))[
                    0
                ]
                WranglerLogger.info(
                    "No Project Name - Using name of first log file in list"
                )
            else:
                project_name = os.path.splitext(os.path.basename(roadway_log_file))[0]
                WranglerLogger.info("No Project Name - Using name of log file")
        if roadway_log_file:
            roadway_changes = Project.read_logfile(roadway_log_file)
        elif roadway_shp_file:
            roadway_changes = gpd.read_file(roadway_shp_file)
            roadway_changes = DataFrame(roadway_changes.drop("geometry", axis=1))
            roadway_changes["model_node_id"] = 0
        elif roadway_csv_file:
            roadway_changes = pd.read_csv(roadway_csv_file)
            roadway_changes["model_node_id"] = 0
        elif roadway_changes:
            pass
        else:
            msg = "No roadway changes given or processed."
            WranglerLogger.info(msg)
            roadway_changes = pd.DataFrame({})

        if base_roadway_network and base_roadway_dir:
            msg = "Method takes only one of 'base_roadway_network' and 'base_roadway_dir'\
                but both given"
            WranglerLogger.error(msg)
            raise ValueError(msg)
        if base_roadway_dir:
            __import__(_parameters.roadway_network_ps.model_roadway_class)
            base_roadway_network = _parameters.roadway_network_ps.model_roadway_class.read(
                os.path.join(base_roadway_dir, "link.json"),
                os.path.join(base_roadway_dir, "node.geojson"),
                os.path.join(base_roadway_dir, "shape.geojson"),
                fast=True,
                recalculate_calculated_variables=recalculate_calculated_variables,
                recalculate_distance=recalculate_distance,
                split_properties=True,
                parameters=_parameters,
                **kwargs,
            )
        elif base_roadway_network:
            pass
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
            parameters=_parameters,
        )

        return project

    @staticmethod
    def read_logfile(logfilename: Union[str, List[str]]) -> DataFrame:
        """
        Reads a Cube log file and returns a dataframe of roadway_changes

        Args:
            logfilename (str or list[str]): File path to CUBE logfile or list of logfile paths.

        Returns:
            A DataFrame reprsentation of the log file.
        """
        if type(logfilename) == str:
            logfilename = [logfilename]

        link_df = pd.DataFrame()
        node_df = pd.DataFrame()

        for file in logfilename:
            WranglerLogger.info("Reading logfile: {}".format(file))
            with open(file) as f:
                _content = f.readlines()

                _node_lines = [
                    x.strip().replace(";", ",") for x in _content if x.startswith("N")
                ]
                WranglerLogger.debug("node lines: {}".format(_node_lines))
                _link_lines = [
                    x.strip().replace(";", ",") for x in _content if x.startswith("L")
                ]
                WranglerLogger.debug("link lines: {}".format(_link_lines))

                _nodecol = ["OBJECT", "OPERATION", "GROUP"] + _node_lines[0].split(",")[
                    1:
                ]
                WranglerLogger.debug("Node Cols: {}".format(_nodecol))
                _linkcol = ["OBJECT", "OPERATION", "GROUP"] + _link_lines[0].split(",")[
                    1:
                ]
                WranglerLogger.debug("Link Cols: {}".format(_linkcol))

                _node_df = pd.DataFrame(
                    [x.split(",") for x in _node_lines[1:]], columns=_nodecol
                )
                WranglerLogger.debug("Node DF: {}".format(_node_df))
                _link_df = pd.DataFrame(
                    [x.split(",") for x in _link_lines[1:]], columns=_linkcol
                )
                WranglerLogger.debug("Link DF: {}".format(_link_df))

                node_df = pd.concat([node_df, _node_df])
                link_df = pd.concat([link_df, _link_df])

        log_df = pd.concat([link_df, node_df], ignore_index=True, sort=False)

        # CUBE logfile headers for string fields: NAME[111] instead of NAME, need to shorten that
        log_df.columns = [c.split("[")[0] for c in log_df.columns]

        WranglerLogger.info(
            "Processed {} Node lines and {} Link lines".format(
                node_df.shape[0], link_df.shape[0]
            )
        )

        return log_df

    @staticmethod
    def determine_roadway_network_changes_compatability(
        base_roadway_network: ModelRoadwayNetwork,
        roadway_changes: DataFrame,
        parameters: Parameters,
    ):
        """
        Checks to see that any links or nodes that change exist in base roadway network.
        """
        WranglerLogger.info(
            """Evaluating compatibility between roadway network changes and base network.
            Not evaluating deletions."""
        )

        # CUBE log file saves all variable names in upper cases, need to convert them to
        # be same as network
        log_to_net_df = pd.read_csv(parameters.log_to_net_crosswalk)
        log_to_net_dict = dict(zip(log_to_net_df["log"], log_to_net_df["net"]))

        dbf_to_net_df = pd.read_csv(parameters.net_to_dbf_crosswalk)
        dbf_to_net_dict = dict(zip(dbf_to_net_df["dbf"], dbf_to_net_df["net"]))

        roadway_changes.rename(columns=log_to_net_dict, inplace=True)
        roadway_changes.rename(columns=dbf_to_net_dict, inplace=True)

        # for links "L"  that change "C",
        # find locations where there isn't a base roadway link

        link_changes_df = roadway_changes[
            (roadway_changes.OBJECT == "L") & (roadway_changes.OPERATION == "C")
        ]

        link_merge_df = pd.merge(
            link_changes_df[["A", "B"]].astype(str),
            base_roadway_network.links_df[["A", "B", "model_link_id"]].astype(str),
            how="left",
            on=["A", "B"],
        )

        missing_links = link_merge_df.loc[link_merge_df["model_link_id"].isna()]

        if missing_links.shape[0]:
            msg = "Network missing the following AB links:\n{}".format(missing_links)
            WranglerLogger.error(msg)
            raise ValueError(msg)

        # for links "N"  that change "C",
        # find locations where there isn't a base roadway node

        node_changes_df = roadway_changes[
            (roadway_changes.OBJECT == "N") & (roadway_changes.OPERATION == "C")
        ]
        node_merge_df = pd.merge(
            node_changes_df[["model_node_id"]],
            base_roadway_network.nodes_df[["model_node_id", "geometry"]],
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
        Determines which changes should be evaluated, initiates
        self.card_data to be an aggregation of transit and highway changes.
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

        Args:
            limit_variables_to_existing_network (bool): True if no ad-hoc variables.
                Default to False.
        """

        for c in self.parameters.string_col:
            if c in self.roadway_changes.columns:
                self.roadway_changes[c] = self.roadway_changes[c].str.lstrip(" ")

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

        def _process_deletions(link_changes_df):
            """"""
            WranglerLogger.debug("Processing link deletions")

            cube_delete_df = link_changes_df[link_changes_df.OPERATION_final == "D"]
            if len(cube_delete_df) > 0:
                links_to_delete = cube_delete_df["model_link_id"].tolist()
                delete_link_dict = {
                    "category": "Roadway Deletion",
                    "links": {"model_link_id": links_to_delete},
                }
                WranglerLogger.debug("{} Links Deleted.".format(len(links_to_delete)))
            else:
                delete_link_dict = None
                WranglerLogger.debug("No link deletions processed")

            return delete_link_dict

        def _process_link_additions(
            link_changes_df, limit_variables_to_existing_network
        ):
            """"""
            WranglerLogger.debug("Processing link additions")
            cube_add_df = link_changes_df[link_changes_df.OPERATION_final == "A"]
            if not cube_add_df.shape[1]:
                WranglerLogger.debug("No link additions processed")
                return {}

            if limit_variables_to_existing_network:
                add_col = [
                    c
                    for c in cube_add_df.columns
                    if c in self.base_roadway_network.links_df.columns
                ]
            else:
                add_col = [
                    c for c in cube_add_df.columns if c not in ["OPERATION_final"]
                ]
                # can leave out "OPERATION_final" from writing out,
                # is there a reason to write it out?

            add_link_properties = cube_add_df[add_col].to_dict("records")

            # WranglerLogger.debug("Add Link Properties: {}".format(add_link_properties))
            WranglerLogger.debug("{} Links Added".format(len(add_link_properties)))

            return {"category": "Add New Roadway", "links": add_link_properties}

        def _process_node_additions(node_add_df):
            """"""
            WranglerLogger.debug("Processing node additions")

            if not node_add_df.shape[1]:
                WranglerLogger.debug("No node additions processed")
                return []

            add_nodes_dict_list = node_add_df.drop(["OPERATION_final"], axis=1).to_dict(
                "records"
            )
            WranglerLogger.debug("{} Nodes Added".format(len(add_nodes_dict_list)))

            return add_nodes_dict_list

        def _process_single_link_change(change_row, changeable_col):
            """"""

            #  1. Find associated base year network values
            base_df = self.base_roadway_network.links_df[
                (self.base_roadway_network.links_df["A"] == change_row.A)
                & (self.base_roadway_network.links_df["B"] == change_row.B)
            ]

            if not base_df.shape[0]:
                msg = f"""No match found in network for AB combination:
                    ({change_row.A},{change_row.B}).
                    Incompatible base network."""
                WranglerLogger.error(msg)
                raise ValueError(msg)

            elif base_df.shape[0] > 1:
                WranglerLogger.warning(
                    f"""Found more than one match in base network for AB combination:
                        ({change_row.A},{change_row.B}).
                        Selecting first one to operate on but AB should be unique to network."""
                )

            base_row = base_df.iloc[0]
            # WranglerLogger.debug("Properties with changes: {}".format(changeable_col))

            # 2. find columns that changed (enough)
            changed_col = []
            for col in changeable_col:
                WranglerLogger.debug("Assessing Column: {}".format(col))
                # if it is the same as before, or a static value, don't process as a change
                if str(change_row[col]) == str(base_row[col]):
                    continue
                if (col == "roadway_class") & (change_row[col] == 0):
                    continue
                # only look at distance if it has significantly changed
                if col == "distance":
                    if (
                        abs(
                            (change_row[col] - float(base_row[col]))
                            / base_row[col].astype(float)
                        )
                        > 0.01
                    ):
                        changed_col.append(col)
                    else:
                        continue
                else:
                    changed_col.append(col)

            WranglerLogger.debug(
                "Properties with changes that will be processed: {}".format(changed_col)
            )

            if not changed_col:
                return pd.DataFrame()

            # 3. Iterate through columns with changed values and structure the changes
            # as expected in  project card
            property_dict_list = []
            processed_properties = []
            for c in changed_col:
                # WranglerLogger.debug("Processing Column: {}".format(c))
                (
                    p_base_name,
                    p_time_period,
                    p_category,
                    _managed_lane,
                ) = column_name_to_parts(c)

                _d = {"existing": base_row[c], "set": change_row[c]}
                if c in Project.CALCULATED_VALUES:
                    _d = {"set": change_row[c]}
                if p_time_period:
                    _d["time"] = list(
                        self.parameters.network_ps.time_period_to_time[p_time_period]
                    )
                    if p_category:
                        _d["category"] = p_category

                # iterate through existing properties that have been changed and see if
                # you should just add
                if p_base_name in processed_properties:
                    for processed_p in property_dict_list:
                        if processed_p["property"] == p_base_name:
                            processed_p["timeofday"] += [_d]
                elif p_time_period:
                    property_dict = {"property": p_base_name, "timeofday": [_d]}
                    processed_properties.append(p_base_name)
                    property_dict_list.append(property_dict)
                else:
                    _d["property"] = c
                    processed_properties.append(_d["property"])
                    property_dict_list.append(_d)

            card_df = pd.DataFrame(
                {
                    "properties": pd.Series([property_dict_list]),
                    "model_link_id": pd.Series(base_row["model_link_id"]),
                }
            )

            # WranglerLogger.debug('single change card_df:\n {}'.format(card_df))

            return card_df

        def _process_link_changes(link_changes_df, changeable_col):
            """"""
            cube_change_df = link_changes_df[link_changes_df.OPERATION_final == "C"]
            if not cube_change_df.shape[0]:
                WranglerLogger.info("No link changes processed")
                return []

            change_link_dict_df = pd.DataFrame(columns=["properties", "model_link_id"])

            for _, row in cube_change_df.iterrows():
                card_df = _process_single_link_change(row, changeable_col)

                change_link_dict_df = pd.concat(
                    [change_link_dict_df, card_df], ignore_index=True, sort=False
                )

            if not change_link_dict_df.shape[0]:
                WranglerLogger.info("No link changes processed")
                return []

            # msg = f'change_link_dict_df Unaggregated:\n {change_link_dict_df}'
            # WranglerLogger.debug(msg)

            # Have to change to string so that it is a hashable type for the aggregation
            change_link_dict_df["properties"] = change_link_dict_df[
                "properties"
            ].astype(str)
            # Group the changes that are the same
            change_link_dict_df = (
                change_link_dict_df.groupby("properties")[["model_link_id"]]
                .agg(lambda x: list(x))
                .reset_index()
            )
            # msg = f"change_link_dict_df Aggregated:\n {change_link_dict_df}"
            # WranglerLogger.debug(msg)

            # Reformat model link id to correct "facility" format
            change_link_dict_df["facility"] = change_link_dict_df.apply(
                lambda x: {"link": [{"model_link_id": x.model_link_id}]}, axis=1
            )

            # msg = f"change_link_dict_df 3: {change_link_dict_df}"
            # WranglerLogger.debug(msg)
            change_link_dict_df["properties"] = change_link_dict_df["properties"].apply(
                lambda x: json.loads(
                    x.replace("'\"", "'").replace("\"'", "'").replace("'", '"')
                )
            )

            change_link_dict_df["category"] = "Roadway Property Change"

            change_link_dict_list = change_link_dict_df[
                ["category", "facility", "properties"]
            ].to_dict("record")

            WranglerLogger.debug(
                "{} Changes Processed".format(len(change_link_dict_list))
            )
            return change_link_dict_list

        def _consolidate_actions(log, base, key_list):
            log_df = log.copy()
            # will be changed if to allow new variables being added/changed
            # that are not in base network
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
        delete_link_dict = _process_deletions(link_changes_df)

        # process additions
        add_link_dict = _process_link_additions(
            link_changes_df, limit_variables_to_existing_network
        )
        add_link_dict["nodes"] = _process_node_additions(node_add_df)

        # process changes
        WranglerLogger.debug("Processing changes")
        WranglerLogger.debug(link_changes_df)
        changeable_col = list(
            (
                set(link_changes_df.columns)
                & set(self.base_roadway_network.links_df.columns)
            )
            - set(Project.STATIC_VALUES)
        )

        cols_in_changes_not_in_net = list(
            set(link_changes_df.columns)
            - set(self.base_roadway_network.links_df.columns)
        )

        if cols_in_changes_not_in_net:
            WranglerLogger.warning(
                f"""The following attributes are specified in the changes but
                    do not exist in the base network: {cols_in_changes_not_in_net}"""
            )

        change_link_dict_list = _process_link_changes(link_changes_df, changeable_col)

        # combine together

        highway_change_list = list(
            filter(None, [delete_link_dict] + [add_link_dict] + change_link_dict_list)
        )

        return highway_change_list


def new_transit_route_change_dict(
    route_row: Union[pd.Series, Mapping], transit_route_property_list, shapes: dict
) -> Mapping:
    """Processes a row of a pandas dataframe or a dictionary with the fields:
    - name
    - direction_id
    - start_time_HHMM
    - end_time_HHMM
    - agency_id
    - routing
    - + all fields in route_property_list

    Args:
        route_row (pd.Series): [description]
        shapes[]
    """
    routing_properties = {
        "property": "routing",
        "set": shapes[route_row["name"]]["node"].tolist(),
    }

    transit_route_properties = [
        {"property": p, "set": route_row[p]} for p in transit_route_property_list
    ]

    add_transit_card_dict = {
        "category": "New Transit Service",
        "facility": {
            "route_id": route_row.name,
            "direction_id": route_row.direction_id,
            "start_time": route_row.start_time_HHMM,
            "end_time": route_row.end_time_HHMM,
            "agency_id": route_row.agency_id,
        },
        "properties": transit_route_properties + [routing_properties],
    }

    WranglerLogger.debug(f"Adding transit line: {route_row.name}")

    return add_transit_card_dict


def delete_route_change_dict(route_row: Union[pd.Series, Mapping]) -> Mapping:
    """
    Creates a project card change formatted dictionary for deleting a line.

    Args:
        route_row: row of df with line to be deleted or a dict with following attributes:
        - name
        - direction_id
        - start_time_HHMM
        - end_time_HHMM

    Returns:
        A project card change-formatted dictionary for the route deletion.
    """

    delete_card_dict = {
        "category": "Delete Transit Service",
        "facility": {
            "route_id": route_row.name,
            "direction_id": route_row.direction_id,
            "start_time": route_row.start_time_HHMM,
            "end_time": route_row.end_time_HHMM,
        },
    }

    WranglerLogger.debug(f"Deleting transit line: {route_row.name}")

    return delete_card_dict


def update_route_prop_change_dict(
    compare_route_row: pd.Series, include_existing: bool = False
) -> Collection[Mapping]:
    """[summary]

    Args:
        compare_route_row (pd.Series): row of df with transit route to be deleted or
            a dict with following attributes:
        include_existing (bool, optional): If set to True, will include 'existing'
            in project card.

    Returns:
        Collection[Mapping[str]]: [description]
    """
    compare_route_row = compare_route_row.dropna(how="any")

    _properties_update_list = []
    for p in compare_route_row.index.get_level_values(0):
        change_item = {}
        change_item["property"] = p
        change_item["set"] = compare_route_row[p, "other"]
        if include_existing:
            change_item["existing"] = compare_route_row[p, "self"]

        _properties_update_list.append(change_item)
    return _properties_update_list


def update_route_routing_change_dict(
    existing_routing_df: Collection[Any], set_routing_df: Collection[Any],
) -> Mapping[str, Any]:
    """Format route changes for project cards. Right now, this matches
        the formatting for cube nodes. Could change in future.

    Args:
        existing_routing (Collection[Any]): [description]
        set_routing (Collection[Any]): [description]
        match_id (Union[str,Collection[str]]): [description]

    Returns:
        Mapping[str,Any]: [description]
    """
    match_id = list(existing_routing_df.columns)
    if match_id != ["N", "stop"]:
        raise NotImplementedError(f"Expecting match_id ['N','stop']; got {match_id}")

    if list(existing_routing_df.columns) != list(set_routing_df.columns):
        raise (
            f"Columns for existing and set don't match.\n\
            Existing: {existing_routing_df.columns}\n\
            Set: {set_routing_df.columns}"
        )

    existing_str = CubeTransitWriter._nodes_df_to_cube_node_strings(existing_routing_df)
    set_str = CubeTransitWriter._nodes_df_to_cube_node_strings(existing_routing_df)
    WranglerLogger.debug(
        f"Existing Str: {existing_str}\n\
        Set Str: {set_str}"
    )

    shape_change_dict = {
        "property": "routing",
        "existing": existing_str,
        "set": set_str,
    }
    return shape_change_dict
