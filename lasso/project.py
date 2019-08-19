from typing import Any, Dict
from network_wrangler import RoadwayNetwork
from network_wrangler import ProjectCard
from .transit import CubeTransit
import pandas as pd
import re

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

    def write_project_card(self):
        """

        Parameters
        -----------

        Returns
        -------
        """
        ## TODO SIJIA
        pass

    @staticmethod
    def create_project(
        roadway_log_file: Optional[str] = None,
        base_roadway_dir: Optional[str] = None,
        base_transit_dir: Optional[str] = None,
        build_transit_dir: Optional[str] = None,
        roadway_changes: Optional[DataFrame] = None,
        transit_changes: Optional[CubeTransit] = None,
        base_roadway_network: Optional[RoadwayNetwork] = None,
        base_transit_network: Optional[CubeTransit] = None) -> Project:
        """

        Parameters
        -----------
        roadway_log_file : str
            filename for consuming logfile

        Returns
        -------
        """

        if build_transit_dir and transit_changes:
            raise("only need one base roadway file")
        if build_transit_dir:
            transit_changes = CubeTransit(build_transit_dir)
        else:
            transit_changes = None

        if roadway_log_file and roadway_changes:
            raise("only need one roadway changes file")
        if roadway_log_file:
            roadway_changes = read_logfile(roadway_log_file)
        else:
            roadway_changes = None

        if base_roadway_network and base_roadway_dir:
            raise("only need one base roadway file")
        if base_roadway_dir:
            base_roadway_network = RoadwayNetwork(base_roadway_dir)
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
            )

        return project

    @staticmethod
    def read_logfile(self, logfilename: str) -> DataFrame:
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

        NodeLines = [x.strip() for x in content if x.startswith('N')]

        LinkLines = [x.strip() for x in content if x.startswith('L')]

        linkcol_names = ['OBJECT', 'OPERATION', 'GROUP'] + LinkLines[0].split(',')[1:]

        nodecol_names = ['OBJECT', 'OPERATION', 'GROUP'] + NodeLines[0].split(',')[1:]

        link_df = pd.DataFrame(data = [re.split(',|;', x) for x in LinkLines[1:]],
                      columns = linkcol_names)

        node_df = pd.DataFrame(data = [re.split(',|;', x) for x in NodeLines[1:]],
                      columns = nodecol_names)

        log_df = pd.concat([link_df, node_df], ignore_index = True, sort = False)

        return log_df

    def evaluate_changes(self):
        """
        Determines which changes should be evaluated.
        """

        if self.roadway_changes:
            self.add_highway_changes()

        if self.transit_changes:
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

        # process deletions


        # process additions

        # process changes


        pass
