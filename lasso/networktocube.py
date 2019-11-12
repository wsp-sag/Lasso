from lasso.TransitNetwork import TransitNetwork
from typing import Any, Dict, Optional


class CubeTransit(object):
    def __init__(self, transit_network: Optional[TransitNetwork] = None):
        """
        """
        ## TODO Sijia
        self.transit_network = transit_network
        self.diff_dict = Dict[str, Any]

        pass

    @staticmethod
    def create_cubetransit(
        transit_dir: Optional[str] = None
        # transit_network: Optional[TransitNetwork] = None
    ):

        transit_network = CubeTransit.read_cube_line_file(transit_dir)

        cubetransit = CubeTransit(transit_network=transit_network)

        return cubetransit

    @staticmethod
    def read_cube_line_file(dirname: str):
        """
        reads a .lin file and stores as TransitNetwork object

        Parameters
        -----------
        dirname:  str, the directory where .lin file is

        Returns
        -------

        """
        ## TODO Sijia
        tn = TransitNetwork("CHAMP", 1.0)
        tn.mergeDir(dirname)

        return tn

    def evaluate_differences(self, base_transit):
        """

        Parameters
        -----------

        Returns
        -------

        """
        ## TODO Sijia
        # loop thru every record in new .lin
        transit_change_list = []
        time_enum = {
            "pk": {"start_time": "06:00:00", "end_time": "09:00:00"},
            "op": {"start_time": "09:00:00", "end_time": "15:00:00"},
        }

        for line in self.transit_network.lines[1:]:
            _name = line.name
            for line_base in base_transit.transit_network.lines[1:]:
                if line_base.name == _name:
                    properties_list = CubeTransit.evaluate_route_level(line, line_base)
                    if len(properties_list) > 0:
                        card_dict = {
                            "category": "Transit Service Property Change",
                            "facility": {
                                "route_id": _name.split("_")[1],
                                "direction_id": int(_name[-1]),
                                "start_time": time_enum[_name[-3:-1]]["start_time"],
                                "end_time": time_enum[_name[-3:-1]]["end_time"],
                            },
                            "properties": properties_list,
                        }
                        transit_change_list.append(card_dict)
                else:
                    continue

        return transit_change_list

    def evaluate_route_level(line_build, line_base):
        properties_list = []
        if line_build.getFreq() != line_base.getFreq():
            _headway_diff = line_build.getFreq() - line_base.getFreq()
            properties_list.append(
                {"property": "headway_secs", "change": _headway_diff * 60}
            )
        # if tn_build.
        return properties_list
