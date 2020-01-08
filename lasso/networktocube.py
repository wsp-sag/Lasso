from typing import Any, Dict, Optional
from lark import Lark

class EloCubeTransit(object):
    TRANSIT_LINE_FILE_GRAMMAR = r"""

    ?start             : WHITESPACE? program_type_line? WHITESPACE? lines
    WHITESPACE        : /[ \t\r\n]/+
    STRING            : /("(?!"").*?(?<!\\)(\\\\)*?"|'(?!'').*?(?<!\\)(\\\\)*?')/i
    SEMICOLON_COMMENT : /;[^\n]*/

    program_type_line : ";;<<" PROGRAM_TYPE ">><<LINE>>;;" WHITESPACE?
    PROGRAM_TYPE      : "PT" | "TRNBUILD"

    lines             : line*
    line              : WHITESPACE? "LINE" WHITESPACE? lin_attributes WHITESPACE? nodes

    lin_attributes    : lin_attr+
    lin_attr          : lin_attr_name WHITESPACE? "=" WHITESPACE? attr_value WHITESPACE? "," WHITESPACE? SEMICOLON_COMMENT* WHITESPACE?
    TIME_PERIOD       : "1".."5"
    !lin_attr_name     : "allstops"i
                        | "color"i
                        | ("freq"i "[" TIME_PERIOD "]")
                        | ("headway"i "[" TIME_PERIOD "]")
                        | "mode"i
                        | "name"i
                        | "oneway"i
                        | "owner"i
                        | "runtime"i
                        | "timefac"i
                        | "xyspeed"i
                        | "longname"i
                        | "shortname"i
                        | ("usera"i TIME_PERIOD)
                        | "vehicletype"i
                        | "operator"i
                        | "faresystem"i

    attr_value        : /[a-zA-Z]/+ | STRING | SIGNED_INT

    nodes             : lin_node+
    lin_node          : LIN_NODESTART? WHITESPACE? SIGNED_INT WHITESPACE? ","? WHITESPACE? SEMICOLON_COMMENT? WHITESPACE? lin_nodeattr*
    LIN_NODESTART     : ("N" | "NODES") WHITESPACE? "="
    lin_nodeattr      : lin_nodeattr_name WHITESPACE? "=" WHITESPACE? attr_value WHITESPACE? ","? WHITESPACE? SEMICOLON_COMMENT*
    lin_nodeattr_name : "access_c"i
                        | "access"i
                        | "delay"i
                        | "xyspeed"i
                        | "timefac"i
                        | "nntime"i
                        | "time"i

    operator          : WHITESPACE? SEMICOLON_COMMENT* WHITESPACE? "OPERATOR" WHITESPACE? opmode_attr* WHITESPACE? SEMICOLON_COMMENT*
    mode              : WHITESPACE? SEMICOLON_COMMENT* WHITESPACE? "MODE" WHITESPACE? opmode_attr* WHITESPACE? SEMICOLON_COMMENT*
    opmode_attr       : ( (opmode_attr_name WHITESPACE? "=" WHITESPACE? attr_value) WHITESPACE? ","? WHITESPACE? )
    opmode_attr_name  : "number" | "name" | "longname"

    %import common.SIGNED_INT
    %ignore WHITESPACE
    %ignore LIN_NODESTART

    """
    #semicolon_comment      WHITESPACE lin_attr* lin_nodeattr*
    def __init__(self, transit_network = None):
        """
        """
        self.transit_network = transit_network
        self.diff_dict = Dict[str, Any]


    @staticmethod
    def create_cubetransit(
        transit_source: Optional[str] = None,
    ):

        transit_network = EloCubeTransit.read_cube(transit_source)

        cubetransit = EloCubeTransit(transit_network=transit_network)

        return cubetransit

    @staticmethod
    def read_cube(transit_source: str):
        """
        reads a .lin file and stores as TransitNetwork object

        Parameters
        -----------
        dirname:  str, the directory where .lin file is

        Returns
        -------

        """

        parser = Lark(EloCubeTransit.TRANSIT_LINE_FILE_GRAMMAR,  debug="debug")#, lexer="contextual",parser="lalr",)

        if "NAME=" in transit_source:
            print("reading transit source as string")
            tn = parser.parse(transit_source)
        else:
            with open(transit_source) as file:
                tn = parser.parse(file.read())

        return tn



class CubeTransit(object):
    def __init__(self, transit_network = None):
        """
        """
        self.transit_network = transit_network
        self.diff_dict = Dict[str, Any]


    @staticmethod
    def create_cubetransit(
        transit_dir: Optional[str] = None,
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
        #undo tn = TransitNetwork("CHAMP", 1.0)
        #undo tn.mergeDir(dirname)
        pass
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
