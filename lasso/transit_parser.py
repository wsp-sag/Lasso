"""
Heavily adopted from Network Wrangler, written by:
- San Francisco County Transportation Authority
- Metropolitan Transportation Commission

https://github.com/BayAreaMetro/NetworkWrangler/blob/master/Wrangler/TransitParser.py

"""

import collections, re

from simpleparse.common import numbers, strings, comments
from simpleparse import generator
from simpleparse.parser import Parser
from simpleparse.dispatchprocessor import *

from .cube_transit import TransitLine


__all__ = ["TransitParser", "PTSystem", "transit_file_def"]

WRANGLER_FILE_SUFFICES = ["lin"]

# PARSER DEFINITION ------------------------------------------------------------------------------
# NOTE: even though XYSPEED and TIMEFAC are node attributes here, I'm not sure that's really ok --
# Cube documentation implies TF and XYSPD are node attributes...
transit_file_def = r"""
transit_file      := smcw*, ( accessli / line / link / pnr / zac / supplink / factor / faresystem / waitcrvdef / crowdcrvdef / operator / mode / vehicletype )+, smcw*, whitespace*
line              := whitespace?, smcw?, c"LINE", whitespace, lin_attr*, lin_node*, whitespace?
lin_attr          := ( lin_attr_name, whitespace?, "=", whitespace?, attr_value, whitespace?,
                       comma, whitespace?, semicolon_comment* )
lin_nodeattr      := ( lin_nodeattr_name, whitespace?, "=", whitespace?, attr_value, whitespace?, comma?, whitespace?, semicolon_comment* )
lin_attr_name     := c"allstops" / c"color" / (c"freq",'[',[1-5],']') / c"mode" / c"name" / c"oneway" / c"owner" / c"runtime" / c"timefac" / c"xyspeed" / c"longname" / c"shortname" / (c"usera",[1-5]) / (c"headway",'[',[1-5],']') / c"vehicletype" / c"operator" / c"faresystem"
lin_nodeattr_name := c"access_c" / c"access" / c"delay" /  c"xyspeed" / c"timefac" / c"nntime" / c"time"
lin_node          := lin_nodestart?, whitespace?, nodenum, spaces*, comma?, spaces*, semicolon_comment?, whitespace?, lin_nodeattr*
lin_nodestart     := (whitespace?, "N", whitespace?, "=")
link              := whitespace?, smcw?, c"LINK", whitespace, link_attr*, whitespace?, semicolon_comment*
link_attr         := (( (link_attr_name, whitespace?, "=", whitespace?,  attr_value) /
                        (word_nodes, whitespace?, "=", whitespace?, nodepair) /
                        (word_modes, whitespace?, "=", whitespace?, numseq) ),
                      whitespace?, comma?, whitespace?)
link_attr_name    := c"dist" / c"speed" / c"time" / c"oneway"
pnr               := whitespace?, smcw?, c"PNR", whitespace, pnr_attr*, whitespace?
pnr_attr          := (( (pnr_attr_name, whitespace?, "=", whitespace?, attr_value) /
                        (word_node, whitespace?, "=", whitespace?, ( nodepair / nodenum )) /
                        (word_zones, whitespace?, "=", whitespace?, numseq )),
                       whitespace?, comma?, whitespace?, semicolon_comment*)
pnr_attr_name     := c"time" / c"maxtime" / c"distfac" / c"cost"
zac               := whitespace?, smcw?, c"ZONEACCESS", whitespace, zac_attr*, whitespace?, semicolon_comment*
zac_attr          := (( (c"link", whitespace?, "=", whitespace?, nodepair) /
                        (zac_attr_name, whitespace?, "=", whitespace?, attr_value) ),
                      whitespace?, comma?, whitespace?)
zac_attr_name     := c"mode"
supplink          := whitespace?, smcw?, c"SUPPLINK", whitespace, supplink_attr*, whitespace?, semicolon_comment*
supplink_attr     := (( (supplink_attr_name, whitespace?, "=", whitespace?, attr_value) /
                        (npair_attr_name, whitespace?, "=", whitespace?, nodepair )),
                       whitespace?, comma?, whitespace?)
npair_attr_name    := c"nodes" / c"n"
supplink_attr_name:= c"mode" / c"dist" / c"speed" / c"oneway" / c"time"
factor            := whitespace?, smcw?, c"FACTOR", whitespace, factor_attr*, whitespace?, semicolon_comment*
factor_attr       := ( (factor_attr_name, whitespace?, "=", whitespace?, attr_value),
                        whitespace?, comma?, whitespace? )
factor_attr_name  := c"maxwaittime" / word_nodes
faresystem        := whitespace?, smcw?, c"FARESYSTEM", whitespace, faresystem_attr*, whitespace?, semicolon_comment*
faresystem_attr   := (( (faresystem_attr_name, whitespace?, "=", whitespace?, attr_value) /
                        (faresystem_fff, whitespace?, "=", whitespace?, floatseq )),
                      whitespace?, comma?, whitespace? )
faresystem_attr_name := c"number" / c"name" / c"longname" / c"structure" / c"same" / c"iboardfare" / c"farematrix" / c"farezones"
faresystem_fff    := c"farefromfs"
waitcrvdef        := whitespace?, smcw?, c"WAITCRVDEF", whitespace, crv_attr*, whitespace?, semicolon_comment*
crowdcrvdef       := whitespace?, smcw?, c"CROWDCRVDEF", whitespace, crv_attr*, whitespace?, semicolon_comment*
crv_attr          := (( (opmode_attr_name, whitespace?, "=", whitespace?, attr_value) /
                        (word_curve, whitespace?, "=", whitespace?, xyseq )),
                       whitespace?, comma?, whitespace? )
operator          := whitespace?, smcw?, c"OPERATOR", whitespace, opmode_attr*, whitespace?, semicolon_comment*
mode              := whitespace?, smcw?, c"MODE", whitespace, opmode_attr*, whitespace?, semicolon_comment*
opmode_attr       := ( (opmode_attr_name, whitespace?, "=", whitespace?, attr_value), whitespace?, comma?, whitespace? )
opmode_attr_name  := c"number" / c"name" / c"longname"
vehicletype       := whitespace?, smcw?, c"VEHICLETYPE", whitespace, vehtype_attr*, whitespace?, semicolon_comment*
vehtype_attr      := ( (vehtype_attr_name, whitespace?, "=", whitespace?, attr_value), whitespace?, comma?, whitespace? )
vehtype_attr_name := c"number" / (c"crowdcurve",'[',[0-9]+,']') / c"crushcap" / c"loaddistfac" / c"longname" / c"name" / c"seatcap"
accessli          := whitespace?, smcw?, nodenumA, spaces?, nodenumB, spaces?, accesstag?, spaces?, (float/int)?, spaces?, semicolon_comment?
accesstag         := c"wnr" / c"pnr"
word_curve        := c"curve"
word_nodes        := c"nodes"
word_node         := c"node"
word_modes        := c"modes"
word_zones        := c"zones"
xyseq             := xy, (spaces?, ",", spaces?, xy)*
xy                := pos_floatnum, spaces?, ("-" / ","), spaces?, pos_floatnum
pos_floatnum      := [0-9]+, [\.]?, [0-9]*
numseq            := int, (spaces?, ("-" / ","), spaces?, int)*
floatseq          := floatnum, (spaces?, ("-" / ","), spaces?, floatnum)*
floatnum          := [-]?, [0-9]+, [\.]?, [0-9]*
nodepair          := nodenum, spaces?, ("-" / ","), spaces?, nodenum
nodenumA          := nodenum
nodenumB          := nodenum
nodenum           := int
attr_value        := alphanums / string_single_quote / string_double_quote
alphanums         := [a-zA-Z0-9_\.]+
<comma>           := [,]
<whitespace>      := [ \t\r\n]+
<spaces>          := [ \t]+
smcw              := whitespace?, (semicolon_comment / c_comment, whitespace?)+
"""


class TransitFileProcessor(DispatchProcessor):
    """ Class to process transit files
    """

    def __init__(self, verbosity=1):
        self.verbosity = verbosity
        self.lines = []
        self.nodes = []
        self.liType = ""
        self.factors = []
        # PT System control statements
        self.waitcrvdefs = []
        self.crowdcrvdefs = []
        self.operators = []
        self.modes = []
        self.vehicletypes = []

        self.linecomments = []

    def crackTags(self, leaf, buffer):
        tag = leaf[0]
        text = buffer[leaf[1] : leaf[2]]
        subtags = leaf[3]

        b = []

        if subtags:
            for leaf in subtags:
                b.append(self.crackTags(leaf, buffer))

        return (tag, text, b)

    def line(self, tup, buffer):
        (tag, start, stop, subtags) = tup
        # this is the whole line
        if self.verbosity >= 1:
            print(tag, start, stop)

        # Append list items for this line
        for leaf in subtags:
            xxx = self.crackTags(leaf, buffer)
            self.lines.append(xxx)

        if self.verbosity == 2:
            # lines are composed of smcw (semicolon-comment / whitespace), line_attr and lin_node
            for linepart in subtags:
                print("  ", linepart[0], " -> [ "),
                for partpart in linepart[3]:
                    print(partpart[0], "(", buffer[partpart[1] : partpart[2]], ")"),
                print(" ]")

    def process_line(self, tup, buffer):
        """
        Generic version, returns list of pieces.
        """
        (tag, start, stop, subtags) = tup

        if self.verbosity >= 1:
            print(tag, start, stop)

        if self.verbosity == 2:
            for part in subtags:
                print(" ", part[0], " -> [ "),
                for partpart in part[3]:
                    print(partpart[0], "(", buffer[partpart[1] : partpart[2]], ")"),
                print(" ]")

        # Append list items for this link
        # TODO: make the others more like this -- let the list separate the parse structures!
        retlist = []
        for leaf in subtags:
            xxx = self.crackTags(leaf, buffer)
            retlist.append(xxx)
        return retlist

    def factor(self, tup, buffer):
        factor = self.process_line(tup, buffer)
        self.factors.append(factor)

    def faresystem(self, tup, buffer):
        fs = self.process_line(tup, buffer)
        self.faresystems.append(fs)

    def waitcrvdef(self, tup, buffer):
        mycrvedef = self.process_line(tup, buffer)
        self.waitcrvdefs.append(mycrvedef)

    def crowdcrvdef(self, tup, buffer):
        mycrvedef = self.process_line(tup, buffer)
        self.crowdcrvdefs.append(mycrvedef)

    def operator(self, tup, buffer):
        myopmode = self.process_line(tup, buffer)
        self.operators.append(myopmode)

    def mode(self, tup, buffer):
        myopmode = self.process_line(tup, buffer)
        self.modes.append(myopmode)

    def vehicletype(self, tup, buffer):
        myvt = self.process_line(tup, buffer)
        self.vehicletypes.append(myvt)

    def smcw(self, tup, buffer):
        """ Semicolon comment whitespace
        """
        (tag, start, stop, subtags) = tup

        if self.verbosity >= 1:
            print(tag, start, stop)

        for leaf in subtags:
            xxx = self.crackTags(leaf, buffer)
            self.linecomments.append(xxx)


class TransitParser(Parser):

    # line files are one of these
    PROGRAM_PT = "PT"
    PROGRAM_TRNBUILD = "TRNBUILD"
    PROGRAM_UNKNOWN = "unknown"

    def __init__(self, filedef=transit_file_def, verbosity=1):
        Parser.__init__(self, filedef)
        self.verbosity = verbosity
        self.tfp = TransitFileProcessor(self.verbosity)

    def setVerbosity(self, verbosity):
        self.verbosity = verbosity
        self.tfp.verbosity = verbosity

    def buildProcessor(self):
        return self.tfp

    def convertLineData(self):
        """ Convert the parsed tree of data into a usable python list of transit lines
            returns (PROGRAM_PT or PROGRAM_TRNBUILD, list of comments and transit line objects)
        """
        program = TransitParser.PROGRAM_UNKNOWN  # default
        rows = []
        currentRoute = None
        currentComments = []

        # try to figure out what type of file this is -- TRNBUILD or PT
        for comment in self.tfp.linecomments:
            if comment[0] == "semicolon_comment":
                cmt = comment[2][0][1]
                # print("cmt={}".format(cmt))
                # note the first semicolon is stripped
                if cmt.startswith(";<<Trnbuild>>;;"):
                    program = TransitParser.PROGRAM_TRNBUILD
                elif cmt.startswith(";<<PT>><<LINE>>;;"):
                    program = TransitParser.PROGRAM_PT
        WranglerLogger.debug("convertLineData: PROGRAM: {}".format(program))

        line_num = 1
        for line in self.tfp.lines:

            # WranglerLogger.debug("{:5} line[0]={}".format(line_num, line[0]))
            line_num += 1

            # Add comments as simple strings
            if line[0] == "smcw":
                cmt = line[1].strip()
                # WranglerLogger.debug("smcw line={}".format(line))

                if currentRoute:
                    # don't add it now since we might mess up the ordering
                    # if we haven't closed out the last line
                    currentComments.append(cmt)
                else:
                    rows.append(cmt)
                continue

            # Handle Line attributes
            if line[0] == "lin_attr":
                key = None
                value = None
                comment = None
                # Pay attention only to the children of lin_attr elements
                kids = line[2]
                for child in kids:
                    if child[0] == "lin_attr_name":
                        key = child[1]
                    if child[0] == "attr_value":
                        value = child[1]
                    if child[0] == "semicolon_comment":
                        comment = child[1].strip()

                # If this is a NAME attribute, we need to start a new TransitLine!
                if key == "NAME":
                    if currentRoute:
                        rows.append(currentRoute)

                    # now add the comments stored up
                    if len(currentComments) > 0:
                        # WranglerLogger.debug("currentComments: {}".format(currentComments))
                        rows.extend(currentComments)
                        currentComments = []

                    currentRoute = TransitLine(name=value)
                else:
                    currentRoute[key] = value  # Just store all other attributes

                # And save line comment if there is one
                if comment:
                    currentRoute.comment = comment
                continue

            # Handle Node list
            if line[0] == "lin_node":
                # Pay attention only to the children of lin_attr elements
                kids = line[2]
                node = None
                for child in kids:
                    if child[0] == "nodenum":
                        node = Node(child[1])
                    if child[0] == "lin_nodeattr":
                        key = None
                        value = None
                        for nodechild in child[2]:
                            if nodechild[0] == "lin_nodeattr_name":
                                key = nodechild[1]
                            if nodechild[0] == "attr_value":
                                value = nodechild[1]
                            if nodechild[0] == "semicolon_comment":
                                comment = nodechild[1].strip()
                        node[key] = value
                        if comment:
                            node.comment = comment
                currentRoute.n.append(node)
                continue

            # Got something other than lin_node, lin_attr, or smcw:
            WranglerLogger.critical(
                "** SHOULD NOT BE HERE: %s (%s)" % (line[0], line[1])
            )

        # End of tree; store final route and return
        if currentRoute:
            rows.append(currentRoute)
        return (program, rows)

    def convertPTSystemData(self):
        """ Convert the parsed tree of data into a PTSystem object
            returns a PTSystem object
        """
        pts = PTSystem()

        for crvdef in self.tfp.waitcrvdefs:
            curve_num = None
            curve_dict = collections.OrderedDict()
            for attr in crvdef:
                # just handle curve attributes
                if attr[0] != "crv_attr":
                    continue
                key = attr[2][0][1]
                val = attr[2][1][1]
                if key == "NUMBER":
                    curve_num = int(val)
                curve_dict[key] = val
            pts.waitCurveDefs[curve_num] = curve_dict

        for crvdef in self.tfp.crowdcrvdefs:
            curve_num = None
            curve_dict = collections.OrderedDict()
            for attr in crvdef:
                # just handle curve attributes
                if attr[0] != "crv_attr":
                    continue
                key = attr[2][0][1]
                val = attr[2][1][1]
                if key == "NUMBER":
                    curve_num = int(val)
                curve_dict[key] = val
            pts.crowdCurveDefs[curve_num] = curve_dict

        for operator in self.tfp.operators:
            op_num = None
            op_dict = collections.OrderedDict()
            for attr in operator:
                # just handle opmode attributes
                if attr[0] != "opmode_attr":
                    continue

                key = attr[2][0][1]
                val = attr[2][1][1]
                if key == "NUMBER":
                    op_num = int(val)
                op_dict[key] = val  # leave as string
            pts.operators[op_num] = op_dict

        for mode in self.tfp.modes:
            mode_num = None
            mode_dict = collections.OrderedDict()
            for attr in mode:
                # just handle opmode attributes
                if attr[0] != "opmode_attr":
                    continue

                key = attr[2][0][1]
                val = attr[2][1][1]
                if key == "NUMBER":
                    mode_num = int(val)
                mode_dict[key] = val  # leave as string
            pts.modes[mode_num] = mode_dict

        for vehicletype in self.tfp.vehicletypes:
            vt_num = None
            vt_dict = collections.OrderedDict()
            for attr in vehicletype:
                # just handle vehtype attributes
                if attr[0] != "vehtype_attr":
                    continue

                key = attr[2][0][1]
                val = attr[2][1][1]
                if key == "NUMBER":
                    vt_num = int(val)
                vt_dict[key] = val  # leave as string
            pts.vehicleTypes[vt_num] = vt_dict

        if (
            len(pts.waitCurveDefs) > 0
            or len(pts.crowdCurveDefs) > 0
            or len(pts.operators) > 0
            or len(pts.modes) > 0
            or len(pts.vehicleTypes) > 0
        ):
            return pts
        return None


__all__ = ["PTSystem"]


class PTSystem:
    """
    Public Transport System definition.  Corresponds to the information in Cube's Public Transport system,
    including data for modes, operators, wait curves and crowding curves.
    """

    def __init__(self):
        self.waitCurveDefs = (
            collections.OrderedDict()
        )  # key is number, value is also ordered dict
        self.crowdCurveDefs = (
            collections.OrderedDict()
        )  # key is number, value is also ordered dict
        self.operators = (
            collections.OrderedDict()
        )  # key is number, value is also ordered dict
        self.modes = (
            collections.OrderedDict()
        )  # key is number, value is also ordered dict
        self.vehicleTypes = (
            collections.OrderedDict()
        )  # key is number, value is also ordered dict

    def isEmpty(self):
        if len(self.operators) > 0:
            return False
        if len(self.modes) > 0:
            return False
        if len(self.vehicleTypes) > 0:
            return False
        return True

    def __repr__(self):
        """ Returns string representation.
        """

        s = ""
        for pt_num, pt_dict in self.modes.items():
            s += "MODE"
            for k, v in pt_dict.items():
                s += " {}={}".format(k, v)
            s += "\n"
        s += "\n"

        for pt_num, pt_dict in self.operators.items():
            s += "OPERATOR"
            for k, v in pt_dict.items():
                s += " {}={}".format(k, v)
            s += "\n"
        s += "\n"

        for pt_num, pt_dict in self.vehicleTypes.items():
            s += "VEHICLETYPE"
            for k, v in pt_dict.items():
                s += " {}={}".format(k, v)
            s += "\n"
        s += "\n"

        for pt_num, pt_dict in self.waitCurveDefs.items():
            s += "WAITCRVDEF"
            for k, v in pt_dict.items():
                s += " {}={}".format(k, v)
            s += "\n"
        s += "\n"

        for pt_num, pt_dict in self.crowdCurveDefs.items():
            s += "CROWDCRVDEF"
            for k, v in pt_dict.items():
                s += " {}={}".format(k, v)
            s += "\n"
        s += "\n"

        return s

    def merge(self, pts):
        """
        Merges another pts with self.
        """
        for key, val_dict in pts.waitCurveDefs.items():
            if key in self.waitCurveDefs:  # collision
                raise NetworkException(
                    "PTSystem: Trying to merge WAITCRVDEF with same key: {}".format(key)
                )
            self.waitCurveDefs[key] = val_dict

        for key, val_dict in pts.crowdCurveDefs.items():
            if key in self.crowdCurveDefs:  # collision
                raise NetworkException(
                    "PTSystem: Trying to merge CROWDCRVDEF with same key: {}".format(
                        key
                    )
                )
            self.crowdCurveDefs[key] = val_dict

        for key, val_dict in pts.operators.items():
            if key in self.operators:  # collision
                raise NetworkException(
                    "PTSystem: Trying to merge OPERATOR with same key: {}".format(key)
                )
            self.operators[key] = val_dict

        for key, val_dict in pts.modes.items():
            if key in self.modes:  # collision
                raise NetworkException(
                    "PTSystem: Trying to merge MODE with same key: {}".format(key)
                )
            self.modes[key] = val_dict

        for key, val_dict in pts.vehicleTypes.items():
            if key in self.vehicleTypes:  # collision
                raise NetworkException(
                    "PTSystem: Trying to merge VEHICLETYPE with same key: {}".format(
                        key
                    )
                )
            self.vehicleTypes[key] = val_dict
