from simpleparse.common import numbers, strings, comments
from simpleparse import generator
from simpleparse.parser import Parser
from simpleparse.dispatchprocessor import *
import collections, re

# from .Factor import Factor
# from .Faresystem import Faresystem
# from .Linki import Linki
from .Logger import WranglerLogger
from .NetworkException import NetworkException
from .Node import Node

# from .PNRLink import PNRLink
from .PTSystem import PTSystem

# from .Supplink import Supplink
from .TransitLine import TransitLine

# from .TransitLink import TransitLink
# from .ZACLink import ZACLink

__all__ = ["TransitParser"]

WRANGLER_FILE_SUFFICES = ["lin", "link", "pnr", "zac", "access", "xfer", "pts"]

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
npair_attr_name    := c"nodes" / c"n"
operator          := whitespace?, smcw?, c"OPERATOR", whitespace, opmode_attr*, whitespace?, semicolon_comment*
mode              := whitespace?, smcw?, c"MODE", whitespace, opmode_attr*, whitespace?, semicolon_comment*
opmode_attr       := ( (opmode_attr_name, whitespace?, "=", whitespace?, attr_value), whitespace?, comma?, whitespace? )
opmode_attr_name  := c"number" / c"name" / c"longname"

vehicletype       := whitespace?, smcw?, c"VEHICLETYPE", whitespace, vehtype_attr*, whitespace?, semicolon_comment*
vehtype_attr      := ( (vehtype_attr_name, whitespace?, "=", whitespace?, attr_value), whitespace?, comma?, whitespace? )
vehtype_attr_name := c"number" / (c"crowdcurve",'[',[0-9]+,']') / c"crushcap" / c"loaddistfac" / c"longname" / c"name" / c"seatcap"s

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
        self.links = []
        self.pnrs = []
        self.zacs = []
        self.accesslis = []
        self.xferlis = []
        self.nodes = []
        self.liType = ""
        self.supplinks = []
        self.factors = []
        self.faresystems = []
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

    def link(self, tup, buffer):
        (tag, start, stop, subtags) = tup

        # this is the whole link
        if self.verbosity >= 1:
            print(tag, start, stop)

        # Append list items for this link
        for leaf in subtags:
            xxx = self.crackTags(leaf, buffer)
            self.links.append(xxx)

        if self.verbosity == 2:
            # links are composed of smcw and link_attr
            for linkpart in subtags:
                print("  ", linkpart[0], " -> [ "),
                for partpart in linkpart[3]:
                    print(partpart[0], "(", buffer[partpart[1] : partpart[2]], ")"),
                print(" ]")

    def pnr(self, tup, buffer):
        (tag, start, stop, subtags) = tup

        if self.verbosity >= 1:
            print(tag, start, stop)

        # Append list items for this link
        for leaf in subtags:
            xxx = self.crackTags(leaf, buffer)
            self.pnrs.append(xxx)

        if self.verbosity == 2:
            # pnrs are composed of smcw and pnr_attr
            for pnrpart in subtags:
                print(" ", pnrpart[0], " -> [ "),
                for partpart in pnrpart[3]:
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

    def convertLinkData(self):
        """ Convert the parsed tree of data into a usable python list of transit links
            returns list of comments and transit link & factor objects
        """
        rows = []
        currentLink = None
        currentFactor = None
        key = None
        value = None
        comments = []

        for link in self.tfp.links:
            # Each link is a 3-tuple:  key, value, list-of-children.

            # Add comments as simple strings:
            if link[0] in ("smcw", "semicolon_comment"):
                if currentLink:
                    currentLink.comment = " " + link[1].strip()  # Link comment
                    rows.append(currentLink)
                    currentLink = None
                else:
                    rows.append(link[1].strip())  # Line comment
                continue

            # Link records
            if link[0] == "link_attr":
                # Pay attention only to the children of lin_attr elements
                kids = link[2]
                for child in kids:
                    if child[0] in ("link_attr_name", "word_nodes", "word_modes"):
                        key = child[1]
                        # If this is a NAME attribute, we need to start a new TransitLink.
                        if key in ("nodes", "NODES"):
                            if currentLink:
                                rows.append(currentLink)
                            currentLink = (
                                TransitLink()
                            )  # Create new dictionary for this transit support link

                    if child[0] == "nodepair":
                        currentLink.setId(child[1])

                    if child[0] in ("attr_value", "numseq"):
                        currentLink[key] = child[1]
                continue

            # Got something unexpected:
            WranglerLogger.critical(
                "** SHOULD NOT BE HERE: %s (%s)" % (link[0], link[1])
            )

        # Save last link too
        if currentLink:
            rows.append(currentLink)

        for factor in self.tfp.factors:
            currentFactor = Factor()

            # factor[0]:
            # ('smcw', '; BART-eBART timed transfer\n',
            #    [('semicolon_comment', '; BART-eBART timed transfer\n',
            #      [('comment', ' BART-eBART timed transfer', [])])])
            # keep as line comment
            if factor[0][0] == "smcw":
                smcw = factor.pop(0)
                rows.append(smcw[1].strip())

            # the rest are attributes
            # [('factor_attr', 'MAXWAITTIME=1, ', [('factor_attr_name', 'MAXWAITTIME', []), ('attr_value', '1', [('alphanums', '1', [])])]),
            #  ('factor_attr', 'NODES=15536\n',   [('factor_attr_name', 'NODES', [('word_nodes', 'NODES', [])]), ('attr_value', '15536', [('alphanums', '15536', [])])])]
            for factor_attr in factor:
                if factor_attr[0] == "semicolon_comment":
                    comments.append(factor_attr[1])
                    continue

                if factor_attr[0] != "factor_attr":
                    WranglerLogger.critical(
                        "** unexpected factor item: {}".format(factor_attr)
                    )

                factor_attr_name = factor_attr[2][
                    0
                ]  # ('factor_attr_name', 'MAXWAITTIME', [])
                factor_attr_val = factor_attr[2][
                    1
                ]  # ('attr_value', '1', [('alphanums', '1', [])])

                # set it
                currentFactor[factor_attr_name[1]] = factor_attr_val[1]

            rows.append(currentFactor)
            if len(comments) > 0:
                rows.extend(comments)
                comments = []

        return rows

    def convertPNRData(self):
        """ Convert the parsed tree of data into a usable python list of PNR objects
            returns list of strings and PNR objects
        """
        rows = []
        currentPNR = None
        key = None
        value = None

        for pnr in self.tfp.pnrs:
            # Each pnr is a 3-tuple:  key, value, list-of-children.
            # Add comments as simple strings

            # Textline Comments
            if pnr[0] == "smcw":
                # Line comment; thus existing PNR must be finished.
                if currentPNR:
                    rows.append(currentPNR)
                    currentPNR = None

                rows.append(pnr[1].strip())  # Append line-comment
                continue

            # PNR records
            if pnr[0] == "pnr_attr":
                # Pay attention only to the children of attr elements
                kids = pnr[2]
                for child in kids:
                    if child[0] in ("pnr_attr_name", "word_node", "word_zones"):
                        key = child[1]
                        # If this is a NAME attribute, we need to start a new PNR.
                        if key in ("node", "NODE"):
                            if currentPNR:
                                rows.append(currentPNR)
                            currentPNR = PNRLink()  # Create new dictionary for this PNR

                    if child[0] == "nodepair" or child[0] == "nodenum":
                        # print "child[0]/[1]",child[0],child[1]
                        currentPNR.id = child[1]
                        currentPNR.parseID()

                    if child[0] in ("attr_value", "numseq"):
                        currentPNR[key.upper()] = child[1]

                    if child[0] == "semicolon_comment":
                        currentPNR.comment = " " + child[1].strip()

                continue

            # Got something unexpected:
            WranglerLogger.critical("** SHOULD NOT BE HERE: %s (%s)" % (pnr[0], pnr[1]))

        # Save last link too
        if currentPNR:
            rows.append(currentPNR)
        return rows


    def convertLinkiData(self, linktype):
        """ Convert the parsed tree of data into a usable python list of ZAC objects
            returns list of strings and ZAC objects
        """
        rows = []
        currentLinki = None
        key = None
        value = None

        linkis = []
        if linktype == "access":
            linkis = self.tfp.accesslis
        elif linktype == "xfer":
            linkis = self.tfp.xferlis
        elif linktype == "node":
            linkis = self.tfp.nodes
        else:
            raise NetworkException("ConvertLinkiData with invalid linktype")

        for accessli in linkis:
            # whitespace?, smcw?, nodenumA, spaces?, nodenumB, spaces?, (float/int)?, spaces?, semicolon_comment?
            if accessli[0] == "smcw":
                rows.append(accessli[1].strip())
            elif accessli[0] == "nodenumA":
                currentLinki = Linki()
                rows.append(currentLinki)
                currentLinki.A = accessli[1].strip()
            elif accessli[0] == "nodenumB":
                currentLinki.B = accessli[1].strip()
            elif accessli[0] == "float":
                currentLinki.distance = accessli[1].strip()
            elif accessli[0] == "int":
                currentLinki.xferTime = accessli[1].strip()
            elif accessli[0] == "semicolon_comment":
                currentLinki.comment = accessli[1].strip()
            elif accessli[0] == "accesstag":
                currentLinki.accessType = accessli[1].strip()
            else:
                # Got something unexpected:
                WranglerLogger.critical(
                    "** SHOULD NOT BE HERE: %s (%s)" % (accessli[0], accessli[1])
                )

        return rows
