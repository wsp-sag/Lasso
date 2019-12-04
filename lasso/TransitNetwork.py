import copy, glob, inspect, math, os, re, shutil, sys  # , xlrd
from collections import defaultdict
from .Factor import Factor
from .Faresystem import Faresystem
from .Linki import Linki
from .Logger import WranglerLogger
from .Network import Network
from .NetworkException import NetworkException
from .PNRLink import PNRLink
from .PTSystem import PTSystem
from .Regexes import nodepair_pattern

# from .TransitAssignmentData import TransitAssignmentData, TransitAssignmentDataException
# from .TransitCapacity import TransitCapacity
from .TransitLine import TransitLine

# from .TransitLink import TransitLink
from .TransitParser import TransitParser, transit_file_def

# from .ZACLink import ZACLink

__all__ = ["TransitNetworkLasso"]


class TransitNetworkLasso(Network):
    """
    Full Cube representation of a transit network (all components)
    """

    FARE_FILES = {
        Network.MODEL_TYPE_CHAMP: [
            "caltrain.fare",
            "smart.fare",
            "ebart.fare",
            "amtrak.fare",
            "hsr.fare",
            "ferry.fare",
            "bart.fare",
            "xfer.fare",
            "farelinks.fare",
        ],
        Network.MODEL_TYPE_TM1: [
            "ACE.far",
            "Amtrak.far",
            "BART.far",
            "Caltrain.far",
            "Ferry.far",
            "HSR.far",
            "SMART.far",
            "xfare.far",
            "farelinks.far",
            "transit_faremat.block",
        ],
        Network.MODEL_TYPE_TM2: ["fares.far", "fareMatrix.txt"],
    }

    # Static reference to a TransitCapacity instance
    capacity = None

    def __init__(
        self,
        modelType,
        modelVersion,
        basenetworkpath=None,
        networkBaseDir=None,
        networkProjectSubdir=None,
        networkSeedSubdir=None,
        networkPlanSubdir=None,
        isTiered=False,
        networkName=None,
    ):
        """
        If *basenetworkpath* is passed and *isTiered* is True, then start by reading the files
        named *networkName*.* in the *basenetworkpath*
        """
        Network.__init__(
            self,
            modelType,
            modelVersion,
            networkBaseDir,
            networkProjectSubdir,
            networkSeedSubdir,
            networkPlanSubdir,
            networkName,
        )
        self.program = (
            TransitParser.PROGRAM_TRNBUILD
        )  # will be one of PROGRAM_PT or PROGRAM_TRNBUILD
        self.lines = []
        self.links = (
            []
        )  # TransitLink instances, Factor instances and comments (strings)
        self.pnrs = {}  # key is file name since these need to stay separated
        self.zacs = []
        self.accessli = []
        self.xferli = []
        self.nodes = []  # transit node coords
        self.supps = []  # Supplinks
        self.faresystems = {}  # key is Id number
        self.ptsystem = PTSystem()  # single instance
        self.farefiles = {}  # farefile name -> [ lines in farefile ]

        for farefile in TransitNetworkLasso.FARE_FILES[self.modelType]:
            self.farefiles[farefile] = []

        self.DELAY_VALUES = None
        self.currentLineIdx = 0

        if basenetworkpath and isTiered:
            if not networkName:
                raise NetworkException(
                    "Cannot initialize tiered TransitNetwork with basenetworkpath %s: no networkName specified"
                    % basenetworkpath
                )

            # for CHAMP and TM2, transit lines are here
            if self.modelType in [Network.MODEL_TYPE_CHAMP, Network.MODEL_TYPE_TM2]:
                for filename in glob.glob(
                    os.path.join(basenetworkpath, networkName + ".*")
                ):
                    suffix = filename.rsplit(".")[-1].lower()
                    if suffix in ["lin", "link", "pnr", "zac", "access", "xfer"]:
                        self.parseFile(filename)

                # this doesn't have to match the network name
                for filename in glob.glob(os.path.join(basenetworkpath, "*.*")):
                    suffix = filename.rsplit(".")[-1].lower()
                    if suffix in ["pts"]:
                        self.parseFile(filename)

            elif self.modelType in [Network.MODEL_TYPE_TM1]:
                # read the the block file to find the line filenames if it exists
                block_filename = os.path.join(
                    basenetworkpath, "transit_lines", networkName + ".block"
                )
                line_filenames = []
                flat_dirs = False

                if os.path.exists(block_filename):
                    WranglerLogger.info("Reading {}".format(block_filename))
                    file_re = re.compile(r"^\s*read\s+file\s*=\s*trn[\\](\S*)$")
                    block_file = open(block_filename, "r")
                    for line in block_file:
                        result = re.match(file_re, line)
                        if result:
                            line_filenames.append(result.group(1))
                    block_file.close()
                else:
                    # if it doesn't exist, assume networkName.lin and flat file structure
                    line_filenames.append("{}.lin".format(networkName))
                    flat_dirs = True

                WranglerLogger.debug("Line filenames: {}".format(line_filenames))

                # read those line files
                for filename in line_filenames:
                    if flat_dirs:
                        self.parseFile(
                            os.path.join(basenetworkpath, filename),
                            insert_replace=False,
                        )
                    else:
                        self.parseFile(
                            os.path.join(basenetworkpath, "transit_lines", filename),
                            insert_replace=False,
                        )

                # now the rest
                if flat_dirs:
                    glob_str = os.path.join(basenetworkpath, "*.*")
                else:
                    glob_str = os.path.join(basenetworkpath, "transit_support", "*.*")

                for filename in glob.glob(glob_str):
                    suffix = filename.rsplit(".")[-1].lower()
                    if suffix in ["dat", "pnr", "sup", "zac", "access", "link", "xfer"]:
                        WranglerLogger.debug("About to read {}".format(filename))
                        if filename.endswith("_access_links.dat"):
                            self.parseFileAsSuffix(filename, "access", False)
                        elif filename.endswith("_xfer_links.dat"):
                            self.parseFileAsSuffix(filename, "xfer", False)
                        elif filename.endswith("Transit_Support_Nodes.dat"):
                            self.parseFileAsSuffix(filename, "node", False)
                        else:
                            self.parseFile(filename, insert_replace=False)

            # fares
            for farefile in TransitNetworkLasso.FARE_FILES[self.modelType]:

                fullfarefile = os.path.join(basenetworkpath, farefile)

                if modelType == Network.MODEL_TYPE_TM2:

                    suffix = farefile.rsplit(".")[-1].lower()

                    if suffix == "far":
                        # parse TM2 fare files
                        self.parseFile(fullfarefile)
                        WranglerLogger.info("Read {}".format(fullfarefile))
                    else:
                        # fare zone matrix files are just numbers
                        Faresystem.readFareZoneMatrixFile(
                            fullfarefile, self.faresystems
                        )

                else:
                    linecount = 0
                    # WranglerLogger.debug("cwd=%s  farefile %s exists? %d" % (os.getcwd(), fullfarefile, os.path.exists(fullfarefile)))

                    if os.path.exists(fullfarefile):
                        infile = open(fullfarefile, "r")
                        lines = infile.readlines()
                        self.farefiles[farefile].extend(lines)
                        linecount = len(lines)
                        infile.close()
                    WranglerLogger.debug(
                        "Read %5d lines from fare file %s" % (linecount, fullfarefile)
                    )

    def __iter__(self):
        """
        Iterator for looping through lines.
        """
        self.currentLineIdx = 0
        return self

    def __next__(self):
        """
        Method for iterator.  Iterator usage::

            net = TransitNetwork()
            net.mergeDir("X:\some\dir\with_transit\lines")
            for line in net:
                print line

        """

        if self.currentLineIdx >= len(self.lines):  # are we out of lines?
            raise StopIteration

        while not isinstance(self.lines[self.currentLineIdx], TransitLine):
            self.currentLineIdx += 1

            if self.currentLineIdx >= len(self.lines):
                raise StopIteration

        self.currentLineIdx += 1
        return self.lines[self.currentLineIdx - 1]

    # python 2 backwards compat
    next = __next__

    def __repr__(self):
        return "TransitNetwork: %s lines, %s links, %s PNRs, %s ZACs" % (
            len(self.lines),
            len(self.links),
            len(self.pnrs),
            len(self.zacs),
        )

    def isEmpty(self):
        """
        TODO: could be smarter here and check that there are no non-comments since those
        don't really count.
        """
        if (
            len(self.lines) == 0
            and len(self.links) == 0
            and len(self.pnrs) == 0
            and len(self.zacs) == 0
            and len(self.accessli) == 0
            and len(self.xferli) == 0
        ):
            return True

        return False

    def clear(self, projectstr):
        """
        Clears out all network data to prep for a project apply, e.g. the MuniTEP project is a complete
        Muni network so clearing the existing contents beforehand makes sense.
        If it's already clear then this is a no-op but otherwise
        the user will be prompted (with the project string) so that the log will be clear.
        """
        if self.isEmpty():
            # nothing to do!
            return

        query = "Clearing network for %s:\n" % projectstr
        query += "   %d lines, %d links, %d pnrs, %d zacs, %d accessli, %d xferli\n" % (
            len(self.lines),
            len(self.links),
            len(self.pnrs),
            len(self.zacs),
            len(self.accessli),
            len(self.xferli),
        )
        query += "Is this ok? (y/n) "
        WranglerLogger.debug(query)
        try:
            response = raw_input("")  # python 2
        except:
            response = input("")  # python 3

        WranglerLogger.debug("response=[%s]" % response)
        if response != "Y" and response != "y":
            exit(0)

        del self.lines[:]
        del self.links[:]
        self.pnrs.clear()
        del self.zacs[:]
        del self.accessli[:]
        del self.xferli[:]
        del self.faresystems[:]

    def clearLines(self):
        """
        Clears out all network **line** data to prep for a project apply, e.g. the MuniTEP project is a complete
        Muni network so clearing the existing contents beforehand makes sense.
        """
        del self.lines[:]

    def validateFrequencies(self):
        """
        Makes sure none of the transit lines have 0 frequencies for all time periods.
        """
        WranglerLogger.debug("Validating frequencies")

        # For each line
        for line in self:
            if not isinstance(line, TransitLine):
                continue

            freqs = line.getFreqs()
            nonzero_found = False
            for freq in freqs:
                if float(freq) > 0:
                    nonzero_found = True
                    break

            if nonzero_found == False:
                raise NetworkException(
                    "Lines {} has only zero frequencies".format(line.name)
                )

    def validateWnrsAndPnrs(self):
        """
        Goes through the transit lines in this network and for those that are offstreet (e.g.
        modes 4 or 9), this method will validate that the xfer/pnr/wnr relationships look ship-shape.
        Pretty verbose in the debug log.
        """
        WranglerLogger.debug("Validating Off Street Transit Node Connections")

        nodeInfo = (
            {}
        )  # lineset => { station node => { xfer node => [ walk node, pnr node ] }}
        setToModeType = {}  # lineset => list of ModeTypes ("Local", etc)
        setToOffstreet = {}  # lineset => True if has offstreet nodes
        doneNodes = set()

        critical_found = False

        # For each line
        for line in self:
            if not isinstance(line, TransitLine):
                continue
            # print "validating", line

            lineset = line.name[0:3]
            if lineset not in nodeInfo:
                nodeInfo[lineset] = {}
                setToModeType[lineset] = []
                setToOffstreet[lineset] = False
            if line.getModeType(self.modelType) not in setToModeType[lineset]:
                setToModeType[lineset].append(line.getModeType(self.modelType))
                setToOffstreet[lineset] = setToOffstreet[
                    lineset
                ] or line.hasOffstreetNodes(self.modelType)

            # for each stop
            for stopIdx in range(len(line.n)):
                if not line.n[stopIdx].isStop():
                    continue

                stopNodeStr = line.n[stopIdx].num

                wnrNodes = set()
                pnrNodes = set()

                if stopNodeStr in nodeInfo[lineset]:
                    continue
                nodeInfo[lineset][stopNodeStr] = {}

                # print " check if we have access to an on-street node"
                for link in self.xferli:
                    if not isinstance(link, Linki):
                        continue
                    # This xfer links the node to the on-street network
                    if link.A == stopNodeStr:
                        nodeInfo[lineset][stopNodeStr][link.B] = ["-", "-"]
                    elif link.B == stopNodeStr:
                        nodeInfo[lineset][stopNodeStr][link.A] = ["-", "-"]

                # print " Check for WNR"
                for zac in self.zacs:
                    if not isinstance(zac, ZACLink):
                        continue

                    m = re.match(nodepair_pattern, zac.id)
                    if m.group(1) == stopNodeStr:
                        # this one is invalid for TM1
                        if self.modelType in [Network.MODEL_TYPE_TM1]:
                            errorstr = "ZONEACCESS link should be funnel-stop but stop-funnel found: {}".format(
                                zac
                            )
                            WranglerLogger.critical(errorstr)
                            critical_found = True
                        else:
                            wnrNodes.add(int(m.group(2)))

                    if m.group(2) == stopNodeStr:
                        wnrNodes.add(int(m.group(1)))

                # print "Check for PNR"
                for pnr_filename in self.pnrs.keys():
                    for pnr in self.pnrs[pnr_filename]:
                        if not isinstance(pnr, PNRLink):
                            continue
                        pnr.parseID()
                        if pnr.station == stopNodeStr and pnr.pnr != PNRLink.UNNUMBERED:
                            pnrNodes.add(int(pnr.pnr))

                # print "Check that our access links go from an onstreet xfer to a pnr or to a wnr"
                for link in self.accessli:
                    if not isinstance(link, Linki):
                        continue
                    try:
                        if int(link.A) in wnrNodes:
                            nodeInfo[lineset][stopNodeStr][link.B][0] = link.A
                        elif int(link.B) in wnrNodes:
                            nodeInfo[lineset][stopNodeStr][link.A][0] = link.B
                        elif int(link.A) in pnrNodes:
                            nodeInfo[lineset][stopNodeStr][link.B][1] = link.A
                        elif int(link.B) in pnrNodes:
                            nodeInfo[lineset][stopNodeStr][link.A][1] = link.B
                    except KeyError:
                        # if it's not offstreet then that's ok
                        if not setToOffstreet[lineset]:
                            continue

                        errorstr = (
                            "Invalid access link found in %s lineset %s (incl offstreet) stopNode %s -- Missing xfer?  A=%s B=%s, xfernodes=%s wnrNodes=%s pnrNodes=%s"
                            % (
                                line.getModeType(self.modelType),
                                lineset,
                                stopNodeStr,
                                link.A,
                                link.B,
                                str(nodeInfo[lineset][stopNodeStr].keys()),
                                str(wnrNodes),
                                str(pnrNodes),
                            )
                        )
                        WranglerLogger.warning(errorstr)
                        # raise NetworkException(errorstr)

        nodeNames = {}
        if "CHAMP_node_names" in os.environ:
            book = xlrd.open_workbook(os.environ["CHAMP_node_names"])
            sh = book.sheet_by_index(0)
            for rx in range(0, sh.nrows):  # skip header
                therow = sh.row(rx)
                nodeNames[int(therow[0].value)] = therow[1].value
            # WranglerLogger.info(str(nodeNames))

        # print it all out
        for lineset in nodeInfo.keys():

            stops = nodeInfo[lineset].keys()
            stops.sort()

            WranglerLogger.debug(
                "--------------- Line set %s %s -- hasOffstreet? %s------------------"
                % (lineset, str(setToModeType[lineset]), str(setToOffstreet[lineset]))
            )
            WranglerLogger.debug(
                "%-40s %10s %10s %10s %10s" % ("stopname", "stop", "xfer", "wnr", "pnr")
            )
            for stopNodeStr in stops:
                numWnrs = 0
                stopname = "Unknown stop name"
                if int(stopNodeStr) in nodeNames:
                    stopname = nodeNames[int(stopNodeStr)]
                for xfernode in nodeInfo[lineset][stopNodeStr].keys():
                    WranglerLogger.debug(
                        "%-40s %10s %10s %10s %10s"
                        % (
                            stopname,
                            stopNodeStr,
                            xfernode,
                            nodeInfo[lineset][stopNodeStr][xfernode][0],
                            nodeInfo[lineset][stopNodeStr][xfernode][1],
                        )
                    )
                    if nodeInfo[lineset][stopNodeStr][xfernode][0] != "-":
                        numWnrs += 1

                if numWnrs == 0 and setToOffstreet[lineset]:
                    errorstr = (
                        "Zero wnrNodes or onstreetxfers for stop %s!" % stopNodeStr
                    )
                    WranglerLogger.critical(errorstr)
                    critical_found = True

        if critical_found:
            raise NetworkException("Critical errors found")

    def line(self, name):
        """
        If a string is passed in, return the line for that name exactly (a :py:class:`TransitLine` object).
        If a regex, return all relevant lines (a list of TransitLine objects).
        If 'all', return all lines (a list of TransitLine objects).
        """
        if isinstance(name, str):
            if name in self.lines:
                return self.lines[self.lines.index(name)]

        if str(type(name)) == str(type(re.compile("."))):
            toret = []
            for i in range(len(self.lines)):
                if isinstance(self.lines[i], str):
                    continue
                if name.match(self.lines[i].name):
                    toret.append(self.lines[i])
            return toret
        if name == "all":
            allLines = []
            for i in range(len(self.lines)):
                allLines.append(self.lines[i])
            return allLines
        raise NetworkException("Line name not found: %s" % (name,))

    def deleteLinkForNodes(self, nodeA, nodeB, include_reverse=True):
        """
        Delete any TransitLink in self.links[] from nodeA to nodeB (these should be integers).
        If include_reverse, also delete from nodeB to nodeA.
        Returns number of links deleted.
        """
        del_idxs = []
        for idx in range(len(self.links) - 1, -1, -1):  # go backwards
            if not isinstance(self.links[idx], TransitLink):
                continue
            if self.links[idx].Anode == nodeA and self.links[idx].Bnode == nodeB:
                del_idxs.append(idx)
            elif (
                include_reverse
                and self.links[idx].Anode == nodeB
                and self.links[idx].Bnode == nodeA
            ):
                del_idxs.append(idx)

        for del_idx in del_idxs:
            WranglerLogger.debug("Removing link %s" % str(self.links[del_idx]))
            del self.links[del_idx]

        return len(del_idxs)

    def numPNRLinks(self):
        """
        Returns number of pnr links.
        """
        num_pnr_links = 0
        for pnr_file in self.pnrs.keys():
            for pnr_link in self.pnrs[pnr_file]:
                if not isinstance(pnr_link, PNRLink):
                    continue
                num_pnr_links += 1
        return num_pnr_links

    def deletePNRLinkForId(self, pnr_id):
        """
        Delete the PNRLink with the given id.
        """
        for pnr_file in self.pnrs.keys():
            del_idxs = []
            # find pnr links to delete
            for idx in range(len(self.pnrs[pnr_file]) - 1, -1, -1):  # go backwards
                if not isinstance(self.pnrs[pnr_file][idx], PNRLink):
                    continue
                if self.pnrs[pnr_file][idx].id == pnr_id:
                    del_idxs.append(idx)

            # delete them
            for del_idx in del_idxs:
                WranglerLogger.debug(
                    "Removing PNR link {} from {}".format(
                        self.pnrs[pnr_file][del_idx], pnr_file
                    )
                )
                del self.pnrs[pnr_file][del_idx]

    def deleteAccessXferLinkForNode(self, nodenum, access_links=True, xfer_links=True):
        """
        Delete any Linki in self.accessli (if access_links) and/or self.xferli (if xfer_links)
        with Anode or Bnode as nodenum.
        Returns number of links deleted.
        """
        del_acc_idxs = []
        if access_links:
            for idx in range(len(self.accessli) - 1, -1, -1):  # go backwards
                if not isinstance(self.accessli[idx], Linki):
                    continue
                if (
                    int(self.accessli[idx].A) == nodenum
                    or int(self.accessli[idx].B) == nodenum
                ):
                    del_acc_idxs.append(idx)

            for del_idx in del_acc_idxs:
                WranglerLogger.debug(
                    "Removing access link %s" % str(self.accessli[del_idx])
                )
                del self.accessli[del_idx]

        del_xfer_idxs = []
        if xfer_links:
            for idx in range(len(self.xferli) - 1, -1, -1):  # go backwards
                if not isinstance(self.xferli[idx], Linki):
                    continue
                if (
                    int(self.xferli[idx].A) == nodenum
                    or int(self.xferli[idx].B) == nodenum
                ):
                    del_xfer_idxs.append(idx)

            for del_idx in del_xfer_idxs:
                WranglerLogger.debug(
                    "Removing xfere link %s" % str(self.xferli[del_idx])
                )
                del self.xferli[del_idx]

        return len(del_acc_idxs) + len(del_xfer_idxs)

    def splitLinkInTransitLines(
        self, nodeA, nodeB, newNode, stop=False, verboseLog=True
    ):
        """
        Goes through each line and for any with links going from *nodeA* to *nodeB*, inserts
        the *newNode* in between them (as a stop if *stop* is True).
        Returns list of lines affected.
        """
        lines_split = []
        totReplacements = 0
        for line in self:
            if line.hasLink(nodeA, nodeB):
                line.splitLink(nodeA, nodeB, newNode, stop=stop, verboseLog=verboseLog)
                totReplacements += 1
                lines_split.append(line.name)

        # log only if instructed instructed
        if verboseLog:
            WranglerLogger.debug(
                "Total Lines with Link %s-%s split:%d"
                % (str(nodeA), str(nodeB), totReplacements)
            )
        return lines_split

    def replaceSegmentInTransitLines(self, nodeA, nodeB, newNodes):
        """
        *newNodes* should include nodeA and nodeB if they are not going away
        """
        totReplacements = 0
        allExp = re.compile(".")
        newSection = newNodes  # [nodeA]+newNodes+[nodeB]
        for line in self.line(allExp):
            if line.hasSegment(nodeA, nodeB):
                WranglerLogger.debug(line.name)
                line.replaceSegment(nodeA, nodeB, newSection)
                totReplacements += 1
        WranglerLogger.debug(
            "Total Lines with Segment %s-%s replaced:%d"
            % (str(nodeA), str(nodeB), totReplacements)
        )

    def setCombiFreqsForShortLine(self, shortLine, longLine, combFreqs):
        """
        Set all five headways for a short line to equal a combined
        headway including long line. i.e. set 1-California Short frequencies
        by inputing the combined frequencies of both lines.

        .. note:: Make sure *longLine* frequencies are set first!
        """
        try:
            longLineInst = self.line(longLine)
        except:
            raise NetworkException("Unknown Route!  %s" % (longLine))
        try:
            shortLineInst = self.line(shortLine)
        except:
            raise NetworkException("Unknown Route!  %s" % (shortLine))

        [tp1Long, tp2Long, tp3Long, tp4Long, tp5Long] = longLineInst.getFreqs()
        [tp1Comb, tp2Comb, tp3Comb, tp4Comb, tp5Comb] = combFreqs
        [tp1Short, tp2Short, tp3Short, tp4Short, tp5Short] = [0, 0, 0, 0, 0]
        if (tp1Long - tp1Comb) > 0:
            tp1Short = tp1Comb * tp1Long / (tp1Long - tp1Comb)
        if (tp2Long - tp2Comb) > 0:
            tp2Short = tp2Comb * tp2Long / (tp2Long - tp2Comb)
        if (tp3Long - tp3Comb) > 0:
            tp3Short = tp3Comb * tp3Long / (tp3Long - tp3Comb)
        if (tp4Long - tp4Comb) > 0:
            tp4Short = tp4Comb * tp4Long / (tp4Long - tp4Comb)
        if (tp5Long - tp5Comb) > 0:
            tp5Short = tp5Comb * tp5Long / (tp5Long - tp5Comb)
        shortLineInst.setFreqs([tp1Short, tp2Short, tp3Short, tp4Short, tp5Short])

    def getCombinedFreq(self, names, coverage_set=False):
        """
        Pass a regex pattern, we'll show the combined frequency.  This
        doesn't change anything, it's just a useful tool.
        """
        lines = self.line(names)
        denom = [0, 0, 0, 0, 0]
        for l in lines:
            if coverage_set:
                coverage_set.discard(l.name)
            freqs = l.getFreqs()
            for t in range(5):
                if float(freqs[t]) > 0.0:
                    denom[t] += 1 / float(freqs[t])

        combined = [0, 0, 0, 0, 0]
        for t in range(5):
            if denom[t] > 0:
                combined[t] = round(1 / denom[t], 2)
        return combined

    def getValueFromXfare(self, fare_filename, from_mode, to_mode):
        """
        Assuming that fare_filename contains XFARE information (e.g. XFAR[from_mode]=to_mode1,to_mode2,...)
        Returns the value set for from_mode to to_mode
        If none found, throws a NetworkException
        """
        xfare_re = re.compile("xfare\[(\d+)\]=((-?\d+)(,\s*-?\d+)*)", re.IGNORECASE)
        if fare_filename not in self.farefiles.keys():
            raise NetworkException("Fare file {} not found".format(fare_filename))

        for line in self.farefiles[fare_filename]:
            # WranglerLogger.debug("getValueFromXfare() line = {}".format(line))
            result = xfare_re.match(line)
            if result == None:
                continue
            my_from_mode = int(result.group(1))
            if my_from_mode != from_mode:
                continue

            my_to_mode_strings = result.group(2).split(",")
            my_to_modes = [int(x) for x in my_to_mode_strings]
            # WranglerLogger.debug("getValueFromXfare my_to_modes={}".format(my_to_modes))
            if len(my_to_modes) < to_mode:
                raise NetworkException(
                    "to_mode {} not found: {}".format(to_mode, my_to_modes)
                )
            return my_to_modes[to_mode - 1]  # index starts at zero

        raise NetworkException("from_mode {} not found".format(from_mode))

    def setValueToXfare(self, fare_filename, from_mode, to_mode, value):
        """
        Assuming that fare_filename contains XFARE information (e.g. XFAR[from_mode]=to_mode1,to_mode2,...)
        Sets the value for from_mode to to_mode to value.
        Throws NetworkException if the appropriate spot isn't found
        """
        xfare_re = re.compile("(xfare\[(\d+)\]=)((-?\d+)(,\s*-?\d+)*)", re.IGNORECASE)
        if fare_filename not in self.farefiles.keys():
            raise NetworkException("Fare file {} not found".format(fare_filename))

        for line_idx in range(len(self.farefiles[fare_filename])):
            line = self.farefiles[fare_filename][line_idx]

            # WranglerLogger.debug("getValueFromXfare() line = {}".format(line))
            result = xfare_re.match(line)
            if result == None:
                continue
            my_from_mode = int(result.group(2))
            if my_from_mode != from_mode:
                continue

            my_to_mode_strings = result.group(3).split(",")
            my_to_modes = [int(x) for x in my_to_mode_strings]
            # WranglerLogger.debug("getValueFromXfare my_to_modes={}".format(my_to_modes))
            if len(my_to_modes) < to_mode:
                raise NetworkException(
                    "to_mode {} not found: {}".format(to_mode, my_to_modes)
                )

            my_to_modes[to_mode - 1] = value  # index starts at zero
            # put the line back together
            my_to_mode_strings = [str(x) for x in my_to_modes]
            comma_str = ","
            line = "{}{}\n".format(result.group(1), comma_str.join(my_to_mode_strings))
            self.farefiles[fare_filename][line_idx] = line
            return

        raise NetworkException("from_mode {} not found".format(from_mode))

    def verifyTransitLineFrequencies(self, frequencies, coverage=None):
        """
        Utility function to verify the frequencies are as expected.

         * *frequencies* is a dictionary of ``label => [ regex1, regex2, [freqlist] ]``
         * *coverage* is a regex string (not compiled) that says we want to know if we verified the
           frequencies of all of these lines.  e.g. ``MUNI*``

        """
        covset = set([])
        if coverage:
            covpattern = re.compile(coverage)
            for i in range(len(self.lines)):
                if isinstance(self.lines[i], str):
                    continue
                if covpattern.match(self.lines[i].name):
                    covset.add(self.lines[i].name)
            # print covset

        labels = frequencies.keys()
        labels.sort()
        for label in labels:
            logstr = "Verifying %-40s: " % label

            for regexnum in [0, 1]:
                frequencies[label][regexnum] = frequencies[label][regexnum].strip()
                if frequencies[label][regexnum] == "":
                    continue
                pattern = re.compile(frequencies[label][regexnum])
                freqs = self.getCombinedFreq(pattern, coverage_set=covset)
                if freqs[0] + freqs[1] + freqs[2] + freqs[3] + freqs[4] == 0:
                    logstr += "-- Found no matching lines for pattern [%s]" % (
                        frequencies[label][regexnum]
                    )
                for timeperiod in range(5):
                    if abs(freqs[timeperiod] - frequencies[label][2][timeperiod]) > 0.2:
                        logstr += "-- Mismatch. Desired %s" % str(frequencies[label][2])
                        logstr += "but got ", str(freqs)
                        lines = self.line(pattern)
                        WranglerLogger.error(logstr)
                        WranglerLogger.error("Problem lines:")
                        for line in lines:
                            WranglerLogger.error(str(line))
                        raise NetworkException("Mismatching frequency")
                logstr += "-- Match%d!" % (regexnum + 1)
            WranglerLogger.debug(logstr)

        if coverage:
            WranglerLogger.debug("Found %d uncovered lines" % len(covset))
            for linename in covset:
                WranglerLogger.debug(self.line(linename))

    def write(
        self,
        path=".",
        name="transit",
        writeEmptyFiles=True,
        suppressQuery=False,
        suppressValidation=False,
        cubeNetFileForValidation=None,
        line_only=False,
    ):
        """
        Write out this full transit network to disk in path specified.
        """
        if not suppressValidation:

            self.validateFrequencies()
            self.validateWnrsAndPnrs()

            if not cubeNetFileForValidation:
                WranglerLogger.fatal(
                    "Trying to validate TransitNetwork but cubeNetFileForValidation not passed"
                )
                exit(2)

            self.checkValidityOfLinks(cubeNetFile=cubeNetFileForValidation)

        if not os.path.exists(path):
            WranglerLogger.debug("\nPath [%s] doesn't exist; creating." % path)
            os.mkdir(path)

        else:
            trnfile = os.path.join(path, name + ".lin")
            if os.path.exists(trnfile) and not suppressQuery:
                print(
                    "File [{}] exists already.  Overwrite contents? (y/n/s) ".format(
                        trnfile
                    )
                )
                try:
                    response = raw_input("")  # python 2
                except:
                    response = input("")  # python 3
                WranglerLogger.debug("response = [%s]" % response)
                if response == "s" or response == "S":
                    WranglerLogger.debug("Skipping!")
                    return

                if response != "Y" and response != "y":
                    exit(0)

        WranglerLogger.info("Writing into %s\\%s" % (path, name))
        logstr = ""
        if len(self.lines) > 0 or writeEmptyFiles:
            # for verifying uniqueness of line names
            line_names = set()

            logstr += " lines"
            f = open(os.path.join(path, name + ".lin"), "w")
            if self.program == TransitParser.PROGRAM_TRNBUILD:
                f.write(";;<<Trnbuild>>;;\n")
            elif self.program == TransitParser.PROGRAM_PT:
                f.write(";;<<PT>><<LINE>>;;\n")
            for line in self.lines:
                if isinstance(line, str):
                    f.write(line)
                else:
                    # write it first
                    f.write(repr(line) + "\n")

                    # Cube TRNBUILD documentation for LINE NAME
                    # It may be up to 12 characters in length, and must be unique.
                    if line.name.upper() in line_names:
                        raise NetworkException(
                            "Line name {} not unique".format(line.name)
                        )
                    if len(line.name) > 12:
                        raise NetworkException(
                            "Line name {} too long".format(line.name)
                        )
                    if line.hasDuplicateStops():
                        raise NetworkException(
                            "Line {} has a stop that occurs more than once".format(
                                line.name
                            )
                        )
                    line_names.add(line.name.upper())
            f.close()

        if line_only:
            logstr += "... done."
            WranglerLogger.debug(logstr)
            WranglerLogger.info("")
            return

        if len(self.links) > 0 or writeEmptyFiles:
            logstr += " links"
            f = open(os.path.join(path, name + ".link"), "w")
            for link in self.links:
                f.write(str(link) + "\n")
            f.close()

        if len(self.pnrs) > 0 or writeEmptyFiles:
            for pnr_file in self.pnrs.keys():
                logstr += " {}_pnr".format(pnr_file)

                # don't prepend name unless it's not there already
                if pnr_file.startswith(name):
                    pnr_out_file = "{}.pnr".format(pnr_file)
                else:
                    pnr_out_file = "{}_{}.pnr".format(name, pnr_file)

                f = open(os.path.join(path, pnr_out_file), "a")
                for pnr in self.pnrs[pnr_file]:
                    f.write(str(pnr) + "\n")
                f.close()

        if len(self.zacs) > 0 or writeEmptyFiles:
            logstr += " zac"
            f = open(os.path.join(path, name + ".zac"), "w")
            for zac in self.zacs:
                f.write(str(zac) + "\n")
            f.close()

        if len(self.accessli) > 0 or writeEmptyFiles:
            logstr += " access"
            f = open(os.path.join(path, name + ".access"), "w")
            for accessli in self.accessli:
                f.write(str(accessli) + "\n")
            f.close()

        if len(self.xferli) > 0 or writeEmptyFiles:
            logstr += " xfer"
            f = open(os.path.join(path, name + ".xfer"), "w")
            for xferli in self.xferli:
                f.write(str(xferli) + "\n")
            f.close()

        if len(self.nodes) > 0 or writeEmptyFiles:
            logstr += " nodes"
            f = open(os.path.join(path, "Transit_Support_Nodes.dat"), "w")
            for nodes in self.nodes:
                f.write(str(nodes) + "\n")
            f.close()

        if len(self.supps) > 0 or writeEmptyFiles:
            logstr += " supps"
            f = open(os.path.join(path, "WALK_access.sup"), "w")
            for supplink in self.supps:
                f.write(str(supplink) + "\n")
            f.close()

        # fares
        if self.modelType in [Network.MODEL_TYPE_CHAMP, Network.MODEL_TYPE_TM1]:

            for farefile in TransitNetworkLasso.FARE_FILES[self.modelType]:
                # don't write an empty one unless there isn't anything there
                if len(self.farefiles[farefile]) == 0:
                    if writeEmptyFiles and not os.path.exists(
                        os.path.join(path, farefile)
                    ):
                        logstr += " " + farefile
                        f = open(os.path.join(path, farefile), "w")
                        f.write("; no fares known\n")
                        f.close()

                else:
                    logstr += " " + farefile
                    f = open(os.path.join(path, farefile), "w")
                    for line in self.farefiles[farefile]:
                        f.write(line)
                    f.close()
        else:
            if len(self.faresystems) > 0 or writeEmptyFiles:
                logstr += " faresystem"
                # fare and farematrix files
                f = open(os.path.join(path, name + ".far"), "w")
                f2 = open(os.path.join(path, name + "_farematrix.txt"), "w")
                for fare_id in sorted(self.faresystems.keys()):
                    f.write(str(self.faresystems[fare_id]) + "\n")
                    f2.write(self.faresystems[fare_id].getFareZoneMatrixLines())
                f.close()
                f2.close()

        if self.modelType == Network.MODEL_TYPE_TM2 and (
            self.ptsystem.isEmpty() == False or writeEmptyFiles
        ):
            logstr += " pts"
            f = open(os.path.join(path, name + ".pts"), "w")
            f.write(str(self.ptsystem) + "\n")
            f.close()

        logstr += "... done."
        WranglerLogger.debug(logstr)
        WranglerLogger.info("")

    def parseAndPrintTransitFile(self, trntxt, verbosity=1):
        """
        Verbosity=1: 1 line per line summary
        Verbosity=2: 1 line per node
        """
        self.parser.setVerbosity(verbosity)
        success, children, nextcharacter = self.parser.parse(
            trntxt, production="transit_file"
        )
        # print(nextcharacter)
        # print(success)
        if not nextcharacter == len(trntxt):
            errorstr = (
                "\n   Did not successfully read the whole file; got to nextcharacter=%d out of %d total"
                % (nextcharacter, len(trntxt))
            )
            errorstr += "\n   Did read %d lines, next unread text = [%s]" % (
                len(children),
                trntxt[nextcharacter : nextcharacter + 200],
            )
            raise NetworkException(errorstr)

        # Convert from parser-tree format to in-memory transit data structures:
        (program, convertedLines) = self.parser.convertLineData()
        convertedLinks = self.parser.convertLinkData()
        convertedPNR = self.parser.convertPNRData()
        convertedZAC = self.parser.convertZACData()
        convertedAccessLinki = self.parser.convertLinkiData("access")
        convertedXferLinki = self.parser.convertLinkiData("xfer")
        convertedNodes = self.parser.convertLinkiData("node")
        convertedSupplinks = self.parser.convertSupplinksData()
        convertedFaresystems = self.parser.convertFaresystemData()
        convertedPTSystem = self.parser.convertPTSystemData()

        return (
            program,
            convertedLines,
            convertedLinks,
            convertedPNR,
            convertedZAC,
            convertedAccessLinki,
            convertedXferLinki,
            convertedNodes,
            convertedSupplinks,
            convertedFaresystems,
            convertedPTSystem,
        )

    def parseFile(self, fullfile, insert_replace=True):
        """
        fullfile is the filename,
        insert_replace=True if you want to replace the data in place rather than appending
        """
        suffix = fullfile.rsplit(".")[-1].lower()
        self.parseFileAsSuffix(fullfile, suffix, insert_replace)

    def parseFileAsSuffix(self, fullfile, suffix, insert_replace):
        """
        This is a little bit of a hack, but it's meant to allow us to do something
        like read an xfer file as an access file...
        """
        self.parser = TransitParser(transit_file_def, 0)
        self.parser.tfp.liType = suffix
        logstr = "   Reading %s as %s" % (fullfile, suffix)
        f = open(fullfile, "r")
        prog, lines, links, pnr, zac, accessli, xferli, nodes, supps, faresys, pts = self.parseAndPrintTransitFile(
            f.read(), verbosity=0
        )
        f.close()
        logstr += self.doMerge(
            fullfile,
            prog,
            lines,
            links,
            pnr,
            zac,
            accessli,
            xferli,
            nodes,
            supps,
            faresys,
            pts,
            insert_replace,
        )
        WranglerLogger.debug(logstr)

    def doMerge(
        self,
        path,
        prog,
        lines,
        links,
        pnrs,
        zacs,
        accessli,
        xferli,
        nodes,
        supps,
        faresys,
        pts,
        insert_replace=False,
    ):
        """
        Merge a set of transit lines & support links with this network's transit representation.
        """

        logstr = " -- Merging"

        if len(lines) > 0:
            logstr += " %s lines" % len(lines)

            if len(self.lines) == 0:
                self.program = prog
            else:
                # don't mix PT and TRNBUILD
                assert (prog == TransitParser.PROGRAM_UNKNOWN) or (prog == self.program)

            extendlines = copy.deepcopy(lines)
            for line in lines:
                if isinstance(line, TransitLine) and (line in self.lines):
                    # logstr += " *%s" % (line.name)
                    if insert_replace:
                        self.lines[self.lines.index(line)] = line
                        extendlines.remove(line)
                    else:
                        self.lines.remove(line)

            if len(extendlines) > 0:
                # for line in extendlines: print line
                self.lines.extend(["\n;######################### From: " + path + "\n"])
                self.lines.extend(extendlines)

        if len(links) > 0:
            logstr += " %d links" % len(links)
            self.links.extend(["\n;######################### From: " + path + "\n"])
            self.links.extend(links)

        if len(pnrs) > 0:
            # if reading X.pnr, use X
            pnr_basename = os.path.basename(path)
            (pnr_root, pnr_ext) = os.path.splitext(pnr_basename)

            logstr += " {} {}_PNRs".format(len(pnrs), pnr_root)
            if pnr_root not in self.pnrs:
                self.pnrs[pnr_root] = []
            self.pnrs[pnr_root].extend(
                ["\n;######################### From: " + path + "\n"]
            )
            self.pnrs[pnr_root].extend(pnrs)

        if len(zacs) > 0:
            logstr += " %d ZACs" % len(zacs)
            self.zacs.extend(["\n;######################### From: " + path + "\n"])
            self.zacs.extend(zacs)

        if len(accessli) > 0:
            logstr += " %d accesslinks" % len(accessli)
            self.accessli.extend(["\n;######################### From: " + path + "\n"])
            self.accessli.extend(accessli)

        if len(xferli) > 0:
            logstr += " %d xferlinks" % len(xferli)
            self.xferli.extend(["\n;######################### From: " + path + "\n"])
            self.xferli.extend(xferli)

        if len(nodes) > 0:
            logstr += " %d nodes" % len(nodes)
            self.nodes.extend(["\n;######################### From: " + path + "\n"])
            self.nodes.extend(nodes)

        if len(supps) > 0:
            logstr += " %d supps" % len(supps)
            self.supps.extend(["\n;######################### From: " + path + "\n"])
            self.supps.extend(supps)

        if len(faresys) > 0:
            logstr += " %d faresystems" % len(faresys)

            # merge the faresystems dictionary
            for (fs_id, fs) in faresys.items():
                if fs_id in self.faresystems:
                    WranglerLogger.fatal("FARESYSTEM definition collision:")
                    WranglerLogger.fatal("  existing: " + str(self.faresystems[fs_id]))
                    WranglerLogger.fatal("       new: " + str(fs))
                    raise NetworkException("FARESYSTEM definition collision")
                else:
                    self.faresystems[fs_id] = fs

        if pts:
            logstr += " 1 PTSystem"
            self.ptsystem.merge(pts)

        logstr += "...done."
        return logstr

    def mergeFile(self, filename, insert_replace=False):
        WranglerLogger.debug("Adding Transit File: %s" % filename)

        suffix = filename.rsplit(".")[-1].lower()

        if suffix not in ["lin", "link", "pnr", "zac", "access", "xfer", "pts"]:
            msg = 'File doesn\'t have a typical transit suffix: "lin", "link", "pnr", "zac", "access", "xfer", "pts" '
            WranglerLogger.warning(msg)

        self.parser = TransitParser(transit_file_def, verbosity=0)
        self.parser.tfp.liType = suffix

        logstr = "   Reading %s" % filename
        f = open(filename, "r")
        prog, lines, links, pnr, zac, accessli, xferli, nodes, supps, faresys, pts = self.parseAndPrintTransitFile(
            f.read(), verbosity=0
        )
        f.close()
        logstr += self.doMerge(
            filename,
            prog,
            lines,
            links,
            pnr,
            zac,
            accessli,
            xferli,
            nodes,
            supps,
            faresys,
            pts,
            insert_replace,
        )
        WranglerLogger.debug(logstr)

    def mergeDir(self, path, insert_replace=False):
        """
        Append all the transit-related files in the given directory.
        Does NOT apply __init__.py modifications from that directory.
        """
        dirlist = os.listdir(path)
        dirlist.sort()
        WranglerLogger.debug("Path: %s" % path)

        for filename in dirlist:
            suffix = filename.rsplit(".")[-1].lower()
            if suffix in ["lin", "link", "pnr", "zac", "access", "xfer", "pts"]:
                self.parser = TransitParser(transit_file_def, verbosity=0)
                self.parser.tfp.liType = suffix
                fullfile = os.path.join(path, filename)
                logstr = "   Reading %s" % filename
                f = open(fullfile, "r")
                prog, lines, links, pnr, zac, accessli, xferli, nodes, supps, faresys, pts = self.parseAndPrintTransitFile(
                    f.read(), verbosity=0
                )
                f.close()
                logstr += self.doMerge(
                    fullfile,
                    prog,
                    lines,
                    links,
                    pnr,
                    zac,
                    accessli,
                    xferli,
                    nodes,
                    supps,
                    faresys,
                    pts,
                    insert_replace,
                )
                WranglerLogger.debug(logstr)

    @staticmethod
    def initializeTransitCapacity(directory="."):
        TransitNetworkLasso.capacity = TransitCapacity(directory=directory)

    def findSimpleDwellDelay(self, line):
        """
        Returns the simple mode/owner-based dwell delay for the given *line*.  This could
        be a method in :py:class:`TransitLine` but I think it's more logical to be
        :py:class:`TransitNetwork` specific...
        """
        # use AM to lookup the vehicle
        simpleDwell = TransitNetworkLasso.capacity.getSimpleDwell(line.name, "AM")

        owner = None
        if "OWNER" in line.attr:
            owner = line.attr["OWNER"].strip(r'"\'')

        if owner and owner.upper() == "TPS":
            simpleDwell -= 0.1

        if owner and owner.upper() == "BRT":
            # (20% Savings Low Floor)*(20% Savings POP)
            simpleDwell = simpleDwell * 0.8 * 0.8
            # but lets not go below 0.3
            if simpleDwell < 0.3:
                simpleDwell = 0.3

        return simpleDwell

    def addDelay(
        self,
        timeperiod="Simple",
        additionalLinkFile=None,
        complexDelayModes=[],
        complexAccessModes=[],
        transitAssignmentData=None,
        MSAweight=1.0,
        previousNet=None,
        logPrefix="",
        stripTimeFacRunTimeAttrs=True,
    ):
        """
        Replaces the old ``addDelay.awk`` script.

        The simple version simply looks up a delay for all stops based on the
        transit line's OWNER and MODE. (Owners ``TPS`` and ``BRT`` get shorter delays.)
        It will also dupe any two-way lines that are one of the complexAccessModes because those
        access mode shutoffs only make sense if the lines are one-way.

        Exempts nodes that are in the network's TransitLinks and in the optional
        *additionalLinkFile*, from dwell delay; the idea being that these are LRT or fixed
        guideway links and the link time includes a dwell delay.

        If *transitAssignmentData* is passed in, however, then the boards, alights and vehicle
        type from that data are used to calculate delay for the given *complexDelayModes*.

        When *MSAweight* < 1.0, then the delay is modified
        to be a linear combination of (prev delay x (1.0-*MSAweight*)) + (new delay x *MSAweight*))

        *logPrefix* is a string used for logging: this method appends to the following files:

           * ``lineStats[timeperiod].csv`` contains *logPrefix*, line name, total Dwell for the line,
             number of closed nodes for the line
           * ``dwellbucket[timeperiod].csv`` contails distribution information for the dwells.
             It includes *logPrefix*, dwell bucket number, and dwell bucket count.
             Currently dwell buckets are 0.1 minutes

        When *stripTimeFacRunTimeAttrs* is passed as TRUE, TIMEFAC and RUNTIME is stripped for ALL
        modes.  Otherwise it's ignored.
        """

        # Use own links and, if passed, additionaLinkFile to form linSet, which is the set of
        # nodes in the links
        linkSet = set()
        for link in self.links:
            if isinstance(link, TransitLink):
                link.addNodesToSet(linkSet)
        # print linkSet
        logstr = "addDelay: Size of linkset = %d" % (len(linkSet))

        if additionalLinkFile:
            linknet = TransitNetworkLasso(self.modelType, self.modelVersion)
            linknet.parser = TransitParser(transit_file_def, verbosity=0)
            f = open(additionalLinkFile, "r")
            junk, junk, additionallinks, junk, junk, junk, junk, junk, junk, junk, junk = linknet.parseAndPrintTransitFile(
                f.read(), verbosity=0
            )
            f.close()
            for link in additionallinks:
                if isinstance(link, TransitLink):
                    link.addNodesToSet(linkSet)
                    # print linkSet
            logstr += " => %d with %s\n" % (len(linkSet), additionalLinkFile)
        WranglerLogger.info(logstr)

        # record keeping for logging
        statsfile = open("lineStats" + timeperiod + ".csv", "a")
        dwellbucketfile = open("dwellbucket" + timeperiod + ".csv", "a")
        totalLineDwell = {}  # linename => total dwell
        totalClosedNodes = {}  # linename => closed nodes
        DWELL_BUCKET_SIZE = 0.1  # minutes
        dwellBuckets = defaultdict(int)  # initialize to index => bucket

        # Dupe the one-way lines for complexAccessModes
        if timeperiod == "Simple" and len(complexAccessModes) > 0:
            line_idx = 0
            while True:
                # out of lines, done!
                if line_idx >= len(self.lines):
                    break

                # skip non-TransitLines
                if not isinstance(self.lines[line_idx], TransitLine):
                    line_idx += 1
                    continue

                # skip non-ComplexAccessMode lines
                if int(self.lines[line_idx].attr["MODE"]) not in complexAccessModes:
                    line_idx += 1
                    continue

                # this is a relevant line -- is it oneway?  then we're ok
                if self.lines[line_idx].isOneWay():
                    line_idx += 1
                    continue

                # make it one way and add a reverse copy
                self.lines[line_idx].setOneWay()
                reverse_line = copy.deepcopy(self.lines[line_idx])
                reverse_line.reverse()

                WranglerLogger.debug(
                    "Reversed line %s to line %s"
                    % (str(self.lines[line_idx]), str(reverse_line))
                )
                self.lines.insert(line_idx + 1, reverse_line)
                line_idx += 2

        # iterate through my lines
        for line in self:

            totalLineDwell[line.name] = 0.0
            totalClosedNodes[line.name] = 0

            # strip the TIMEFAC and the RUNTIME, if desired
            if stripTimeFacRunTimeAttrs:
                if "RUNTIME" in line.attr:
                    WranglerLogger.debug("Stripping RUNTIME from %s" % line.name)
                    del line.attr["RUNTIME"]
                if "TIMEFAC" in line.attr:
                    WranglerLogger.debug("Stripping TIMEFAC from %s" % line.name)
                    del line.attr["TIMEFAC"]

            # Passing on all the lines that do not have service during the specific time of day
            if (
                timeperiod in TransitLine.HOURS_PER_TIMEPERIOD[self.modelType]
                and line.getFreq(timeperiod, self.modelType) == 0.0
            ):
                continue

            simpleDwellDelay = self.findSimpleDwellDelay(line)

            for nodeIdx in range(len(line.n)):

                # linkSet nodes exempt - don't add delay 'cos that's inherent to the link
                if int(line.n[nodeIdx].num) in linkSet:
                    continue
                # last stop - no delay, end of the line
                if nodeIdx == len(line.n) - 1:
                    continue
                # dwell delay for stop nodes only
                if not line.n[nodeIdx].isStop():
                    continue

                # =======================================================================================
                # turn off access?
                if (
                    transitAssignmentData
                    and (nodeIdx > 0)
                    and (int(line.attr["MODE"]) in complexAccessModes)
                ):
                    try:
                        loadFactor = transitAssignmentData.loadFactor(
                            line.name,
                            abs(int(line.n[nodeIdx - 1].num)),
                            abs(int(line.n[nodeIdx].num)),
                            nodeIdx,
                        )
                    except:
                        WranglerLogger.warning(
                            "Failed to get loadfactor for (%s, A=%d B=%d SEQ=%d); assuming 0"
                            % (
                                line.name,
                                abs(int(line.n[nodeIdx - 1].num)),
                                abs(int(line.n[nodeIdx].num)),
                                nodeIdx,
                            )
                        )
                        loadFactor = 0.0

                    # disallow boardings (ACCESS=2) (for all nodes except first stop)
                    # if the previous link has load factor greater than 1.0
                    if loadFactor > 1.0:
                        line.n[nodeIdx].attr["ACCESS"] = 2
                        totalClosedNodes[line.name] += 1

                # =======================================================================================
                # Simple delay if
                # - we do not have boards/alighting data,
                # - or if we're not configured to do a complex delay operation
                if not transitAssignmentData or (
                    int(line.attr["MODE"]) not in complexDelayModes
                ):
                    if simpleDwellDelay > 0:
                        line.n[nodeIdx].attr["DELAY"] = str(simpleDwellDelay)
                    totalLineDwell[line.name] += simpleDwellDelay
                    dwellBuckets[
                        int(math.floor(simpleDwellDelay / DWELL_BUCKET_SIZE))
                    ] += 1
                    continue

                # Complex Delay
                # =======================================================================================
                vehiclesPerPeriod = line.vehiclesPerPeriod(timeperiod, self.modelType)
                try:
                    boards = transitAssignmentData.numBoards(
                        line.name,
                        abs(int(line.n[nodeIdx].num)),
                        abs(int(line.n[nodeIdx + 1].num)),
                        nodeIdx + 1,
                    )
                except:
                    WranglerLogger.warning(
                        "Failed to get boards for (%s, A=%d B=%d SEQ=%d); assuming 0"
                        % (
                            line.name,
                            abs(int(line.n[nodeIdx].num)),
                            abs(int(line.n[nodeIdx + 1].num)),
                            nodeIdx + 1,
                        )
                    )
                    boards = 0

                # At the first stop, vehicle has no exits and load factor
                if nodeIdx == 0:
                    exits = 0
                else:
                    try:
                        exits = transitAssignmentData.numExits(
                            line.name,
                            abs(int(line.n[nodeIdx - 1].num)),
                            abs(int(line.n[nodeIdx].num)),
                            nodeIdx,
                        )
                    except:
                        WranglerLogger.warning(
                            "Failed to get exits for (%s, A=%d B=%d SEQ=%d); assuming 0"
                            % (
                                line.name,
                                abs(int(line.n[nodeIdx - 1].num)),
                                abs(int(line.n[nodeIdx].num)),
                                nodeIdx,
                            )
                        )
                        exits = 0

                if MSAweight < 1.0:
                    try:
                        existingDelay = float(
                            previousNet.line(line.name).n[nodeIdx].attr["DELAY"]
                        )
                    except:
                        WranglerLogger.debug(
                            "No delay found for line %s node %s -- using 0"
                            % (line.name, previousNet.line(line.name).n[nodeIdx].num)
                        )
                        existingDelay = (
                            0.0
                        )  # this can happen if no boards/alights and const=0
                else:
                    MSAdelay = -99999999
                    existingDelay = 0.0

                (
                    delay_const,
                    delay_per_board,
                    delay_per_alight,
                ) = transitAssignmentData.capacity.getComplexDwells(
                    line.name, timeperiod
                )

                WranglerLogger.debug(
                    "line name=%s, timeperiod=%s, delay_const,perboard,peralight=%.3f, %.3f, %.3f"
                    % (
                        line.name,
                        timeperiod,
                        delay_const,
                        delay_per_board,
                        delay_per_alight,
                    )
                )

                dwellDelay = (1.0 - MSAweight) * existingDelay + MSAweight * (
                    (delay_per_board * float(boards) / vehiclesPerPeriod)
                    + (delay_per_alight * float(exits) / vehiclesPerPeriod)
                    + delay_const
                )
                line.n[nodeIdx].attr["DELAY"] = "%.3f" % dwellDelay
                totalLineDwell[line.name] += dwellDelay
                dwellBuckets[int(math.floor(dwellDelay / DWELL_BUCKET_SIZE))] += 1
                # end for each node loop

            statsfile.write(
                "%s,%s,%f,%d\n"
                % (
                    logPrefix,
                    line.name,
                    totalLineDwell[line.name],
                    totalClosedNodes[line.name],
                )
            )
            # end for each line loop

        for bucketnum in dwellBuckets.keys():
            count = dwellBuckets[bucketnum]
            dwellbucketfile.write("%s,%d,%d\n" % (logPrefix, bucketnum, count))
        statsfile.close()
        dwellbucketfile.close()

    def checkCapacityConfiguration(self, complexDelayModes, complexAccessModes):
        """
        Verify that we have the capacity configuration for all lines in the complex modes.
        To save heart-ache later.
        return Success
        """
        if not TransitNetworkLasso.capacity:
            TransitNetworkLasso.capacity = TransitCapacity()

        failures = 0
        for line in self:
            linename = line.name.upper()
            mode = int(line.attr["MODE"])
            if mode in complexDelayModes or mode in complexAccessModes:

                for timeperiod in ["AM", "MD", "PM", "EV", "EA"]:
                    if line.getFreq(timeperiod, self.modelType) == 0:
                        continue

                    try:
                        (
                            vehicletype,
                            cap,
                        ) = TransitNetworkLasso.capacity.getVehicleTypeAndCapacity(
                            linename, timeperiod
                        )
                        if mode in complexDelayModes:
                            (
                                delc,
                                delpb,
                                delpa,
                            ) = TransitNetworkLasso.capacity.getComplexDwells(
                                linename, timeperiod
                            )

                    except NetworkException as e:
                        print(e)
                        failures += 1
        return failures == 0

    def moveBusesToHovAndExpressLanes(self):
        """
        Moves transit lines from GP links to equivalent HOV links and equivalent Express Lane links
        """
        # In order to run this, the roadway network needs written for us to read
        # so pick a place to write it
        import tempfile

        tempdir = tempfile.mkdtemp()
        WranglerLogger.debug("Writing roadway network to tempdir {}".format(tempdir))

        Network.allNetworks["hwy"].write(
            path=tempdir,
            name="freeflow.net",
            writeEmptyFiles=False,
            suppressQuery=True,
            suppressValidation=True,
        )
        tempnet = os.path.join(tempdir, "freeflow.net")

        # Read it
        import Cube

        link_vars = ["LANES", "USE", "FT", "TOLLCLASS", "ROUTENUM", "ROUTEDIR", "PROJ"]
        (nodes_dict, links_dict) = Cube.import_cube_nodes_links_from_csvs(
            tempnet,
            extra_link_vars=link_vars,
            links_csv=os.path.join(tempdir, "cubenet_links.csv"),
            nodes_csv=os.path.join(tempdir, "cubenet_nodes.csv"),
            exportIfExists=True,
        )
        WranglerLogger.debug(
            "Have {} nodes and {} links".format(len(nodes_dict), len(links_dict))
        )

        # links_dict: (a,b) => list with distance followed by extra_link_vars
        links_list = []
        for a_b_tuple in links_dict.keys():
            # put all attributes into a list
            distance = float(links_dict[a_b_tuple][0])
            lanes = int(links_dict[a_b_tuple][1])
            use = int(links_dict[a_b_tuple][2])
            ft = int(links_dict[a_b_tuple][3])
            tollclass = int(links_dict[a_b_tuple][4])
            routenum = int(links_dict[a_b_tuple][5])
            routedir = links_dict[a_b_tuple][6].strip(" '")
            if routedir == "' '":
                routedir = ""
            proj = links_dict[a_b_tuple][7].strip(" '")
            if proj == "' '":
                proj = ""

            link_list = [a_b_tuple[0], a_b_tuple[1]] + [
                distance,
                lanes,
                use,
                ft,
                tollclass,
                routenum,
                routedir,
                proj,
            ]
            # append to list of links
            links_list.append(link_list)

        # let's use pandas for this
        import pandas

        pandas.options.display.width = 500
        pandas.options.display.max_columns = 100

        link_cols = ["a", "b", "DISTANCE"] + link_vars
        links_df = pandas.DataFrame.from_records(data=links_list, columns=link_cols)
        WranglerLogger.debug("\n:{}".format(links_df.head()))

        # filter out HOV and express lane links
        hov_links_df = links_df.loc[(links_df.USE == 2) | (links_df.USE == 3)]
        el_links_df = links_df.loc[links_df.TOLLCLASS >= 11]
        gp_links_df = links_df.loc[
            (links_df.USE == 1)
            & (
                (links_df.FT <= 3)
                | (links_df.FT == 5)
                | (links_df.FT == 7)
                | (links_df.FT == 8)
                | (links_df.FT == 10)
            )
        ]
        dummy_links_df = links_df.loc[links_df.FT == 6]

        WranglerLogger.debug(
            "Found {} hov links, {} express lane links and {} general purpose links".format(
                len(hov_links_df), len(el_links_df), len(gp_links_df)
            )
        )

        # dummy B -> hov A, a_GP1 will be the first point of dummy access link
        hov_group1_df = pandas.merge(
            left=hov_links_df,
            right=dummy_links_df[["a", "b"]],
            how="inner",
            left_on=["a"],
            right_on=["b"],
            suffixes=["", "_GP1"],
        ).drop(columns="b_GP1")

        # hov B -> dummy A, b_GP2 will be the second point of dummy egress link
        hov_group1_df = pandas.merge(
            left=hov_group1_df,
            right=dummy_links_df[["a", "b"]],
            how="inner",
            left_on=["b"],
            right_on=["a"],
            suffixes=["", "_GP2"],
        ).drop(columns="a_GP2")

        # merge to the full GP links for complete info
        hov_group1_df = pandas.merge(
            left=hov_group1_df,
            right=gp_links_df,
            how="inner",
            left_on=["a_GP1", "b_GP2"],
            right_on=["a", "b"],
            suffixes=["", "_GP"],
        ).drop(columns=["a_GP1", "b_GP2"])

        WranglerLogger.debug(
            "Found general purpose links for {} out of {} hov links: \n{}".format(
                len(hov_group1_df), len(hov_links_df), hov_group1_df.head()
            )
        )

        # Note which links don't have GP equivalents
        hov_unmatched_df = pandas.merge(
            left=hov_links_df,
            right=hov_group1_df[["a", "b", "a_GP", "b_GP"]],
            how="left",
        )
        WranglerLogger.debug("\n{}".format(hov_unmatched_df.head()))
        hov_unmatched_df = hov_unmatched_df.loc[
            pandas.isnull(hov_unmatched_df.a_GP)
        ].drop(columns=["a_GP", "b_GP"])
        WranglerLogger.debug(
            "hov links without match ({}):\n{}".format(
                len(hov_unmatched_df), hov_unmatched_df
            )
        )

        # replace all instances of a_GP, b_GP with a_GP,a,hov,b_hov,b_gp
        # keep hov_nodes and gp_nodes
        lines_moved = []
        hov_nodes = {}
        gp_nodes = {}
        hov_dict_list = hov_group1_df.to_dict(orient="records")
        for hov_record in hov_dict_list:
            # split twice
            lines_split1 = self.splitLinkInTransitLines(
                int(hov_record["a_GP"]),
                int(hov_record["b_GP"]),
                newNode=-1 * int(hov_record["a"]),
                stop=False,
                verboseLog=False,
            )
            lines_split2 = self.splitLinkInTransitLines(
                int(hov_record["a"]),
                int(hov_record["b_GP"]),
                newNode=-1 * int(hov_record["b"]),
                stop=False,
                verboseLog=False,
            )
            lines_moved.extend(lines_split1)
            lines_moved.extend(lines_split2)
            lines_moved = sorted(set(lines_moved))
            # keep these for fixing up lines
            hov_nodes[int(hov_record["a"])] = int(hov_record["a_GP"])
            hov_nodes[int(hov_record["b"])] = int(hov_record["b_GP"])
            gp_nodes[-1 * int(hov_record["a_GP"])] = int(hov_record["a"])
            gp_nodes[-1 * int(hov_record["b_GP"])] = int(hov_record["b"])

        # when two links in a row are moved, there can be an artifact where the dummy link is used twice -- remove these
        for line in self.line(re.compile(".")):
            line.removeDummyJag(gp_nodes)

        WranglerLogger.info(
            "Moved the following {} lines to hov links: {}".format(
                len(lines_moved), lines_moved
            )
        )

        ##################  do it again for express lanes ##################

        # dummy B -> el A, a_GP1 will be the first point of dummy access link
        el_group1_df = pandas.merge(
            left=el_links_df,
            right=dummy_links_df[["a", "b"]],
            how="inner",
            left_on=["a"],
            right_on=["b"],
            suffixes=["", "_GP1"],
        ).drop(columns="b_GP1")

        # el B -> dummy A, b_GP2 will be the second point of dummy egress link
        el_group1_df = pandas.merge(
            left=el_group1_df,
            right=dummy_links_df[["a", "b"]],
            how="inner",
            left_on=["b"],
            right_on=["a"],
            suffixes=["", "_GP2"],
        ).drop(columns="a_GP2")

        # merge to the full GP links for complete info
        el_group1_df = pandas.merge(
            left=el_group1_df,
            right=gp_links_df,
            how="inner",
            left_on=["a_GP1", "b_GP2"],
            right_on=["a", "b"],
            suffixes=["", "_GP"],
        ).drop(columns=["a_GP1", "b_GP2"])

        WranglerLogger.debug(
            "Found general purpose links for {} out of {} el links: \n{}".format(
                len(el_group1_df), len(el_links_df), el_group1_df.head()
            )
        )

        # Note which links don't have GP equivalents
        el_unmatched_df = pandas.merge(
            left=el_links_df, right=el_group1_df[["a", "b", "a_GP", "b_GP"]], how="left"
        )
        WranglerLogger.debug("\n{}".format(el_unmatched_df.head()))
        el_unmatched_df = el_unmatched_df.loc[pandas.isnull(el_unmatched_df.a_GP)].drop(
            columns=["a_GP", "b_GP"]
        )
        WranglerLogger.debug(
            "el links without match ({}):\n{}".format(
                len(el_unmatched_df), el_unmatched_df
            )
        )

        # replace all instances of a_GP, b_GP with a_GP,a,el,b_el,b_gp
        # keep el_nodes and gp_nodes
        lines_moved = []
        el_nodes = {}
        gp_nodes = {}
        el_dict_list = el_group1_df.to_dict(orient="records")
        for el_record in el_dict_list:
            # split twice
            lines_split1 = self.splitLinkInTransitLines(
                int(el_record["a_GP"]),
                int(el_record["b_GP"]),
                newNode=-1 * int(el_record["a"]),
                stop=False,
                verboseLog=False,
            )
            lines_split2 = self.splitLinkInTransitLines(
                int(el_record["a"]),
                int(el_record["b_GP"]),
                newNode=-1 * int(el_record["b"]),
                stop=False,
                verboseLog=False,
            )
            lines_moved.extend(lines_split1)
            lines_moved.extend(lines_split2)
            lines_moved = sorted(set(lines_moved))
            # keep these for fixing up lines
            el_nodes[int(el_record["a"])] = int(el_record["a_GP"])
            el_nodes[int(el_record["b"])] = int(el_record["b_GP"])
            gp_nodes[-1 * int(el_record["a_GP"])] = int(el_record["a"])
            gp_nodes[-1 * int(el_record["b_GP"])] = int(el_record["b"])

        # when two links in a row are moved, there can be an artifact where the dummy link is used twice -- remove these
        for line in self.line(re.compile(".")):
            line.removeDummyJag(gp_nodes)

        WranglerLogger.info(
            "Moved the following {} lines to el links: {}".format(
                len(lines_moved), lines_moved
            )
        )

        # remove the temp dir
        shutil.rmtree(tempdir)

    def checkValidityOfLinks(self, cubeNetFile):
        """
        Checks the validity of each of the transit links against the given cubeNetFile.
        That is, each link in a .lin should either be in the roadway network, or in a .link file.
        """
        import Cube

        extra_link_vars = []
        if self.modelType == Network.MODEL_TYPE_CHAMP:
            extra_link_vars = [
                "STREETNAME",
                "LANE_AM",
                "LANE_OP",
                "LANE_PM",
                "BUSLANE_AM",
                "BUSLANE_OP",
                "BUSLANE_PM",
            ]

        (nodes_dict, links_dict) = Cube.import_cube_nodes_links_from_csvs(
            cubeNetFile,
            extra_link_vars=extra_link_vars,
            extra_node_vars=[],
            links_csv=os.path.join(os.getcwd(), "cubenet_validate_links.csv"),
            nodes_csv=os.path.join(os.getcwd(), "cubenet_validate_nodes.csv"),
            exportIfExists=True,
        )
        for line in self:

            # todo fix this
            line_is_oneway = True

            last_node = None
            for node in line:

                # this is the first node - nothing to do
                if not last_node:
                    last_node = node
                    continue

                # we need to check this link but possibly also the reverse
                link_list = [(abs(last_node), abs(node))]
                if not line_is_oneway:
                    link_list.append((abs(node), abs(last_node)))

                # check the link(s)
                for (a, b) in link_list:

                    # it's a road link
                    if (a, b) in links_dict:
                        continue

                    found_link = False
                    for link in self.links:
                        if not isinstance(link, TransitLink):
                            continue

                        if link.Anode == a and link.Bnode == b:
                            found_link = True
                            break

                        if not link.isOneway() and link.Anode == b and link.Bnode == a:
                            found_link = True
                            break

                    if found_link:
                        continue

                    WranglerLogger.warn(
                        "TransitNetwork.checkValidityOfLinks: (%d, %d) not in the roadway network nor in the off-road links (line %s)"
                        % (a, b, line.name)
                    )

                last_node = node

    def applyProject(self, parentdir, networkdir, gitdir, projectsubdir=None, **kwargs):
        """
        Apply the given project by calling import and apply.  Currently only supports
        one level of subdir (so projectsubdir can be one level, no more).
        e.g. parentdir=``tmp_blah``, networkdir=``Muni_GearyBRT``, projectsubdir=``center_center``

        See :py:meth:`Wrangler.Network.applyProject` for argument details.
        """
        # paths are already taken care of in checkProjectVersion
        if projectsubdir:
            projectname = projectsubdir
        else:
            projectname = networkdir

        evalstr = "import %s; %s.apply(self" % (projectname, projectname)
        for key in kwargs.keys():
            val = kwargs[key]
            evalstr += ", %s=%s" % (key, str(val))
        evalstr += ")"
        try:
            exec(evalstr)
        except:
            print("Failed to exec [%s]".format(evalstr))
            raise

        evalstr = "dir(%s)" % projectname
        projectdir = eval(evalstr)
        # WranglerLogger.debug("projectdir = " + str(projectdir))
        pyear = eval("%s.year()" % projectname) if "year" in projectdir else None
        pdesc = eval("%s.desc()" % projectname) if "desc" in projectdir else None

        # print "projectname=" + str(projectname)
        # print "pyear=" + str(pyear)
        # print "pdesc=" + str(pdesc)

        # fares
        for farefile in TransitNetworkLasso.FARE_FILES[self.modelType]:
            fullfarefile = os.path.join(gitdir, farefile)
            linecount = 0
            # WranglerLogger.debug("cwd=%s  farefile %s exists? %d" % (os.getcwd(), fullfarefile, os.path.exists(fullfarefile)))

            if os.path.exists(fullfarefile):
                infile = open(fullfarefile, "r")
                lines = infile.readlines()
                self.farefiles[farefile].extend(lines)
                linecount = len(lines)
                infile.close()
                WranglerLogger.debug(
                    "Read %5d lines from fare file %s" % (linecount, fullfarefile)
                )

        return self.logProject(
            gitdir=gitdir,
            projectname=(
                networkdir + "\\" + projectsubdir if projectsubdir else networkdir
            ),
            year=pyear,
            projectdesc=pdesc,
        )
