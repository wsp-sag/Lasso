import copy, glob, inspect, math, os, re, shutil, sys
from collections import defaultdict
from .Logger import WranglerLogger
from .PTSystem import PTSystem
from .TransitLine import TransitLine
from .NetworkException import NetworkException
from .TransitParser0 import TransitParser, transit_file_def


__all__ = ["TransitNetworkLasso"]


class TransitNetworkLasso():
    ##rename
    """
    Cube representation of a transit network
    """

    TRANSIT_FILE_SUFFIXES = ["lin", "link", "pnr", "zac", "access", "xfer", "pts"]

    def __init__(
        self,
        network_dir = None,
        network_files = [],
        parameters = None,
    ):
        """
        If *basenetworkpath* is passed and *isTiered* is True, then start by reading the files
        named *networkName*.* in the *basenetworkpath*
        """

        try:
            self.cube_program =  parameters[program]
            assert self.cube_program in ["TRNBUILD", "PT"]
        except:
            self.cube_program = "TRNBUILD"
        #will be one of PROGRAM_PT or PROGRAM_TRNBUILD

        self.lines = []
        self.links = (
            []
        )  # TransitLink instances, Factor instances and comments (strings)
        self.pnrs = {}  # key is file name since these need to stay separated
        self.nodes = []  # transit node coords

        self.DELAY_VALUES = None
        self.currentLineIdx = 0

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
        ):
            return True

        return False


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

        return (
            program,
            convertedLines,
            convertedLinks,
            convertedPNR,
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
        (
            prog,
            lines,
            links,
            pnr,
        ) = self.parseAndPrintTransitFile(f.read(), verbosity=0)
        f.close()
        logstr += self.doMerge(
            fullfile,
            prog,
            lines,
            links,
            pnr,
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

        logstr += "...done."
        return logstr

    def mergeFile(self, filename, insert_replace=False):
        WranglerLogger.debug("Adding Transit File: %s" % filename)

        suffix = filename.rsplit(".")[-1].lower()

        if suffix not in TransitNetworkLasso.TRANSIT_FILE_SUFFIXES:
            msg = 'File doesn\'t have a typical transit suffix: {} '.format(TransitNetworkLasso.TRANSIT_FILE_SUFFIXES)
            WranglerLogger.warning(msg)

        self.parser = TransitParser(transit_file_def, verbosity=0)
        self.parser.tfp.liType = suffix

        logstr = "   Reading %s" % filename
        f = open(filename, "r")
        (
            prog,
            lines,
            links,
            pnr,
        ) = self.parseAndPrintTransitFile(f.read(), verbosity=2)
        f.close()
        logstr += self.doMerge(
            filename,
            prog,
            lines,
            links,
            pnr,
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
            if suffix in TransitNetworkLasso.TRANSIT_FILE_SUFFIXES:
                self.mergeFile(filename)
