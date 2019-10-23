
import copy, glob, inspect, math, os, re, shutil, sys
from collections import defaultdict

from .transit_parser import TransitParser, transit_file_def

__all__ = [ 'process_line_file', 'TransitLine']

def process_line_file(line_file):
    parser = TransitParser(transit_file_def, 0)
    with open(line_file) as line_file:
        lines = self.parseAndPrintTransitFile(line_file.read(), verbosity=0)
        self.parser.setVerbosity(verbosity)
        success, children, nextcharacter = self.parser.parse(trntxt, production="transit_file")
        if not nextcharacter==len(trntxt):
            errorstr  = "\n   Did not successfully read the whole file; got to nextcharacter=%d out of %d total" % (nextcharacter, len(trntxt))
            errorstr += "\n   Did read %d lines, next unread text = [%s]" % (len(children), trntxt[nextcharacter:nextcharacter+200])
            raise NetworkException(errorstr)

        # Convert from parser-tree format to in-memory transit data structures:
        (program, lines) = self.parser.convertLineData()
    return lines
