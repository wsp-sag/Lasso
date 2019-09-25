import os
import glob
import re

import pytest

from lasso import process_line_file

"""
Run tests from bash/shell
Run just the tests labeled project using `pytest -m project`
To run with print statments, use `pytest -s -m project`
"""

EX_DIR = os.path.join(os.getcwd(),'examples','cube')

## create list of example transit files to use as input
transit_file_list=glob.glob(os.path.join(EX_DIR,"*.LIN"))

print("FILES TO READ:", transit_file_list)

@pytest.mark.parametrize("transit_filename", transit_file_list)
def test_read_cube_linefile(request,transit_filename):
    print("\n--Starting:",request.node.name)

    print("Reading: {}".format(transit_filename))
    line = process_line_file(transit_filename)
    print(line)
