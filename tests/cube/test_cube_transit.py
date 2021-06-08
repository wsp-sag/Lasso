import pytest
import os

from lasso.cube import CubeTransit

"""
Run tests from bash/shell
usage:
    pytest -s lasso/cube/tests/test_cube_transit.py
"""

BASE_TRANSIT_DIR = os.path.join(os.getcwd(), "examples", "stpaul")
CUBE_DIR = os.path.join(BASE_TRANSIT_DIR, "cube")


##############
# 1. Parsing #
##############
@pytest.mark.transit
@pytest.mark.travis
def test_parse_transit_linefile(request):
    print("\n--Starting:", request.node.name)
    test_lin = """
    ;;<<PT>><<LINE>>;;
    LINE NAME="0_452-111_452_pk1",
    MODE=5,
    HEADWAY[1]=10,
    NODES=
     39249,
     -39240,
     54648

     LINE NAME="0_134-111_134_pk1",
      LONGNAME="Ltd Stop - Highland - Cleveland - Cretin - Mpls",
      HEADWAY[1]=20,
      MODE=5,
      ONEWAY=T,
      OPERATOR=3,
     NODES=
      39249,
      -39240,
      54648,
      43503,
      -55786,
      -55785,
      55782,
      -55781,
      -55779

     LINE NAME="0_134-111_134_pk0",
      LONGNAME="Ltd Stop - Highland - Cleveland - Cretin - Mpls",
      HEADWAY[1]=90,
      MODE=5,
      ONEWAY=T,
      OPERATOR=3,
     NODES=
      83733,
      -9533,
      20208,
      84250,
      92566,
      129190
    """

    tn = CubeTransit.from_source(test_lin)
    print("TYPE", tn)
    ex_line_name = tn.routes[1]
    print("Route: {}".format(ex_line_name))
    print("Properties: ", tn.properties_of_route(ex_line_name))
    print("Nodes: ", tn.shapes_of_route(ex_line_name))


@pytest.mark.travis
@pytest.mark.transit
def test_parse_transit_linefile_with_node_vars(request):
    print("\n--Starting:", request.node.name)
    test_lin = """
    ;;<<PT>><<LINE>>;;
    LINE NAME="0_452-111_452_pk1",
    MODE=5,
    HEADWAY[1]=10,
    NODES=
     39249,ACCESS=1,
     -39240,
     54648

     LINE NAME="0_134-111_134_pk1",
      LONGNAME="Ltd Stop - Highland - Cleveland - Cretin - Mpls",
      HEADWAY[1]=20,
      MODE=5,
      ONEWAY=T,
      OPERATOR=3,
     NODES=
      39249,
      -39240,
      54648,
      43503,ACCESS=-1,
      -55786,
      -55785,
      55782,
      -55781,
      -55779

     LINE NAME="0_134-111_134_pk0",
      LONGNAME="Ltd Stop - Highland - Cleveland - Cretin - Mpls",
      HEADWAY[1]=90,
      MODE=5,
      ONEWAY=T,
      OPERATOR=3,
     NODES=
      83733,
      -9533,
      20208,NNTIME=1.5,
      84250,NNTIME=1.5,
      92566,
      129190
    """

    tn = CubeTransit.from_source(test_lin)
    print("TYPE", tn)
    ex_line_name = tn.routes[1]
    print("Line: {}".format(ex_line_name))
    print("Properties: ", tn.properties_of_route(ex_line_name))
    print("Nodes: ", tn.shapes_of_route(ex_line_name))


##################
# 2. Input       #
# - from file    #
# - from dir     #
# - from wrangler#
# - from gtfs    #
##################


@pytest.mark.travis
@pytest.mark.transit
def test_read_cube_transit_from_file(request):
    print("\n--Starting:", request.node.name)

    cube_t = os.path.join(BASE_TRANSIT_DIR, "cube", "transit_orig.lin")
    tnet = CubeTransit.from_source(cube_t)

    print(f"Routes: {tnet.routes}")


@pytest.mark.travis
@pytest.mark.transit
def test_read_cube_transit_from_dir(request):
    print("\n--Starting:", request.node.name)

    tn = CubeTransit.from_source(CUBE_DIR)

    print("Read {} routes:\n{}".format(len(tn.routes), "\n - ".join(tn.routes)))
    print("Source files: {}".format("\n - ".join(tn.source_list)))
    ## todo write an assert that actually tests something


@pytest.mark.travis
@pytest.mark.transit
def test_read_cube_transit_from_wrangler_object(request):
    print("\n--Starting:", request.node.name)
    from network_wrangler import TransitNetwork

    wrangler_transit = TransitNetwork.read(BASE_TRANSIT_DIR)
    tn = CubeTransit.from_source(wrangler_transit)

    print("Read {} routes:\n{}".format(len(tn.routes), "\n - ".join(tn.routes)))


@pytest.mark.travis
@pytest.mark.transit
def test_read_cube_transit_from_gtfs(request):
    print("\n--Starting:", request.node.name)
    from lasso.cube import CubeTransit

    tn = CubeTransit.from_source(BASE_TRANSIT_DIR)
    print("Read {} routes:\n{}".format(len(tn.routes), "\n - ".join(tn.routes)))


##########################
# 3. Translation/Output  #
# - gtfs to cube         #
# - cube to cube         #
##########################


@pytest.mark.travis
@pytest.mark.transit
def test_write_cube_transit_fr_standard(request):
    print("\n--Starting:", request.node.name)
    from lasso.cube import CubeTransit

    tn = CubeTransit.from_source(BASE_TRANSIT_DIR)
    tn.write_cube(outpath="t_transit_test_from_std.lin")
    # todo write an assert


@pytest.mark.travis
@pytest.mark.transit
def test_write_cube_transit_fr_cube(request):
    print("\n--Starting:", request.node.name)
    from lasso.cube import CubeTransit

    cube_t = os.path.join(BASE_TRANSIT_DIR, "cube", "transit_orig.lin")
    tn = CubeTransit.from_source(cube_t)
    print(f"tnet.route_properties_by_time_df:\n{tn.route_properties_by_time_df}")
    tn.write_cube(outpath="t_transit_test_from_cube.lin")
