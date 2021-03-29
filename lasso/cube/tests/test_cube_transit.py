import pytest
import os

BASE_TRANSIT_DIR = os.path.join(os.getcwd(), "examples", "stpaul")


@pytest.mark.travis
@pytest.mark.transit
def test_read_std_transit_from_wrangler_object(request):
    print("\n--Starting:", request.node.name)
    from lasso.cube import CubeTransit

    tnet = CubeTransit.from_source(BASE_TRANSIT_DIR)
    print(f"routes: {tnet.routes}")


@pytest.mark.travis
@pytest.mark.transit
def test_read_std_transit_from_file(request):
    print("\n--Starting:", request.node.name)
    from lasso.cube import CubeTransit

    tnet = CubeTransit.from_source(BASE_TRANSIT_DIR)
    print(f"routes: {tnet.routes}")


@pytest.mark.travis
@pytest.mark.transit
def test_read_cube_transit_from_file(request):
    from lasso.cube import CubeTransit

    print("\n--Starting:", request.node.name)
    tnet = CubeTransit.from_source(
        os.path.join(BASE_TRANSIT_DIR, "cube", "transit_orig.lin")
    )
    print(f"routes: {tnet.routes}")


@pytest.mark.menow
@pytest.mark.travis
@pytest.mark.transit
def test_write_cube_transit_standard(request):
    print("\n--Starting:", request.node.name)
    from lasso.cube import CubeTransit

    tnet = CubeTransit.from_source(BASE_TRANSIT_DIR)
    tnet.write_cube(outpath="t_transit_test.lin")
