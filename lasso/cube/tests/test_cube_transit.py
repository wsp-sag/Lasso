import pytest
import os


"""
Run tests from bash/shell
usage:
    pytest -s lasso/cube/tests/test_cube_transit.py
"""

BASE_TRANSIT_DIR = os.path.join(os.getcwd(), "examples", "stpaul")


@pytest.mark.travis
@pytest.mark.transit
def test_read_std_transit_from_wrangler_object(request):
    print("\n--Starting:", request.node.name)
    from network_wrangler import TransitNetwork
    from lasso.cube import CubeTransit

    wrangler_transit = TransitNetwork.read(BASE_TRANSIT_DIR)
    tnet = CubeTransit.from_source(wrangler_transit)
    print(f"routes: {tnet.routes}")


@pytest.mark.travis
@pytest.mark.transit
def test_read_std_transit_from_gtfs(request):
    print("\n--Starting:", request.node.name)
    from lasso.cube import CubeTransit

    tnet = CubeTransit.from_source(BASE_TRANSIT_DIR)
    print(f"routes: {tnet.routes}")


@pytest.mark.travis
@pytest.mark.transit
def test_read_cube_transit_from_file(request):
    print("\n--Starting:", request.node.name)
    from lasso.cube import CubeTransit

    cube_t = os.path.join(BASE_TRANSIT_DIR, "cube", "transit_orig.lin")
    tnet = CubeTransit.from_source(cube_t)
    print(f"routes: {tnet.routes}")


@pytest.mark.travis
@pytest.mark.transit
def test_write_cube_transit_fr_standard(request):
    print("\n--Starting:", request.node.name)
    from lasso.cube import CubeTransit

    tnet = CubeTransit.from_source(BASE_TRANSIT_DIR)
    tnet.write_cube(outpath="t_transit_test_from_std.lin")


@pytest.mark.travis
@pytest.mark.transit
def test_write_cube_transit_fr_cube(request):
    print("\n--Starting:", request.node.name)
    from lasso.cube import CubeTransit

    cube_t = os.path.join(BASE_TRANSIT_DIR, "cube", "transit_orig.lin")
    tnet = CubeTransit.from_source(cube_t)
    print(f"tnet.route_properties_by_time_df:\n{tnet.route_properties_by_time_df}")
    tnet.write_cube(outpath="t_transit_test_from_cube.lin")


route_edits = [
    [
        "Adding stops at node 3 and 4",
        "N=1,2,-3,-4,-5,-6,7,8,9,10",
        "N=1,2,3,4,-5,-6,7,8,9,10",
        [2, -3, -4, -5],
        [2, 3, 4, -5],
    ],
    ["Extend from 4 to 7", "N=1,2,3,4", "N=1,2,3,4,5,6,7", [3, 4], [3, 4, 5, 6, 7],],
    [
        "Reroute between 3 and 4",
        "N=1,2,3,4",
        "1,2,3,31,35,37,4",
        [2, 3, 4],
        [2, 3, 31, 35, 37, 4],
    ],
    ["Shorten from 7 to 5", "N=1,2,3,4,5,6,7", "N=1,2,3,4,5", [4, 5, 6, 7], [4, 5],],
    [
        "Shorten from from 1 to 4",
        "N=1,2,3,4,5,6,7",
        "4,5,6,7",
        [1, 2, 3, 4, 5],
        [4, 5],
    ],
    [
        "Edit within a loop",
        "N=1,2,3,4,15,16,4,5,6",
        "N=1,2,3,4,25,26,27,4,5,6",
        [3, 4, 15, 16, 4, 5],
        [3, 4, 25, 26, 27, 4, 5],
    ],
    [
        "Edit after a loop",
        "N=1,2,3,4,15,16,4,5,6",
        "N=1,2,3,4,15,16,4,55,66",
        [16, 4, 5, 6],
        [16, 4, 55, 66],
    ],
    [
        "Edit before a loop",
        "N=1,2,3,4,15,16,4,5,6",
        "N=1,22,33,4,15,16,4,5,6",
        [1, 2, 3, 4, 15],
        [1, 22, 33, 4, 15],
    ],
]


@pytest.mark.menow
@pytest.mark.travis
@pytest.mark.transit
@pytest.mark.parametrize(
    "name,base_r,edit_r,expected_existing,expected_set", route_edits
)
def test_compare_route_shapes(
    request, name, base_r, edit_r, expected_existing, expected_set
):
    print("\n--Starting:", request.node.name)
    print("\n--------Edit:", name)
    base_t = f"""
    ;;<<PT>><<LINE>>;;
    LINE NAME="1234",
     LONGNAME="Adding stops at node 3 and 4",
     HEADWAY[1]=60, MODE=7, {base_r}
    """
    edit_t = f"""
    ;;<<PT>><<LINE>>;;
    LINE NAME="1234",
     LONGNAME="Adding stops at node 3 and 4",
     HEADWAY[1]=60, MODE=7, {edit_r}
    """

    from lasso.cube import CubeTransit

    base_tnet = CubeTransit.from_source(base_t)
    edit_tnet = CubeTransit.from_source(edit_t)

    from lasso.model_transit import evaluate_route_shape_changes

    change_list = evaluate_route_shape_changes(base_tnet, edit_tnet, n_buffer_vals=2,)
    change_dict = change_list[0]

    print(f"\nexisting: {base_r}\nedit: {edit_r}\nchange_dict: {change_dict}")
    assert (change_dict["existing"], change_dict["set"]) == (
        expected_existing,
        expected_set,
    )
