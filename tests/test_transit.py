import pandas as pd
import pytest


@pytest.mark.transit
def test_create_delete_route_change_dict(request):
    print("\n--Starting:", request.node.name)
    from lasso.model_transit import _create_delete_route_change_dict

    df = pd.DataFrame(
        {
            "name": "Line to be deleted",
            "direction_id": 0,
            "start_time_HHMM": "6:00",
            "end_time_HHMM": "9:00",
        }
    )
    test_delete_change_dict = _create_delete_route_change_dict(df.iloc[0])
    expected_delete_change_dict = {}
    assert test_delete_change_dict == expected_delete_change_dict


@pytest.mark.transit
def test_update_line_change_dict(request):
    print("\n--Starting:", request.node.name)
    from lasso.model_transit import _update_line_change_dict

    df = pd.DataFrame(
        {
            ("name", "self"): "Line to be updated",
            ("headway_secs", "self"): 300,
            ("headway_secs", "other"): 30,
            ("start_time_HHMM", "self"): "6:00",
            ("end_time_HHMM", "self"): "9:00",
        }
    )

    ##todo
    test_update_change_dict = _update_line_change_dict(df.iloc[0])
    expected_update_change_dict = {}
    assert test_update_change_dict == expected_update_change_dict


@pytest.mark.transit
def test_new_line_change_dict(request):
    print("\n--Starting:", request.node.name)
    from lasso.model_transit import _new_line_change_dict

    df = pd.DataFrame(
        {
            "name": "New Line",
            "direction_id": 0,
            "agency_id": 5,
            "start_time_HHMM": "7:00",
            "end_time_HHMM": "10:00",
        }
    )
    test_new_line_change_dict = _new_line_change_dict(df.iloc[0])
    expected_new_line_change_dict = {}
    assert test_new_line_change_dict == expected_new_line_change_dict
