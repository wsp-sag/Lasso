import pytest

from lasso import time_utils
from lasso.time_utils import _hhmmss_to_datetime
from lasso.time_utils import _secs_to_datetime


@pytest.mark.time
def test__hhmmss_to_datetime():
    dt1 = _hhmmss_to_datetime("12:35:00")
    dt2 = _hhmmss_to_datetime("12:35")
    assert dt1 == dt2
    ##todo this isn't really doing anything


@pytest.mark.time
def test__secs_to_datetime():
    dt1_hhmmss = _hhmmss_to_datetime("14:00")
    dt1_sec = _secs_to_datetime(14 * 60 * 60)

    dt2_hhmmss = _hhmmss_to_datetime("7:35:11")
    dt2_sec = _secs_to_datetime(7 * 3600 + 35 * 60 + 11)

    assert dt1_hhmmss == dt1_sec
    assert dt2_hhmmss == dt2_sec


@pytest.mark.time
def test_datetime_to_time_period_abbr():

    tod_map = {
        "AM": ("6:00", "10:00"),
        "MD": ("10:00", "15:00"),
        "PM": ("15:00", "19:00"),
        "NT": ("19:00", "6:00"),
    }

    tp_abbr1_result = time_utils.datetime_to_time_period_abbr(
        _hhmmss_to_datetime("15:00:10"), tod_map,
    )

    tp_abbr2_result = time_utils.datetime_to_time_period_abbr(
        _hhmmss_to_datetime("5:00"), tod_map,
    )

    assert tp_abbr1_result == "PM"
    assert tp_abbr2_result == "NT"


@pytest.mark.time
def test_hhmmss_to_time_period_abbr():

    tod_map = {
        "AM": ("06:00", "10:00"),
        "MD": ("10:00", "15:00"),
        "PM": ("15:00", "19:00"),
        "NT": ("19:00", "06:00"),
    }

    tp_abbr1_result = time_utils.hhmmss_to_time_period_abbr("07:00:00", tod_map,)

    tp_abbr2_result = time_utils.hhmmss_to_time_period_abbr("19:00", tod_map,)

    assert tp_abbr1_result == "AM"
    assert tp_abbr2_result == "NT"


@pytest.mark.time
def test_timec_to_time_period():
    tod_map = {
        "AM": ("06:00", "10:00"),
        "MD": ("10:00", "15:00"),
        "PM": ("15:00", "19:00"),
        "NT": ("19:00", "06:00"),
    }

    tp_abbr1_result = time_utils.time_sec_to_time_period(3 * 3600, tod_map,)

    tp_abbr2_result = time_utils.time_sec_to_time_period(16 * 3600, tod_map,)

    assert tp_abbr1_result == "NT"
    assert tp_abbr2_result == "PM"


@pytest.mark.time
def test_get_timespan_from_tranit_network_model_time_period():
    tod_map = {
        "AM": ("06:00", "10:00"),
        "MD": ("10:00", "15:00"),
        "PM": ("15:00", "19:00"),
        "NT": ("19:00", "06:00"),
    }

    transit_tod_map = {
        1: "AM",
        2: "MD",
    }

    timespan_result_1 = time_utils.get_timespan_from_transit_network_model_time_period(
        [1], tod_map, transit_tod_map,
    )

    timespan_result_2 = time_utils.get_timespan_from_transit_network_model_time_period(
        [1, 2], tod_map, transit_tod_map,
    )

    assert timespan_result_1 == ("06:00:00", "10:00:00")
    assert timespan_result_2 == ("06:00:00", "15:00:00")


@pytest.mark.time
def test_get_timespan_from_network_model_time_period_abbr():
    tod_map = {
        "AM": ("6:00", "10:00"),
        "MD": ("10:00", "15:00"),
        "PM": ("15:00", "19:00"),
        "NT": ("19:00", "6:00"),
    }

    timespan_result_1 = time_utils.get_timespan_from_network_model_time_period_abbr(
        ["MD"], tod_map,
    )

    timespan_result_2 = time_utils.get_timespan_from_network_model_time_period_abbr(
        ["AM", "MD"], tod_map,
    )

    assert timespan_result_1 == ("10:00:00", "15:00:00")
    assert timespan_result_2 == ("06:00:00", "15:00:00")
