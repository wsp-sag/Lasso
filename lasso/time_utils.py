"""
Functions which support datetime and time period operations.

Includes:
    :py:func:`_hhmmss_to_datetime`
    :py:func:`_secs_to_datetime`
    :py:func:`datetime_to_time_period_abbr`
    :py:func:`hhmmss_to_time_period_abbr`
    :py:func:`time_sec_to_time_period`
    :py:func:`get_timespan_from_transit_network_model_time_period`
    :py:func:`get_timespan_from_network_model_time_period_abbr`
"""

import datetime

from typing import Mapping, Collection

from network_wrangler import WranglerLogger


def _hhmmss_to_datetime(hhmmss_str: str):
    """
    Creates a datetime time object from a string of hh:mm:ss

    Args:
        hhmmss_str: string of hh:mm:ss
    Returns:
        dt: datetime.time object representing time
    """
    import datetime

    dt = datetime.time(*[int(i) for i in hhmmss_str.split(":")])

    return dt


def _secs_to_datetime(secs: int) -> datetime.time:
    """
    Creates a datetime time object from a seconds from midnight

    Args:
        secs: seconds from midnight
    Returns:
        dt: datetime.time object representing time
    """

    dt = (datetime.datetime.min + datetime.timedelta(seconds=secs)).time()

    return dt


def datetime_to_time_period_abbr(
    this_datetime: datetime.time,
    time_period_abbr_to_time: Mapping[str, Collection[str]],
) -> str:
    """[summary]

    Args:
        this_datetime (datetime.time): [description]
        time_period_abbr_to_time (Mapping[str,Collection[str,str]]): Should be contained in
            parameters.network_model_ps.time_period_abbr_to_time
        as_int (bool, optional): [description]. Defaults to False.

    Returns:
        str: model time period abbieviation
    """
    tp_abbr_to_dt = {
        abbr: list(map(_hhmmss_to_datetime, _times))
        for abbr, _times in time_period_abbr_to_time.items()
    }

    # Initially assign to the time period spanning midnight, if it exists.
    this_tp_abbr = None
    for _tp_abbr, (_start_time_dt, _end_time_dt) in tp_abbr_to_dt.items():
        if _start_time_dt > _end_time_dt:
            this_tp_abbr = _tp_abbr
            break

    for _tp_abbr, (_start_time_dt, _end_time_dt) in tp_abbr_to_dt.items():
        if (_start_time_dt <= this_datetime) and (_end_time_dt > this_datetime):
            this_tp_abbr = _tp_abbr

    if not this_tp_abbr:
        msg = f"""Can't find appropriate time period for {this_datetime} using
                  lookup: {time_period_abbr_to_time}."""
        raise ValueError(msg)

    return this_tp_abbr


def hhmmss_to_time_period_abbr(
    time_hhmmss: str, time_period_abbr_to_time: Mapping[str, Collection[str]]
) -> str:
    """[summary]

    Args:
        time_hhmmss (str): [description]
        time_period_abbr_to_time (Mapping[str,Collection[str,str]]): Should be contained in
            parameters.network_model_ps.time_period_abbr_to_time
        as_int (bool, optional): [description]. Defaults to False.

    Returns:
        str: model time period abbieviation
    """
    time_dt = _hhmmss_to_datetime(time_hhmmss)
    tp_abbr = datetime_to_time_period_abbr(time_dt, time_period_abbr_to_time)

    return tp_abbr


def time_sec_to_time_period(
    time_secs: int, time_period_abbr_to_time: Mapping[str, Collection[str]]
) -> str:
    """
    Converts seconds from midnight to the model time period.

    Args:
        time_secs (int): seconds from midnight.
        time_period_abbr_to_time (Mapping[str,Collection[str,str]]): Should be contained in
            parameters.network_model_ps.time_period_abbr_to_time
        as_int (bool, optional): [description]. Defaults to False.

    Returns:
        str: model time period abbieviation
    """

    time_dt = _secs_to_datetime(time_secs)

    tp_abbr = datetime_to_time_period_abbr(time_dt, time_period_abbr_to_time)

    return tp_abbr


def get_timespan_from_transit_network_model_time_period(
    transit_network_model_time_periods: Collection[int],
    time_period_abbr_to_time: Mapping[str, Collection[str]],
    transit_to_network_time_periods: Mapping[int, str],
) -> Collection[str]:
    """Calculate the start and end times from a list of transit network model time periods.
    WARNING: Doesn't take care of discongruous time periods!!!!

    Args:
        transit_network_model_time_periods (Collection[int]): list of integers representing transit
            network model time periods to find time span for.
        time_period_abbr_to_time (Mapping[str,Collection[str,str]]): Should be contained in
            parameters.network_model_ps.time_period_abbr_to_time
        transit_to_network_time_periods (Mapping): transit_to_network_time_periods

    Returns:
        Collection[str]: Tuple of start and end times in HH:MM:SS strings
    """

    model_time_period_abbrs = [
        transit_to_network_time_periods[t] for t in transit_network_model_time_periods
    ]

    timespan = get_timespan_from_network_model_time_period_abbr(
        model_time_period_abbrs=model_time_period_abbrs,
        time_period_abbr_to_time=time_period_abbr_to_time,
    )
    return timespan


def get_timespan_from_network_model_time_period_abbr(
    model_time_period_abbrs: Collection[str],
    time_period_abbr_to_time: Mapping[str, Collection[str]],
) -> Collection[str]:
    """
    Calculate the start and end times of the property change
    WARNING: Doesn't take care of discongruous time periods!!!!
    ##todo doesn't take care of overnight.
    Args:
        model_time_period_abbrs: list of model time period abbreviations
        time_period_abbr_to_time (Mapping[str,Collection[str,str]]): Should be contained in
            parameters.network_model_ps.time_period_abbr_to_time

    Returns:
        Collection[str]: Tuple of start and end times in HH:MM:SS strings
    """

    msg = f"current_model_time_period_numbers:{model_time_period_abbrs}"
    WranglerLogger.debug(msg)

    start_times_dt = [
        datetime.datetime.strptime(time_period_abbr_to_time[tp][0], "%H:%M")
        for tp in model_time_period_abbrs
    ]
    end_times_dt = [
        datetime.datetime.strptime(time_period_abbr_to_time[tp][1], "%H:%M")
        for tp in model_time_period_abbrs
    ]

    earliest_start_time = min(start_times_dt).strftime("%H:%M:%S")
    latest_end_time = max(end_times_dt).strftime("%H:%M:%S")

    msg = f"timespan: {earliest_start_time} --> {latest_end_time}"
    WranglerLogger.debug(msg)

    return earliest_start_time, latest_end_time
