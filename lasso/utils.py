"""
Functions which support Lasso but are not specific to a class.
"""
from typing import Mapping, Collection, Union, Any
import os

import geopandas as gpd
from pandas import DataFrame
import numpy as np

from .logger import WranglerLogger

import cProfile
import functools
import pstats
import tempfile


def profile_me(func):
    """Function wrapper to do performance profiling using `cProfile`."""

    @functools.wraps(func)
    def wrapped_func(*args, **kwargs):
        file = tempfile.mktemp()
        profiler = cProfile.Profile()
        profiler.runcall(func, *args, **kwargs)
        profiler.dump_stats(file)
        metrics = pstats.Stats(file)
        stats = metrics.strip_dirs().sort_stats("time")
        stats.print_stats()
        WranglerLogger.info(stats)

    return wrapped_func


def get_shared_streets_intersection_hash(lat, lon, osm_node_id=None):
    """
    Calculated per:
       https://github.com/sharedstreets/sharedstreets-js/blob/0e6d7de0aee2e9ae3b007d1e45284b06cc241d02/src/index.ts#L553-L565
    Expected in/out
      -93.0965985, 44.952112199999995 osm_node_id = 954734870
       69f13f881649cb21ee3b359730790bb9

    """
    import hashlib

    message = "Intersection {0:.5f} {0:.5f}".format(lon, lat)  # noqa F523
    if osm_node_id:
        message += " {}".format(osm_node_id)
    unhashed = message.encode("utf-8")
    hash = hashlib.md5(unhashed).hexdigest()
    return hash


def column_name_to_parts(
    c: str,
    parameters=None,
    delim="_",
    in_order: Collection[str] = ["managed", "base_name", "category", "time_period"],
) -> Collection[Union[str, bool]]:
    """Transforms a roadway column name for a property with a given basename (e.g. LANES),
    category (e.g. HOV), managed status ("ML"), and time period (e.g. "AM) into a tuple
    (basename, time_period, category, managed).

    Will look for categories and time periods in parameter set if given, otherwise
    will use default parameters.

    Parameters used:
    - timeperiod abbreviations: `parameters.network_model_ps.network_time_period_abbr`
    - categories: `parameters.roadway_network_ps.category_grouping.keys()`

    Args:
        c ([type]): composite column name
        parameters (Parameters, optional): Defaults to default Parameters().
        delim: what column parts are delimted by. Defaults to "_".
        in_order: what order the variables are concatenated together in in the input.
            Defaults to: managed, base_name, category, time_period

    Returns:
        Tuple of base_name, time_period, category, managed.
    """
    if in_order != ["managed", "base_name", "category", "time_period"]:
        raise NotImplementedError

    if not parameters:
        from .parameters import Parameters

        parameters = Parameters()

    tps = parameters.network_model_ps.network_time_period_abbr
    cats = list(parameters.roadway_network_ps.category_grouping.keys())
    ML_FLAG = "ML"

    parts = c.split(delim)

    if parts[0] is ML_FLAG:
        managed = True
    else:
        parts.insert(0, False)
        managed = False

    base_name = parts[1]

    if parts[2] in cats:
        category = parts[2]
    elif parts[3] in cats:
        category = parts[3]
    else:
        category = None

    if parts[2] in tps:
        timeperiod = parts[2]
    elif parts[3] in tps:
        timeperiod = parts[3]
    else:
        timeperiod = None

    return base_name, timeperiod, category, managed


def fill_df_na(df: DataFrame, type_lookup: Mapping) -> DataFrame:
    """
    Fill na values with zeros and "" for a dataframe based on if they are
    numeric columns or strings in the parameters. Right now is looking for
    NA based on: np.nan, "", float("nan"), "NaN".

    Args:
        df: dataframe
        type_lookup: a dictionary mapping field names to types of str, int, or float

    Returns: A DataFrame with filled NA values based on type.
    """

    WranglerLogger.debug("Filling nan for network from network wrangler")

    numeric_fields = [c for c, t in type_lookup.items() if t in [int, float]]

    numeric_cols = [c for c in list(df.columns) if c in numeric_fields]
    non_numeric_cols = [c for c in list(df.columns) if c not in numeric_cols]

    non_standard_nan = [np.nan, "", float("nan"), "NaN", "NAType"]

    df[numeric_cols] = df[numeric_cols].replace(non_standard_nan, 0).fillna(0)
    df[non_numeric_cols] = df[non_numeric_cols].fillna("")

    return df


def coerce_df_types(
    df: DataFrame, type_lookup: dict = None, fill_na: bool = False
) -> DataFrame:
    """
    Coerce types and fills NAs for dataframe (i.e. links_df) based on types in type_lookup.

    Args:
        df: a dataframe to coerce the types for
        type_lookup: a dictionary mapping field names to types of str, int, or float.
            If not specified, will use roadway_net.parameters.roadway_network_ps.field_type.
        fill_na: if True, will fill NA values with null equiv of the type specified.
            Defaults to False.

    Returns: A dataframe with coerced types.
    """

    WranglerLogger.debug("Coercing types based on:\n {}".format(type_lookup))

    if fill_na:
        df = fill_df_na(df, type_lookup=type_lookup)

    for c in list(df.columns):
        if type_lookup.get(c):
            try:
                df[c] = df[c].astype(type_lookup[c])
            except ValueError:
                WranglerLogger.warning(
                    f"couldn't coerce column {c} to a {type_lookup[c]}."
                )
        else:
            WranglerLogger.debug(
                "Dataframe column {} not found in type lookup, leaving as type: {}".format(
                    c, df[c].dtype
                )
            )

    WranglerLogger.debug("DF types now:\n {}".format(df.dtypes))

    return df


def add_id_field_to_shapefile(
    shapefile_filename: str, outfile_filename: str = None, fieldname: str = None
) -> str:
    """
    Reads in a shapefile and adds a field with an ID based on row #s, saving it as a new shapefile.

    Args:
        shapefile_filename: shapefile to add ID to
        outfile_filename: shapefile to save as
        fieldname: name of the field to add as the ID field

    Returns: The string of the filename of the shapefile with the added ID.
    """
    gdf = gpd.read_file(shapefile_filename)
    if fieldname in gdf.columns:
        raise ValueError("Field {} already a column in shapefile".format(fieldname))
    gdf[fieldname] = range(1, 1 + len(gdf))
    if not outfile_filename:
        outfile_filename = "{0}_{2}{1}".format(
            *os.path.splitext(shapefile_filename) + ("with_id",)
        )
    gdf.to_file(outfile_filename)
    return outfile_filename


def write_df_to_fixed_width(
    df: DataFrame,
    data_outfile: str,
    header_outfile: str,
    overwrite: bool = False,
    sep: str = ";",
) -> DataFrame:
    """Writes a dataframe as a fixed format file with separate header file.

    Args:
        df (DataFrame): dataframe to write out
        data_outfile (str): txt file to write data to
        header_outfile (str): file to write header information to as a csv with
            header: header, width
        sep (str): separator for data_outfile. Defaults to ";"

    Returns:
       DataFrame with columns for header, width of ff fields
    """
    if not overwrite:
        for f in [data_outfile, header_outfile]:
            if os.path.exists(f):
                raise ValueError(
                    "data_outfile: {} already exists and overwrite set to False."
                )

    _ff_df, _max_width_dict = df_as_fixed_width(df)

    _ff_df.to_csv(header_outfile, sep=sep, index=False, header=False)

    _max_width_df = DataFrame(
        list(_max_width_dict.items()), columns=["header", "width"]
    )
    _max_width_df.to_csv(header_outfile, index=False)

    WranglerLogger(
        "Wrote:\n - fixed width file: {}\n - header file: {}".format(
            data_outfile, header_outfile
        )
    )

    return _max_width_df


def fill_df_cols(
    df: DataFrame, fill_dict: Mapping[str, Any], overwrite: bool = False,
) -> DataFrame:
    """Fills any number of columns (keys) with the mapped values.  Overwrites existing
    data and columns unless overwrite = False.

    Args:
        df: dataframe to modify.
        fill_dict: mapping of variable names to initialize to.
        overwrite: If True, will overwrite existing columns. Defaults to False.

    returns: Updated dataframe with columns filled.
    """
    for var, init_val in fill_dict.items():
        if var in df.columns and overwrite is False:
            continue
        df[var] = init_val

    return df


def df_as_fixed_width(df: DataFrame) -> Collection[Union[DataFrame, dict]]:
    """
    Convert dataframe to fixed width format, geometry column is dropped.

    Args:
        df (pandas DataFrame): input dataframe to be transformed

    Returns:
        pandas dataframe:  dataframe with fixed width for each column and types all strings
        dict: dictionary with columns names as keys, column width as values.
    """
    WranglerLogger.info("Starting fixed width conversion")

    fw_df = df.drop("geometry", axis=1).copy()
    # get the max length for each variable column
    max_width_dict = dict(
        [
            (v, fw_df[v].apply(lambda r: len(str(r)) if r is not None else 0).max())
            for v in fw_df.columns.values
        ]
    )

    fw_df = fw_df.astype("string")

    for c in fw_df.columns:
        fw_df["pad"] = fw_df[c].apply(lambda x: " " * (max_width_dict[c] - len(x)))
        fw_df[c] = fw_df.apply(lambda x: x["pad"] + x[c], axis=1)

    return fw_df, max_width_dict
