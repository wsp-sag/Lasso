from typing import Mapping, Collection, Union
import os

import geopandas as gpd
from pandas import DataFrame
import numpy as np

from .logger import WranglerLogger


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
    order: Collection[str] = ["managed", "base_name", "category", "time_period"],
) -> Collection[str]:
    """[summary]

    Args:
        c ([type]): composite column name
        parameters (Parameters, optional): Defaults to default Parameters().
        delim = what column parts are delimted by. Defaults to "_".
        order = what order the variables are concatenated together in. Defaults to:
            managed, base_name, category,

    Returns:
        Collection[str]: base_name, time_period, category, managed
    """

    if not parameters:
        from .parameters import Parameters

        parameters = Parameters()

    if c[0:2] == "ML":
        managed = True
    else:
        managed = False

    time_period = None
    category = None

    tps = parameters.network_ps.network_time_period_abbr
    cats = list(parameters.roadway_network_ps.category_grouping.keys)

    if c.split("_")[-1] in tps:
        time_period = c.split("_")[-1]
        base_name = c.split(time_period)[-2][:-1]
        if c.split("_")[-2] in cats:
            category = c.split("_")[-2]
            base_name = c.split(category)[-2][:-1]
    elif c.split("_")[-1] in cats:
        category = c.split("_")[-1]
        base_name = c.split(category)[-2][:-1]
    else:
        category = None
        time_period = None

    return base_name, time_period, category, managed


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

    non_standard_nan = [np.nan, "", float("nan"), "NaN"]

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
            df[c] = df[c].astype(type_lookup[c])
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

    Returns: the string of the filename of the shapefile with the added ID.
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
       DataFrame: with columns for header, width of ff fields
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
