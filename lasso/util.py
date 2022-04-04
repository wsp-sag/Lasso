import pandas as pd

def get_shared_streets_intersection_hash(lat, long, osm_node_id=None):
    """
    Calculated per:
       https://github.com/sharedstreets/sharedstreets-js/blob/0e6d7de0aee2e9ae3b007d1e45284b06cc241d02/src/index.ts#L553-L565
    Expected in/out
      -93.0965985, 44.952112199999995 osm_node_id = 954734870
       69f13f881649cb21ee3b359730790bb9

    """
    import hashlib

    message = "Intersection {0:.5f} {0:.5f}".format(long, lat)
    if osm_node_id:
        message += " {}".format(osm_node_id)
    unhashed = message.encode("utf-8")
    hash = hashlib.md5(unhashed).hexdigest()
    return hash


def hhmmss_to_datetime(hhmmss_str: str):
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


def secs_to_datetime(secs: int):
    """
    Creates a datetime time object from a seconds from midnight

    Args:
        secs: seconds from midnight
    Returns:
        dt: datetime.time object representing time
    """
    import datetime

    dt = (datetime.datetime.min + datetime.timedelta(seconds=secs)).time()

    return dt


def column_name_to_parts(c, parameters=None):

    if not parameters:
        from .parameters import Parameters

        parameters = Parameters()

    if c[0:2] == "ML":
        managed = True
    else:
        managed = False

    time_period = None
    category = None

    if c.split("_")[0] not in parameters.properties_to_split.keys():
        return c, None, None, managed

    tps = parameters.time_period_to_time.keys()
    cats = parameters.categories.keys()

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
        msg = "Can't split property correctly: {}".format(c)
        WranglerLogger.error(msg)

    return base_name, time_period, category, managed

def update_crs_nodes_df(
    nodes_df: pd.DataFrame,
    to_crs:int,
    from_crs:int=None,
    keep_gdf: bool = False,
    )->pd.DataFrame:
    """
    Changes the CRS with a dataframe with X and Y columns.
    """
    import geopandas as gpd

    if not type(nodes_df) == gpd.GeoDataFrame:
        if not from_crs:
            raise ValueError("Must provide a from_crs if not inputing a GeoDataFrame")

        nodes_gdf = gpd.GeoDataFrame(
            nodes_df,
            geometry=gpd.points_from_xy(nodes_df.X, nodes_df.Y),
            crs=from_crs,
        )

    nodes_gdf= nodes_gdf.to_crs(epsg=to_crs)

    nodes_gdf["X"] = nodes_gdf.geometry.apply(
        lambda g: g.x
    )

    nodes_gdf["Y"] = nodes_gdf.geometry.apply(
        lambda g: g.y
    )

    if keep_gdf:
        return nodes_gdf
    else:
        return pd.DataFrame(nodes_gdf.drop(columns=["geometry"]))
