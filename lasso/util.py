from typing import Mapping

from pd import DataFrame

from .logger import WranglerLogger


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
        msg = "Can't split property correctly: {}".format(c)
        WranglerLogger.error(msg)

    return base_name, time_period, category, managed

def fill_df_na(df: DataFrame, type_lookup: Mapping) -> DataFrame:
    """
    Fill na values with zeros and "" for a dataframe based on if they are numeric columns or strings 
    in the parameters. Right now is looking for
    NA based on: np.nan, "", float("nan"), "NaN".

    Args:
        df: dataframe
        type_lookup: a dictionary mapping field names to types of str, int, or float

    Returns: A DataFrame with filled NA values based on type.
    """

    WranglerLogger.info("Filling nan for network from network wrangler")

    numeric_fields = [c for c,t in type_lookup if t in [int,float]]

    numeric_cols = [c for c in list(df.columns) if c in numeric_fields]
    non_numeric_cols = [c for c in list(df.columns) if c not in numeric_col]

    non_standard_nan = [np.nan, "", float("nan"), "NaN"] 

    df[numeric_cols] = df[numeric_cols].replace(non_standard_nan,0).fillna(0) 
    df[non_numeric_cols] = df[non_numeric_cols].fillna("")

    return df

def coerce_df_types(df: DataFrame, type_lookup: dict = None) -> DataFrame:
    """
    Coerce types and fills NAs for dataframe (i.e. links_df) based on types in type_lookup.

    Args:
        df: a dataframe to coerce the types for
        type_lookup: a dictionary mapping field names to types of str, int, or float. 
            If not specified, will use roadway_net.parameters.roadway_network_ps.field_type.
        
    Returns: A dataframe with coerced types.
    """

    WranglerLogger.debug("Coercing types based on:\n {}".format(type_lookup))

    for c in list(df.columns):
        if type_lookup.get(c):
            df[c] = df[c].astype(type_lookup[c])
        else:
            WranglerLogger.debug("Dataframe column {} not found in type lookup, leaving as type: {}".format(c,df[c].dtype ))
    
    df = fill_df_na(df, type_lookup = type_lookup)

    WranglerLogger.debug("DF types now:\n {}".format(df.dtypes))

    return df

def add_id_field_to_shapefile(shapefile_filename: str, fieldname: str) -> str:
    """
    Reads in a shapefile and adds a field with an ID based on row #s, saving it as a new shapefile.

    Args:
        shapefile_filename: shapefile to add ID to
        fieldname: name of the field to add as the ID field

    Returns: the string of the filename of the shapefile with the added ID.
    """
    gdf = gpd.read_file(shapefile_filename)
    if fieldname in gdf.columns:
        raise ValueError("Field {} already a column in shapefile".format(fieldname))
    gdf[fieldname]= range(1, 1 + len(gdf))
    outfile_filename = "{0}_{2}{1}".format(*os.path.splitext(shapefile_filename) + ("with_id",))
    gdf.to_file(outfile_filename)
    return outfile_filename
