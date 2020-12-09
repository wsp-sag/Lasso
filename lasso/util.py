from functools import partial
import pyproj
from shapely.ops import transform
from shapely.geometry import Point, Polygon

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


def geodesic_point_buffer(lat, lon, meters):
    """
    creates circular buffer polygon for node

    Args:
        lat: node lat
        lon: node lon
        meters: buffer distance, radius of circle
    Returns:
        Polygon
    """
    proj_wgs84 = pyproj.Proj('+proj=longlat +datum=WGS84')
    # Azimuthal equidistant projection
    aeqd_proj = '+proj=aeqd +lat_0={lat} +lon_0={lon} +x_0=0 +y_0=0'
    project = partial(
        pyproj.transform,
        pyproj.Proj(aeqd_proj.format(lat=lat, lon=lon)),
        proj_wgs84)
    buf = Point(0, 0).buffer(meters)  # distance in meters
    return Polygon(transform(project, buf).exterior.coords[:])

def create_locationreference(node, link):
    node['X'] = node['geometry'].apply(lambda p: p.x)
    node['Y'] = node['geometry'].apply(lambda p: p.y)
    node['point'] = [list(xy) for xy in zip(node.X, node.Y)]
    node_dict = dict(zip(node.model_node_id, node.point))

    link['A_point'] = link['A'].map(node_dict)
    link['B_point'] = link['B'].map(node_dict)
    link['locationReferences'] = link.apply(lambda x: [{'sequence':1,
                                                        'point': x['A_point'],
                                                        'distanceToNextRef':x['length'],
                                                        'bearing' : 0,
                                                        'intersectionId':x['fromIntersectionId']},
                                                                         {'sequence':2,
                                                             'point': x['B_point'],
                                                             'intersectionId':x['toIntersectionId']}],
                                                   axis = 1)

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
