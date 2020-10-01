import os
import numpy as np
import pandas as pd

from network_wrangler import RoadwayNetwork
from .parameters import Parameters
from .roadway import ModelRoadwayNetwork
from .logger import WranglerLogger

def calculate_number_of_lanes(
    roadway_net=None,
    parameters=None,
    lanes_lookup_file=None,
    network_variable="lanes",
    overwrite=False,
):

    """
    Computes the number of lanes using a heuristic defined in this method.


    Args:
        roadway_net (RoadwayNetwork): Network Wrangler RoadwayNetwork object
        parameters (Parameters): Lasso parameters object
        lanes_lookup_file (str): File path to lanes lookup file.
        network_variable (str): Name of the lanes variable
        overwrite (boolean): Overwrite existing values

    Returns:
        RoadwayNetwork
    """

    # TODO: handle a missing logger
    update_lanes = False

    WranglerLogger.info(
        "Calculating Number of Lanes as network variable: '{}'".format(
            network_variable,
        )
    )

    if type(parameters) is dict:
        parameters = Parameters(**parameters)
    elif isinstance(parameters, Parameters):
        parameters = Parameters(**parameters.__dict__)
    else:
        msg = "Parameters should be a dict or instance of Parameters: found {} which is of type:{}".format(
            parameters, type(parameters)
        )
        WranglerLogger.error(msg)
        raise ValueError(msg)

    # TODO: handle a missing network

    if network_variable in roadway_net.links_df:
        if overwrite:
            WranglerLogger.info(
                "Overwriting existing number of lanes variable '{}' already in network".format(
                    network_variable
                )
            )
            roadway_net.links_df.drop([network_variable], axis = 1)
        else:
            WranglerLogger.info(
                "Number of lanes variable '{}' updated for some links. Returning without overwriting for those links. Calculating for other links".format(
                    network_variable
                )
            )
            update_lanes = True

    """
    Verify inputs
    """

    lanes_lookup_file = (
        lanes_lookup_file
        if lanes_lookup_file
        else parameters.lanes_lookup_file
    )
    if not lanes_lookup_file:
        msg = "'lanes_lookup_file' not found in method or lasso parameters.".format(
            lanes_lookup_file
        )
        WranglerLogger.error(msg)
        raise ValueError(msg)

    """
    Start actual process
    """
    WranglerLogger.debug("Calculating Centroid Connectors")
    roadway_net.calculate_centroidconnect()

    WranglerLogger.debug(
        "Computing number lanes using: {}".format(
            lanes_lookup_file,
        )
    )

    lanes_df = pd.read_csv(lanes_lookup_file)

    join_df = pd.merge(
        roadway_net.links_df,
        lanes_df,
        how="left",
        on="model_link_id",
    )

    def _set_lanes(x):
        try:
            if x.centroidconnect == 1:
                return int(1)
            elif x.ROUTE_SYS in ['04', '05', '09', '07', '10']:
                return int(max([x.anoka, x.hennepin, x.carver, x.dakota, x.washington]))
            elif max([x.widot, x.mndot])>0:
                return int(max([x.widot, x.mndot]))
            elif x.osm_min>0:
                return int(x.osm_min)
            elif x.naive>0:
                return int(x.naive)
        except:
            return int(0)

    if update_lanes:
        join_df[network_variable + "_cal"] = join_df.apply(lambda x: _set_lanes(x), axis=1)
        roadway_net.links_df[network_variable] = np.where(
            roadway_net.links_df[network_variable] > 0,
            roadway_net.links_df[network_variable],
            join_df[network_variable + "_cal"],
        )
    else:
        join_df[network_variable] = join_df.apply(lambda x: _set_lanes(x), axis=1)
        roadway_net.links_df[network_variable] = join_df[network_variable]

    WranglerLogger.info(
        "Finished calculating number of lanes to: {}".format(network_variable)
    )

    return roadway_net
