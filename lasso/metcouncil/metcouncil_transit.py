import pandas as pd

from ..cube.cube_model_transit import CubeTransit

MC_TP_PROPERTY = "HEADWAY"

MC_ROUTE_PROPS = [
    "LINE NAME",
    "LONGNAME",
    "HEADWAY",
    "MODE",
    "ONEWAY",
    "OPERATOR",
    "NODES",
]

MC_NODE_PROPS = []


class MetCouncilTransit(CubeTransit):
    def __init__(self, **kwargs):

        super().__init__(
            tp_prop=MC_TP_PROP,
            route_properties=MC_ROUTE_PROPS,
            node_properties=MC_NODE_PROPS,
            **kwargs,
        )

    def calculate_model_mode(
        self, model_route_properties_df: pd.DataFrame
    ) -> pd.Series:
        """Assigns a model mode number by following logic.

        Uses GTFS route_type variable:
            https://developers.google.com/transit/gtfs/reference

        For rail, uses mode based on mappping of route_type stored in
            ::self.parameters.transit_network_ps.transit_value_lookups

        For buses, uses route id numbers and route name as follows:

        - If the route_long_name contains 'express' -> express, mode 7
        - If route_id starts with a number >= 100 -> surban local, mode 6
        - Defaults to urban local, mode 5.

        Args:
            model_route_properties_df (pd.DataFrame): dataframe of route properties

        Returns:
            pd.Series: mode number
        """
        gtfs_route_type_to_mode = {
            0: 8,
            2: 9,
        }

        mode_s = model_route_properties_df["route_type"].map(gtfs_route_type_to_mode)

        _express_sel = model_route_properties_df["route_long_name"].str.contains(
            "express", case=False, regex=False
        )

        mode_s.loc[(_express_sel) & (mode_s.isna())] = 7

        # suburban locals' route_id should start with numbers 100+
        _suburban_local_sel = model_route_properties_df["route_id"].str.contains(
            "^1[0-9][0-9]", regex=True
        )

        mode_s.loc[(_suburban_local_sel) & (mode_s.isna())] = 6

        # otherwise, assume an urban local
        mode_s.fillna(5)

        return mode_s
