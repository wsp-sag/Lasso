from typing import Mapping, Any

from pandas import DataFrame
from geopandas import GeoDataFrame

from network_wrangler import update_df, RoadwayNetwork

from ..parameters import Parameters, RoadwayNetworkModelParameters
from ..model_roadway import ModelRoadwayNetwork
from ..logger import WranglerLogger

from .defaults import MC_DEFAULT_PARAMS


class MtcRoadwayNetwork(ModelRoadwayNetwork):
    """MTC specific methods for :py:class:`ModelRoadwayNetwork`

    .. highlight:: python
    Typical usage example:
    ::
        net = MtcCouncilRoadwayNetwork.read(
            link_filename=STPAUL_LINK_FILE,
            node_filename=STPAUL_NODE_FILE,
            shape_filename=STPAUL_SHAPE_FILE,
            fast=True,
        )
        net.roadway_standard_to_met_council_network()

    """

    @classmethod
    def convert_from_model_roadway_net(
        cls,
        model_roadway_network: ModelRoadwayNetwork,
    ) -> None:
        """Static method for converting from a model roadway network
        to a Mtc flavor. Doesn't do anything other than change
        the __class__.

        Args:
            model_roadway_network (ModelRoadwayNetwork): :py:class:`ModelRoadwayNetwork` instance.
        """
        WranglerLogger(f"Converting ModelRoadwayNetwork to {cls} flavor.")
        model_roadway_network.__class__ = cls

    @staticmethod
    def read(
        link_filename: str,
        node_filename: str,
        shape_filename: str,
        fast: bool = False,
        parameters: Parameters = None,
        parameters_dict: Mapping[str, Any] = {},
        **kwargs,
    ):
        """
        Reads in links and nodes network standard.

        Args:
            link_filename (str): File path to link json.
            node_filename (str): File path to node geojson.
            shape_filename (str): File path to link true shape geojson
            fast (bool): boolean that will skip validation to speed up read time.
            parameters: an instance of Parameters.
                If not specified, will use default MetCouncil parameters overridden by any relevant
                parameters in parameters_dict or in other, additional kwargs.
            parameters_dict: dictionary of parameter settings which override parameters instance.
                Defaults to {}.
        Returns:
            MtcCouncilModelRoadwayNetwork
        """
        WranglerLogger.info("Reading as a MetCouncil Model Roadway Network")

        if not parameters:
            _params_dict = MC_DEFAULT_PARAMS
            _params_dict.update(parameters_dict)
            if kwargs:
                _params_dict.update(
                    {
                        k: v
                        for k, v in kwargs.items()
                        if k in Parameters.parameters_list()
                    }
                )

            WranglerLogger.debug(
                "[metcouncil.__init__] Initializing parameters with MtcCouncil defaults."
            )
            # WranglerLogger.debug(f"[metcouncil.__init__.MC_DEFAULT_PARAMS] {_params_dict}")

            parameters = Parameters.initialize(base_params_dict=_params_dict)

        WranglerLogger.debug(
            "Using MetCouncilRoadwayNetwork parameters:      {}".format(parameters)
        )

        model_roadway_network = ModelRoadwayNetwork.read(
            link_filename,
            node_filename,
            shape_filename,
            fast=fast,
            parameters=parameters,
            **kwargs,
        )
        model_roadway_network.__class__ = MetCouncilRoadwayNetwork
        return model_roadway_network

    def calculate_facility_type(
        links_df: DataFrame,
        parameters=None,
        update_method: str = "update if found",
        update_network_variable: bool = False,
    ):

        """
        Computes the number of lanes using a heuristic defined in this method.

        Args:
            links_df: links dataframe to calculate number of lanes for. Defaults to self.links_df.
            network_variable: Name of the lanes variable. Defaults to "lanes".
            update_method: update_method: update method to use in network_wrangler.update_df.
                One of "overwrite all",
                "update if found", or "update nan". Defaults to "update if found".

        Returns:
            GeoDataFrame of links_df
        """
        WranglerLogger.info("Calculating Facility Type")

        if links_df is None:
            _links_update_df = copy.deepcopy(self.links_df)
        else:
            _links_update_df = links - df

        _links_update_df["oneWay_binary"] = _links_update_df["oneWay"].astype("bool")
        _links_update_df["ft"] = _links_update_df.apply(
            _calculate_facility_type, axis=1
        )

        _links_out_df = update_df(
            links_df,
            _links_update_df,
            "model_link_id",
            update_fields=["ft"],
            method=update_method,
        )
        return _links_out_df

    def _calculate_facility_type(x: pd.Series) -> int:
        """Facility_type heuristics

        Args:
            x (pd.Series): [description]

        Returns:
            int: [description]
        """

        if x.roadway == "motorway":
            return 1

        if x.roadway == "trunk":
            if x.oneWay_binary == 1:
                return 2

        if x.roadway in ["motorway_link", "trunk_link"]:
            return 3

        if x.roadway in ["primary", "secondary", "tertiary"]:
            if x.oneWay_binary == 1:
                if x[network_variable_lanes] > 1:
                    return 4

        if x.roadway in ["trunk", "primary", "secondary", "tertiary"]:
            if x.oneWay_binary == 0:
                if x[network_variable_lanes] > 1:
                    return 5

        if x.roadway == "trunk":
            if x.oneWay_binary == 0:
                if x[network_variable_lanes] == 1:
                    return 6

        if x.roadway in ["primary", "secondary", "tertiary"]:
            if x.oneWay_binary in [0, 1]:
                return 6

        if x.roadway in ["primary_link", "secondary_link", "tertiary_link"]:
            if x.oneWay_binary in [0, 1]:
                return 6

        if x.roadway in ["residential", "residential_link"]:
            if x.oneWay_binary in [0, 1]:
                return 7

        if x.roadway in ["taz", "maz", "tap"]:
            return 8

        if x.roadway in ["ml_access", "ml_egress"]:
            return 8

        return 99

    def calculate_lanes(
        roadway_network=None,
        parameters=None,
        network_variable: str = "lanes",
        osm_lanes_attributes: str = None,
        tam_tm2_attributes: str = None,
        sfcta_attributes: str = None,
        pems_attributes: str = None,
        tomtom_attributes: str = None,
        overwrite: bool = False,
    ):
        pass
