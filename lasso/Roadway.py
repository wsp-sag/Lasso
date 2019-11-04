import geopandas as gpd
from geopandas import GeoDataFrame
from pandas import DataFrame
from network_wrangler import RoadwayNetwork
from .Parameters import Parameters

class ModelRoadwayNetwork(RoadwayNetwork):

    def __init__(self, nodes: GeoDataFrame, links: DataFrame, shapes: GeoDataFrame, parameters = {}):
        super().__init__(nodes, links, shapes)
        print("PARAMS", parameters)
        #will have to change if want to alter them
        self.parameters = Parameters(**parameters)

    @staticmethod
    def read(
        link_file: str, node_file: str, shape_file: str, fast: bool = False, parameters = {}
    ):
        #road_net =  super().read(link_file, node_file, shape_file, fast=fast)
        road_net = RoadwayNetwork.read(link_file, node_file, shape_file, fast=fast)

        m_road_net = ModelRoadwayNetwork(road_net.nodes_df, road_net.links_df, road_net.shapes_df, parameters = parameters)

        return m_road_net

    @staticmethod
    def from_RoadwayNetwork(roadway_network_object, parameters = {}):
        return ModelRoadwayNetwork(roadway_network_object.nodes_df, roadway_network_object.links_df, roadway_network_object.shapes_df, parameters=parameters)

    def split_properties_by_time_period_and_category(self):
        '''
        Splits properties by time period, assuming a variable structure of

        Params
        ------
        properties_to_split: dict
             dictionary of output variable prefix mapped to the source variable and what to stratify it by
             e.g.
             {
                 'transit_priority' : {'v':'transit_priority', 'times_periods':DEFAULT_TIME_PERIOD_TO_TIME },
                 'traveltime_assert' : {'v':'traveltime_assert', 'times_periods':DEFAULT_TIME_PERIOD_TO_TIME },
                 'lanes' : {'v':'lanes', 'times_periods':DEFAULT_TIME_PERIOD_TO_TIME },
                 'price' : {'v':'price', 'times_periods':DEFAULT_TIME_PERIOD_TO_TIME ,'categories': DEFAULT_CATEGORIES},
                 'access' : {'v':'access', 'times_periods':DEFAULT_TIME_PERIOD_TO_TIME},
             }

        '''
        import itertools

        for out_var, params in self.parameters.properties_to_split.items():
            if params["v"] not in self.links_df.columns:
                raise ValueError("Specified variable to split: {} not in network variables: {}".format(params["v"], str(self.links_df.columns)))
            if params.get("time_periods") and params.get("categories"):
                for time_suffix, category_suffix in itertools.product(params['time_periods'], params['categories']):
                    self.links_df[out_var+"_"+time_suffix+"_"+category_suffix] = \
                        self.get_property_by_time_period_and_group(
                            params["v"],
                            category = params['categories'][category_suffix],
                            time_period = params['time_periods'][time_suffix],
                        )
            elif params.get("time_periods"):
                for time_suffix in params['time_periods']:
                    self.links_df[out_var+"_"+time_suffix] = \
                        self.get_property_by_time_period_and_group(
                            params["v"],
                            category = None,
                            time_period = params['time_periods'][time_suffix],
                        )
            else:
                raise ValueError("Shoudn't have a category without a time period: {}".format(params))


    def create_calculated_variables(self):
        '''
        Params
        -------
        '''

        for method in self.parameters.calculated_variables_roadway:
            eval(method)

    def calculate_county(self, network_variable = 'county'):
        '''
        This uses the centroid of the geometry field to determine which county it should be labeled.
        This isn't perfect, but it much quicker than other methods.

        params
        -------

        '''

        centroids_gdf = self.links_df.copy()
        centroids_gdf['geometry'] = centroids_gdf['geometry'].centroid

        county_gdf = gpd.read_file(self.parameters.county_shape)
        county_gdf = county_gdf.to_crs(epsg=RoadwayNetwork.EPSG)
        joined_gdf = gpd.sjoin(centroids_gdf, county_gdf,  how='left', op='intersects')

        self.links_df[network_variable] = joined_gdf[self.parameters.county_variable_shp]


    def calculate_area_type(self, network_variable = 'area_type'):
        if not self.parameters.taz_shape:
            return

        taz_gdf = gpd.read_file(self.parameters.taz_shape)
        taz_gdf = taz_gdf.to_crs(epsg=RoadwayNetwork.EPSG)

        centroids_gdf = self.links_df.copy()
        centroids_gdf['geometry'] = centroids_gdf['geometry'].centroid

        joined_gdf = gpd.sjoin(centroids_gdf, taz_gdf,  how='left', op='intersects')
        ## QUESTION FOR MET COUNCIL: HOW IS AREA TYPE CURRENTLY  CALCULATED

    def calculate_centroid_connector(self,network_variable = 'centroid_connector', as_integer = True):
        '''
        Params
        ------
        network_variable: str
          variable that should be written to in the network
        as_integer: bool
          if true, will convert true/false to 1/0s
        '''

        self.links_df[network_variable] = False

        self.links_df.loc[
            (self.links_df['A'] <= self.parameters.highest_taz_number) |
            (self.links_df['B'] <= self.parameters.highest_taz_number),
            network_variable
            ] = True

        if as_integer:
            self.links_df[network_variable] = self.links_df[network_variable].astype(int)


    def calculate_mpo(self, county_network_variable='county', network_variable = 'mpo', as_integer = True):
        '''
        Params
        ------
        county_variable: string
          name of the variable where the county names are stored.
        network_variable: string
          name of the variable that should be written to
        as_integer: bool
          if true, will convert true/false to 1/0s
        '''

        mpo = self.links_df[county_network_variable].isin(self.parameters.mpo_counties)

        if as_integer:
            mpo = mpo.astype(int)

        self.links_df[network_variable] = mpo

    def calculate_roadway_class_index(self, network_variable = 'roadway_class_index'):
        pass

    def calculate_assignment_group(self, network_variable = 'assignment_group'):
        pass
