import geopandas as gpd
from geopandas import GeoDataFrame
from pandas import DataFrame

from network_wrangler import RoadwayNetwork
from .Parameters import Parameters

class ModelRoadwayNetwork(RoadwayNetwork):

    def __init__(self, nodes: GeoDataFrame, links: DataFrame, shapes: GeoDataFrame):
        super().__init__(nodes, links, shapes)

        #will have to change if want to alter them
        self.parameters = Parameters()

    @staticmethod
    def read(
        link_file: str, node_file: str, shape_file: str, fast: bool = False
    ) -> RoadwayNetwork:
        #road_net =  super().read(link_file, node_file, shape_file, fast=fast)
        road_net = RoadwayNetwork.read(link_file, node_file, shape_file, fast=fast)

        m_road_net = ModelRoadwayNetwork(road_net.nodes_df, road_net.links_df, road_net.shapes_df)

        return m_road_net

    def split_properties_by_time_period_and_category(self,properties_to_split):
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

        for out_var, params in properties_to_split.items():
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


    def create_calculated_variables(self, calculated_variables):
        '''
        Params
        -------
        roadway_network
            network_wrangler RoadwayNetwork object

        calculated_variables_dict
            variable name: method to calculate variable
        '''

        for method in calculated_variables:
            eval(method)

    def calculate_county(self, county_shapefile, county_variable_shp, network_variable = 'county'):
        '''
        This uses the centroid of the geometry field to determine which county it should be labeled.
        This isn't perfect, but it much quicker than other methods.

        params
        -------
        county_shapefile: string
          file location of the shapefile with county borders and names
         county_variable_shp
          the variable from the shapefile that should be returned
        '''

        centroids_gdf = self.links_df.copy()
        centroids_gdf['geometry'] = centroids_gdf['geometry'].centroid

        county_gdf = gpd.read_file(county_shapefile)
        county_gdf = county_gdf.to_crs(epsg=RoadwayNetwork.EPSG)
        joined_gdf = gpd.sjoin(centroids_gdf, county_gdf,  how='left', op='intersects')

        self.links_df[network_variable] = joined_gdf[county_variable_shp]


    def calculate_area_type(self, taz_shapefile, taz_data):
        centroids_gdf = self.links_df.copy()
        centroids_gdf['geometry'] = centroids_gdf['geometry'].centroid

        taz_gdf = gpd.read_file(taz_shapefile)
        taz_gdf = taz_gdf.to_crs(epsg=RoadwayNetwork.EPSG)

        joined_gdf = gpd.sjoin(centroids_gdf, taz_gdf,  how='left', op='intersects')
        ## QUESTION FOR MET COUNCIL: HOW IS AREA TYPE CURRENTLY  CALCULATED

    def calculate_centroid_connector(self,highest_taz_number, network_variable = 'centroid_connector', as_integer = True):
        '''
        Params
        ------
        highest_taz_number: int
          highest number to assume is a taz
        network_variable: str
          variable that should be written to in the network
        as_integer: bool
          if true, will convert true/false to 1/0s
        '''

        self.links_df[network_variable] = False

        self.links_df[
            self.links_df['A'] <= highest_taz_number or
            self.links_df['B'] <= highest_taz_number
            ][network_variable] = True

        if as_integer:
            self.links_df[network_variable] = self.links_df[network_variable].astype(int)


    def calculate_mpo(self, counties_in_mpo, county_network_variable='county', network_variable = 'mpo', as_integer = True):
        '''
        Params
        ------
        counties_in_mpo: list
          list of counties that are in the MPO boundary
        county_variable: string
          name of the variable where the county names are stored.
        network_variable: string
          name of the variable that should be written to
        as_integer: bool
          if true, will convert true/false to 1/0s
        '''

        mpo = self.links_df[county_network_variable].isin(counties_in_mpo)

        if as_integer:
            mpo = mpo.astype(int)

        self.links_df[network_variable] = mpo

    def calculate_roadway_class_index(self, network_variable = 'roadway_class_index'):
        pass

    def calculate_assignment_group(self, network_variable = 'assignment_group'):
        pass
