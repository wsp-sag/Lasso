import geopandas as gpd
import pandas as pd
import glob
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

    def split_properties_by_time_period_and_category(self, properties_to_split = None):
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

        if properties_to_split == None:
            properties_to_split = self.parameters.properties_to_split

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
        '''
        This uses the centroid of the geometry field to determine which area type it should be labeled.
        PER PRD

        params
        -------

        '''
        if not self.parameters.taz_shape:
            return

        centroids_gdf = self.links_df.copy()
        centroids_gdf['geometry'] = centroids_gdf['geometry'].centroid

        area_type_gdf = gpd.read_file(self.parameters.area_type_shape)
        area_type_gdf = area_type_gdf.to_crs(epsg=RoadwayNetwork.EPSG)
        joined_gdf = gpd.sjoin(centroids_gdf, area_type_gdf,  how='left', op='intersects')
        joined_gdf[self.parameters.area_type_variable_shp] = joined_gdf[self.parameters.area_type_variable_shp].map(self.parameters.area_type_code_dict).fillna(10).astype(int)

        self.links_df[network_variable] = joined_gdf[self.parameters.area_type_variable_shp]
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


    def calculate_assignment_group(self, network_variable = 'assignment_group'):
        """
        join network with mrcc and widot roadway data by shst js matcher returns
        """
        self.calculate_centroid_connector()

        mrcc_gdf = gpd.read_file(self.parameters.mrcc_roadway_class_shape)
        mrcc_gdf['LINK_ID'] = range(1, 1+len(mrcc_gdf))
        mrcc_shst_ref_df = ModelRoadwayNetwork.read_match_result(self.parameters.mrcc_shst_data)

        widot_gdf = gpd.read_file(self.parameters.widot_roadway_class_shape)
        widot_gdf['LINK_ID'] = range(1, 1+len(widot_gdf))
        widot_shst_ref_df = ModelRoadwayNetwork.read_match_result(self.parameters.widot_shst_data)

        join_gdf = ModelRoadwayNetwork.get_attribute(
                           self.links_df,
                           "shstGeometryId",
                           mrcc_shst_ref_df,
                           mrcc_gdf,
                           self.parameters.mrcc_roadway_class_variable_shp)

        join_gdf = ModelRoadwayNetwork.get_attribute(
                           join_gdf,
                           "shstGeometryId",
                           widot_shst_ref_df,
                           widot_gdf,
                           self.parameters.widot_roadway_class_variable_shp)

        osm_asgngrp_crosswalk_df = pd.read_csv(self.parameters.osm_assgngrp_dict)
        mrcc_asgngrp_crosswalk_df = pd.read_excel(self.parameters.mrcc_assgngrp_dict,
                                         sheet_name= "mrcc_ctgy_asgngrp_crosswalk",
                                         dtype = {"ROUTE_SYS" : str,
                                         "ROUTE_SYS_ref" : str,
                                         "assignment_group" : int})
        widot_asgngrp_crosswak_df = pd.read_csv(self.parameters.widot_assgngrp_dict)

        join_gdf = pd.merge(join_gdf,
                    osm_asgngrp_crosswalk_df.rename(columns = {"assignment_group":"assignment_group_osm"}),
                    how = "left",
                    on = "roadway")

        print(join_gdf.columns)
        print(mrcc_asgngrp_crosswalk_df.columns)

        join_gdf = pd.merge(join_gdf,
                    mrcc_asgngrp_crosswalk_df.rename(columns = {"assignment_group" : "assignment_group_mrcc"}),
                    how = "left",
                    on = self.parameters.mrcc_roadway_class_variable_shp)

        join_gdf = pd.merge(join_gdf,
                    widot_asgngrp_crosswak_df.rename(columns = {"assignment_group" : "assignment_group_widot"}),
                    how = "left",
                    on = self.parameters.widot_roadway_class_variable_shp)

        def get_asgngrp(x):
            try:
                if x.centroid_connector == 1:
                    return 9
                elif x.assignment_group_mrcc > 0:
                    return int(x.assignment_group_mrcc)
                elif x.assignment_group_widot > 0:
                    return int(x.assignment_group_widot)
                else:
                    return int(x.assignment_group_osm)
            except:
                return 0

        join_gdf[network_variable] = join_gdf.apply(lambda x: get_asgngrp(x),
                                       axis = 1)

        self.links_df[network_variable] = join_gdf[network_variable]



    def calculate_roadway_class(self, network_variable = 'roadway_class'):
        """
        roadway_class is a lookup based on assignment group

        """

        asgngrp_rc_num_crosswalk_df = pd.read_csv(self.parameters.roadway_class_dict)

        join_gdf = pd.merge(self.links_df,
                            asgngrp_rc_num_crosswalk_df,
                            how = "left",
                            on = "assignment_group")

        self.links_df[network_variable] = join_gdf[network_variable]


    def calculate_count(self, network_variable = 'AADT'):

        """
        join the network with count node data, via SHST API node match result
        """
        mndot_count_shst_df = pd.read_csv(self.parameters.mndot_count_shst_data)

        widot_count_shst_df = pd.read_csv(self.parameters.widot_count_shst_data)

        join_gdf = pd.merge(self.links_df,
                            mndot_count_shst_df,
                            how = "left",
                            on = "shstReferenceId")
        join_gdf[self.parameters.mndot_count_variable_shp].fillna(0)

        join_gdf = pd.merge(self.links_df,
                            widot_count_shst_df,
                            how = "left",
                            on = "shstReferenceId")
        join_gdf[self.parameters.widot_count_variable_shp].fillna(0)

        join_gdf[network_variable] = join_gdf[[self.parameters.mndot_count_variable_shp, self.parameters.widot_count_variable_shp]].max(axis = 1).astype(int)

        self.links_df[network_variable] = join_gdf[network_variable]

    @staticmethod
    def read_match_result(path):
        """
        read the shst geojson match returns

        return shst dataframe
        """
        refId_gdf = DataFrame()
        refid_file = glob.glob(path)
        for i in refid_file:
            new = gpd.read_file(i)
            refId_gdf = pd.concat([refId_gdf, new],
                                    ignore_index = True,
                                    sort = False)
        return refId_gdf

    @staticmethod
    def get_attribute(
        links_df,
        join_key, #either "shstReferenceId", or "shstGeometryId", tests showed the latter gave better coverage
        source_shst_ref_df, # source shst refId
        source_gdf, # source dataframe
        field_name#, # targetted attribute from source
        ):

        join_refId_df = pd.merge(links_df,
                                source_shst_ref_df[[join_key, "pp_link_id", "score"]].rename(columns = {"pp_link_id" : "source_link_id",
                                                                                                 "score" : "source_score"}),
                                how = "left",
                                on = join_key)

        join_refId_df = pd.merge(join_refId_df,
                                source_gdf[['LINK_ID', field_name]].rename(columns = {"LINK_ID" : "source_link_id"}),
                                how = "left",
                                on = "source_link_id")

        join_refId_df.sort_values(by = ["model_link_id", "source_score"],
                                  ascending = True,
                                  na_position = "first",
                                  inplace = True)

        join_refId_df.drop_duplicates(subset = ["model_link_id"],
                                      keep = "last",
                                      inplace = True)

        #self.links_df[field_name] = join_refId_df[field_name]

        return join_refId_df[links_df.columns.tolist() + [field_name]]
