import os


class Parameters():
    """
    # TODO: this whole flow needs work.
    """



    """
    Time period and category  splitting info
    """
    DEFAULT_TIME_PERIOD_TO_TIME = {
      'AM': ('6:00','9:00'), ##TODO FILL IN with real numbers
      'MD': ('9:00','16:00'),
      'PM': ('16:00','19:00'),
      'NT': ('19:00','6:00'),
    }

    DEFAULT_CATEGORIES = {
        #suffix, source (in order of search)
        'sov': ['sov','default'],
        'hov2': ['hov2','default', 'sov'],
        'hov3': ['hov3','hov2','default','sov'],
        'truck':['trk','sov','default'],
    }

    #prefix, source variable, categories
    DEFAULT_VARIABLES_TO_SPLIT = {
        'transit_priority' : {'v':'transit_priority', 'time_periods':DEFAULT_TIME_PERIOD_TO_TIME },
        'traveltime_assert' : {'v':'traveltime_assert', 'time_periods':DEFAULT_TIME_PERIOD_TO_TIME },
        'lanes' : {'v':'lanes', 'time_periods':DEFAULT_TIME_PERIOD_TO_TIME },
        'price' : {'v':'price', 'time_periods':DEFAULT_TIME_PERIOD_TO_TIME ,'categories': DEFAULT_CATEGORIES},
        'access' : {'v':'access', 'time_periods':DEFAULT_TIME_PERIOD_TO_TIME},
    }

    """
    Details for calculating the county based on the centroid of the link.
    The COUNTY_VARIABLE should be the name of a field in shapefile.
    """

    DEFAULT_COUNTY_SHAPE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.realpath(__file__))), "metcouncil_data", "Counties.shp"
    )
    DEFAULT_COUNTY_VARIABLE_SHP   = 'CO_NAME'

    DEFAULT_MPO_COUNTIES = ['ANOKA', 'DAKOTA', 'HENNEPIN', 'RAMSEY', 'SCOTT', 'WASHINGTON', 'CARVER']
    DEFAULT_TAZ_SHAPE = None
    DEFAULT_TAZ_DATA  = None
    DEFAULT_HIGHEST_TAZ_NUMBER = 9999

    DEFAULT_CALCULATED_VARIABLES_ROADWAY = [
      'self.calculate_area_type(DEFAULT_TAZ_SHAPE, DEFAULT_TAZ_DATA, network_variable = "area")',
      'self.calculate_county(DEFAULT_COUNTY_SHAPE, DEFAULT_COUNTY_VARIABLE_SHP, network_variable = "county")',
      'self.calculate_centroid_connector(DEFAULT_HIGHEST_TAZ_NUMBER, network_variable = "centroid_connector")',
      'self.calculate_mpo(DEFAULT_MPO_COUNTIES, network_variable = "mpo")',
      'self.calculate_roadway_class_index(network_variable="roadway_class_index")',
      'self.calculate_assignment_group(network_variable="assignment_group")',
    ]


    DEFAULT_OUTPUT_VARIABLES = [
        'model_link_id',
        'A',
        'B',
        'geometryId',
        'distance',
        'roadway',
        'name',
        'roadway_class',
        'bike_access',
        'transit_access',
        'walk_access',
        'drive_access',
        'truck_access',
        'transit_priority_AM',
        'transit_priority_MD',
        'transit_priority_PM',
        'transit_priority_NT',
        'traveltime_assert_AM',
        'traveltime_assert_MD',
        'traveltime_assert_PM',
        'traveltime_assert_NT',
        'lanes_AM',
        'lanes_MD',
        'lanes_PM',
        'lanes_NT',
        'price_sov_AM',
        'price_hov2_AM',
        'price_hov3_AM',
        'price_truck_AM',
        'price_sov_MD',
        'price_hov2_MD',
        'price_hov3_MD',
        'price_truck_MD',
        'price_sov_PM',
        'price_hov2_PM',
        'price_hov3_PM',
        'price_truck_PM',
        'price_sov_NT',
        'price_hov2_NT',
        'price_hov3_NT',
        'price_truck_NT',
        'roadway_class_index',
        'assignment_group',
        'access_AM',
        'access_MD',
        'access_PM',
        'access_NT',
        'mpo',
        'area_type',
        'county',
        'centroid_connector',
        'mrcc_id',
        'AADT',
        'count_year',
        'count_AM',
        'count_MD',
        'count_PM',
        'count_NT',
        'count_daily',
    ]




    def __init__(self, time_period_to_time=None, calculated_variables_roadway=None, **kwargs):
        self.time_period_to_time = Parameters.DEFAULT_TIME_PERIOD_TO_TIME
        if time_period_to_time:
            self.time_period_to_time = time_period_to_time

        self.calculated_variables_roadway = Parameters.DEFAULT_CALCULATED_VARIABLES_ROADWAY
        if calculated_variables_roadway:
            self.calculated_variables_roadway = calculated_variables_roadway

        self.time_to_time_period= {v: k for k, v in self.time_period_to_time.items()}
