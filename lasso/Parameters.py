import os


class Parameters():
    """
    # TODO: this whole flow needs work.
    """



    """
    Time period splitting info


    """
    DEFAULT_TIME_PERIOD_TO_TIME = {
      'AM': ('6:00','9:00'), ##TODO FILL IN with real numbers
      'MD': ('9:00','16:00'),
      'PM': ('16:00','19:00'),
      'NT': ('19:00','6:00'),
    }

    DEFAULT_VARIABLES_TO_SPLIT_BY_TIME_PERIOD = [
        'transit_priority',
        'traveltime_assert',
        'lanes',
        'price',
        'access',
    ]

    """
    Details for calculating the county based on the centroid of the link.
    The COUNTY_VARIABLE should be the name of a field in shapefile


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

    def __init__(self, time_period_to_time=None, calculated_variables_roadway=None, **kwargs):
        self.time_period_to_time = Parameters.DEFAULT_TIME_PERIOD_TO_TIME
        if time_period_to_time:
            self.time_period_to_time = time_period_to_time

        self.calculated_variables_roadway = Parameters.DEFAULT_CALCULATED_VARIABLES_ROADWAY
        if calculated_variables_roadway:
            self.calculated_variables_roadway = calculated_variables_roadway

        self.time_to_time_period= {v: k for k, v in self.time_period_to_time.items()}
