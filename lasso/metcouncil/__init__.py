import os
from ..parameters import Parameters, FileParameters, NetworkModelParameters, TransitNetworkModelParameters, RoadwayNetworkModelParameters,DemandModelParameters

from metcouncil import *
import defaults



def initialize_metcouncil_added_data(**kwargs):
    

def initialize_metcouncil_parameters(**kwargs):
    """Initializes a Parameters data class with default MetCouncil parameters which 
    can be overwritten by including a keyword argument from any of the parameters 
    classes.

    Args:
        kwarg: a keyword argument of any of the parameters classes in parameters.py 
        which will be used to add-to or overwrite any defaults. 

    Returns: Parameters data class with MetCouncil parameters. 
    """

    file_parameters = FileParameters(
        **defaults.MC_FILE_PS.update(
            {
                k:v for k,v in kwargs if k in vars(FileParameters).keys()
            }
        )
    )
    
    
    network_model_parameters = NetworkModelParameters(
        **defaults.MC_NETWORK_MODEL_PS.update(
            {
                k:v for k,v in kwargs if k in vars(NetworkModelParameters).keys()
            }
        )
    )
  
    roadway_network_parameters = RoadwayNetworkModelParameters(
        network_model_parameters,
        **defaults.MC_ROADWAY_MODEL_PS.update(
            {
                k:v for k,v in kwargs if k in vars(RoadwayNetworkModelParameters).keys()
            }
        )
    )
    
    transit_network_parameters = TransitNetworkModelParameters(
        network_model_parameters,
        **defaults.MC_TRANSIT_NETWORK_MODEL_PS.update(
            {
                k:v for k,v in kwargs if k in vars(TransitNetworkModelParameters).keys()
            }
        )
    )

    demand_model_parameters = DemandModelParameters(
        network_model_parameters,
        **defaults.DEMAND_MODEL_PS.update(
            {
                k:v for k,v in kwargs if k in vars(DemandModelParameters).keys()
            }
        )
    )

    parameters = Parameters(
        file_ps = file_parameters,
        roadway_network_ps = roadway_network_parameters,
        transit_network_ps= transit_network_parameters,
        demand_model_ps= demand_model_parameters,
    )

    return metcouncil_parameters