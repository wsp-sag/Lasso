import os
from ..parameters import Parameters
from ..logger import WranglerLogger

from .metcouncil import *
from .defaults import MC_DEFAULT_PARAMS 


msg = "[metcouncil.__init__.MC_DEFAULT_PARAMS] Initializing parameters with these MetCouncil defaults:\n      {}".format(MC_DEFAULT_PARAMS)
WranglerLogger.debug(msg)
print(msg)

mc_parameters = Parameters.initialize(MC_DEFAULT_PARAMS)

msg = "[metcouncil.__init__.mc_parameters] MetCouncil parameters:      {}".format(mc_parameters)
WranglerLogger.debug(msg)
print(msg)

WranglerLogger.info("Initialized Default MetCouncil Parameters")
