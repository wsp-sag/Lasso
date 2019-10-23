import collections

from .NetworkException import NetworkException

__all__ = ['PTSystem']

class PTSystem:
    """
    Public Transport System definition.  Corresponds to the information in Cube's Public Transport system,
    including data for modes, operators, wait curves and crowding curves.
    """

    def __init__(self):
        self.waitCurveDefs  = collections.OrderedDict()  # key is number, value is also ordered dict
        self.crowdCurveDefs = collections.OrderedDict()  # key is number, value is also ordered dict
        self.operators      = collections.OrderedDict()  # key is number, value is also ordered dict
        self.modes          = collections.OrderedDict()  # key is number, value is also ordered dict
        self.vehicleTypes   = collections.OrderedDict()  # key is number, value is also ordered dict

    def isEmpty(self):
        if len(self.operators   ) > 0: return False
        if len(self.modes       ) > 0: return False
        if len(self.vehicleTypes) > 0: return False
        return True

    def __repr__(self):
        """ Returns string representation.
        """

        s = ""
        for pt_num, pt_dict in self.modes.items():
            s += "MODE"
            for k,v in pt_dict.items(): s+= " {}={}".format(k,v)
            s+= "\n"
        s += "\n"

        for pt_num, pt_dict in self.operators.items():
            s += "OPERATOR"
            for k,v in pt_dict.items(): s+= " {}={}".format(k,v)
            s+= "\n"
        s += "\n"

        for pt_num, pt_dict in self.vehicleTypes.items():
            s += "VEHICLETYPE"
            for k,v in pt_dict.items(): s+= " {}={}".format(k,v)
            s+= "\n"
        s += "\n"

        for pt_num, pt_dict in self.waitCurveDefs.items():
            s += "WAITCRVDEF"
            for k,v in pt_dict.items(): s+= " {}={}".format(k,v)
            s+= "\n"
        s += "\n"

        for pt_num, pt_dict in self.crowdCurveDefs.items():
            s += "CROWDCRVDEF"
            for k,v in pt_dict.items(): s+= " {}={}".format(k,v)
            s+= "\n"
        s += "\n"

        return s

    def merge(self, pts):
        """
        Merges another pts with self.
        """
        for key,val_dict in pts.waitCurveDefs.items():
            if key in self.waitCurveDefs: # collision
                raise NetworkException("PTSystem: Trying to merge WAITCRVDEF with same key: {}".format(key))
            self.waitCurveDefs[key] = val_dict

        for key,val_dict in pts.crowdCurveDefs.items():
            if key in self.crowdCurveDefs: # collision
                raise NetworkException("PTSystem: Trying to merge CROWDCRVDEF with same key: {}".format(key))
            self.crowdCurveDefs[key] = val_dict

        for key,val_dict in pts.operators.items():
            if key in self.operators: # collision
                raise NetworkException("PTSystem: Trying to merge OPERATOR with same key: {}".format(key))
            self.operators[key] = val_dict

        for key,val_dict in pts.modes.items():
            if key in self.modes: # collision
                raise NetworkException("PTSystem: Trying to merge MODE with same key: {}".format(key))
            self.modes[key] = val_dict

        for key,val_dict in pts.vehicleTypes.items():
            if key in self.vehicleTypes: # collision
                raise NetworkException("PTSystem: Trying to merge VEHICLETYPE with same key: {}".format(key))
            self.vehicleTypes[key] = val_dict
