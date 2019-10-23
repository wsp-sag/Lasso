__all__ = ['Factor']

class Factor(dict):
    """
    Transit config (or "factor")
    All attributes are stored in the dictionary
    """
    def __init__(self):
        dict.__init__(self)
        self.comment=''

    def __repr__(self):
        s = "FACTOR "

        fields = []
        for k in sorted(self.keys()):
            fields.append("{}={}".format(k, self[k]))

        s += ", ".join(fields)
        s += self.comment

        return s
