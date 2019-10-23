def get_shared_streets_intersection_hash(lat, long):
    """
    Calculated per:
       https://github.com/sharedstreets/sharedstreets-js/blob/0e6d7de0aee2e9ae3b007d1e45284b06cc241d02/src/index.ts#L553-L565
    Expected in/out
     "Intersection -74.00482177734375 40.741641998291016"
     69f13f881649cb21ee3b359730790bb9

    Currently in/outing
     'Intersection -74.00482177734375 40.741641998291016'
     2a4a9e4ad1923f11ec46224f834d69a2

     Tested float precisions of various levels...no luck

    """
    import hashlib

    unhashed = "Intersection {0:.14f} {0:.15f}".format(long, lat).encode("utf-8")
    hash = hashlib.md5(unhashed).encode("ascii").hexdigest()
    return hash
