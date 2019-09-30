class AttributeDictionary(dict):
    """
    Copied from CorePackages/gbd
    Common pattern to access obj.foo rather than obj['foo']
    """

    def __init__(self, *args, **kwargs):
        super(AttributeDictionary, self).__init__(*args, **kwargs)
        self.__dict__ = self
