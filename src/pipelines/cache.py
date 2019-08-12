class SourceCache():

    singleton = None

    def __init__(self):

        """
        Class constructor.

        Returns:
            (SourceCache): A new instance of SourceCache class.
        """

        self._sources = {}

    @classmethod
    def instance(cls):

        """
        Gets or creates the singleton instance of the SourceCache class.

        Returns:
            (SourceCache): The SourceCache singleton instance.
        """

        if cls.singleton is None:
            cls.singleton = cls()
        return cls.singleton

    def exists(self, source_name):

        """
        Checks if the given dataframe is already cached.

        Parameters:
            source_name (str): The dataframe name.

        Returns:
            (bool): True if the dataframe is cached, otherwise returns False.
        """

        return source_name in self._sources

    def get_source(self, source_name):

        """
        Gets the cached dataframe corresponding the given name.

        Parameters:
            source_name (str): The dataframe name.

        Returns:
            (DataFrame): The cached dataframe.
        """

        print('Retrieving {} dataframe from cache'.format(source_name))
        return self._sources[source_name]

    def set_source(self, source_name, source_data):

        """
        Caches the given dataframe.

        Parameters:
            source_name (str): The dataframe name.
            source_data (DataFrame): The dataframe itself.
        """

        print('Caching {} dataframe'.format(source_name))
        self._sources[source_name] = source_data
