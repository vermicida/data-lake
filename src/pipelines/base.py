from pipelines.cache import SourceCache
from pyspark.sql import SparkSession


class PipelineBase():

    def __init__(self):

        """
        Class constructor.

        Returns:
            (PipelineBase): A new instance of PipelineBase class.
        """

        self._cache = SourceCache.instance()

    @classmethod
    def _get_spark_session(cls):

        """
        Get the current Spark session or create a new one if it doesn't exists.

        Returns:
            (SparkSession): A Spark session.
        """

        opt_key = 'spark.jars.packages'
        opt_value = 'org.apache.hadoop:hadoop-aws:2.7.0'
        return SparkSession.builder.config(opt_key, opt_value).getOrCreate()

    def _show_info(self):

        """
        Shows info related the dataframes wrangled in this pipeline.
        """

        pass

    def _load_data(self):

        """
        Loads source data into one or more dataframes.
        """

        raise NotImplementedError()

    def _wrangle(self):

        """
        Wrangles the data from the dataframes previously loaded.
        """

        raise NotImplementedError()

    def _write(self):

        """
        Writes the wrangled dataframes to a given output path.
        """

        raise NotImplementedError()

    @classmethod
    def create(cls):

        """
        Creates a new instance of PipelineBase (or any other inherited from it).

        Returns:
            (PipelineBase): A new instance of this (cls) class.
        """

        return cls()

    def execute(self):

        """
        Executes the defined pipeline.
        """

        self._load_data()
        self._wrangle()
        self._show_info()
        self._write()
