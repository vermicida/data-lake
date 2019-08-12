import config
import os
import pyspark.sql.functions as F
import pyspark.sql.types as T
from datetime import datetime
from pipelines.base import PipelineBase


class TimePipeline(PipelineBase):

    def _load_data(self):

        """
        Loads source data into one or more dataframes.
        """

        if not self._cache.exists(config.DATAFRAME_LOG_DATA):
            source_path = os.path.join(config.S3_LOG_DATA, '*/*/*.json')
            dataframe = self._get_spark_session().read.json(source_path)
            self._cache.set_source(config.DATAFRAME_LOG_DATA, dataframe)

    def _wrangle(self):

        """
        Wrangles the data from the dataframes previously loaded.
        """

        dataframe = self._cache.get_source(config.DATAFRAME_LOG_DATA)

        to_timestamp = F.udf(lambda x: datetime.fromtimestamp(x / 1000), T.TimestampType())

        dataframe = dataframe \
            .select('ts') \
            .where(dataframe.ts.isNotNull()) \
            .dropDuplicates() \
            .withColumn('start_time', to_timestamp(dataframe.ts))

        dataframe = dataframe \
            .withColumn('hour', F.hour(dataframe.start_time)) \
            .withColumn('day', F.dayofmonth(dataframe.start_time)) \
            .withColumn('week', F.weekofyear(dataframe.start_time)) \
            .withColumn('month', F.month(dataframe.start_time)) \
            .withColumn('year', F.year(dataframe.start_time)) \
            .withColumn('weekday', F.dayofweek(dataframe.start_time))

        self._cache.set_source(config.DATAFRAME_TIME, dataframe)

    def _show_info(self):

        """
        Shows info related the dataframes wrangled in this pipeline.
        """

        dataframe = self._cache.get_source(config.DATAFRAME_TIME)
        dataframe.printSchema()

    def _write(self):

        """
        Writes the wrangled dataframes to a given output path.
        """

        output_path = os.path.join(config.S3_OUTPUT, config.DATAFRAME_TIME)
        dataframe = self._cache.get_source(config.DATAFRAME_TIME)

        print('Writing dataframe to {}'.format(output_path))

        dataframe.write.parquet(
            output_path,
            mode='overwrite',
            partitionBy=['year', 'month']
        )
