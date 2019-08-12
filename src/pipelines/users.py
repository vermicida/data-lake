import config
import os
import pyspark.sql.functions as F
from pipelines.base import PipelineBase


class UsersPipeline(PipelineBase):

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

        dataframe = dataframe \
            .dropDuplicates(['userId']) \
            .where(dataframe.userId != '') \
            .select([
                'userId',
                'firstName',
                'lastName',
                'gender',
                'level'
            ]) \
            .withColumnRenamed('userId', 'user_id') \
            .withColumnRenamed('firstName', 'first_name') \
            .withColumnRenamed('lastName', 'last_name') \
            .withColumn('user_id', F.expr('cast(user_id  as int)'))

        self._cache.set_source(config.DATAFRAME_USERS, dataframe)

    def _show_info(self):

        """
        Shows info related the dataframes wrangled in this pipeline.
        """

        dataframe = self._cache.get_source(config.DATAFRAME_USERS)
        dataframe.printSchema()

    def _write(self):

        """
        Writes the wrangled dataframes to a given output path.
        """

        output_path = os.path.join(config.S3_OUTPUT, config.DATAFRAME_USERS)
        dataframe = self._cache.get_source(config.DATAFRAME_USERS)

        print('Writing dataframe to {}'.format(output_path))

        dataframe.write.parquet(
            output_path,
            mode='overwrite'
        )
