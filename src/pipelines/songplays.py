import config
import os
import pyspark.sql.functions as F
from pipelines.base import PipelineBase


class SongplaysPipeline(PipelineBase):

    def _load_data(self):

        """
        Loads source data into one or more dataframes.
        """

        if not self._cache.exists(config.DATAFRAME_SONG_DATA):
            source_path = os.path.join(config.S3_SONG_DATA, 'A/A/A/*.json')  # Note: song database is way big, so we get only a slice of it.
            dataframe = self._get_spark_session().read.json(source_path)
            self._cache.set_source(config.DATAFRAME_SONG_DATA, dataframe)

        if not self._cache.exists(config.DATAFRAME_LOG_DATA):
            source_path = os.path.join(config.S3_LOG_DATA, '*/*/*.json')
            dataframe = self._get_spark_session().read.json(source_path)
            self._cache.set_source(config.DATAFRAME_LOG_DATA, dataframe)

    def _wrangle(self):

        """
        Wrangles the data from the dataframes previously loaded.
        """

        songs = self._cache.get_source(config.DATAFRAME_SONG_DATA)
        logs = self._cache.get_source(config.DATAFRAME_LOG_DATA)

        dataframe = songs.join(
            logs,
            (songs.artist_name == logs.artist) & (songs.title == logs.song)
        )

        dataframe = dataframe \
            .where(logs.page == 'NextSong') \
            .select([
                logs.ts,
                logs.userId,
                logs.level,
                songs.song_id,
                songs.artist_id,
                logs.sessionId,
                logs.location,
                logs.userAgent
            ]) \
            .withColumnRenamed('userId', 'user_id') \
            .withColumnRenamed('sessionId', 'session_id') \
            .withColumnRenamed('userAgent', 'user_agent') \
            .withColumn('songplay_id', F.monotonically_increasing_id())

        self._cache.set_source(config.DATAFRAME_SONGPLAYS, dataframe)

    def _show_info(self):

        """
        Shows info related the dataframes wrangled in this pipeline.
        """

        dataframe = self._cache.get_source(config.DATAFRAME_SONGPLAYS)
        dataframe.printSchema()

    def _write(self):

        """
        Writes the wrangled dataframes to a given output path.
        """

        output_path = os.path.join(config.S3_OUTPUT, config.DATAFRAME_SONGPLAYS)
        dataframe = self._cache.get_source(config.DATAFRAME_SONGPLAYS)

        print('Writing dataframe to {}'.format(output_path))

        dataframe.write.parquet(
            output_path,
            mode='overwrite'
        )
