import config
import os
import pyspark.sql.functions as F
from pipelines.base import PipelineBase


class SongsPipeline(PipelineBase):

    def _load_data(self):

        """
        Loads source data into one or more dataframes.
        """

        if not self._cache.exists(config.DATAFRAME_SONG_DATA):
            source_path = os.path.join(config.S3_SONG_DATA, 'A/A/A/*.json')  # Note: song database is way big, so we get only a slice of it.
            dataframe = self._get_spark_session().read.json(source_path)
            self._cache.set_source(config.DATAFRAME_SONG_DATA, dataframe)

    def _wrangle(self):

        """
        Wrangles the data from the dataframes previously loaded.
        """

        dataframe = self._cache.get_source(config.DATAFRAME_SONG_DATA)

        dataframe = dataframe \
            .dropDuplicates(['song_id']) \
            .where(dataframe.song_id != '') \
            .select([
                'song_id',
                'title',
                'artist_id',
                'year',
                'duration'
            ]) \
            .withColumn('year', F.expr('cast(year  as smallint)'))

        self._cache.set_source(config.DATAFRAME_SONGS, dataframe)

    def _show_info(self):

        """
        Shows info related the dataframes wrangled in this pipeline.
        """

        dataframe = self._cache.get_source(config.DATAFRAME_SONGS)
        dataframe.printSchema()

    def _write(self):

        """
        Writes the wrangled dataframes to a given output path.
        """

        output_path = os.path.join(config.S3_OUTPUT, config.DATAFRAME_SONGS)
        dataframe = self._cache.get_source(config.DATAFRAME_SONGS)

        print('Writing dataframe to {}'.format(output_path))

        dataframe.write.parquet(
            output_path,
            mode='overwrite',
            partitionBy=['year', 'artist_id']
        )
