import config
import os
from pipelines.base import PipelineBase


class ArtistsPipeline(PipelineBase):

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
            .dropDuplicates(['artist_id']) \
            .where(dataframe.artist_id != '') \
            .select([
                'artist_id',
                'artist_name',
                'artist_location',
                'artist_latitude',
                'artist_longitude'
            ]) \
            .withColumnRenamed('artist_name', 'name') \
            .withColumnRenamed('artist_location', 'location') \
            .withColumnRenamed('artist_latitude', 'latitude') \
            .withColumnRenamed('artist_longitude', 'longitude')

        self._cache.set_source(config.DATAFRAME_ARTISTS, dataframe)

    def _show_info(self):

        """
        Shows info related the dataframes wrangled in this pipeline.
        """

        dataframe = self._cache.get_source(config.DATAFRAME_ARTISTS)
        dataframe.printSchema()

    def _write(self):

        """
        Writes the wrangled dataframes to a given output path.
        """

        output_path = os.path.join(config.S3_OUTPUT, config.DATAFRAME_ARTISTS)
        dataframe = self._cache.get_source(config.DATAFRAME_ARTISTS)

        print('Writing dataframe to {}'.format(output_path))

        dataframe.write.parquet(
            output_path,
            mode='overwrite'
        )
