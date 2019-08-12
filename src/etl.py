import config
import os
import pipelines


def main():

    # Sets the AWS credentials in the environment, so PySpark can use them automatically.
    os.environ['AWS_ACCESS_KEY_ID'] = config.AWS_ACCESS_KEY_ID
    os.environ['AWS_SECRET_ACCESS_KEY'] = config.AWS_SECRET_ACCESS_KEY

    # Executes the pipelines to bring song and event data to the Sparkify data lake.
    pipelines.SongsPipeline.create().execute()
    pipelines.ArtistsPipeline.create().execute()
    pipelines.UsersPipeline.create().execute()
    pipelines.TimePipeline.create().execute()
    pipelines.SongplaysPipeline.create().execute()


if __name__ == "__main__":
    main()
