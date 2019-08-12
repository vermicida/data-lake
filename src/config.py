import configparser
import os


_config = configparser.ConfigParser()
_config.read(os.path.join(os.getcwd(), 'sparkify.cfg'))

# ------------- #
# AWS constants #
# ------------- #

AWS_ACCESS_KEY_ID = _config['AWS']['ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = _config['AWS']['SECRET_ACCESS_KEY']

# ------------ #
# S3 constants #
# ------------ #

S3_LOG_DATA = _config['S3']['LOG_DATA']
S3_SONG_DATA = _config['S3']['SONG_DATA']
S3_OUTPUT = _config['S3']['OUTPUT']

# ------------------- #
# Dataframe constants #
# ------------------- #

DATAFRAME_SONG_DATA = _config['DATAFRAME']['SONG_DATA']
DATAFRAME_LOG_DATA = _config['DATAFRAME']['LOG_DATA']
DATAFRAME_SONGS = _config['DATAFRAME']['SONGS']
DATAFRAME_ARTISTS = _config['DATAFRAME']['ARTISTS']
DATAFRAME_USERS = _config['DATAFRAME']['USERS']
DATAFRAME_TIME = _config['DATAFRAME']['TIME']
DATAFRAME_SONGPLAYS = _config['DATAFRAME']['SONGPLAYS']
