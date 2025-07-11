import os
from dotenv import load_dotenv
from pathlib import Path
script_directory = Path(__file__).resolve().parent

load_dotenv(script_directory /'.env')

#load_dotenv('../../../')

"""Global settings, can be configured by user with utils.config()."""

import logging as lg

# locations to save data, logs, images, and cache
data_folder = "data"
logs_folder = "logs"

# write log to file and/or to console
log_file = True
log_level = lg.INFO
log_name = "ObsCd"
log_filename = "ObsCd"

# Database settings
url = os.getenv('url')
user = os.getenv('user')
pw = os.getenv('pw')
db = os.getenv('db')
