import os
from dotenv import load_dotenv
from pathlib import Path
script_directory = Path(__file__).resolve().parent

load_dotenv(script_directory /'.env')

#load_dotenv('../../../')

<<<<<<< HEAD
=======

>>>>>>> 1a332bbab5f5c27d0639afbd859c4ec088640fe3
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
url = "hippocampus.cswst4rid7eb.us-east-2.rds.amazonaws.com"
user = "odc_writer"
pw = "writejacobs1918"
db = "postgres"
