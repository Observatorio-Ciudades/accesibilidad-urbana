import logging
import datetime as dt

logging.basicConfig(filename='../logs/a-u_{}.log'.format(('{:%Y-%m-%d}').format(dt.datetime.now())), filemode='a', format='%(name)s - %(asctime)s - %(levelname)s - %(message)s',level=logging.INFO)
logging.info(('Script initalized'))