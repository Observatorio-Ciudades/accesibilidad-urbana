import logging
import datetime as dt

logging.basicConfig(filename='../logs/a-u_{}.log'.format(('{:%Y-%m-%d}').format(dt.datetime.now())), filemode='a', format='%(name)s - %(asctime)s - %(levelname)s - %(message)s',level=logging.INFO)

#Resolution for the HEXbins more details: https://h3geo.org/docs/core-library/restable
resolution=9 #Aprox 100 m2
amenities = ['supermercados','farmacias']