from dotenv import load_dotenv
from dataimporters.data_importer import CurrencyRatesLoader
import logging
import os

load_dotenv()
CONNECTION_STRING = os.getenv('PY_DWH_CONNECTION_STRING')

FORMAT = '%(asctime)-15s %(name)s %(levelname)s %(message)s'
logging.basicConfig(format=FORMAT, level=logging.INFO)
logger = logging.getLogger('currency_rates_logger')


def currency_rates_loader():
    logger.info('Currencies rates import -- start')
    with CurrencyRatesLoader() as importer:
        importer.run_loader()
    logger.info('Currencies rates import -- successful')


currency_rates_loader()