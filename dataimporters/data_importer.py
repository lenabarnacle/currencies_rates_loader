from dotenv import load_dotenv
from sqlalchemy.exc import SQLAlchemyError
from datamodels.datamodel import entity_currency_rates
from dataimporters.base_importer import BaseImporter
from datetime import date, datetime, timedelta
from sqlalchemy import create_engine
import constants as const
import pandas as pd
import xml.etree.ElementTree as ET
import xmldict
import requests
import logging
import os

load_dotenv()
CONNECTION_STRING = os.getenv('PY_DWH_CONNECTION_STRING')
CBR_URL = os.getenv('CBR_URL')

logger = logging.getLogger('currency_rates_logger')


def get_dates_list(start_date, end_date=date.today() + timedelta(1)):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


class CurrencyRatesLoader(BaseImporter):

    def __enter__(self):
        self.connect(CONNECTION_STRING)
        return self

    def __exit__(self, type, value, traceback):
        return self.disconnect()

    def get_start_date(self, connection_string=CONNECTION_STRING):
        logger.info('Getting the start date')
        query = 'select max(currencies_rates.date) as start_date from sa.currencies_rates'
        start_date = pd.read_sql(query, create_engine(connection_string))
        start_date = start_date['start_date'][0]
        if not start_date:
            start_date = date.today()
        return start_date

    def get_dates_range(self, start_date):
        logger.info('Getting the dates range')
        dates_range = list()
        for date in get_dates_list(start_date + timedelta(1)):
            dates_range.append(date.strftime('%d/%m/%Y'))
        return dates_range

    def transform_currency_rates_data(self, for_date):
        logger.info('Transforming currencies rates data')
        for_date = pd.DataFrame.from_dict(for_date, orient='index').reset_index()
        for_date = for_date.rename(columns={'index': 'currency', 0: 'nominal', 1: 'nominal_value'})
        for_date['nominal'] = for_date['nominal'].astype(int)
        for_date['nominal_value'] = for_date['nominal_value'].str.replace(',', '.')
        for_date['nominal_value'] = for_date['nominal_value'].astype(float)
        for_date['value'] = for_date['nominal_value'] / for_date['nominal']
        for_date['value'] = round(for_date['value'], 4)
        for_date = for_date.drop(['nominal', 'nominal_value'], axis=1)
        return for_date

    def get_currency_rates(self, dates_range, currencies_list=const.CURRENCY_LIST):
        logger.info('Getting currencies rates data')
        currencies = pd.DataFrame({'currency': [], 'date': [], 'value': []})

        for date in dates_range:
            print(date)
            url = CBR_URL + f'?date_req={date}'
            get_xml = requests.get(url)
            xml_data = ET.XML(get_xml.content)
            xml_to_dict = xmldict.xml_to_dict(xml_data)
            xml_to_dict = xml_to_dict['ValCurs']['Valute']

            for_date = dict()
            for x in xml_to_dict:
                if x['CharCode'] in currencies_list:
                    for_date[x['CharCode']] = [x['Nominal'], x['Value']]
            for_date = self.transform_currency_rates_data(for_date)
            for_date['date'] = datetime.strptime(date, '%d/%m/%Y')

            currencies = currencies.append(for_date)
        currencies = currencies.to_dict(orient='records')
        return currencies

    def save_currency_rates(self, currencies):
        logger.info('Importing currencies rates to database -- start')
        try:
            self.session.query(entity_currency_rates)
            list_of_entity_check_your_skin = [entity_currency_rates(**e) for e in currencies]
            self.session.add_all(list_of_entity_check_your_skin)
            self.session.commit()
            logger.info('Importing currencies rates to database -- finish')
        except SQLAlchemyError as err:
            logger.info('SQLAlchemyError', err)
            raise SystemExit(err)

    def run_loader(self):
        start_date = self.get_start_date()
        dates_range = self.get_dates_range(start_date)
        if dates_range:
            currencies = self.get_currency_rates(dates_range)
            self.save_currency_rates(currencies)
        else:
            logger.info('There is no new data')