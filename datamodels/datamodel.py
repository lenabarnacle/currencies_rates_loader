from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Table, MetaData

Base = declarative_base()

table_currency_rates = Table("currencies_rates",
                             MetaData(schema="sa"),
                             Column("currency", primary_key=True),
                             Column("date", primary_key=True),
                             Column("value"))

class entity_currency_rates(Base):
    __table__ = table_currency_rates
    currency = table_currency_rates.c.currency
    date = table_currency_rates.c.date
    value = table_currency_rates.c.value