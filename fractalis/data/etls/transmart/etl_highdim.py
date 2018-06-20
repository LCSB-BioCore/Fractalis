"""Provides highdim concept ETL for tranSMART."""

from fractalis.data.etls.transmart.shared import create_etl_type, NUMERICAL_FIELD

HighdimETL = create_etl_type(
    name_='transmart_highdim_etl',
    produces_='numerical_array',
    field_name=NUMERICAL_FIELD
)
