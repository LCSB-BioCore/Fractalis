"""Provides numerical concept ETL for tranSMART."""

from fractalis.data.etls.transmart.shared import create_etl_type, NUMERICAL_FIELD

NumericalETL = create_etl_type(
    name_='transmart_numerical_etl',
    produces_='numerical',
    field_name=NUMERICAL_FIELD
)
