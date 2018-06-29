"""Provides categorical concept ETL for tranSMART."""

from fractalis.data.etls.transmart.shared import create_etl_type, CATEGORICAL_FIELD

CategoricalETL = create_etl_type(
    name_='transmart_categorical_etl',
    produces_='categorical',
    field_name=CATEGORICAL_FIELD
)
