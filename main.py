import logging
from geo_data_etl import GeoDataETL
from database_etl import DatabaseETL

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    existing_dataset_url = 'https://raw.githubusercontent.com/p1p3dream/baseops/main/geotest.geojson'
    county_geojson_url = 'https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json'

    geo_etl = GeoDataETL()
    merged_df = geo_etl.etl_process(existing_dataset_url, county_geojson_url)

    if merged_df is not None:
        database_url = 'postgresql://postgres:postgres@localhost:5432/postgres'
        table_name = "geo_test"

        db_etl = DatabaseETL(database_url, table_name)
        db_etl.etl_process(merged_df)

    logging.info("Data ETL process completed successfully.")
