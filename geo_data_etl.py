import geopandas as gpd
import pandas as pd
from datetime import datetime
import logging
import os
from s3_connection import S3Connection

class GeoDataETL:
    def __init__(self):
        self.database_url = os.getenv('DATABASE_URL', 'postgresql://postgres:postgres@localhost:5432/postgres')
        self.s3_bucket = os.getenv('S3_BUCKET', 'baseops-data-dev')
        self.base_directory = 'raw/'

    def load_geo_data(self, existing_dataset_url, county_geojson_url):
        try:
            gdf = gpd.read_file(existing_dataset_url)
            county_gdf = gpd.read_file(county_geojson_url)
            return gdf, county_gdf
        except Exception as e:
            logging.error(f"Error loading GeoData: {str(e)}")
            return None, None

    def preprocess_data(self, gdf, county_gdf):
        try:
            county_gdf.columns = county_gdf.columns.str.lower()
            county_gdf = county_gdf[['name', 'geometry']].copy().rename(columns={'name': 'event_county'})
            county_gdf['event_county'] = county_gdf['event_county'].str.title()

            gdf['event_date'] = pd.to_datetime(gdf['event_date'])
            start_date = datetime(2021, 1, 1)
            end_date = datetime(2021, 12, 31)
            return gdf[(gdf['event_date'] >= start_date) & (gdf['event_date'] <= end_date)]
        except Exception as e:
            logging.error(f"Error preprocessing data: {str(e)}")
            return None

    def spatial_join(self, filtered_df, county_gdf):
        try:
            merged_df = gpd.sjoin(filtered_df, county_gdf, how="inner", op="intersects")
            merged_df.drop_duplicates(subset=['event_id'], inplace=True)
            return merged_df
        except Exception as e:
            logging.error(f"Error performing spatial join: {str(e)}")
            return None

    def save_to_s3(self, df, key):
        try:
            s3 = S3Connection(self.s3_bucket)
            s3.save_to_s3(df, key)
        except Exception as e:
            logging.error(f"Error saving to S3: {str(e)}")

    def etl_process(self, existing_dataset_url, county_geojson_url):
        gdf, county_gdf = self.load_geo_data(existing_dataset_url, county_geojson_url)
        if gdf is None or county_gdf is None:
            return None

        filtered_df = self.preprocess_data(gdf, county_gdf)
        if filtered_df is None:
            return None
        
        merged_df = self.spatial_join(filtered_df, county_gdf)
        if merged_df is None:
            return None

        current_date = datetime.now().strftime("%Y-%m-%d")
        merged_df_key = f'{self.base_directory}merged_df/date={current_date}/merged_df.parquet'
        gdf_key = f'{self.base_directory}gdf/date={current_date}/gdf.parquet'

        self.save_to_s3(merged_df, merged_df_key)
        self.save_to_s3(filtered_df, gdf_key)

        return merged_df
