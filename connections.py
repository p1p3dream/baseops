import psycopg2
from sqlalchemy import create_engine
import s3fs
import logging

class DatabaseConnection:
    def __init__(self, database_url):
        self.database_url = database_url

    def connect(self):
        return psycopg2.connect(self.database_url)

    def create_engine(self):
        return create_engine(self.database_url)

class S3Connection:
    def __init__(self, s3_bucket):
        self.s3_bucket = s3_bucket

    def save_to_s3(self, df, key):
        try:
            s3 = s3fs.S3FileSystem()
            parquet_path = f's3://{self.s3_bucket}/{key}'
            df.to_parquet(path=parquet_path, engine='pyarrow', compression='snappy', index=False)
            logging.info(f"Saved to S3: {parquet_path}")
        except Exception as e:
            logging.error(f"Error saving to S3: {str(e)}")
