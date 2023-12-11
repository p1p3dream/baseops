import logging
from database_connection import DatabaseConnection
from sql_queries import SQLQueries

class DatabaseETL:
    def __init__(self, database_url, table_name):
        self.database_url = database_url
        self.table_name = table_name
        self.db_connection = DatabaseConnection(database_url)

    def create_postgis_extension(self):
        try:
            with self.db_connection.connect() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(SQLQueries.create_postgis_extension())
                    conn.commit()
        except Exception as e:
            logging.error(f"Error creating PostGIS extension: {str(e)}")

    def create_table(self):
        try:
            with self.db_connection.connect() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(SQLQueries.create_table_query(self.table_name))
                    conn.commit()
        except Exception as e:
            logging.error(f"Error creating table: {str(e)}")

    def merge_data(self, gdf):
        try:
            with self.db_connection.create_engine().connect() as conn:
                for index, row in gdf.iterrows():
                    conn.execute(SQLQueries.insert_or_update_query(self.table_name), (
                        row['id'],
                        row['event_id'],
                        row['event_date'],
                        row['event_state'],
                        row['event_county_right'],
                        row['event_city'],
                        row['event_type'],
                        row['event_source'],
                        row['geometry'].wkt
                    ))
                    conn.commit()
        except Exception as e:
            logging.error(f"Error merging data into the database: {str(e)}")

    def etl_process(self, gdf):
        self.create_postgis_extension()
        self.create_table()
        self.merge_data(gdf)
