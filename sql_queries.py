class SQLQueries:
    @staticmethod
    def create_postgis_extension():
        return "CREATE EXTENSION IF NOT EXISTS postgis;"

    @staticmethod
    def create_table_query(table_name):
        return f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id SERIAL PRIMARY KEY,
                event_id INT,
                event_date DATE,
                event_state VARCHAR(2),
                event_county VARCHAR,
                event_city VARCHAR,
                event_type VARCHAR,
                event_source VARCHAR,
                geometry GEOMETRY(GEOMETRY, 4326)
            );
        """

    @staticmethod
    def insert_or_update_query(table_name):
        return f"""
            INSERT INTO {table_name} (
                id,
                event_id,
                event_date,
                event_state,
                event_county,
                event_city,
                event_type,
                event_source,
                geometry
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, ST_GeomFromText(%s, 4326))
            ON CONFLICT (id) DO UPDATE
            SET
                event_id = EXCLUDED.event_id,
                event_date = EXCLUDED.event_date,
                event_state = EXCLUDED.event_state,
                event_county = EXCLUDED.event_county,
                event_city = EXCLUDED.event_city,
                event_type = EXCLUDED.event_type,
                event_source = EXCLUDED.event_source,
                geometry = EXCLUDED.geometry;
        """
