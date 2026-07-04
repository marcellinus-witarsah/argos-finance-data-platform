from src.extractor.base_extractor import BaseExtractor
import trino.dbapi

class TrinoExtractor(BaseExtractor):
    def __init__(self, conn: trino.dbapi.Connection):
        self.conn = conn
    
    def extract(self, table: str, columns: list[str]):
        # Create a cursor object to execute queries
        cur = self.conn.cursor()
        cur.execute(
            f"""
                SELECT {','.join(columns)} 
                FROM {table}
            """)

        # Fetch and print the results
        return cur.fetchall()