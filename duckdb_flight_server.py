import re

import duckdb
import pyarrow as pa
import pyarrow.flight as flight


class DuckDBFlightServer(flight.FlightServerBase):
    def __init__(self, location="grpc://localhost:8815", db_path="duck_flight.db"):
        super().__init__(location)
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)

    def do_get(self, context, ticket):
        """Handle 'GET' requests from clients to retrieve data."""
        query = ticket.ticket.decode("utf-8")
        result_table = self.conn.execute(query).fetch_arrow_table()
        # Convert to record batches with alignment
        batches = result_table.to_batches(max_chunksize=1024)  # Use power of 2 for alignment
        return flight.RecordBatchStream(pa.Table.from_batches(batches))

    def do_put(self, context, descriptor, reader, writer):
        """Handle 'PUT' requests to upload data to the DuckDB instance."""
        table = reader.read_all()
        if not descriptor.path:
            raise ValueError("Missing table name in descriptor")
        table_name = descriptor.path[0].decode('utf-8')
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table_name):
            raise ValueError(f"Invalid table name: {table_name}")
        
        # Create table if it doesn't exist
        self.conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                batch_id BIGINT,
                timestamp VARCHAR,
                value DOUBLE,
                category VARCHAR
            )
        """)
        
        # Convert to record batches for better alignment
        batches = table.to_batches(max_chunksize=1024)
        aligned_table = pa.Table.from_batches(batches)
        self.conn.register("temp_table", aligned_table)
        
        # Insert new data
        self.conn.execute(f"INSERT INTO {table_name} SELECT * FROM temp_table")

    def do_action(self, context, action):
        """Handle custom actions like executing SQL queries."""
        if action.type == "query":
            query = action.body.to_pybytes().decode("utf-8")
            self.conn.execute(query)
            return []
        else:
            raise NotImplementedError(f"Unknown action type: {action.type}")


if __name__ == "__main__":
    server = DuckDBFlightServer()
    print("Starting DuckDB Flight server on localhost:8815")
    server.serve()
