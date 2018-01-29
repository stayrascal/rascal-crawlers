import sys
import time

from happybase import Batch

import happybase as hb

TABLE_NAME = "TABLE_NAME"

class HBaseConnector:
    def __init__(self, host, table_prefix):
        self.pool = hb.ConnectionPool(
            size = 16,
            host = host,
            autoconnect = True,
            table_prefix = table_prefix
        )
    
    def insert_data(self, datas):
        with self.pool.connection() as connection:
            tables = connection.tables()
            if TABLE_NAME not in tables:
                connection.create_table(TABLE_NAME, families={'page': dict()})
            
            table = connection.table(TABLE_NAME)
            current_timestamp = int(round(time.time() * 1000))
            with Batch(table, batch_size=1000, timestamp=current_timestamp) as batch:
                for data in datas:
                    row_key = data['link'][::-1]
                    batch.put(row_key, { 
                        b'page:url': data['link'],
                        b'page:html': data['page'],
                        b'page:spider': data['spider'],
                        b'page:date': data['date']
                    })

    def scan_data(self):
        with self.pool.connection() as connection:
            table = connection.table(TABLE_NAME)
            for key, data in table.scan():
                yield key, data

            