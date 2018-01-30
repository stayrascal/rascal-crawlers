import sys
import time

from happybase import Batch

import happybase as hb

class HBaseConnector:
    def __init__(self, host, table_prefix, table_name):
        self.pool = hb.ConnectionPool(
            size = 16,
            host = host,
            autoconnect = True,
            table_prefix = table_prefix
        )
        self.table_name = table_name
    
    def insert_data(self, datas):
        with self.pool.connection() as connection:
            tables = connection.tables()
            if TABLE_NAME not in tables:
                connection.create_table(self.table_name, families={'page': dict()})
            
            table = connection.table(self.table_name)
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
            table = connection.table(self.table_name)
            for key, data in table.scan():
                yield key, data

            