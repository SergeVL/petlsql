import psycopg2
from urllib.parse import urlparse
from .virtdb import DB, VirtualDB


class PostgresDB(DB):
    def create_connection(self, url, config=None):
        pr = urlparse(url)
        connargs = dict(host=pr.hostname, port=pr.port, user=pr.username, password=pr.password, database=pr.path[1:])
        return psycopg2.connect(**connargs)

    def extractcursor(self, conn):
        return conn.cursor(name='arbitrary')

    def loadcursor(self, conn):
        return conn.cursor()

    def _get_tables(self):
        c = self.conn.cursor()
        c.execute("""select t2.nspname||'.'||t1.relname from pg_class as t1 
        left join pg_catalog.pg_namespace as t2 on t1.relnamespace=t2.oid 
        where relkind='r' and relname !~ '^(pg_|sql_)'""")
        return set([r[0] for r in c.fetchall()])

VirtualDB.register_db_driver('postgres', PostgresDB)
