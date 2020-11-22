import sqlite3
from .virtdb import DB, VirtualDB, path_from_url


class SqliteDB(DB):
    def create_connection(self, url, config=None):
        _, dbname, query = path_from_url(url)
        return sqlite3.connect(self.dbname)

    def extractcursor(self, conn):
        return conn.cursor()

    def loadcursor(self, conn):
        return conn.cursor()

    def _get_tables(self):
        c = self.conn.cursor()
        rs = c.execute("SELECT name FROM sqlite_master WHERE type ='table' AND name NOT LIKE 'sqlite_%'")
        return set([r[0] for r in rs])


VirtualDB.register_db_driver('sqlite', SqliteDB)


