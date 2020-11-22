import inspect
from collections.abc import Iterable
import importlib as im
import petl as etl
from pathlib import Path
from urllib.parse import urlparse

from . import Aggregator
from . import sqlib
from .sql.ast import PyVar, Identifier, View
from .virtsql import compile, execute, SQLError
from .run import filter_keys


__all__ = ("VirtualDB",)


class DirectoryDB:
    _ext_ = {}

    def __init__(self, url, config={}, skip_root=True):
        self.skip_root = skip_root
        self.config = config
        if url:
            _, path, query = path_from_url(url)
        else:
            path = '.'
        self.path = Path(path).resolve()

    def has_view(self, path, query):
        if path.startswith('/'):
            if self.skip_root:
                return False
            path = Path(path)
        else:
            path = self.path / path
        p = self.path / path
        return p.exists() and p.suffix[1:] in self._ext_

    def get_view(self, path, query):
        if path.startswith('/'):
            if self.skip_root:
                return
            path = Path(path)
        else:
            path = self.path / path
        ext = path.suffix[1:]
        extractor = self._ext_.get(ext)[0]
        if extractor:
            args = self.config.get(ext, {})
            if query:
                args.update(str2dict(query))
        return View(path, extractor, path, **args)

    def load_data(self, data, path, **kwargs):
        if path.startswith('/'):
            if self.skip_root:
                return
            path = Path(path)
        else:
            path = self.path / path
        ext = path.suffix[1:]
        loader = self._ext_.get(ext)[1]
        if loader:
            kwargs.update(self.config.get(ext, {}))
            return loader(data, path, **kwargs)


class DB:
    def __init__(self, url, config={}):
        self.conn = self.create_connection(url, config=config)
        # _, path, query = path_from_url(url)
        # self.dbname = path
        self._tables = self._get_tables()

    def has_view(self, path, query=None):
        return path in self._tables

    def get_view(self, path, query=None):
        if self.has_view(path):
            return View(path, self.extract_data, f'SELECT * FROM {path}')

    def _get_tables(self):
        return set()

    def extract_data(self, sql):
        return etl.fromdb(lambda: self.extractcursor(self.conn), sql)

    def load_data(self, data, tablename, append=False, **kwargs):
        if '.' in tablename:
            schema, tablename = tablename.split('.', 1)
            kwargs["schema"] = schema

        f = etl.appenddb if append else etl.todb
        return f(data, lambda: self.loadcursor(self.conn), tablename, **kwargs)



class VirtualDB:
    _drivers = {}
    global_dir = DirectoryDB(None, skip_root=False)

    def __init__(self, **views):
        self._module = None
        self.views = {}
        self.databases = {}
        for k, v in views.items():
            self.addView(k, v)

    def addView(self, name, *args, **kwargs):
        value = None
        if args and len(args) == 1:
            data = args[0]
        else:
            data = kwargs.get('data')
        if isinstance(data, Iterable):
            value = etl.wrap(data)
        elif 'sql' in kwargs:
            value = compile(kwargs['sql'], self, name)
        if kwargs.get('cache', False):
            value = list(iter(value))
        if value is not None:
            self.views[name] = value

    def addDatabase(self, url, name=None, config=None):
        # path = Path(path)
        db = self.databaseByUrl(url, config)
        if db:
            if name is None:
                name = db.name
            self.databases[name] = db

    def _find_cursor_source(self, name):
        db = query = path = ''
        if isinstance(name, Identifier):
            if '.' in name:
                db, path = name.split('.', 1)
            else:
                path = name
        else:
            if ":" in name:
                db, path, query = path_from_url(name)
        if db == 'file':
            r = self.global_dir
        elif db in self.databases:
            r = self.databases[db]
        else:
            r = self
        return r, path, query

    def hasView(self, name):
        db, path, query = self._find_cursor_source(name)
        return db.has_view(path, query)

    def getView(self, name):
        db, path, query = self._find_cursor_source(name)
        return db.get_view(path, query)

    def has_view(self, path, query):
        return path in self.views

    def get_view(self, path, query):
        if path in self.views:
            return View(path, lambda x: x, self.views[path])

    def databaseByUrl(self, url, config=None):
        r = urlparse(url)
        cls = self._drivers.get(r.scheme)
        if cls is not None:
           return cls(url, config)

    def importlib(self, name, asname=None):
        m = im.import_module(name)
        self.globals[asname or name] = m

    def importfile(self, path):
        path = Path(path)
        if path.is_file():
            src = path.read_text()
            exec(src, self.globals)

    def function(self, f, name=None):
        if inspect.isfunction(f) or (inspect.isclass(f) and issubclass(f, Aggregator)):
            if name is None:
                name = f.__name__
            self.globals[name] = f

    def print(self, sql, title=None, limit=10):
        try:
            if isinstance(sql, str):
                sql = execute(sql, self)
            if isinstance(title, str):
                print(title)
            print(etl.look(sql, limit=limit))
        except SQLError as e:
            print(e)

    def load(self, sql, target, append=False, **kwargs):
        try:
            if isinstance(sql, str):
                sql = execute(sql, self)
            db, tblname, _ = self._find_cursor_source(target)
            if db == self:
                raise Exception(f"{target}: Unknown target database")
            db.load_data(sql, tblname, append=append, **kwargs)
        except SQLError as e:
            print(e)

    @property
    def globals(self):
        if self._module is None:
            self._module = {'sqlib': sqlib}
        return self._module

    def addGlobalVar(self, value, varname=None):
        if not isinstance(varname, str):
            if varname is None:
                varname = "v{}".format(id(value))
            else:
                varname = "v{}".format(id(varname))
        self.globals[varname] = value
        return varname

    def find_var(self, name):
        r = self.globals
        if '.' in name:
            for n in name.split('.'):
                if isinstance(r, dict):
                    r = r.get(n)
                else:
                    r = getattr(r, n)
                if r is None:
                    break
        else:
            r = r.get(name)
        if r is not None:
            return PyVar(name, code=r)

    @staticmethod
    def register_db_driver(name, dcls):
        VirtualDB._drivers[name] = dcls

    @staticmethod
    def register_file_driver(name, extractor, loader):
        DirectoryDB._ext_[name] = extractor, loader


VirtualDB.register_db_driver("file", DirectoryDB)


def tocsv(data, target, append, **kwargs):
    kwargs = filter_keys(kwargs, ("encoding", "errors", "write_header", "dialect",
                                  "delimiter", "quotechar", "escapechar", "doublequote", "skipinitialspace",
                                  "lineterminator", "quoting"))
    if append:
        return etl.appendcsv(data, target, **kwargs)
    else:
        return etl.tocsv(data, target, **kwargs)


VirtualDB.register_file_driver("csv", etl.fromcsv, tocsv)


def topickle(data, target, append, **kwargs):
    kwargs = filter_keys(kwargs, ("protocol", "write_header"))
    if append:
        return etl.appendpickle(data, target, **kwargs)
    else:
        return etl.topickle(data, target, **kwargs)


VirtualDB.register_file_driver("pickle", etl.frompickle, topickle)


def path_from_url(url):
    pr = urlparse(url)
    if pr.netloc:
        path = pr.netloc+pr.path
    else:
        path = pr.path
    return pr.scheme, path, pr.query

def str2dict(opts):
    return dict([i.split('=', 1) for i in opts.split('&')])

