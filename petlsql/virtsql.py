from functools import singledispatch
# from itertools import chain
from functools import partial
from collections import defaultdict, OrderedDict

from .sql import Scanner, Parser
from .sql.ast import *
from .run import *
from .skan_ast import skan_ast
from .plan import plan

def parse_sql(sqlstr, sqlid=''):
    scanner = Scanner.Scanner(sqlstr)
    parser = Parser.Parser(sqlid)
    parser.Parse(scanner)
    if parser.Successful():
        return parser.result
    raise SQLError('Compile SQL SQLError: {}'.format(sqlstr))


def compile(sql, db, sqlid=''):
    if isinstance(sql, str):
        sql = parse_sql(sql, sqlid=sqlid)
    return Compiler(db).run(sql)


def execute(sql, db, sqlid='', params=None):
    if isinstance(sql, str):
        sql = compile(sql, db, sqlid=sqlid)
    return sql(params=params)


class Compiler:
    def __init__(self, db, parent=None):
        self.parent = parent
        self.db = db
        self.tables = []
        self.columns = {}
        self.varcount = 0
        self.unknown_vars = set()

    def var_name(self, var):
        if var.id is None:
            self.varcount += 1
            var.id = "c{}".format(self.varcount)
        return str(var.id)

    @property
    def module(self):
        return self.db.globals

    def find_vars(self, source, cnames):
        return [self.find_var(source, c) for c in cnames]

    def find_var(self, source, name):
        if source is None:
            for tbl in self.tables:
                var = tbl.find_var(name)
                if var is not None:
                    return var
        else:
            var = source.find_var(name)
        if var is None:
            var = self.db.find_var(name)
        if var is None:
            var = self.find_sql_var(name)
        # print("find_sql_var:", name, var)
        if var is None:
            raise NotFound("Var {} not found".format(name))
        return var

    def find_sql_var(self, name):
        cs = self.ast.columns
        if isinstance(cs, Columns):
            return cs.get_sql_var(name)

    def run(self, ast):
        self.ast = ast
        if ast.withcontext:
            for name, item in ast.withcontext.views.items():
                item.view = View(name, compile(item, self.db, name))
        columns = skan_ast.collect(ast, compiler=self)
        self.ensure_unique(columns)
        if self.unknown_vars:
            raise SQLError("unrecognized vars {}".format(', '.join(self.unknown_vars)))
        # print("columns:", columns)

        kwargs = dict(compiler=self, db=self.db, header=set())
        f = plan(ast.source, **kwargs)
        if ast.selector is not None:
            f = plan(ast.selector, f=f, **kwargs)
            # print("f2:", f)
        if ast.groupby:
            f = plan(ast.groupby, f=f, **kwargs)
        if ast.orders:
            f = plan(ast.orders, f=f, **kwargs)
        f = plan(ast.columns, f=f, **kwargs)
        if ast.distinct:
            f = partial(distinct_execute, f)
        r = ast.view = View('', f)
        return r

    def ensure_unique(self, columns):
        columns = set(map(str, columns))
        cc = defaultdict(int)
        for tbl in self.tables:
            for c in tbl.columns:
                cc[c] += 1
        for tbl in self.tables:
            for cname, c in tbl.use.items():
                if cname in columns and cc[cname] > 1:
                    c.aliasing()
                self.columns[cname] = c.name








