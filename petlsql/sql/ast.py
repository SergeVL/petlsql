from enum import Enum
import petl as etl
from .. import Aggregator

class SQLError(Exception):
    pass


class NotFound(Exception):
    pass


class Join(Enum):
    INNER = 0
    LEFT = 1
    RIGHT = 2
    FULL = 3
    UNION = 4


class SqlFunc(Enum):
  ROW_NUMBER = 0
  CAST = 1
  NULLIF = 2
  COALESCE = 3
  SUBSTRING = 4
  UPPER = 5
  LOWER = 6
  TRIM = 7
  OVERLAY = 8


class Trim(Enum):
    LEADING = 0
    TRAILING = 1
    BOTH = 2


class Aggregate(Enum):
    COUNT = 0
    AVG = 1
    MAX = 2
    MIN = 3
    SUM = 4
    LIST = 5


class Identifier(str):
    pass


class PyVar:
    def __init__(self, val, code=None):
        self.val = val
        self.code = code


class Column:
    def __init__(self, tbl, column, alias=None):
        self.table = tbl
        self.column = column
        self.alias = alias

    @property
    def name(self):
        return self.alias or self.column

    def aliasing(self):
        if self.alias is None:
            self.alias = "{}_{}".format(self.table.name, self.column)

    def __repr__(self):
        return "Column<{}.{} as {}>".format(self.table, self.column, self.alias)

    def __str__(self):
        return self.name


class Table:
    def __init__(self, tblname, id):
        self.id = id
        self.tblname = tblname
        self.view = None
        self.columns = []
        self.use = {}
        self.rownumber = None

    @property
    def name(self):
        return self.id or self.tblname

    def setView(self, view):
        self.view = view
        self.columns = view.header()

    def _rownumber_alias(self):
        return "{}_@row".format(self.name)

    def use_all(self):
        for c in self.columns:
            use = self.use.get(c)
            if use is None:
                self.use[c] = Column(self, c)

    def find_var(self, cname):
        if '.' in cname:
            tid, cname = str(cname).split('.', 1)
            if not(tid == self.id or tid == self.tblname):
                return
        if cname.lower() == 'rownumber':
            if self.rownumber is None:
                    self.rownumber = Column(self, 'rownumber', self._rownumber_alias())
            return self.rownumber
        r = self.use.get(cname)
        if r is None:
            if cname in self.columns:
                r = self.use[cname] = Column(self, cname)
        return r

    def __str__(self):
        r = self.tblname
        if self.id:
            r += " as "+self.id
        return r

    def __repr__(self):
        return f"Table<{str(self)}>"


class Var:
    _fields = ("value", )

    def __init__(self, id, value):
        self.id = id
        self.value = value

    @property
    def name(self):
        r = self.id
        if r is None:
            if isinstance(self.value, (Column, Identifier)):
                return str(self.value)

    def __str__(self):
        r = str(self.value)
        if self.id is not None:
            r = f"{r} as {self.id}"
        return r

    def __repr__(self):
        return "Var<{}>".format(str(self))


class AllColumns:
    def __str__(self):
        return "*"


class Columns:
    _fields = ("columns", )

    def __init__(self):
        self.columns = []
        self.deps = None

    def append(self, c):
        self.columns.append(c)

    def remove(self, c):
        self.columns.remove(c)

    def get_sql_var(self, name):
        for v in self.columns:
            if v.id == name:
                return v

    def __iter__(self):
        for v in self.columns:
            yield v

    def __str__(self):
        return  ',\n\t'.join(map(str, self.columns))


class View:
    def __init__(self, name, f, *args, **kwargs):
        self.name = name
        self.args = args
        self.kwargs = kwargs
        self.f = f
        self._header = None

    def header(self):
        if self._header is None:
            self._header = self().header()
        return self._header

    def __call__(self, **kwargs):
        kwargs.update(self.kwargs)
        return etl.wrap(self.f(*self.args, **kwargs))

    def __iter__(self):
        return iter(self())


class WithContext:
    def __init__(self):
        self.views = {}

    def addView(self, name, ast):
        self.views[name] = ast

    def hasView(self, name):
        return name in self.views

    def getView(self, name):
        return self.views[name].view


class SelectAst:
    _fields = ("source", "selector", "columns", "groupby", "orders", "params")
    
    def __init__(self):
        self.parent = None
        self.withcontext = None
        self.header = None
        self.distinct = False
        self.columns = None
        self.source = None
        self.selector = None
        self.groupby = None
        self.orders = None
        self.f = None
        self.params = {}

    def set_header(self, names):
        self.header = names

    def addColumn(self, *args):
        if self.columns is None:
            self.columns = Columns()
        self.columns.append(Var(*args))

    def allColumns(self):
        self.columns = AllColumns()

    def set_where(self, cond):
        self.selector = WhereAst(cond)

    def set_order_by(self, vars, desc):
        self.orders = OrderBy(vars, desc)

    def addParam(self, paramId):
        r = self.params.get(paramId)
        if r is None:
            self.params[paramId] = r = Param(paramId)
        return r

    def set_group_by(self, vars):
        self.groupby = GroupBy(vars)

    def __str__(self):
        cols = str(self.columns)
        if self.distinct:
            cols = "DISTINCT "+cols
        return """SELECT {}
        FROM {}
        {}{}{}
        """.format(cols, self.source or "", self.selector or "",
                    self.groupby or "",
                    self.orders or "")


class WhereAst:
    _fields = ("cond", )

    def __init__(self, cond):
        self.cond = cond
        self.deps = None

    def __str__(self):
        return "WHERE {}\n".format(self.cond)


class JoinCursor:
    _fields = ("source1", "source2", "join")

    def __init__(self, src1, src2, joinType, join=None):
        self.source1 = src1
        self.source2 = src2
        self.jointype = joinType
        self.join = join
        self.deps = None
        
    def __str__(self):
        return """%s
        %s %s %s
        """ % (repr(self.source1), self.jointype, repr(self.source2), self.join)

    def find_var(self, cname):
        r = self.source1.find_var(cname)
        if r is None:
            r = self.source2.find_var(cname)
        return r


class JoinCondition:
    _fields = ("cond",)

    def __init__(self,cond):
        self.cond = cond

    def __str__(self):
        return "ON {}".format(self.cond)


class JoinUsing:

    def __init__(self,columns):
        self.columns = columns
        self.lkeys = self.rkeys = None

    def __str__(self):
        return "using({})".format(", ".join(self.columns))


class GroupBy:
    _fields = ("keys",)
    def __init__(self, keys):
        self.keys = keys
        self.deps = None
        self.vars = []

    def __str__(self):
        return "GROUP BY {}\n".format(', '.join(map(str, self.keys)))

    def addVar(self, var):
        self.vars.append(var)

class OrderBy:
    _fields = ("items",)

    def __init__(self, items, desc=False):
        self.items = items
        self.desc = desc

    def __str__(self):
        return "ORDER BY {} {}".format(', '.join(map(str,self.items)), "ASC" if self.asc else "DESC")


class BracesExpr:
    _fields = ("arg",)
    def __init__(self, arg):
        self.arg = arg


class BinaryExpr:
    _fields = ("arg1", "arg2")

    def __init__(self, op, arg1, arg2):
        self.op = op
        self.arg1 = arg1
        self.arg2 = arg2

    def __str__(self):
        return "{} {} {}".format(self.arg1, self.op, self.arg2)


class ConditionExpr:
    _fields = ("args",)
    
    def __init__(self, op, args):
        self.op = op.lower()
        self.args = args

    def __str__(self):
        r = f" {self.op} "
        return r.join(map(str, self.args))

    def __repr__(self):
        return "Condition<{}>".format(str(self))

class Negation:
    _fields = ("arg", )
    
    def __init__(self,arg):
        self.arg = arg

    def __str__(self):
        return "not "+str(self.arg)


class CompareExpr(BinaryExpr):
    pass


class BetweenExpr:
    _fields = ("arg", "fromarg", "toarg")

    def __init__(self, symmetric, is_true, args):
        self.args = args
        self.is_true = is_true
        self.symmetric = symmetric


class InExpr:
    _fields = ("arg", )
    def __init__(self, arg, is_true, vals):
        self.arg = arg
        self.is_true = is_true
        self.values = vals


class LikeExpr:
    _fields = ("arg", )
    def __init__(self, arg, pat, isre, is_true, esc):
        self.arg = arg
        self.pattern = pat
        self.isre = isre
        self.is_true = is_true
        self.escape = esc


class ContainingExpr:
    _fields = ("arg", )
    def __init__(self, arg, pat, is_true):
        self.arg = arg
        self.pat = pat
        self.is_true = is_true


class StartingExpr(ContainingExpr):
    pass


class DistinctFrom:
    _fields = ("args", )
    def __init__(self, is_true, arg1, arg2):
        self.is_true = is_true
        self.args = arg1, arg2


class Check:
    _fields = ("arg", )
    def __init__(self, arg1, is_true, arg2):
        self.arg = arg1
        self.is_true = is_true
        self.val = arg2


class SimpleCase:
    _fields = ("cond", "value")
    def __init__(self, cond, value):
        self.cond = cond
        self.value = value


class SimpleSwitch:
    _fields = ("val", "cases", "elsevalue")

    def __init__(self, val, cases):
        self.val = val
        self.cases = cases
        self.elsevalue = None


class SearchCase:
    _fields = ("cond", "value")
    def __init__(self, cond, value):
        self.cond = cond
        self.value = value

class SearchedSwitch:
    _fields = ("cases", "elsevalue")
    def __init__(self, cases):
        self.cases = cases
        self.elsevalue = None


class AggregateFunc:
    _fields = ("arg", "selector")
    def __init__(self, func, distinct, arg, selector=None):
        self.func = func
        self.distinct = distinct
        self.arg = arg
        self.selector = selector


class Function:
    _fields = ("id", "args")
    def __init__(self, id, args):
        self.id = id
        self.args = args


class SQLFunction:
    _fields = ("args",)
    def __init__(self, id, args):
        self.id = id
        self.args = args

    def __repr__(self):
        return f"SQLFunc<{self.id}>"


class Param:
    def __init__(self,id):
        self.id = id


def is_aggregate(ast):
    try:
        return isinstance(ast, AggregateFunc) \
            or (isinstance(ast, Function) and isinstance(ast.id, PyVar) \
                and issubclass(ast.id.code, Aggregator))
    except:
        return False

class _Stopper:
    __slots__ = ('stop', 'follow', 'post_proc')

    def __init__(self):
        self.stop = False
        self.follow = None
        self.post_proc = None

    def __call__(self):
        self.stop = True

def iter_fields(tree, _fields=None):
    if _fields is None:
        _fields = getattr(tree.__class__, '_fields', [])
    for field in _fields:
        yield field, getattr(tree, field, None)


class Walker:
    """
    @Walker decorates a function of the form:

    @Walker
    def transform(tree, **kw):
        ...
        return new_tree


    Which is used via:

    new_tree = transform.recurse(old_tree, initial_ctx)
    new_tree = transform.recurse(old_tree)
    new_tree, collected = transform.recurse_collect(old_tree, initial_ctx)
    new_tree, collected = transform.recurse_collect(old_tree)
    collected = transform.collect(old_tree, initial_ctx)
    collected = transform.collect(old_tree)

    The `transform` function takes the tree to be transformed, in addition to
    a set of `**kw` which provides additional functionality:


    - `set_ctx`: this is a function, used via `set_ctx(name=value)` anywhere in
      `transform`, which will cause any children of `tree` to receive `name` as
      an argument with a value `value.
    - `set_ctx_for`: this is similar to `set_ctx`, but takes an additional
      parameter `tree` (i.e. `set_ctx_for(tree, name=value)`) and `name` is
      only injected into the parameter list of `transform` when `tree` is the
      AST snippet being transformed.
    - `collect`: this is a function used via `collect(thing)`, which adds
      `thing` to the `collected` list returned by `recurse_collect`.
    - `stop`: when called via `stop()`, this prevents recursion on children
      of the current tree.

    These additional arguments can be declared in the signature, e.g.:

    @Walker
    def transform(tree, ctx, set_ctx, **kw):
        ... do stuff with ctx ...
        set_ctx(...)
        return new_tree

    for ease of use.
    """
    def __init__(self, func):
        self.func = func

    def walk_children(self, tree, sub_kw=[], _fields=None, **kw):
        if isinstance(tree, list):
            if len(tree) > 0:
                aggregates = []
                new_tree = []

                for t in tree:
                    new_t, new_a = self.recurse_collect(t, sub_kw, **kw)
                    if type(new_t) is list:
                        new_tree.extend(new_t)
                    else:
                        new_tree.append(new_t)
                    aggregates.extend(new_a)

                tree[:] = filter(lambda x: x != None, new_tree)
                return aggregates
            else:
                return tree
        else:
            aggregates = []

            for field, old_value in iter_fields(tree, _fields):
                specific_sub_kw = [
                    (k, v)
                    for item, kws in sub_kw
                    if item is old_value
                    for k, v in kws.items()
                ]
                new_value, new_aggregate = self.recurse_collect(old_value, sub_kw, **dict(list(kw.items()) + specific_sub_kw))
                aggregates.extend(new_aggregate)
                setattr(tree, field, new_value)

            return aggregates

    def recurse(self, tree, **kw):
        """Traverse the given AST and return the transformed tree."""
        return self.recurse_collect(tree, **kw)[0]

    def collect(self, tree, **kw):
        """Traverse the given AST and return the transformed tree."""
        return self.recurse_collect(tree, **kw)[1]

    def recurse_collect(self, tree, sub_kw=[], **kw):
        """Traverse the given AST and return the transformed tree together
        with any values which were collected along with way."""

        if tree.__class__.__module__ == Walker.__module__:
            aggregates = []
            stop_now = _Stopper() #[False, None, None]

            new_ctx = dict(**kw)
            new_ctx_for = sub_kw[:]

            def set_ctx(**new_kw):
                new_ctx.update(new_kw)

            def set_ctx_for(tree, **kw):
                new_ctx_for.append((tree, kw))

            new_tree = self.func(
                tree=tree,
                collect=aggregates.append,
                set_ctx=set_ctx,
                set_ctx_for=set_ctx_for,
                stop=stop_now,
                **kw
            )
            if new_tree is not None:
                tree = new_tree

            if not stop_now.stop:
                newaggregates = self.walk_children(tree, new_ctx_for, stop_now.follow, **new_ctx)
                if stop_now.post_proc:
                    tree, newaggregates = stop_now.post_proc(tree, newaggregates, **new_ctx)
                if newaggregates:
                    aggregates.extend(newaggregates)

        else:
            aggregates = self.walk_children(tree, sub_kw, **kw)

        return tree, aggregates


class Tree:
    def __init__(self, func):
        self.func = func

    def walk_children(self, tree, _fields=None, **kw):
        if isinstance(tree, list):
            result = []
            if len(tree) > 0:
                for t in tree:
                    r = self.recurse(t, **kw)
                    if type(r) is list:
                        result.extend(r)
                    else:
                        result.append(r)

                result = list(filter(None, result))
        else:
            result = {}
            for field, old_value in iter_fields(tree, _fields):
                r = self.recurse(old_value, **kw)
                if r is not None:
                    result[field] = r
        return result

    def recurse(self, tree, **kw):
        result = self.walk_children(tree, **kw)
        if tree.__class__.__module__ == Walker.__module__:
            result = self.func(tree=tree, children=result, **kw)
        return result
