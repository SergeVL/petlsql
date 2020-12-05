from .sql.ast import *
from functools import singledispatch

@Walker
def skan_ast(tree, *args, **kwargs):
    return scan(tree, *args, **kwargs)


@singledispatch
def scan(ast, *args, **kwargs):
    # print(".:", type(ast), ast)
    return ast


@scan.register(SelectAst)
def _(ast, compiler, set_ctx, stop, **kwargs):
    # print("select:", ast)
    stop.follow = ("source", "columns", "selector", "groupby", "orders", "params")
    if compiler.ast != ast:
        cls = type(compiler)
        c = cls(compiler.db, parent=compiler)
        c.ast = ast
        set_ctx(compiler=c)


@scan.register(Table)
def _(ast, compiler, **kwargs):
    # print("table:", ast, type(ast.tblname))
    tblname = ast.tblname
    compiler.tables.append(ast)
    root = compiler.ast
    if root.withcontext and root.withcontext.hasView(tblname):
        ast.setView(root.withcontext.getView(tblname))
    elif compiler.db.hasView(tblname):
        ast.setView(compiler.db.getView(tblname))
    else:
        raise Exception(f"{tblname}: Unknown table")

@scan.register(JoinCursor)
def _(ast, set_ctx_for, stop, **kwargs):
    if ast.join:
        set_ctx_for(ast.join, context=ast)
    stop.post_proc = post_join


def post_join(ast, aggregates, **kwargs):
    try:
        keys = ast.join.keys
        alldeps = []
        has = False
        for ks in keys:
            deps = []
            for k in ks:
                if isinstance(k, Var):
                    # if not k.done:
                    #     k.done = True
                    deps.append(k)
            if deps:
                has = True
                for item in deps:
                    try:
                        aggregates.remove(item)
                    except ValueError:
                        pass
            alldeps.append(deps)
        if has:
            ast.deps = alldeps
    except:
        pass
    return ast, aggregates


@scan.register(JoinUsing)
def _(ast, context, compiler, **kwargs):
    lkeys = compiler.find_vars(context.source1, ast.columns)
    rkeys = compiler.find_vars(context.source2, ast.columns)
    ast.keys = lkeys, rkeys


@scan.register(JoinCondition)
def _(ast, compiler, context, **kwargs):
    try:
        lkeys, rkeys = collect_eq_condition(ast.cond)
        lkeys = compiler.find_vars(context.source1, lkeys)
        rkeys = compiler.find_vars(context.source2, rkeys)
        ast.keys = lkeys, rkeys
        # print("cond|", ast.keys)
    except _StopException as e:
        print("!!!", e)
        raise NotImplementedError("join on condition must be AND =")


@scan.register(Identifier)
def _(ast, compiler, collect, **kwargs):
    r = compiler.find_var(None, str(ast))
    if isinstance(r, (Column, Var)):
        # print("c found:", ast, repr(r))
        collect(r)
    if r is None:
        compiler.unknown_vars.add(ast)
    return r or ast


@scan.register(Var)
def _(ast, stop, collect, **kwargs):
    stop.post_proc = post_var
    return ast


def post_var(ast, aggregates, compiler, **kwargs):
    if ast.id is None and isinstance(ast.value, Column):
        ast.id = ast.value
    return ast, aggregates


@scan.register(WhereAst)
def _(ast, stop, **kwargs):
    stop.post_proc = post_clause


def post_clause(ast, aggregates, **kwargs):
    # print("post:", type(ast), ast, "/aggregates:", aggregates)
    deps = []
    for item in aggregates:
        if isinstance(item, Var):
            # if not item.done:
            #     item.done = True
            deps.append(item)
    if deps:
        for item in deps:
            aggregates.remove(item)
        ast.deps = deps
    return ast, aggregates


@scan.register(Columns)
def _(ast, stop, **kwargs):
    stop.post_proc = post_clause


@scan.register(AllColumns)
def _(ast, compiler, **kwargs):
    # print("*")
    for tbl in compiler.tables:
        tbl.use_all()


@scan.register(GroupBy)
def _(ast, stop, **kwargs):
    # print("g.:", ast)
    stop.post_proc = post_group


def post_group(ast, aggregates, compiler, **kwargs):
    post_clause(ast, aggregates, **kwargs)
    cs = compiler.ast.columns
    vars = []
    if isinstance(cs, Columns):
        for v in cs:
            if is_aggregate(v.value):
                # v.done = True
                vars.append(v)
    ast.vars = vars
    return ast, aggregates


class _StopException(Exception):
    pass


@Walker
def collect_equal_conditions(tree, collect, **kw):
    # print(".>", type(tree), tree)
    if isinstance(tree, ConditionExpr):
        if tree.op != "and":
            raise _StopException("not and")
    elif isinstance(tree, CompareExpr):
        if tree.op != "==":
            raise _StopException("not eq: {}".format(tree.op))
        collect((tree.arg1, tree.arg2))
    elif isinstance(tree, Negation):
        raise _StopException("Negation")


def collect_eq_condition(cond):
    r = collect_equal_conditions.collect(cond)
    return list(zip(*r))
