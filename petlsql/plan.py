from functools import singledispatch, partial
from collections import OrderedDict
from .run import *
from .sql.ast import *
from .compile_ast import compile_ast


@singledispatch
def plan(ast, f=None, **kwargs):
    print("?????", type(ast))
    return f


@plan.register(Table)
def _(ast, db, header, **kwargs):
    # print("***2", db, kwargs)
    fields = {}
    attrs = dict(fieldmap=fields)
    if ast.rownumber:
        name = ast.rownumber.alias
        attrs['row_number'] = name
        header.add(name)
    for k, c in ast.use.items():
        name = str(c.alias or k)
        fields[name] = str(c.column)
        header.add(name)
    return partial(table_execute, ast.view, **attrs)


@plan.register(JoinCursor)
def _(ast, **kwargs):
    args = {}
    if ast.deps:
        print("plan JoinCursor:", ast.deps)
        ldeps, rdeps = ast.deps
        lvars = compile_vars(ldeps, **kwargs)
        if lvars:
            args['addLfields'] = lvars
        rvars = compile_vars(rdeps, **kwargs)
        if rvars:
            args['addRfields'] = rvars
    if ast.jointype != Join.UNION:
        args.update(plan(ast.join, **kwargs))
    return partial(join_execute, plan(ast.source1, **kwargs), plan(ast.source2, **kwargs), ast.jointype, **args)


@plan.register(JoinUsing)
def _(ast, **kwargs):
    lkey, rkey = ast.keys
    return dict(lkey=[str(k) for k in lkey], rkey=[str(k) for k in rkey])

@plan.register(JoinCondition)
def _(ast, **kwargs):
    if ast.keys:
        lkey, rkey = ast.keys
        return dict(lkey=[str(k) for k in lkey], rkey=[str(k) for k in rkey])

# JoinCondition

@plan.register(WhereAst)
def _(ast, f, **kwargs):
    fselect = compile_ast(ast, **kwargs)
    args = dict(selector=fselect)
    if ast.deps:
        print("plan Where:", ast.deps)
        vs = compile_vars(ast.deps, **kwargs)
        if vs:
            args['addfields'] = vs
    f = partial(select_execute, f, **args)
    return f

@plan.register(Columns)
def _(ast, compiler, f, **kwargs):
    if ast.deps:
        print("plan Columns:", ast.deps)
    fields = OrderedDict()
    tc = []
    for c in ast:
        if c.done:
            continue
        v = c.value
        if isinstance(v, Column):
            # tec = v.name, v.alias
            if c.id is not None:
                tc.append(v)
            v = v.name
            # print(">>>?", tec, v)
        else:
            if not is_aggregate(v):
                # print("c?:", v, c.done)
                v = compile_ast(v, compiler)
        fields[c.name] = v
    # print("cs?:", fields)
    if fields:
        f = partial(fieldmap_execute, f, fieldmap=fields)
        for c in tc:
            c.alias = None
        # print("????", fields)
    return f


@plan.register(AllColumns)
def _(ast, f, header, **kwargs):
    return f

def _reducer(keys, rows, aggragates=[]):
    rec = list(keys)
    rows = list(rows)
    for f in aggragates:
        try:
            val = f(rows)
        except:
            val = None
        rec.append(val)
    return rec


@plan.register(GroupBy)
def _(ast, f, compiler, **kwargs):
    # print("plan GroupBy:", ast.deps, ast.vars)
    keys = []
    hs = []
    aggregates = []
    args = dict(key=keys, header=hs)
    if ast.deps:
        vs = compile_vars(ast.deps, compiler, **kwargs)
        if vs:
            args['addfields'] = vs
    for var in ast.keys:
        name = var.name
        keys.append(name)
        hs.append(name)
    for var in ast.vars:
        name = var.id
        if name is None:
            name = compiler.new_var()
        hs.append(name)
        val = compile_ast(var.value, compiler)
        var.done = True
        aggregates.append(val)
        # print("g var:", var, val)
    # print(aggregates, args)
    args['reducer'] = partial(_reducer, aggragates=aggregates)
    return partial(reducer_execute, f, **args)


@plan.register(OrderBy)
def _(ast, f, compiler, **kwargs):
    key = []
    for k in ast.items:
        name = k.name
        # print("order:", type(k), k, k.name)
        key.append(name)
    return partial(sort_execute, f, key=key)
    return f


def compile_vars(vars, compiler, header, **kwargs):
    fields = []
    tc = []
    for var in vars:
        if var.done:
            continue
        name = var.name
        if name in header:
            print(f"var {name} alredy exist")
            continue
        val = var.value
        if isinstance(val, Column):
            # tec = v.name, v.alias
            if var.id is not None:
                tc.append(val)
            val = val.name
            # print(">>>?", tec, v)
        else:
            val = compile_ast(val, compiler)
        fields.append((name, val))
        header.add(name)
    for c in tc:
        c.alias = None
    return fields
