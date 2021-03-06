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
    c1, c2 = plan(ast.source1, **kwargs), plan(ast.source2, **kwargs)
    args = {}
    if ast.deps:
        # print("plan JoinCursor:", ast.deps, kwargs["header"])
        ldeps, rdeps = ast.deps
        lvars = compile_vars(ldeps, **kwargs)
        if lvars:
            args['addLfields'] = lvars
        rvars = compile_vars(rdeps, **kwargs)
        if rvars:
            args['addRfields'] = rvars
    if ast.jointype != Join.UNION:
        args.update(plan(ast.join, **kwargs))
    # print ("join:", args, kwargs["header"])
    return partial(join_execute, c1, c2, ast.jointype, **args)


@plan.register(JoinUsing)
def _(ast, **kwargs):
    lkey, rkey = ast.keys
    return dict(lkey=[k.name for k in lkey], rkey=[k.name for k in rkey])


@plan.register(JoinCondition)
def _(ast, **kwargs):
    if ast.keys:
        lkey, rkey = ast.keys
        return dict(lkey=[k.name for k in lkey], rkey=[k.name for k in rkey])


@plan.register(WhereAst)
def _(ast, f, **kwargs):
    fselect = compile_ast(ast, **kwargs)
    # print("select:", fselect)
    args = dict(selector=fselect)
    if ast.deps:
        # print("plan Where:", ast.deps)
        vs = compile_vars(ast.deps, **kwargs)
        if vs:
            args['addfields'] = vs
    # print("sel:", args, kwargs['header'])
    f = partial(select_execute, f, **args)
    return f


@plan.register(Columns)
def _(ast, f, **kwargs):
    compiler = kwargs['compiler']
    header = kwargs['header']
    # if ast.deps:
    #     print("plan Columns:", ast.deps, header)
    fields = []
    grouping = compiler.ast.groupby is not None
    groups = []
    for var in ast:
        name = compiler.var_name(var)
        if name in header:
            v = name
        else:
            v = var.value
            if is_aggregate(v):
                if grouping:
                    v = name
                else:
                    groups.append((name, compile_ast(var.value, **kwargs)))
                    continue
            else:
                if grouping:
                    continue
                elif isinstance(v, Column):
                    if var.id == v: # column without alias
                        name, v = v.column, name
                    else:
                        v = v.name
                else:
                    v = compile_ast(v, **kwargs)
        fields.append((name, v))
    if groups:
        if fields:
            raise SQLError("Wrong select statement: combine aggregates with not aggregates")
        header, fs = zip(*groups)
        # print("cs:", header, fs)
        return partial(aggregate_execute, f, header=header, aggregates=fs)
    elif fields:
        keys, values = zip(*fields)
        # print("cs:", keys, values, header)
        if keys == values:
            if set(keys) == header:
                # print("as is:", f)
                return f
            else:
                return partial(cut_execute, f, fields=keys)
        # print("fields?:", fields, header)
        header.clear()
        header.update(keys)
        # print("cs1:", fields)
        return partial(fieldmap_execute, f, fieldmap=OrderedDict(fields))
    else:
        return f


@plan.register(AllColumns)
def _(ast, f, header, **kwargs):
    return f


def _reducer(keys, rows, aggragates=[]):
    if not isinstance(keys, (tuple, list)):
        keys = [keys]
    else:
        keys = list(keys)
    # print("reduc:", keys, type(keys))
    rec = keys
    rows = list(rows)
    for f in aggragates:
        try:
            val = f(rows)
        except:
            val = None
        rec.append(val)
    # print("reduc1:", rec)
    return rec


@plan.register(GroupBy)
def _(ast, f, **kwargs):
    compiler = kwargs['compiler']
    header = kwargs['header']
    # print("plan GroupBy:", ast.deps, ast.keys, header)
    keys = []
    hs = []
    aggregates = []
    args = dict(key=keys, header=hs)
    if ast.deps:
        vs = compile_vars(ast.deps, compiler, **kwargs)
    else:
        vs = []
        # print("plan deps GroupBy:", kwargs.get('header'))
    for var in ast.keys:
        if isinstance(var, str):
            name = var
        else:
            name = var.name
        keys.append(name)
        hs.append(name)
        if name not in header:
            v = compiler.find_sql_var(name)
            if v is not None:
               v = compile_ast(v, **kwargs)
               vs.append((name, v))
    if vs:
        args['addfields'] = vs
    # print("group by vars:", ast.vars)
    cs = compiler.ast.columns
    for var in ast.vars:
        name = compiler.var_name(var)
        hs.append(name)
        val = compile_ast(var.value, **kwargs)
        # var.done = True
        aggregates.append(val)
        # print("g var:", var, val)
    # print("group by:", args, aggregates)
    args['reducer'] = partial(_reducer, aggragates=aggregates)
    #new header
    header = kwargs['header']
    header.clear()
    header.update(hs)
    # print("group by:", args)
    return partial(reducer_execute, f, **args)


@plan.register(OrderBy)
def _(ast, f, **kwargs):
    key = []
    vs = []
    compiler = kwargs['compiler']
    header = kwargs['header']
    args = dict(key=key, reverse=ast.desc)
    for var in ast.items:
        if isinstance(var, str):
            name = var
        else:
            name = var.name
        # print("order:", type(var), var, name)
        if name not in header:
            v = compiler.find_sql_var(name)
            if v is not None:
                v = compile_ast(v, **kwargs)
                vs.append((name, v))
            else:
                # print("h:", var, header)
                raise SQLError("Order by unknown field {}".format(name))
        key.append(name)
        if vs:
            args['addfields'] = vs
    # print("order by:", args)
    return partial(sort_execute, f, **args)


def compile_vars(vars, compiler, header, **kwargs):
    fields = []
    # tc = []
    for var in vars:
        name = compiler.var_name(var)
        if name in header:
            continue
        val = var.value
        if var.id == var.value:
            print("it's column and must be in header: {}".format(var))
            # pass # it's column and must be in header
        else:
            fields.append((name, compile_ast(val, compiler=compiler, header=header, **kwargs)))
            header.add(name)
        # if isinstance(val, Column):
        #     # tec = v.name, v.alias
        #     if var.id is not None:
        #         tc.append(val)
        #     val = val.name
        #     # print(">>>?", tec, v)
        # else:
        #     val = compile_ast(val, compiler)
        # fields.append((name, val))
    # for var in tc:
    #     var.alias = None
    return fields
