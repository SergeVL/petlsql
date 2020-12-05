from functools import singledispatch
from .sql.ast import *
import re


def compile_function(compiler, fid, src, arg='rec'):
    if '\n' in src:
        src = """
def {}({}):
    {}
    """.format(fid, arg, src)
    else:
        src = """
def {}({}):
    return {}
    """.format(fid, arg, src)
    # print("func:\n", src)
    exec(src, compiler.module)
    return compiler.module.get(fid)


def compile_ast(ast, compiler, **kwargs):
    r = comp(ast, compiler=compiler, **kwargs)
    if isinstance(r, Expression):
        r = compile_function(compiler, "f{}".format(id(ast)), r)
    return r


@singledispatch
def comp(ast, **kwargs):
    # print("e:", type(ast), ast)
    return str(ast)


class Expression(str):
    pass


@comp.register(Column)
def _(ast, compiler, **kwargs):
    # print("cc:", repr(ast))
    return Expression("rec.{}".format(ast.name))


@comp.register(BinaryExpr)
def _(ast, **kwargs):
    op = ast.op
    if op == "||":
        op = "+"
    return Expression("{} {} {}".format(comp(ast.arg1, **kwargs), op, comp(ast.arg2, **kwargs)))

@comp.register(ConditionExpr)
def _(ast, **kwargs):
    delimeter = " {} ".format(ast.op)
    return Expression(delimeter.join([comp(arg, **kwargs) for arg in ast.args]))


@comp.register(Negation)
def _(ast, **kwargs):
    return Expression("not {}".format(comp(ast.arg, **kwargs)))


@comp.register(BetweenExpr)
def _(ast, **kwargs):
    sufix = "_SYMMETRIC" if ast.symmetric else ""
    prefix = "" if ast.is_true else "not "
    return Expression("{}sqlib.BETWEEN{}({})".format(prefix, sufix, ', '.join([comp(arg, **kwargs) for arg in ast.args])))


rexchars = '[]\^$.-|?*+(){}'

def _escape(m):
    return '@%d@' % ord(m.group(1))


def _unescape(m):
    c = chr(m.group(1))
    if c in rexchars:
        return '\\'+c
    return c


@comp.register(LikeExpr)
def _(ast, **kwargs):
    db = kwargs['db']
    arg = comp(ast.arg, **kwargs)
    pattern = ast.pattern
    escape = ast.escape
    if escape:
        if len(escape) > 1:
            escape = escape[0]
        pattern = re.sub('\\' + escape + "(.)", _escape, pattern)
    if not ast.isre:
        for ch in rexchars:
            pattern.replace(ch, '\\'+ch)
    pattern = re.sub(r'%', '.*', pattern)
    pattern = re.sub(r'_', '.', pattern)
    if escape:
        pattern = re.sub(r'@(\d+)@', _unescape, pattern)
    if pattern[0] != '^':
        pattern = '^'+pattern
    if pattern[-1] != '$':
        pattern += '$'
    pattern = re.compile(pattern)
    rex = db.addGlobalVar(pattern, ast)
    prefix = "" if ast.is_true else "NOT"
    return Expression('sqlib.{}MATCH({},{})'.format(prefix, rex, arg))


@comp.register(InExpr)
def _(ast, **kwargs):
    arg = comp(ast.arg, **kwargs)
    values = [comp(v, **kwargs) for v in ast.values]
    prefix = "" if ast.is_true else "NOT"
    return Expression('sqlib.{}IN({},{})'.format(prefix, arg, ', '.join(values)))

@comp.register(ContainingExpr)
def _(ast, **kwargs):
    arg = comp(ast.arg, **kwargs)
    prefix = "" if ast.is_true else "NOT"
    return Expression('sqlib.{}CONTAINS({},"{}")'.format(prefix, arg, ast.pat))

@comp.register(StartingExpr)
def _(ast, **kwargs):
    arg = comp(ast.arg, **kwargs)
    prefix = "" if ast.is_true else "NOT"
    return Expression('sqlib.{}STARTSWITH({},"{}")'.format(prefix, arg, ast.pat))

@comp.register(DistinctFrom)
def _(ast, **kwargs):
    args = [comp(arg, **kwargs) for arg in ast.args]
    prefix = "" if ast.is_true else "NOT"
    return Expression('sqlib.{}DISTINCTFROM({})'.format(prefix, ', '.join(args)))


@comp.register(BracesExpr)
def _(ast, **kwargs):
    arg = comp(ast.arg, **kwargs)
    return Expression('({})'.format(arg))


@comp.register(SimpleSwitch)
def _(ast, **kwargs):
    val = comp(ast.val, **kwargs)
    cases = [comp(c, **kwargs) for c in ast.cases]
    src = "val = {}\n    if val is not None:\n        ".format(val)
    for i in range(1, len(cases)):
        cases[i] = "el" + cases[i]
    src += "\n        ".join(cases)
    if ast.elsevalue:
        src += "\n        else: return "+comp(ast.elsevalue, **kwargs)
    return compile_function(kwargs['compiler'], "r{}".format(id(ast)), src)


@comp.register(SimpleCase)
def _(ast, **kwargs):
    is_compare = isinstance(ast.cond, CompareExpr)
    if is_compare:
        ast.cond.arg1 = PyVar("val")
    cond = comp(ast.cond, **kwargs)
    if not is_compare:
        cond = "val == "+cond
    value = comp(ast.value, **kwargs)
    return "if {}: return {}".format(cond, value)


@comp.register(SearchedSwitch)
def _(ast, **kwargs):
    cases = [comp(c, **kwargs) for c in ast.cases]
    for i in range(1, len(cases)):
        cases[i] = "el" + cases[i]
    src = "\n    ".join(cases)
    if ast.elsevalue:
        src += "\n    else: return "+comp(ast.elsevalue, **kwargs)
    return compile_function(kwargs['compiler'], "r{}".format(id(ast)), src)


@comp.register(SearchCase)
def _(ast, **kwargs):
    cond = comp(ast.cond, **kwargs)
    value = comp(ast.value, **kwargs)
    return "if {}: return {}".format(cond, value)


@comp.register(Function)
def _(ast, **kwargs):
    #FIXME check signature if ast.id.code
    if isinstance(ast.id, PyVar):
        func = ast.id.val
        args = [comp(arg, **kwargs) for arg in ast.args]
        if is_aggregate(ast):
            if len(args) == 1:
                arg = args[0]
            else:
                arg = "[{}]".format(', '.join(args))
            src = 'sqlib.AGGREGATOR({}, [{} for rec in rows])'.format(func, arg)
            return compile_function(kwargs['compiler'], "r{}".format(id(ast)), src, arg='rows')
        return Expression('{}({})'.format(func, ', '.join(args)))


@comp.register(Var)
def _(ast, **kwargs):
    return comp(ast.value, **kwargs)

@comp.register(PyVar)
def _(ast, **kwargs):
    return ast.val


_type2func = {
    int: "sqlib.INT",
    str: "sqlib.STR",
    float: "sqlib.FLOAT",
    bool : "sqlib.BOOL"
}

def type2func(t):
    return _type2func.get(t)


@comp.register(SQLFunction)
def _(ast, **kwargs):
    args = [comp(arg, **kwargs) for arg in ast.args]
    aid = ast.id
    if aid == SqlFunc.CAST:
        return Expression("{}({})".format(type2func(args[0]), args[1]))
    elif aid == SqlFunc.TRIM:
        args = args[1:]
        args.reverse()
        case = ast.args[0]
        prefix = "L" if case == Trim.LEADING else "R" if case == Trim.TRAILING else ""
        return Expression("sqlib.{}{}({})".format(prefix, aid.name, ', '.join(args)))
    elif aid == SqlFunc.COALESCE:
        args = ["lambda rec: {}".format(arg) for arg in args]
        return Expression("sqlib.COALESCE(rec, {})".format(', '.join(args)))
    else:
        return Expression("sqlib.{}({})".format(aid.name, ', '.join(args)))


@comp.register(str)
def _(ast, **kwargs):
    return '"{}"'.format(ast)

@comp.register(type)
def _(ast, **kwargs):
    return ast


@comp.register(int)
@comp.register(float)
@comp.register(bool)
def _(ast, **kwargs):
    return str(ast)


@comp.register(WhereAst)
def _(ast, **kwargs):
    return comp(ast.cond, **kwargs)


@comp.register(Check)
def _(ast, compiler=None, **kwargs):
    arg = comp(ast.arg, compiler=compiler, **kwargs)
    if '\n' in arg:
        f = compiler.compile_function(arg)
    else:
        f = "lambda rec: {}".format(arg)
    return Expression("sqlib.CHECK{}(rec, {}, {})".format("" if ast.is_true else "NOT", f, ast.val))


@comp.register(AggregateFunc)
def _(ast, **kwargs):
    arg = comp(ast.arg, **kwargs)
    funcname = ast.func.name
    filter = comp(ast.selector, **kwargs) if ast.selector else ""
    if ast.distinct:
        funcname += "_DISTINCT"
    if ast.arg is None and ast.func == Aggregate.COUNT:
        if filter:
            src = "len(list(filter(lambda rec: {}, rows)))".format(filter)
        else:
            src = "len(rows)"
    else:
        if filter:
            filter = " if "+filter
        src = "sqlib.{}([{} for rec in rows{}])".format(funcname, arg, filter)
    return compile_function(kwargs['compiler'], "r{}".format(id(ast)), src, arg='rows')


@comp.register(list)
def _(ast, **kwargs):
    r = [comp(x, **kwargs) for x in ast]
    return Expression('({})'.format(', '.join(r)))
