from .sql.ast import *
import petl as etl


def filter_keys(kwargs, keys):
    r = {}
    for k in keys:
        if k in kwargs:
            r[k] = kwargs[k]
    return r


def table_execute(view, **kwargs):
    r = iter(view) #@
    if 'row_number' in kwargs:
        r = etl.addrownumbers(r, field=kwargs['row_number'])
    if 'fieldmap' in kwargs:
        r = etl.fieldmap(r, kwargs['fieldmap'])
    return r


def join_execute(cl, cr, join, **kwargs):
    cl, cr = cl(), cr()
    if 'addLfields' in kwargs:
        cl = etl.addfields(cl, kwargs['addLfields'])
    if 'addRfields' in kwargs:
        cr = etl.addfields(cr, kwargs['addRfields'])
    args = cl, cr
    if join == Join.UNION:
        c = etl.crossjoin(*args)
    else:
        kwargs = filter_keys(kwargs, ("key", "lkey", "rkey", "missing", "presorted", "buffersize", "tempdir", "cache"))
        if join == Join.INNER:
            c = etl.join(*args, **kwargs)
        elif join == Join.LEFT:
            c = etl.leftjoin(*args, **kwargs)
        elif join == Join.RIGHT:
            c = etl.rightjoin(*args, **kwargs)
        elif join == Join.FULL:
            c = etl.outerjoin(*args, **kwargs)
    return c


def addfields_execute(c, addfields={}, **kwargs):
    r = c()
    if addfields:
        r = etl.addfields(r, addfields)
    return r


def fieldmap_execute(c, fieldmap={}, **kwargs):
    r = c()
    if fieldmap:
        r = etl.fieldmap(r, fieldmap)
    return r


def select_execute(c, selector, **kwargs):
    r = c()
    if 'addfields' in kwargs:
        r = etl.addfields(r, kwargs['addfields'])
    if selector:
        r = etl.select(r, selector)
    return r

def reducer_execute(c, **kwargs):
    r = c()
    if 'addfields' in kwargs:
        r = etl.addfields(r, kwargs['addfields'])
    kwargs = filter_keys(kwargs, ("key", "reducer", "header"))
    return etl.rowreduce(r, **kwargs)

def sort_execute(c, **kwargs):
    r = c()
    kwargs = filter_keys(kwargs, ("key", "reverse"))
    r = etl.sort(r, **kwargs)
    return r

def unique_execute(c):
    return etl.unique(c())
