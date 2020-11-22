
def LTRIM(s, chars=None):
    if isinstance(s, (str, bytes)):
        return s.lstrip(chars)


def RTRIM(s, chars=None):
    if isinstance(s, (str, bytes)):
        return s.rstrip(chars)


def TRIM(s, chars=None):
    if isinstance(s, (str, bytes)):
        return s.strip(chars)


def SUBSTRING(s, p, l=None):
    if s is not None:
        p -= 1
        return s[p:] if l is None else s[p:p+l]


def LOWER(v):
    if isinstance(v, str):
        return v.lower()


def UPPER(v):
    if isinstance(v, str):
        return v.upper()


def NULLIF(v1, v2):
    return None if v1 == v2 else v1


def INT(v):
    if v is not None:
        return int(v)


def FLOAT(v):
    if v is not None:
        return float(v)


def STR(v):
    if v is not None:
        return str(v)


def BOOL(v):
    if isinstance(v, str) and len(v)>0:
        v = v[0]
        if v in 'YyTtДд':
            return True
        elif v in 'NnFfНн':
            return False
    elif v is not None:
        return bool(v)


def COALESCE(rec, *args):
    for arg in args:
        try:
            arg = arg(rec)
            if arg is not None:
                return arg
        except:
            pass


def CHECK(rec, f, val):
    try:
        return f(rec) is val
    except:
        return False


def CHECKNOT(rec, f, val):
    try:
        return f(rec) is not val
    except:
        return False


def OVERLAY(value, rep, p, l=None):
    if value is not None and rep is not None:
        p -= 1
        return value[:p] + rep + (value[p+l:] if l is not None else value[p+len(rep):])


def COUNT(values):
    values = list(filter(None, values))
    return len(values)


def COUNT_DISTINCT(values):
    values = set(filter(None, values))
    return len(values)


def MIN(values):
    values = list(filter(None, values))
    if values:
        return min(values)


def MAX(values):
    values = list(filter(None, values))
    if values:
        return max(values)


MIN_DISTINCT = MIN
MAX_DISTINCT = MAX


def SUM(values):
    values = list(filter(None, values))
    if values:
        return sum(values)


def SUM_DISTINCT(values):
    values = set(filter(None, values))
    if values:
        return sum(values)


def AVG(values):
    values = list(filter(None, values))
    if values:
        return sum(values)/len(values)


def AVG_DISTINCT(values):
    values = set(filter(None, values))
    if values:
        return sum(values)/len(values)


def LIST(values):
    return list(filter(None, values))


def LIST_DISTINCT(values):
    return list(set(filter(None, values)))


def AGGREGATOR(cls, values):
    r = cls()
    for i in values:
        r.step(i)
    return r.finalize()


def BETWEEN(value, A, B):
    if value is not None:
        return value >=A and value <= B


def BETWEEN_SYMMETRIC(value, A, B):
    if value is not None:
        return (value >= A and value <= B) or (value >= B and value <= A)


def MATCH(rex, value):
    if value is not None:
        return rex.match(value)


def NOTMATCH(rex,value):
    if value is not None:
        return not rex.match(value)


def IN(arg, *values):
    if arg is not None:
        return arg in values


def NOTIN(arg, *values):
    if arg is not None:
        return arg not in values


def CONTAINS(arg, s):
    if arg is not None:
        return arg.find(s) != -1


def NOTCONTAINS(arg, s):
    if arg is not None:
        return arg.find(s) == -1


def STARTSWITH(arg, s):
    if arg is not None:
        return arg.startswith(s)


def NOTSTARTSWITH(arg, s):
    if arg is not None:
        return not arg.startswith(s)


def DISTINCTFROM(A, B):
    return A != B


def NOTDISTINCTFROM(A, B):
    return A == B

