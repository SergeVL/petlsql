import yaml
import yaml.constructor
from collections import OrderedDict
from pathlib import Path

from .virtdb import VirtualDB, etl
from .run import filter_keys


def _construct_mapping(loader, node, deep=False):
    if isinstance(node, yaml.MappingNode):
        loader.flatten_mapping(node)
    else:
        raise yaml.constructor.ConstructorError(None, None,
            'expected a mapping node, but found %s' % node.id, node.start_mark)

    mapping = OrderedDict()
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        try:
            hash(key)
        except TypeError as exc:
            raise yaml.constructor.ConstructorError('while constructing a mapping',
                node.start_mark, 'found unacceptable key (%s)' % exc, key_node.start_mark)
        value = loader.construct_object(value_node, deep=deep)
        mapping[key] = value
    return mapping


def construct_yaml_map(loader, node):
    data = OrderedDict()
    yield data
    value = _construct_mapping(loader, node)
    data.update(value)


_Loader = getattr(yaml, 'CLoader', yaml.Loader)


class Loader(_Loader):
    pass


Loader.add_constructor('tag:yaml.org,2002:map', construct_yaml_map)
Loader.add_constructor('tag:yaml.org,2002:omap', construct_yaml_map)


class Dumper(yaml.SafeDumper):
    pass


def _dict_representer(dumper, data):
    return dumper.represent_mapping(
        yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
        data.items())


Dumper.add_representer(OrderedDict, _dict_representer)


def fromyaml(source):
    with Path(source).open('rb') as f:
        data = yaml.load(f, Loader=Loader)
    return etl.fromdicts(data)


def toyaml(data, target, append, **kwargs):
    kwargs = filter_keys(kwargs, ("default_style", "canonical", "indent", "width",
                                  "allow_unicode", "line_break", "encoding",
                                  "explicit_start", "explicit_end", "tags", "sort_keys"))
    with Path(target).open('a' if append else 'w') as f:
        for d in iterdicts(data):
            yaml.dump([d], f, Dumper=Dumper, **kwargs)


def iterdicts(table):
    it = iter(table)
    hdr = next(it)
    for row in it:
        yield asdict(hdr, row)


def asdict(hdr, row):
    flds = [str(f) for f in hdr]
    try:
        items = [(flds[i], row[i]) for i in range(len(flds)) if row[i] is not None]
    except IndexError:
        items = list()
        for i, f in enumerate(flds):
            try:
                v = row[i]
            except IndexError:
                v = None
            if v is not None:
                items.append((f, v))
    return OrderedDict(items)


VirtualDB.register_file_driver("yaml", fromyaml, toyaml)








def dump(data, stream=None, Dumper=None, **kwds):
    d = dumper(Dumper)
    if isinstance(stream, Path):
        with Path(stream).open('w') as f:
            # print(type(data))
            return yaml.dump(data, stream=f, Dumper=d, **kwds)
    return yaml.dump(data, stream=stream, Dumper=d, **kwds)
