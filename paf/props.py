import collections

def check_key(key):
    if not isinstance(key, str):
        raise ValueError("property key is not a string")

def check_value(value):
    if not isinstance(value, (str, int)):
        raise ValueError("property value is not an string or "
                             "number: '%s'" % value)

def to_str(props):
    kvs = []
    for key, values in props.items():
        for value in values:
            if isinstance(value, str):
                kvs.append("'%s': '%s'" % (key, value))
            else:
                kvs.append("'%s': %d" % (key, value))
    return "{%s}" % ", ".join(kvs)
