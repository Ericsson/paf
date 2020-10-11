# SPDX-License-Identifier: BSD-3-Clause
# Copyright(c) 2020 Ericsson AB

import re

BEGIN_EXPR = '('
END_EXPR = ')'
ANY = '*'
ESCAPE = '\\'

NOT = '!'
AND = '&'
OR = '|'
EQUAL = '='
GREATER_THAN = '>'
LESS_THAN = '<'

SPECIALS = {
    BEGIN_EXPR, END_EXPR, ANY, ESCAPE, AND, OR, EQUAL, GREATER_THAN, LESS_THAN
}


class ParseError(Exception):
    def __init__(self, state, error_desc):
        Exception.__init__(self, "'%s' (offset %d): %s" %
                           (state.data, state.offset, error_desc))


class ParseState:
    def __init__(self, data):
        self.data = data
        self.offset = 0

    def consume(self, c):
        self.expect(c)
        self.offset += 1

    def current(self):
        self._verify_offset()
        return self.data[self.offset]

    def _verify_offset(self):
        if self.offset >= len(self.data):
            raise ParseError(self, "Unexpected end of expression")

    def skip(self):
        self._verify_offset()
        self.offset += 1

    def expect(self, expected):
        actual = self.current()
        if actual != expected:
            raise ParseError(self, "Expected to find '%s', but found '%s'" %
                             (expected, actual))
        self.offset += 1

    def is_current(self, expected):
        actual = self.current()
        return actual == expected

    def __len__(self):
        return len(self.data) - self.offset


def _parse_str(state):
    escaped = False
    out_str = ""
    while True:
        in_c = state.current()
        special = in_c in SPECIALS
        if escaped:
            if not special:
                raise ParseError(state, "Escaped character '%s' is not "
                                 "a special character" % in_c)
            out_str += in_c
            state.skip()
            escaped = False
        else:
            if in_c == ESCAPE:
                escaped = True
            elif special:
                return out_str
            else:
                out_str += in_c
            state.skip()


def _check_value(state, value):
    if value == "":
        raise ParseError(state, "Zero-length (sub)string values "
                         "not permitted")


def _check_key(state, key):
    if key == "":
        raise ParseError(state, "Zero-length keys not permitted")


def _parse_equal(state, key):
    state.expect(EQUAL)
    value = _parse_str(state)

    if not state.is_current(ANY):
        _check_value(state, value)
        return Equal(key, value)

    state.skip()

    if value == "":
        initial = None
    else:
        initial = value

    intermediate = []

    while True:
        value = _parse_str(state)

        if state.is_current(ANY):
            _check_value(state, value)
            intermediate.append(value)
            state.skip()
        else:
            if value == "":
                final = None
            else:
                final = value
            break

    if initial is None and len(intermediate) == 0 and final is None:
        return Present(key)
    else:
        return Substring(key, initial=initial, intermediate=intermediate,
                         final=final)


def _parse_greater_and_less_than(state, key, op_class):
    state.expect(op_class.op)

    value = _parse_str(state)

    int_value = None

    if value.strip() == value:
        try:
            int_value = int(value)
        except ValueError:
            pass

    if int_value is None:
        raise ParseError(state, "'%s' is not an integer")

    return op_class(key, int_value)


def _parse_simple(state):
    key = _parse_str(state)
    _check_key(state, key)

    if state.is_current(EQUAL):
        return _parse_equal(state, key)
    elif state.is_current(GREATER_THAN):
        return _parse_greater_and_less_than(state, key, GreaterThan)
    elif state.is_current(LESS_THAN):
        return _parse_greater_and_less_than(state, key, LessThan)
    else:
        raise ParseError(state, "Expected to find '%s', '%s' or '%s'" %
                         (EQUAL, GREATER_THAN, LESS_THAN))


def _parse_not(state):
    state.expect(NOT)

    state.expect(BEGIN_EXPR)

    operand = Not(_parse(state))

    state.expect(END_EXPR)

    return operand


def _parse_composite(state, op_class):
    state.expect(op_class.op)

    operands = []

    while True:
        if state.is_current(BEGIN_EXPR):
            state.skip()
            operands.append(_parse(state))
            state.expect(END_EXPR)
        elif state.is_current(END_EXPR):
            if len(operands) < 2:
                raise ParseError(state, "Operator '%s' requires at least two "
                                 "operand expressions" %
                                 op_class.op)
            return op_class(*operands)
        else:
            raise ParseError(state, "Expected to find '%s' or '%s'" %
                             (BEGIN_EXPR, END_EXPR))


def _parse(state):
    if state.is_current(AND):
        filter = _parse_composite(state, And)
    elif state.is_current(OR):
        filter = _parse_composite(state, Or)
    elif state.is_current(NOT):
        filter = _parse_not(state)
    else:
        filter = _parse_simple(state)
    return filter


def parse(filter_s):
    state = ParseState(filter_s)

    state.expect(BEGIN_EXPR)

    filter = _parse(state)

    state.expect(END_EXPR)

    if (len(state) > 0):
        raise ParseError(state, "Data after end of expression")
    return filter


def escape(in_str):
    out_str = ""
    for in_c in in_str:
        if in_c in SPECIALS:
            out_str += ESCAPE
        out_str += in_c
    return out_str


class Filter:
    def __eq__(self, other):
        return str(self) == str(other)


class Comparison(Filter):
    def __init__(self, key, value):
        self.key = key
        self.value = value

    def match(self, service):
        values = service.get(self.key)
        if values is not None:
            for value in values:
                if self.compare(value):
                    return True
        return False

    def __str__(self):
        return "%s%s%s%s%s" % (BEGIN_EXPR, escape(self.key), self.op,
                               escape(str(self.value)), END_EXPR)


class Equal(Comparison):
    op = EQUAL

    def compare(self, value):
        if type(value) == str:
            return self.value == value
        else:
            return self.value == str(value)

    def __init__(self, key, value):
        Comparison.__init__(self, key, value)


class GreaterThan(Comparison):
    op = GREATER_THAN

    def compare(self, value):
        return type(value) == int and value > self.value

    def __init__(self, key, value):
        Comparison.__init__(self, key, value)


class LessThan(Comparison):
    op = LESS_THAN

    def compare(self, value):
        return type(value) == int and value < self.value

    def __init__(self, key, value):
        Comparison.__init__(self, key, value)


class Present(Filter):
    def __init__(self, key):
        self.key = key

    def match(self, service):
        return self.key in service

    def __str__(self):
        return "%s%s%s%s%s" % (BEGIN_EXPR, escape(self.key), EQUAL,
                               ANY, END_EXPR)


class Substring(Filter):
    def __init__(self, key, initial=None, intermediate=[], final=None):
        self.key = key
        self.initial = initial
        self.intermediate = intermediate
        self.final = final

        pattern = "^"
        if initial is not None:
            pattern += "%s.*" % re.escape(initial)
        else:
            pattern += ".*"

        for im in intermediate:
            pattern += "%s.*" % re.escape(im)

        if final is not None:
            pattern += re.escape(self.final)

        pattern += "$"

        self.substring_re = re.compile(pattern)

    def match(self, service):
        values = service.get(self.key)
        if values is not None:
            for value in values:
                if self.substring_re.search(value) is not None:
                    return True
        return False

    def __str__(self):
        s = "%s%s%s" % (BEGIN_EXPR, escape(self.key), EQUAL)
        if self.initial is not None:
            s += "%s%s" % (escape(self.initial), ANY)
        else:
            s += ANY
        for im in self.intermediate:
            s += "%s%s" % (escape(im), ANY)
        if self.final is not None:
            s += escape(self.final)
        s += END_EXPR
        return s


class Not(Filter):
    def __init__(self, operand):
        self.operand = operand

    def match(self, service):
        return not self.operand.match(service)

    def __str__(self):
        return "%s%s%s%s" % (BEGIN_EXPR, NOT, str(self.operand), END_EXPR)


class CompositeFilter:
    def __init__(self, *operands):
        self.operands = operands
        assert len(operands) >= 2

    def __str__(self):
        s = "%s%s" % (BEGIN_EXPR, self.op)
        for operand in self.operands:
            s += str(operand)
        s += END_EXPR
        return s


class And(CompositeFilter):
    op = AND

    def match(self, service):
        for filter in self.operands:
            if not filter.match(service):
                return False
        return True


class Or(CompositeFilter):
    op = OR

    def match(self, service):
        for filter in self.operands:
            if filter.match(service):
                return True
        return False
