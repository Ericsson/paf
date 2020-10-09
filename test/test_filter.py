# SPDX-License-Identifier: BSD-3-Clause
# Copyright(c) 2020 Ericsson AB

import pytest
import paf.filter as filter

def parse_verify(filter_s):
    f = filter.parse(filter_s)
    assert str(f) == filter_s
    return f

def test_presence():
    assert parse_verify('(key=*)').match({ 'key': { 'value' } })
    assert parse_verify('(key=*)').match({ 'key': { 'value0', 'value1' } })
    assert not parse_verify('(key=*)').match({ 'key1': { 'value1' } })

def test_equal():
    assert parse_verify('(key=value)').match({ 'key': { 'value' } })
    assert not parse_verify('(key=value)').match({ 'key': { 'not-value' } })
    assert parse_verify('(k\\)ey=va\\=lue)').match({ 'k)ey': { 'va=lue' } })
    assert parse_verify('(key=\\\\)').match({'key': { '\\' }})

def test_greater_than():
    assert not parse_verify('(key>42)').match({ 'foo': { 4711 } })
    assert not parse_verify('(key>42)').match({ 'key': { 'value' } })
    assert not parse_verify('(key>42)').match({ 'key': { 17 } })
    assert not parse_verify('(key>42)').match({ 'key': { 42 } })
    assert parse_verify('(key>42)').match({ 'key': { 99 } })
    assert parse_verify('(key>42)').match({ 'key': { 1, 2, 3, 99, 4 } })
    assert parse_verify('(|(key>42)(key=42))').match({ 'key': { 42 } })
    assert parse_verify('(key>-42)').match({ 'key': { -17 } })

def test_less_than():
    assert not parse_verify('(key<42)').match({ 'foo': { 4711 } })
    assert not parse_verify('(key<42)').match({ 'key': { 'value' } })
    assert parse_verify('(key<42)').match({ 'key': { 17 } })
    assert not parse_verify('(key<42)').match({ 'key': { 42 } })
    assert not parse_verify('(key<42)').match({ 'key': { 99 } })
    assert parse_verify('(key<42)').match({ 'key': { 99, 1 } })
    assert parse_verify('(key<42)').match({ 'key': { 1, 2, 3, 4 } })
    assert parse_verify('(|(key<42)(key=42))').match({ 'key': { 42 } })
    assert parse_verify('(key<-42)').match({ 'key': { -99 } })

def test_substring():
    assert parse_verify('(key=v*e)').match({ 'key': { 'value' } })
    assert not parse_verify('(key=v*e)').match({ 'key1': { 'value' } })
    assert parse_verify('(key=v*e*)').match({ 'key': { 'value' } })
    assert parse_verify('(key=*v*e*)').match({ 'key': { 'value' } })
    assert not parse_verify('(key=*v*e*)').match({ 'key': { 'calue' } })
    assert not parse_verify('(key=a*)').match({ 'key': { 'value' } })
    assert parse_verify('(key=foo.*)').match({ 'key': { 'foo.txt' } })
    assert parse_verify('(key=f[oo.*)').match({ 'key': { 'f[oo.txt' } })

def test_equal_multivalue():
    assert parse_verify('(key=value)').match({ 'key': { 'value', 99 } })
    assert parse_verify('(key=99)').match({ 'key': { 'value', 99 } })

def test_not():
    assert not parse_verify('(!(key=value))').match({ 'key': { 'value' } })
    assert parse_verify('(!(!(key=value)))').match({ 'key': { 'value' } })
    assert not parse_verify('(!(!(key=value)))').match({ 'key':
                                                         { 'not-value' }})
    
def test_and():
    assert parse_verify('(&(key0=value0)(key1=*))').match({
        'key0': { 'value0' },
        'key1': { 'value1' },
        'key2': { 'value2' }
    })
    assert not parse_verify('(&(key0=value0)(key1=*))').match({
        'key1': { 'value1' },
        'key2': { 'value2' }
    })
    assert parse_verify('(&(key0=value0)(key1=value1)(key2=value2))').match({
        'key0': { 'value0' },
        'key1': { 'value1' },
        'key2': { 'value2' }
    })
    assert not parse_verify('(&(key0=value0)(key1=value1)'
                            '(key2=value2))').match({
                                'key0': { 'value0' },
                                'key1': { 'not-value1' },
                                'key2': { 'value2' }
                            })
    assert not parse_verify('(&(key0=value0)(key1=value1)'
                            '(key2=value2))').match({
                                'key0': { 'value0' },
                                'key2': { 'value2' },
                                'key3': { 'value3' }
                            })
    assert parse_verify('(&(key>5)(key<10))').match({
        'key': { 7 }
    })
    assert not parse_verify('(&(key>5)(key<10))').match({
        'key': { 10 }
    })

def test_or():
    assert parse_verify('(|(key0=value0)(key1=*))').match({
        'key0': { 'value0' },
        'key1': { 'value1' },
        'key2': { 'value2' },
    })
    assert parse_verify('(|(key0=value0)(key1=value1))').match({
        'key0': { 'value0' }
    })
    assert parse_verify('(|(key0=value0)(key1=value1))').match({
        'key1': { 'value1' }
    })
    assert not parse_verify('(|(key0=value0)(key1=value1))').match({
        'key0': { 'not-value0' }
    })
    assert not parse_verify('(|(key0=value0)(key1=*))').match({})

def test_complex():
    f = parse_verify('(&(key0=value0)(!(|(key1=value1)(key2=value2))))')
    assert f.match({ 'key0': { 'value0' } })
    assert f.match({ 'key0': { 'value0' }, 'key1': { 'not-value1' } })
    assert not f.match({ 'key0': { 'value0' }, 'key1': { 'value1' }})
    assert not f.match({})

    f = parse_verify('(|(key0=*)(&(key1=value1)(key2=value2)))')
    assert f.match({ 'key0': { 'value0' } })
    assert f.match({ 'key0': { 'value0' },  'key1': {'value1' } })
    assert f.match({
        'key0': { 'value0' },
        'key1': { 'value1' },
        'key2': { 'value2' }
    })
    assert f.match({
        'key0': { 'not-value0' },
        'key1': { 'value1' },
        'key2': { 'value2' }
    })
    assert f.match({
        'key1': { 'value1' },
        'key2': { 'value2' }
    })
    assert not f.match({
        'key1': { 'value1'}
    })
    assert not f.match({})

def test_malformed_filters():
    malformed_filters = [
        '(key=)',
        '(=value)',
        '(key)',
        '(&(key0=value0))',
        '(%(key0=value0))',
        '(|(key0=value0)(key1=value1)',
        '((|(key0=value0)(key1=value1))',
        '(&(key0=value0)(key1=value1))(key2=value2)',
        '(name=invalid\\aquote)',
        '(num>foo)',
        '(num< 99)',
        '(num< 99)',
        '(num>)',
        '(>9)',
        '(num<99.99)'
    ]
    for tf in malformed_filters:
        with pytest.raises(filter.ParseError):
            filter.parse(tf)

def test_escape():
    assert filter.escape('foo') == 'foo'
    assert filter.escape('foo\\bar') == 'foo\\\\bar'
    assert filter.escape('foo*') == 'foo\\*'
    assert filter.escape('fo(o)') == 'fo\\(o\\)'
    assert filter.escape('') == ''
    assert filter.escape('=foo') == '\\=foo'
    assert filter.escape('fo<o') == 'fo\\<o'
    assert filter.escape('>foo') == '\\>foo'
