AC_INIT(paf, 0.1, [mattias.ronnblom@ericsson.com])

AM_INIT_AUTOMAKE([foreign])

AC_ARG_ENABLE([python2],
    AS_HELP_STRING([--enable-python2], [use Python 2]))

AC_ARG_VAR(FORCE_PYTHON)
AS_IF([test "x$FORCE_PYTHON" = "x"],
	    [AS_IF([test "x$enable_python2" = "xyes"], [AM_PATH_PYTHON([2.7])],
	          [AM_PATH_PYTHON(3.5)])],
	    [AC_SUBST(PYTHON, $FORCE_PYTHON)])

AM_CONDITIONAL([PYTHON2], [test "x$enable_python2" = "xyes"])

AC_CONFIG_FILES([Makefile])
AC_CONFIG_FILES([app/pafd], [chmod a+x app/pafd])
AC_CONFIG_FILES([app/pafc], [chmod a+x app/pafc])
AC_CONFIG_FILES([app/pafbench], [chmod a+x app/pafbench])

AC_OUTPUT
