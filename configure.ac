AC_INIT(paf, 0.1, [mattias.ronnblom@ericsson.com])

AM_INIT_AUTOMAKE([foreign])

AC_ARG_VAR(FORCE_PYTHON)
AS_IF([test "x$FORCE_PYTHON" = "x"],
	    [AM_PATH_PYTHON(3.5)],
	    [AC_SUBST(PYTHON, $FORCE_PYTHON)])

AC_CONFIG_FILES([Makefile])
AC_CONFIG_FILES([app/pafd], [chmod a+x app/pafd])
AC_CONFIG_FILES([app/pafc], [chmod a+x app/pafc])
AC_CONFIG_FILES([app/pafbench], [chmod a+x app/pafbench])

AC_OUTPUT
