bin_SCRIPTS = app/pafd app/pafc app/pafbench

pkgpython_PYTHON = paf/__init__.py paf/client.py paf/filter.py paf/props.py \
	paf/sd.py paf/xcm.py paf/eventloop.py paf/proto.py paf/server.py \
	paf/compat.py

UMLSRC=doc/pub_client_long_disconnect.plantuml \
	doc/pub_client_short_disconnect.plantuml \
	doc/sub_client_long_disconnect.plantuml \
	doc/sub_client_short_disconnect.plantuml \
	doc/server_restart.plantuml
UMLPNG=$(patsubst %.plantuml,%.png,$(UMLSRC))

if PYTHON2
PYTEST=py.test
else
PYTEST=py.test-3
endif

all: check

check:
	export PYTHONPATH=$(PWD):$$PYTHONPATH && \
	export PATH=$(PWD)/app:$$PATH && \
	cd test && \
	$(PYTEST) -vv -s $(TESTS)

.PHONY: doc
doc: $(UMLPNG)
%.png: %.plantuml
	plantuml $<

count:
	@echo "Server:"
	@wc -l `ls -1 paf/*.py| grep -v client | grep -v xcm`
	@echo "Client [Python]:"
	@wc -l paf/client.py
	@echo "Test:"
	@wc -l test/*.py
	@echo "Applications:"
	@wc -l `git ls-files app`
clean-local:
	for d in paf app test; do \
		rm -f $${d}/*.pyc; \
		rm -rf $${d}/__pycache__; \
	done
	rm -rf .cache
	rm -f test/domains.d/*
	rm -f $(UMLPNG)
distclean-local: clean-local
	find . -name '*~' -print0 | xargs -0 rm -f
	rm -f missing install-sh missing configure aclocal.m4 Makefile.in \
		py-compile
	rm -rf autom4te.cache
