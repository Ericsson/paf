PYTHON=python3

XCMCERTNAMES=server client0 client1 client2
XCMCERTDIRS=$(foreach name,$(XCMCERTNAMES), test/cert/cert-$(name))
XCMCERTS=$(foreach dir,$(XCMCERTDIRS), $(dir)/cert.pem)

MANSRC=doc/man/pafd.8.md doc/man/pafc.1.md
MANHTML=$(patsubst %.md,%.html,$(MANSRC))
MANROFF=$(patsubst %.md,%,$(MANSRC))

all: build

.PHONY: build
build:
	$(PYTHON) setup.py build

install:
	if [ -n "$(PREFIX)" ]; then \
		args="--prefix $(PREFIX)"; \
	fi; \
	$(PYTHON) setup.py install $$args

flake8:
	$(PYTHON) -m flake8 paf test app/pafd app/pafc app/pafbench

check: testcert flake8
	export PYTHONPATH=$(PWD):$$PYTHONPATH && \
	export PATH=$(PWD)/app:$$PATH && \
	cd test && \
	py.test-3 -vv $(TESTOPTS) -s $(TESTS)

testcert: .testcert

.testcert: test/cert.yaml
	./test/gencert.py < test/cert.yaml && touch .testcert

doc: $(MANHTML) $(MANROFF)

$(MANHTML) $(MANROFF): $(MANSRC)
	ronn $(MANSRC)

count:
	@echo "Server:"
	@wc -l `ls -1 paf/*.py| grep -v client | grep -v xcm`
	@echo "Client [Python]:"
	@wc -l paf/client.py
	@echo "Test:"
	@wc -l test/*.py
	@echo "Applications:"
	@wc -l `git ls-files app`

clean:
	for d in paf app test; do \
		rm -f $${d}/*.pyc; \
		rm -rf $${d}/__pycache__; \
	done
	rm -f test/domains.d/*
	rm -f test/test-pafd.conf
	rm -f .testcert
	rm -rf test/cert
	rm -f $(MANHTML) $(MANROFF)
	rm -rf build
	rm -rf dist
