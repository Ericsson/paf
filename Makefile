PYTHON=python3

XCMCERTNAMES=server client0 client1 client2
XCMCERTDIRS=$(foreach name,$(XCMCERTNAMES), test/cert/cert-$(name))
XCMCERTS=$(foreach dir,$(XCMCERTDIRS), $(dir)/cert.pem)

UMLSRC=doc/pub_client_long_disconnect.plantuml \
	doc/pub_client_short_disconnect.plantuml \
	doc/sub_client_long_disconnect.plantuml \
	doc/sub_client_short_disconnect.plantuml \
	doc/server_restart.plantuml
UMLPNG=$(patsubst %.plantuml,%.png,$(UMLSRC))

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
	$(PYTHON) -m flake8 src test app/pafd app/pafc app/pafbench

check: cert flake8
	export PYTHONPATH=$(PWD):$$PYTHONPATH && \
	export PATH=$(PWD)/app:$$PATH && \
	cd test && \
	py.test-3 -vv $(TESTOPTS) -s $(TESTS)

test/cert/ca/ca-cert.pem: test/cert/cert.conf
	mkdir -p test/cert/ca
	openssl req \
		-x509 -newkey rsa:4096 -keyout test/cert/ca/ca-key.pem \
		-sha256 -days 1000 -nodes -subj '/CN=localhost' \
		-out test/cert/ca/ca-cert.pem

define cert_template
$(1)/cert.pem: test/cert/ca/ca-cert.pem test/cert/cert.conf
	mkdir -p $(1)
	openssl genrsa -out $(1)/key.pem 2048
	openssl req \
		-new -sha256 -key $(1)/key.pem \
		-config test/cert/cert.conf -out $(1)/cert.csr
	openssl x509 \
		-req -in $(1)/cert.csr \
		-CA test/cert/ca/ca-cert.pem \
		-CAkey test/cert/ca/ca-key.pem \
		-CAcreateserial -days 1000 -sha256 -extensions v3_req \
		-extfile test/cert/cert.conf -out $(1)/cert.pem
	openssl verify -CAfile test/cert/ca/ca-cert.pem \
		$(1)/cert.pem
	cp test/cert/ca/ca-cert.pem $(1)/tc.pem
	rm $(1)/cert.csr
endef

$(foreach dir,$(XCMCERTDIRS),$(eval $(call cert_template,$(dir))))

cert: $(XCMCERTS)

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

clean:
	for d in paf app test; do \
		rm -f $${d}/*.pyc; \
		rm -rf $${d}/__pycache__; \
	done
	rm -f test/domains.d/*
	rm -f test/test-pafd.conf
	rm -rf $(XCMCERTDIRS) test/cert/ca
	rm -f $(UMLPNG)
	rm -rf build
	rm -rf dist
