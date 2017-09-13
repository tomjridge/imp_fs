SHELL:=/bin/bash
BASH_ENV=bash_env.sh
export BASH_ENV

# shouldn't shell builtin source bash_env anyway?
libname:=$(shell source bash_env.sh && echo $${libname})#
# $(info libname is $(libname)!)

# ----------------------------------------

all: _links _depend
	$$ocamlc -c $$mls
	$$ocamlopt -c $$mls
#	mk_cma
#	mk_cmxa
#	$(MAKE) $$natives $$bytes


_links:
	mk_links
	touch $@

_mlis: 
	mk_mlis

install:
	mk_meta
	-remove
	install

_remove: 
	remove

_reinstall: 
	remove
	install

# README.html: README.adoc
# 	asciidoctor $<

_depend:
	mkdir -p _depend
	mk_depend

_codetags:
	mk_codetags

_doc:
	echo Make sure to build first
	mk_doc
	cp *.html *.css ../docs

clean:
	clean
	rm -f *.html *.css

real_clean: clean
	rm_links
	rm -rf _links _depend

FORCE:


