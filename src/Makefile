SHELL:=/bin/bash
BASH_ENV=bash_env.sh
export BASH_ENV

# shouldn't shell builtin source bash_env anyway?
libname:=$(shell source bash_env.sh && echo $${libname})#
# $(info libname is $(libname)!)

# ----------------------------------------

all: links depend
	$$ocamlc -c $$mls
	$$ocamlopt -c $$mls
	mk_cma
	mk_cmxa
#	$(MAKE) $$natives $$bytes


links:
	mk_links
	touch $@

mlis: 
	mk_mlis

install:
	mk_meta
	-remove
	install

remove: 
	remove

reinstall: 
	remove
	install

# README.html: README.adoc
# 	asciidoctor $<

depend:
	mkdir -p depend
	mk_depend

codetags:
	mk_codetags

doc:
	echo Make sure to build first
	mk_doc
	cp *.html *.css ../docs

clean:
	clean
	rm -f *.html *.css

real_clean: clean
	rm_links
	rm -rf links depend

FORCE:


