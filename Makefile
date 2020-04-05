TMP_DOC_DIR:=/tmp/tjr_impfs
scratch:=/tmp/l/github/scratch

default: all

-include Makefile.ocaml

all:: 
	$(DUNE) build bin/v1_main.exe
	find _build -name "v1_main.exe" -exec cp \{\} . \;


run:
	time $(DUNE) exec main

# for auto-completion of Makefile target
clean::
	rm *.exe
