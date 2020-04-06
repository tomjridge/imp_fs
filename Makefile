TMP_DOC_DIR:=/tmp/tjr_impfs
scratch:=/tmp/l/github/scratch

default: all

-include Makefile.ocaml

all:: 
	$(DUNE) build bin/v1_main.exe
	find _build -name "v1_main.exe" -exec cp \{\} . \;


run_v1:
	./v1_main.exe -s -f -o auto_unmount -o sync_read -o debug fuse_mount/ 2>&1


# for auto-completion of Makefile target
clean::
	rm *.exe
