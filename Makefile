TMP_DOC_DIR:=/tmp/tjr_impfs
scratch:=/tmp/l/github/scratch

default: all

-include Makefile.ocaml

all:: 
	$(DUNE) build bin/v1_main.exe
	find _build -name "v1_main.exe" -exec cp \{\} . \;


# FIXME why -o sync_read in the following?
FUSE_OPTIONS:=-s -f -o auto_unmount -o sync_read -o debug
# NOTE following may have to end in slash
FUSE_MNT_PT:=./fuse_mount/

-include Makefile.local # put your modifications here eg no debug flag

run_v1:
	./v1_main.exe $(FUSE_OPTIONS) $(FUSE_MNT_PT)  2>&1


# for auto-completion of Makefile target
clean::
	rm *.exe
