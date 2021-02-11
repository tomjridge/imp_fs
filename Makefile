default: all

-include Makefile.ocaml

all:: 
	$(DUNE) build bin/v1_main.exe
	$(DUNE) build bin/v2_main.exe
	$(DUNE) build bin/util_main.exe
	cp _build/default/bin/v1_main.exe .
	cp _build/default/bin/v2_main.exe .
	cp _build/default/bin/util_main.exe .

# find _build -name "v2_main.exe" -exec cp \{\} . \;


# FIXME why -o sync_read in the following?
FUSE_OPTIONS:=-s -f -o auto_unmount -o sync_read -o debug
# NOTE following may have to end in slash
FUSE_MNT_PT:=./fuse_mount/

-include Makefile.local # put your modifications here eg no debug flag

update_generated_doc::
	cd src/common && (ocamldoc_pyexpander fv2_types.ml)
	cd src/common && (ocamldoc_pyexpander file_impl_v2.ml)
	cd src/common && (ocamldoc_pyexpander usedlist_impl.ml)
	cd src/common && (ocamldoc_pyexpander dir_impl.ml)
	cd src/freelist && (ocamldoc_pyexpander freelist_intf.ml)
	cd src/freelist && (ocamldoc_pyexpander fl_summary.t.ml > fl_summary.ml)
	cd src/v1 && (ocamldoc_pyexpander v1_generic.ml)
	cd src && (ocamldoc_pyexpander summary.t.ml > summary.ml)

run_v1:
	test -d tmp || { echo "Missing ./tmp directory"; exit -1; }
	test -d tmp/v1_files || { echo "Missing ./tmp/v1_files directory"; exit -1; }
	OCAMLRUNPARAM=b ./v1_main.exe $(FUSE_OPTIONS) $(FUSE_MNT_PT)  2>&1

# following for v2
run_create:
	test -d tmp || { echo "Missing ./tmp directory"; exit -1; }
	-cp tmp/v2.store /tmp/v2.store.`date +'%F_%X'`
	./v2_main.exe create  # create empty fs
	./v2_main.exe restore # check we can restore

# run from clean state
run_from_clean:
	test -d tmp || { echo "Missing ./tmp directory"; exit -1; }
	./v2_main.exe create # create empty fs
	$(MAKE) run_from_existing

run_from_existing:
	test -d tmp || { echo "Missing ./tmp directory"; exit -1; }
	OCAMLRUNPARAM=b ./v2_main.exe $(FUSE_OPTIONS) $(FUSE_MNT_PT)  2>&1

run_tests:
	$(DUNE) build src-test/test.exe
	$(DUNE) exec src-test/test.exe

test_sqlite_dir: 
	./util_main.exe test sqlite_dir

test_sqlite_gom: 
	./util_main.exe test sqlite_gom

# for auto-completion of Makefile target
clean::
	rm -f *.exe
	rm -f src/*/GEN*.ml_
