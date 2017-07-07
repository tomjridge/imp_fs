PKGS:=tjr_btree,tjr_lib,ppx_poly_record,Fuse,extunix

mls:=entry.ml object_map.ml path_resolution.ml imp_fs.ml # imp_fuse.ml

all: link
	ocamlfind ocamlc -thread -package $(PKGS) $(mls)

link:
	ln -s b_*/* c_*/* d_*/* .
	touch $@

.depend: FORCE
	ocamlfind ocamldep -package $(PKGS) $(mls) >$@

clean:
	rm -f *.{cmo,cmi,cmx} .depend

FORCE:
