PKGS:=tjr_btree,tjr_lib,ppx_poly_record,Fuse,extunix

all:
	ocamlfind ocamlc -thread -package $(PKGS) object_map.ml path_resolution.ml imp_fs.ml imp_fuse.ml

clean:
	rm -f *.{cmo,cim,cmx}
