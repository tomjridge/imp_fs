set -a # export all vars
# set -x # debug

libname=imp_fs
Libname=Imp_fs

# for depend
src_subdirs=`echo {a,b,c,d}_*`
src_subdirs=`echo {a,am,b}_*`

mls_in_subdirs=`ls {a,b,c,d}_*/*.ml`
mls_in_subdirs=`ls {a,am,b}_*/*.ml`


meta_description="ImpFS, a modern filesystem with some interesting features."

required_packages="tjr_btree,tjr_lib,ppx_poly_record,Fuse,extunix,core,bin_prot,ppx_bin_prot"

natives=""
bytes=""


# common ---------------------------------------------------------------



# set these env vars before including the file
function check_env_vars () {
    # http://stackoverflow.com/questions/31164284/shell-script-exiting-script-if-variable-is-null-or-empty
    : ${libname?Need a value}
    : ${Libname?Need a value}
    : ${src_subdirs?Need a value}
    : ${mls_in_subdirs?Need a value}
    : ${meta_description?Need a value}
    : ${required_packages?Need a value}
}
check_env_vars

PKGS="-package $required_packages"


# root=$(realpath $(dirname $BASH_SOURCE))/../..
# 
#  # if using nix, this may not be present
# test -f $root/config.sh && source $root/config.sh


SYNTAX="" # "-syntax camlp4o" # simplify: use for every file
FLGS="-g -thread -bin-annot" 
INLINE="-inline 0" # inline 0 for debugging native
FOR_PACK="-for-pack $Libname"

    # 8~"pattern-matching is not exhaustive"; 
    # 11~"this match case is unused";
    # 20~argument will not be used FIXME re-enable this
    # 26~"unused variable s2"
    # 40~It is not visible in the current scope, and will not be selected if the type becomes unknown.
WARN="-w @f@p@u@s@40-8-11-26-40-20"

    # these include syntax, so should work on all files; may be
    # overridden in ocamlc.sh; FIXME don't need -bin-annot twice
  ocamlc="$DISABLE_BYTE ocamlfind ocamlc   -bin-annot         $FLGS $FOR_PACK $WARN $PKGS $SYNTAX"
ocamlopt="$DISABLE_NTVE ocamlfind ocamlopt -bin-annot $INLINE $FLGS $FOR_PACK $WARN $PKGS $SYNTAX"
ocamldep="ocamlfind ocamldep $PKGS"


# mls ----------------------------------------

mls=`test -e depend/xxx && cat depend/*`

cmos="${mls//.ml/.cmo}"
cmxs="${mls//.ml/.cmx}"


# cma,cmxa -------------------------------------------------------------

function mk_cma() {
         # NOTE -bin-annot
	$DISABLE_BYTE ocamlfind ocamlc -bin-annot -pack -o $libname.cmo $cmos
  $DISABLE_BYTE ocamlfind ocamlc -g -a -o $libname.cma $libname.cmo
}

function mk_cmxa() {
	$DISABLE_NTVE ocamlfind ocamlopt -pack -o $libname.cmx $cmxs
  $DISABLE_NTVE ocamlfind ocamlopt -g -a -o $libname.cmxa $libname.cmx
}


# depend ----------------------------------------

function mk_depend() {
    mkdir -p depend
    for f in ${src_subdirs}; do
        (cd $f && ocamldep -one-line -sort *.ml > ../depend/$f)
    done
    touch depend/xxx
}



# links ----------------------------------------

function init() {
    link_files="${mls_in_subdirs}"
}

function mk_links() {
    echo "mk_links..."
    init
    ln -s $link_files .
}


function rm_links() {
    echo "rm_links..."
    init
    for f in $link_files; do rm -f `basename $f`; done
}


# mlis ----------------------------------------

function mk_mlis() {
    echo "mk_mlis..."
    for f in $mls_in_subdirs; do $ocamlc -i $f > tmp/${f/.ml/.mli}; done
}



# meta ----------------------------------------

function mk_meta() {
local gv=`git rev-parse HEAD`
local d=`date`
cat >META <<EOF
name="$libname"
description="$meta_description"
version="$d $gv"
requires="$required_packages"
archive(byte)="$libname.cma"
archive(native)="$libname.cmxa"
EOF

}


# codetags ----------------------------------------

function mk_codetags() {
    init # link_files
    for f in XXX TODO FIXME NOTE QQQ; do     # order of severity
        grep --line-number $f $link_files || true; 
    done
}


# doc ----------------------------------------------------

function mk_doc() {
    ocamlfind ocamldoc -short-paths $PKGS $WARN -html `cat depend/*`
}


# clean ----------------------------------------------------------------

function clean() {
	rm -f *.{cmi,cmo,cmx,o,cmt} a.out *.cma *.cmxa *.a *.byte *.native
}

# ocamlfind install, remove, reinstall --------------------

function install() {
    # assumes packing
	  ocamlfind install $libname META $libname.{cmi,cmo,cma,cmx,cmxa,a,cmt} *.cmt *.ml
    # FIXME how to install cmt file for libname?
}

function remove() {
    ocamlfind remove $libname
}
