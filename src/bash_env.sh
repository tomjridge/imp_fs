set -a # export all vars
# set -x # debug

libname=imp_fs
Libname=Imp_fs
src_subdirs=`echo {b,c,d}_*`
mls_in_subdirs=`ls {b,c,d}_*/*.ml`
meta_description="ImpFS, a modern filesystem with some interesting features."

required_packages="tjr_btree,tjr_lib,ppx_poly_record,Fuse,extunix,core,bin_prot,ppx_bin_prot"

natives=""
bytes=""

source bash_env.common



