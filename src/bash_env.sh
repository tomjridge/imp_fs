set -a # export all vars
# set -x # debug

libname=imp_fs
Libname=Imp_fs
src_subdirs=`echo {a,b}_*`
#src_subdirs=`echo a_*`
mls_in_subdirs=`ls {a,b}_*/*.ml`
#mls_in_subdirs=`ls a_*/*.ml`
meta_description="ImpFS, a modern filesystem with some interesting features."

required_packages="tjr_btree,tjr_lib,ppx_poly_record,Fuse,extunix,core,bin_prot,ppx_bin_prot"

natives=""
bytes=""

source bash_env.common



