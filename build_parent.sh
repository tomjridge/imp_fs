#!/bin/bash

# build all dependent projects; assumes these live in ..

# NOTE this should match the relevant Dockerfile; FIXME move Dockerfile here?

DST=..

for f in a_tjr_lib b_isa_btree b_tjr_fs_shared b_tjr_monad c_path_resolution c_tjr_btree c_tjr_net d_mini-fs d_tjr_pcache; do
    make -C $DST/$f clean 
    make -C $DST/$f || exit -1
done


