#!/bin/bash

# build all dependent projects; assumes these live in ..

# NOTE this should match the relevant Dockerfile; FIXME move Dockerfile here?

DST=..

for f in isa_btree tjr_monad tjr_lib tjr_fs_shared tjr_pcache tjr_btree path_resolution tjr_net mini-fs; do
    make -C $DST/$f clean
    make -C $DST/$f 
done


