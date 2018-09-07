#!/bin/bash

set -e

f="a_tjr_lib
b_isa_btree
b_tjr_fs_shared
b_tjr_monad
c_path_resolution
c_tjr_btree
c_tjr_net
d_mini-fs
d_tjr_pcache"

for g in $f; do make -C $g; done
