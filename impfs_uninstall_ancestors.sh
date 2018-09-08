#!/bin/bash

set -e

# d_tjr_pcache
# d_mini-fs
# c_tjr_net
# c_path_resolution

f="c_tjr_btree
b_tjr_monad
b_tjr_fs_shared
b_isa_btree
a_tjr_lib"

for g in $f; do make -C $g uninstall; done

