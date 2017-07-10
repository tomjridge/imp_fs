open Tjr_btree
open Block

type state = {
  root: blk_id;
  free_oid: int;
}

let the_state = ref {root=0; free_oid=0 }
