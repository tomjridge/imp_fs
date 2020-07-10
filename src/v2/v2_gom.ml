(** The "global object map" GOM

This is just an instance of Tjr_btree, mapping object id (which we
   identify with dir_entry) to blk_id 

FIXME this should use allocation via a usedlist

*)
open V2_intf

let blk_sz = Shared_ctxt.blk_sz 

module Dir_entry = struct
  open Bin_prot.Std
  type t = (int,int,int) dir_entry'[@@deriving bin_io]
  let max_sz = 9
end

module S = struct
  let k_mshlr : _ bp_mshlr = (module Dir_entry)
  type k = Dir_entry.t[@@deriving bin_io]
  type v = Shared_ctxt.r
  type r = Shared_ctxt.r
  type t = Shared_ctxt.t
  let k_cmp: k -> k -> int = Stdlib.compare
  let monad_ops = Shared_ctxt.monad_ops
  (* let k_mshlr = dir_entry_mshlr *)
  let v_mshlr = bp_mshlrs#r_mshlr
  let r_mshlr = bp_mshlrs#r_mshlr

  let k_size = let module X = (val k_mshlr) in X.max_sz
  let v_size = let module X = (val v_mshlr) in X.max_sz
  let cs = Tjr_btree.Bin_prot_marshalling.make_constants ~blk_sz ~k_size ~v_size
  let r_cmp = Shared_ctxt.r_cmp
end

module T = Tjr_btree.Make_6.Make_v2(S)

(** Use the uncached method from the following to implement the GOM *)
let gom_factory : (Dir_entry.t, S.v, S.r, S.t, T.leaf, T.node,
 (T.node, T.leaf) Isa_btree.dnode, T.ls, T.blk, T.wbc)
Tjr_btree.Make_6.btree_factory
= T.btree_factory

let write_empty_leaf = gom_factory#write_empty_leaf

(** Currently we used the uncached B-tree *)
let uncached = gom_factory#uncached

