(** A persistent log, used to reduce traffic via the B-tree. NOTE
    append-only logs are quickest when data must be stored persistently *)

open Tjr_fs_shared.Monad
open Imp_pervasives
open X.Block


type 't store_ops = {
  alloc_block: unit -> (X.Block.blk_id,'t) m;
  free_block: blk_id -> (unit,'t) m;
  write_block: blk_id -> blk -> (unit,'t) m;
  (* NOTE we don't need read_block except for debugging *)
  read_block: blk_id -> (blk,'t) m
}


(* NOTE we expect to have a value of type 'kv_map, and associated map
   operations from Tjr_map 

let kv_map : ('k,'v,'kv_map) Tjr_map.map_ops = ...

*)

(* the current kvs that are stored in the block *)
module Current_kvs = struct
  type ('k,'v,'kv_map) current_kvs = {
    kvs: ('k*'v) list;  (* most recent at front *)
    kvs_map: 'kv_map;
  }
end
open Current_kvs


module Partial_block = struct
  type partial_block = blk * int
  (* the int indicates how much of the blk is actually used *)
end
open Partial_block


type ('k,'v,'kv_map,'current) log_state = {
  current_blk_id: blk_id;
  current_blk: partial_block;
  current_kvs: ('k,'v,'kv_map) current_kvs;

  non_current_kvs_as_map: 'kv_map;
  non_current_size: unit -> int;  
  (* NOTE not in the monad - assumed to execute quickly *)
}  


(* record the kvs in the current block; probably store as list and map? *)
type ('k,'v,'kv_map,'current) current_ops = {
  (* FIXME from map? or just a record of those inserted? just a record
     of those inserted; most recent at front *)
  current_kvs: 'current -> ('k * 'v) list;  
  kv_map: 'current -> 'kv_map;
}  



(* the main operations for the persistent log *)

(* NOTE assume the log_state is accessible via mref from 't *)

type ('k,'v,'kv_map,'current,'t) plog_ops = {
  blk_sz: int;
  find: 'k -> 'v option;  (* in current or non-current; NOTE in-memory so no monad *)
  insert: 'k -> 'v -> (unit,'t) m;

  delete: 'k -> (unit,'t) m;  
  (* FIXME how to implement delete??? we need a concrete repn of operations *)

  kvs: unit -> 'kv_map;  (* both current and non; FIXME or just return a list ? *)

  (* this is the essential operation: we give up any knowledge of the
     non-current kvs *)
  detach: unit -> ('current,'t) m;
}

(*
let mk_persistent_log ~store_ops
*)
