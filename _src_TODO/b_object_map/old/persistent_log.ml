(** A persistent log, used to reduce traffic via the B-tree. NOTE
    append-only logs are quickest when data must be stored persistently *)

open Tjr_fs_shared.Monad
open Imp_pervasives
open X.Block


(* we construct on top of a persistent_chunked_list *)

module Pcl = Persistent_chunked_list
open Pcl


(* we need a concrete representation of actions; these are the
   elements that get written to disk *)


(* FIXME need insert_many *)
type ('k,'v) op = Insert of 'k * 'v | Delete of 'k

(* we have to decide what information we need to keep for the "current
   chunk" *)

(* FIXME this doesn't work; why?
type ('k,'v,'repr) chunk_state = (('k,'v)op,'repr) P.state = {
  elts: (('k,'v) op) list;
  elts_repr: 'repr
}

*)

type ('k,'v,'repr) chunk_state = (('k,'v)op,'repr) pcl_state

(* we have to fix on a representation for the list of ops; for the
   time being, let's just keep this abstract; eventually it will be
   bytes *)

module Repr : sig 
  type ('k,'v) repr
  val repr_ops : ( ('k,'v)op, ('k,'v)repr) repr_ops
end = struct
  type ('k,'v) repr = ('k,'v) op  list
  let repr_ops = {
    nil=[];
    snoc=(fun e es -> if List.length es < 10 then `Ok (es@[e]) else `Error_too_large);
(*    wf_size=(fun es -> List.length es < 10);  (* FIXME *) *)
  }
end



(* test  ---------------------------------------------------------- *)

(* now we want to use the Persistent_chunked_list, which rests on
   persistent_list *)


module Test : sig end = struct

  module T = Persistent_list.Test

  let list_ops = T.ops ()

  let repr_ops = Repr.repr_ops

  let read_state = T.read_state

  let chunked_list = make_persistent_chunked_list ~list_ops ~repr_ops ~read_state

  let _ = chunked_list
end




(* old -------------------------------------------------------------- *)

(*

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
*)
