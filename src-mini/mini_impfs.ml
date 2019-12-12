(** A mini version of ImpFS (imperative) *)

module M = Tjr_monad

(* open Write_back_cache *)
type ('k,'v) wbc

type lock

type ('k,'v)map

type blk_id
(* type timestamp *)
(* avoid making an explict record for this *)
type ('k,'v) lru = (module Lru.F.S with type k='k and type v='v)

type did
type id
type dep

type op (* add, del *)

type dinode = {
  ptr_to_btree_root_and_pcache_root: unit (* FIXME *)
}

(** Type for the state of a particular directory; we use the
   convention that m is the in-memory state, and d is the on-disk
   state. *)
module Dir = struct
  type entries_m = (string,op*dep option) wbc

  (** The first field records whether the did has been synced to disk
     in the did_blk_map; once synced, it never changes; this allows us
     to avoid many potential syncs to the did_blk_map if we already
     know the entry is synced. *)
  type m = { 
    did_m                     : did;
    did_synced_in_did_blk_map : bool;
    (* dinode_blk_id             : blk_id; *)
    (* dinode                    : dinode; *)
    entries_m                 : entries_m;
    dummy: unit;
  }

  type entries_d = (string,id)map

  type d = {
    did_d           : did;
    entries_d       : entries_d;
    (* dinode_blk_id_d : blk_id; *)
    (* dinode_d        : dinode; *)
    dummy: unit;
  }

  let empty_wbc: (string,op*dep option) wbc = failwith "FIXME"

  (* type ops = Sync | Sync_1 of string *)

  let merge_entries: entries_m -> entries_d -> entries_d = failwith "FIXME"

  let maybe_sync_did _did : (unit,'t)M.m = failwith "FIXME"

  let sync (m,d) =
    let _ = maybe_sync_did m.did_m in
    (* FIXME here we need to ensure that did_synced_in_did_blk_map is true *)
    let m' = { m with did_synced_in_did_blk_map=true; entries_m=empty_wbc} in
    let d' = { d with entries_d = merge_entries m.entries_m d.entries_d } in
    (m',d')

  let sync_1 _name (m,_d) = 
    let _ = maybe_sync_did m.did_m in
    failwith "FIXME"
end

let did_to_dir _did : (Dir.m,'t)m = failwith ""

(** This is to ensure that there is only one Dir.m for each did; we
   use "with_dir did" to take the relevant dir, and release it after
   updates. This is an LRU which gets filled on demand. Since this is
   purely in-memory, there is no need to sync anything. *)
module Did_dir_map = struct
  type ops = With_did
  type m = (did,Dir.m * lock) map  (* the bool is the lock *)
end

(** A map from did to the blk_id for the corresponding dinode. Once
   created, entries are never modified; however, we need a sync
   operation to ensure that the did->blk_id entry is on disk before we
   link a new dir into its parent. We use "sync_1(did)" and record the
   sync status in the dir object (to avoid repeated unnecessary
   syncs).

   We can also assume that this map has its root recorded in blk 0, so
   we don't need a root manager.
*)
module Did_blk_map = struct
  type ops = Sync | Sync_1 of did
  type m = {
    entries : (did,blk_id)wbc;
  }
  type d = {
    entries_d : (did,blk_id)map;
  }
end

  

module Disk_state = struct
  type t = {
    did_blk_map: (did,blk_id)map;
  }    
    
  
end
