(** A mini version of ImpFS (imperative) *)

module M = Tjr_monad


type monadic_t

type 'a mm = ('a,monadic_t) M.m

let return (_x:'a) : 'a mm = failwith ""

(* open Write_back_cache *)
(* type ('k,'v) wbc *)

type lock

type ('k,'v)map

type blk_id

(* avoid making an explict record for this *)
(* open Tjr_lru.Lru_ops *)

type did
type id

type op = Add of id | Del of id 

type dinode = {
  ptr_to_btree_root_and_pcache_root: unit (* FIXME *)
}


(* FIXME from here 

(** {2 Dirs} *)


(** Type for the state of a particular directory; we use the
   convention that m is the in-memory state, and d is the on-disk
   state. *)
module Dir = struct

  let per_dir_wbc_capacity = 10

  module K = struct type t = string let compare: t -> t -> int = Stdlib.compare end
  module V = struct type t = op end

  module Wbc = Wbc_2.Make(K)(V)

  type entries_m = Wbc.t

  let wbc_ops : (string,op,entries_m) Wbc.wbc_ops = Wbc.wbc_ops

  let empty_wbc: entries_m = wbc_ops.empty per_dir_wbc_capacity
                                     


  (** sync_did_blk_map is typically a completed promise; it is wrapped
     in lazy to avoid syncing directories that are never used *)
  type m = { 
    did_m            : did;
    sync_did_blk_map : (unit mm) Lazy.t;
    entries_m        : entries_m;
  }

  module String_map = Tjr_map.Make_map_ops(
    struct type t = string let compare: t -> t -> int = String.compare end)
  type entries_d = id String_map.t
  let entries_ops : (string,id,entries_d)Tjr_map.map_ops = String_map.map_ops

  type d = {
    did_d     : did;
    entries_d : entries_d;
  }

  let merge_entries: entries_m -> entries_d -> entries_d = failwith "FIXME"

  let sync (m,d) =
    (* NOTE this needs to be in the monad *)
    let _ = m.sync_did_blk_map in 
    (* FIXME here we need to ensure that did_synced_in_did_blk_map is true *)
    let m' = { m with sync_did_blk_map=lazy(return ()); entries_m=empty_wbc} in
    let d' = { d with entries_d = merge_entries m.entries_m d.entries_d } in
    (m',d')

  let sync_1 _name (m,_d) = 
    let _ = m.sync_did_blk_map in
    failwith "FIXME"
end

let did_to_dir _did : (Dir.m,'t)m = failwith ""




(** {2 Dep map} *)

(** The dep map is a map of entries: from -> to, corresponding to a
   rename. When we flush from, we first flush to. If we rename to to
   to', we need to update the map so that from -> to' *)

module Dep_map = struct

  (** We need to be able to add an entry on a rename; we also need to
     be able to alter an entry if we rename the dst/f again. This
     means that we need to index by the dst,id pair. Obviously we also
     need to lock this for concurrent use. FIXME we may need to lock
     multiple objects simultaneously. 

      NOTE that in an entry (from,to) we expect the id to be the same
  *)
  type to_  = { dst:did; dst_name:string; id:id }
  type from = { src:did; src_name:string; id:id }
           
  (** An LRU of identifiers; we promote an id whenever we deal with it; overkill? *)
  module Lru_ = Tjr_lru.Make_lru(struct type t = id let compare: t -> t -> int = Pervasives.compare end)(struct type t = unit end)
  let lru_ops = Lru_.lru_ops

  (** A map from from<->to *)
  module B = Bimap.Make_bimap(struct type t = from let compare = Pervasives.compare end)(struct type t = to_ let compare = Pervasives.compare end)
  let bimap_ops = B.bimap_ops

  (** The dependency map; an LRU which tracks from<->to entries *)
  type dep_map = {
    lru: Lru_.t;
    bimap: B.t
  }

  type t = dep_map

  type with_dep_map = (t,monadic_t) M.with_state


  (** Functionally update a dependency map.

sync_did: if we need to sync a directory to disk, we also need to
     sync_1 any dependencies; this returns the list of dependencies
     for a given directory *)
  type dep_map_ops = {
    rename     : from:from -> to_:to_ -> t -> t;
    maybe_trim : t -> (from*to_)list * t;  (* FIXME we should trim a particular id? *)
    sync_did   : did -> t -> (from*to_)list * t; 
  }

  (* FIXME actually, we are not working with a bimap, since rename x
     y, rename x' y overwrites the dependence on x y; so we are
     working with a function where we can go from a y to the
     corresponding x; an invertible function rather than a relation *)
(*
  let rename ~from ~to_ t = 
    (* lookup to see if there is a dependency for xxx->from already *)
    bimap_ops.find_y to_ t |> function
    | None -> (
        (* just add to t *)
        bimap_ops.add (from,to_) t )
    | Some (from',to') -> 
*)   

  
end




(** {2 Did_dir map} *)

(** This is to ensure that there is only one Dir.m for each did; we
   use "with_dir did" to take the relevant dir, and release it after
   updates. This is an LRU which gets filled on demand. Since this is
   purely in-memory, there is no need to sync anything. *)
module Did_dir_map = struct
  type ops = With_did
  type m = (did,Dir.m * lock) map 
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
  type entries
  type ('a,'b,'c) wbc_ops
  let wbc_ops: (did,blk_id,entries) wbc_ops = failwith ""
  type m = {
    entries : entries;
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



(*

(** A dependency: we flush after an addition has been performed on another directory *)
type dep = Sync_after_add of { 
    did  : did; 
    name : string;
    id   : id
  }

(* type timestamp *)

*)
*)
