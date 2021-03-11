(** This version focuses on the front end caching and locking, and
   uses SQLite as the backend for the metadata, and some other
   filesystem as a backend for file data. 

*)

[@@@warning "-33"]

open V3_abstract
open Tjr_monad.With_lwt

module Lru = Lru_two_gen

module Refs_with_dirty_flags = struct
  type 'a ref = {
    mutable value: 'a;
    mutable dirty: bool
  }

  (** values start off clean *)
  let ref value = {value;dirty=false}

  let is_dirty x = x.dirty

  let clean x = x.dirty <- false

  (** assigning sets the dirty flag *)
  let ( := ) r value = r.value <-value; r.dirty <- true

  let (!) r = r.value
end
open struct module R = Refs_with_dirty_flags end


module S0 (* : S0 *) = struct
  open Bin_prot.Std

  type t = lwt
  let monad_ops = lwt_monad_ops

  type fid = int[@@deriving bin_io]
  type did = int[@@deriving bin_io]
  type sid = int[@@deriving bin_io]

  type dh  = int[@@deriving bin_io]

  type tid = int[@@deriving bin_io]
end
open S0

module S1 = V3_abstract.S1(S0)
open S1


module S2 = V3_abstract.S2(S0)

(** What we have to implement *)
module type T2 = S2.T2

module T2 : T2 = struct

  let root_did = 0

  (* dir  - implemented using a 2-gen LRU *)

  let max_sz = 1000
  let default_cache_size = 1000

  module Dir = struct

    (* Directory entries cache *)
    module S1 = struct
      type k = str_256
      type v = dir_entry 
      type t = S0.t
      let monad_ops = monad_ops
    end
    module Entries_cache = Lru.With_explicit_cache.Make(S1)
    type entries_cache = Entries_cache.c

    type sqlite_dir_ops = (str_256,dir_entry,t,did) Sqlite_dir.dir_ops

    (* sqlite connection *)
    let sqlite_dir ~(did:did) : sqlite_dir_ops = failwith "FIXME"[@@warning "-27"]

    type per_dir = {
      lock           : unit;
      parent         : did R.ref;
      times          : times R.ref;
      entries_cache  : entries_cache;
      entries        : (str_256,dir_entry,t) Lru.Map_m.map_m;
      sqldir         : sqlite_dir_ops
    }

    let per_dir_cache : (did,per_dir)Hashtbl.t = Hashtbl.create default_cache_size

    (* Live directories *)
    module S0 = struct
      type k = did
      type v = per_dir
      type t = lwt
      let monad_ops = lwt_monad_ops
    end
    module Live_dirs = Lru.With_explicit_cache.Make(S0)
    let live_dirs = Live_dirs.create_initial_cache ~max_sz

    
    (* how to recover a directory from the lower layer *)
    let m3 : _ Lru.map_lower = {
      find_opt=fun did -> 
        sqlite_dir ~did |> fun sqldir -> 
        sqldir.Dv3.get_meta () >>= fun (parent,times) ->         
        Some {
          lock=();
          parent=R.ref parent;
          times=R.ref times;
          entries_cache=Entries_cache.create_initial_cache ~max_sz;
          entries=
            (*             (* if there is no entry in the entry cache, use sqlite *)
            let m3 : _ Lru.map_lower = { find_opt = fun k -> per_dir.sqldir.find k } in
            *)
            failwith "FIXME";
          sqldir
        } |> return
    }

    let live_dirs_find did = 
      Live_dirs.ops.find_opt ~m3 ~c:live_dirs did >>= function
      | None -> 
        failwith "impossible: did not recognized, but m3 always \
                  creates a new C2 value in the cache and \
                  returns from that"
      | Some per_dir -> return per_dir

    (* FIXME in following we need to take locks *)
    (* FIXME after each operation, may need a trim of live_dirs, and
       per-dir entries *)
    let dir_ops : dir_ops = { 
      find=(fun ~did s -> 
          live_dirs_find did >>= fun per_dir -> 
          per_dir.entries.find_opt s);
      insert=(fun ~did k v -> 
          live_dirs_find did >>= fun per_dir -> 
          per_dir.entries.insert k v);            
      delete=(fun ~did ~name ~obj:_FIXME ~reduce_obj_nlink:() -> 
          live_dirs_find did >>= fun per_dir -> 
          per_dir.entries.delete name);
      set_times=R.(fun ~did times -> 
          live_dirs_find did >>= fun per_dir -> 
          per_dir.times := times; 
          return ());
      get_times=R.(fun ~did -> 
          live_dirs_find did >>= fun per_dir -> 
          !(per_dir.times) |> return );
      set_parent=R.(fun ~did ~parent -> 
          live_dirs_find did >>= fun per_dir -> 
          per_dir.parent := parent; 
          return ());
      get_parent=R.(fun ~did -> 
          live_dirs_find did >>= fun per_dir -> 
          !(per_dir.parent) |> return);
      sync=failwith""
    }

    end


    let dir_cache : (did,dir_cache_entry)


    type k = did
    type v = (

  let flush_m2 

  let lru = Tjr_lib_core.Lru_two_gen.create_imperative ~monad_ops ~max_sz:100 ~flush_m2:

  let dir : dir_ops = 
    let open struct
      let find ~did name = failwith ""
    end
    in

end
