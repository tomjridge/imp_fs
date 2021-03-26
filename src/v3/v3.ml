(** This version focuses on the front end caching and locking, and
   uses SQLite as the backend for the metadata, and some other
   filesystem as a backend for file data. 

*)

[@@@warning "-33-27"]

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

module T2 (* : T2 *) = struct

  let root_did = 0

  (* dir  - implemented using a 2-gen LRU *)

  let max_sz = 1000
  let default_cache_size = 1000

  let db : Sqlite_dir.db = failwith ""

  (* FIXME locking, trimming *)
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

    type sql_dir_ops = (str_256,dir_entry,t,did) Sqlite_dir.dir_ops

    (* sqlite connection *)
    let sql_dir_ops : sql_dir_ops = failwith "FIXME"[@@warning "-27"]

    type per_dir = {
      lock           : unit;
      parent         : did R.ref;
      times          : times R.ref;
      entries_cache  : entries_cache;
      entries        : (str_256,dir_entry,t) Lru.Map_m.map_m;
      (* sqldir         : sql_dir_ops *)
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
        sql_dir_ops.get_meta ~did >>= fun (parent,times) ->         
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
      set_times=(fun ~did times -> 
          live_dirs_find did >>= fun per_dir -> 
          R.(per_dir.times := times); 
          return ());
      get_times=(fun ~did -> 
          live_dirs_find did >>= fun per_dir -> 
          R.(!(per_dir.times)) |> return);
      set_parent=(fun ~did ~parent -> 
          live_dirs_find did >>= fun per_dir -> 
          R.(per_dir.parent := parent); 
          return ());
      get_parent=R.(fun ~did -> 
          live_dirs_find did >>= fun per_dir -> 
          R.(!(per_dir.parent)) |> return);
      sync=failwith""
    }


    let max_did = ref (sql_dir_ops.max_did ()) 

    let dirs_ops : dirs_ops = {
      delete=(fun _did -> return ()); (* FIXME probably remove from underlying db *)
      create=(fun ~parent ~name ~times -> 
          incr(max_did);
          let new_did = !max_did in
          sql_dir_ops.pre_create ~new_did ~parent ~times;
          dir_ops.insert ~did:parent name (Did new_did)          
        );
      rename=(fun rename_case -> 
          (* rename is currently handled by flushing the objects
             involved and then performing the operations on the
             underlying store; the objs involved should be locked so
             we don't have to worry that they may be modified after
             flush; of course this is crude and rather inefficient *)
          match rename_case with 
          | Rename_file_missing { times; src=(did1,n1,fid);dst=(did2,n2) } -> 
            dir_ops.sync ~did:did1 >>= fun () ->
            dir_ops.sync ~did:did2 >>= fun () -> 
            let ops : _ Sqlite_dir.Op.op list = [
              Delete (did1,n1);
              Insert (did2,n2,Fid fid)
            ] in
            sql_dir_ops.exec ops
          | Rename_file_file {times; src=(did1,n1,fid1); dst=(did2,n2,fid2) } -> 
            dir_ops.sync ~did:did1 >>= fun () ->
            dir_ops.sync ~did:did2 >>= fun () -> 
            let ops : _ Sqlite_dir.Op.op list = [
              Delete (did1,n1);
              Delete (did2,n2);
              Insert (did2,n2,Fid fid1)
            ] in
            sql_dir_ops.exec ops
          | Rename_dir_missing {times; src=(did1,n1,fid1); dst=(did2,n2) } -> 
            dir_ops.sync ~did:did1 >>= fun () ->
            dir_ops.sync ~did:did2 >>= fun () -> 
            let ops : _ Sqlite_dir.Op.op list = [
              Delete (did1,n1);
              Insert (did2,n2,Fid fid1)
            ] in
            sql_dir_ops.exec ops
        );
    }

  end (* Dir *)

  let dir_ops = Dir.dir_ops
  let dirs_ops = Dir.dirs_ops

end
