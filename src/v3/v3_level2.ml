(** This version focuses on the front end caching and locking, and
   uses SQLite as the backend for the metadata, and some other
   filesystem as a backend for file data.

We maintain a cache of live files, live dirs, and the gom.

We use kref reference counting to ensure that objects are not removed
   from the cache whilst we operate on them. Locked objects can never
   be removed from the cache, but we maintain an invariant that any
   locked object must have an existing kref, so it should never be the
   case that there are no references but an object is locked.

In order to operate on an object, we first obtain a kref: krefs.get id

This ensures that the object is in the cache and will not be flushed.

*)

[@@@warning "-33-27"]

open V3_intf
open V3_level1
open Tjr_monad.With_lwt

module R = V3_intf.Refs_with_dirty_flags 

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

module S1 = V3_intf.S1(S0)
open S1


module Level2_provides = V3_intf.Level2_provides(S0)

(** What we have to implement *)
module type T2 = Level2_provides.T2



(** {2 Directories} *)

(* Dir entries cache: use Lru_with_slow_operations *)

module Lru = Tjr_lib.Lru_with_slow_operations

let lru : (str_256,dir_entry,unit) Lru.lru_module = Lru.make ()
module Entries_cache = (val lru)
type entries_cache = Entries_cache.cache_state'
let entries_cache_ops = Entries_cache.ops

type sql_dir_ops = (str_256,dir_entry,t,did) Sqlite_dir.dir_ops
  
type per_dir = {
  lock           : lwt_mutex;
  parent         : did R.ref;
  times          : times R.ref;
  entries_cache  : entries_cache;
}

(** Live dirs; use V3_live_object_cache *)

module S2 (* : V3_live_object_cache.S *) = struct
  type t = lwt
  type id = did
  type a = per_dir
end

module Live_dirs = V3_live_object_cache.Make(S2)


type config = {
  entries_cache_capacity   : int;
  entries_cache_trim_delta : int;
  live_dirs_capacity       : int;
  live_dirs_trim_delta     : int
}


module type STAGE1 = sig
  val config : config
  val sql_dir_ops : sql_dir_ops
end

module Stage2(Stage1:STAGE1) = struct
  open Stage1

  let new_did = 
    let x = ref @@ sql_dir_ops.max_did () in
    incr x;
    fun () -> 
      let y = !x in
      incr x;
      y

  let resurrect did =
    sql_dir_ops.get_meta ~did >>= fun (parent,times) -> 
    let bot = () in
    let bot_ops : _ Lru.Bot.ops = {
      find_opt = (fun () k -> sql_dir_ops.find ~did k);
      sync = (fun () -> return ()); 
      (* sqlite should sync automatically with every transaction *)
      exec = (fun () ~sync ops -> 
          assert(sync); (* We assume the sync flag is always true FIXME remove this flag *)
          sql_dir_ops.exec (ops |> List.map (function
              | `Delete k -> Sqlite_dir.Op.Delete(did,k)
              | `Insert (k,v) -> Sqlite_dir.Op.Insert(did,k,v))))
    }
    in 
    let entries_cache = 
      entries_cache_ops.initial_cache_state 
        ~cap:config.entries_cache_capacity 
        ~trim_delta:config.entries_cache_trim_delta 
        ~bot
        ~bot_ops
    in
    return 
      { lock=Lwt_mutex_ops.create_mutex(); 
        parent=R.ref parent;
        times=R.ref times;
        entries_cache;
      }

  (* NOTE entries_cache_ops has a sync operation which will sync as a
     batch to the underlying db, but this is separate to the sync of the
     dirty meta; FIXME add a "dirties" call to the lru, to return ops
     that we can then sync with the dirty meta *)
  let finalise (xs:(did*per_dir) list) =
    let module Op = Sqlite_dir.Op in
    let ops (did,x) = 
      let dirty_meta = R.[
          if is_dirty x.parent then Some (Op.Set_parent(did,!(x.parent))) else None;
          if is_dirty x.times then Some (Op.Set_times(did,!(x.times))) else None;
        ] |> List.filter_map (fun x -> x) in
      let dirty_entries = 
        entries_cache_ops.unsafe_clean x.entries_cache |> List.map (function
            | `Insert (k,v) -> Op.Insert(did,k,v)
            | `Delete k -> Op.Delete(did,k))
      in
      dirty_meta@dirty_entries
    in
    xs |> List.map ops |> List.concat |> sql_dir_ops.exec

  let live_dirs_ops = Live_dirs.make_cache_ops ~resurrect ~finalise
  let live_dirs = 
    live_dirs_ops.create 
      ~config:V3_live_object_cache.{
          cache_size=config.live_dirs_capacity; trim_delta=config.live_dirs_trim_delta}

  let ensure_dir_is_live did = live_dirs_ops.get did live_dirs

  let sync_dir_when_lock_held did = 
    let ops = live_dirs_ops in
    ensure_dir_is_live did >>= fun kref -> 
    ops.kref_to_obj kref |> fun per_dir -> 
    finalise [(did,per_dir)] >>= fun () -> 
    ops.put kref;
    return ()

  let dir : dir_ops = 
    let ops = live_dirs_ops in
    let find ~did k = 
      ensure_dir_is_live did >>= fun kref -> 
      ops.kref_to_obj kref |> fun per_dir -> 
      entries_cache_ops.find_opt per_dir.entries_cache k >>= fun v -> 
      ops.put kref;
      return v
    in
    let insert ~did k v =
      ensure_dir_is_live did >>= fun kref -> 
      ops.kref_to_obj kref |> fun per_dir -> 
      entries_cache_ops.insert per_dir.entries_cache k v >>= fun () -> 
      ops.put kref;
      return ()
    in      
    (* NOTE did_locked just means that the did is locked in the live
       dirs; FIXME why do we need obj if the dir is locked? FIXME
       reduce_obj_nlink? *)
    let delete ~did_locked:() ~did ~name:k ~obj:dir_v ~reduce_obj_nlink:() = 
      ensure_dir_is_live did >>= fun kref -> 
      ops.kref_to_obj kref |> fun per_dir -> 
      entries_cache_ops.delete per_dir.entries_cache k >>= fun () -> 
      ops.put kref;
      return ()
    in
    let set_times ~did times = 
      ensure_dir_is_live did >>= fun kref -> 
      ops.kref_to_obj kref |> fun per_dir -> 
      R.(per_dir.times := times);
      ops.put kref;
      return ()
    in
    let get_times ~did = 
      ensure_dir_is_live did >>= fun kref -> 
      ops.kref_to_obj kref |> fun per_dir -> 
      let times = R.(!(per_dir.times)) in
      ops.put kref;
      return times
    in
    let get_parent ~did =
      ensure_dir_is_live did >>= fun kref -> 
      ops.kref_to_obj kref |> fun per_dir -> 
      let parent = R.(!(per_dir.parent)) in
      ops.put kref;
      return parent
    in
    let sync ~did = 
      ensure_dir_is_live did >>= fun kref -> 
      ops.kref_to_obj kref |> fun per_dir -> 
      lwt_mutex_ops.lock per_dir.lock >>= fun () -> 
      finalise [(did,per_dir)] >>= fun () -> 
      lwt_mutex_ops.unlock per_dir.lock >>= fun () ->
      ops.put kref;
      return ()
    in
    { find;insert;delete;set_times;get_times;get_parent;sync }

  let dirs = 
    let delete did = return () in
    (* FIXME at the moment, we don't actually delete directories from the db *)

    let create_and_add_to_parent ~parent_locked:() ~parent ~name ~times =
      (* make new dir, sync; add to parent; sync parent *)
      (* FIXME since parent is locked, we know it is in the cache, so
         this is unnecessary *)
      let new_did = new_did () in
      let _ = 
        sql_dir_ops.pre_create 
          ~note_does_not_touch_parent:()
          ~new_did ~parent ~times
      in
      dir.insert ~did:parent name (Did new_did) >>= fun () -> 
      (* FIXME probably not necessary to sync the parent here *)
      sync_dir_when_lock_held parent >>= fun () -> 
      return ()
    in

    let rename ~locks_held:() rename_case =
      (* FIXME implement these in sqlite_dir *)
      failwith "FIXME"
    in
    { delete; create_and_add_to_parent; rename }

  
  
      
      


end (* Stage2 *)


(*

(* FIXME locking, trimming *)
module Dir = struct


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
    delete=(fun ~did_locked:_ ~did ~name ~obj:_FIXME ~reduce_obj_nlink:() -> 
        live_dirs_find did >>= fun per_dir -> 
        per_dir.entries.delete name);
    set_times=(fun ~did times -> 
        live_dirs_find did >>= fun per_dir -> 
        R.(per_dir.times := times); 
        return ());
    get_times=(fun ~did -> 
        live_dirs_find did >>= fun per_dir -> 
        R.(!(per_dir.times)) |> return);
    (* set_parent=(fun ~did ~parent -> 
        live_dirs_find did >>= fun per_dir -> 
        R.(per_dir.parent := parent); 
        return ()); *)
    get_parent=R.(fun ~did -> 
        live_dirs_find did >>= fun per_dir -> 
        R.(!(per_dir.parent)) |> return);
    sync=failwith""
  }


  let max_did = ref (sql_dir_ops.max_did ()) 

  let dirs_ops : dirs_ops = {
    delete=(fun _did -> return ()); (* FIXME probably remove from underlying db *)
    create=(fun ~parent_locked ~parent ~name ~times -> 
        incr(max_did);
        let new_did = !max_did in
        sql_dir_ops.pre_create ~new_did ~parent ~times;
        dir_ops.insert ~did:parent name (Did new_did)          
      );
    rename=(fun ~locks_held:_ rename_case -> 
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



module T2 (* : T2 *) = struct

  let root_did = 0

  let max_sz = 1000
  let default_cache_size = 1000

  let db : Sqlite_dir.db = failwith ""


  let dir_ops = Dir.dir_ops
  let dirs_ops = Dir.dirs_ops

end
*)
