(** V3 level 2 is the lowest implementation level; SQLite is used to implement directories
    and metadata (?) and some other filesystem is used to implement files.

This version focuses on the front end caching and locking, so we delegate dirs and files
to SQLite/the existing filesystem.

We maintain a cache of live files, live dirs.

We use kref reference counting to ensure that objects are not removed from the cache
whilst we operate on them. Locked objects can never be removed from the cache, but we
maintain an invariant that any locked object must have an existing kref, so it should
never be the case that there are no references but an object is locked.

In order to operate on an object, we first obtain a kref: [krefs.get id]. This ensures
that the object is in the cache and will not be flushed.
*)

[@@@warning "-33-27"]

open V3_intf
open V3_level1
open Tjr_monad.With_lwt

(** {2 Preamble} *)

let line s = Printf.printf "%s: Reached line %d\n%!" "V3_level2" s; true

module R = V3_util.Refs_with_dirty_flags 

module S0 (* : S0 *) = V3_base_types
open S0

module S1 = V3_intf.S1(S0)
open S1


module Level2_provides = V3_intf.Level2_provides(S0)

(** What we have to implement - see {!Stage2} below. *)
module type T2 = Level2_provides.T2



(** {2 Directories} *)

(** Configuration params known at module init time *)
type config = {
  entries_cache_capacity   : int;
  entries_cache_trim_delta : int;
  live_dirs_capacity       : int;
  live_dirs_trim_delta     : int;
  file_data_path           : string;
  live_files_capacity       : int;
  live_files_trim_delta     : int;
}

(** NOTE nothing in here is actually exposed in {!Stage2} below; could just open the
    structure rather than binding it *)
module Directories_prelude = struct

  (* Dir entries cache: use Lru_with_slow_operations *)

  module Lru = Lru_with_slow_operations

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

  type per_file = {
    fd: Lwt_unix.file_descr;
  }  
end
open Directories_prelude


(** Stage 1 is immediately after connection to the SQLite backend; it includes the
    [sql_dir_ops] live connection to the database *)
module type STAGE1 = sig
  val config : config
  val sql_dir_ops : sql_dir_ops
end

(** Stage 2 uses the connection to SQLite to build the rest of the {!T2} interface *)
module Stage2(Stage1:STAGE1) : T2 = struct
  open Stage1

  let dont_log = !Util.dont_log

  module Live_dirs = V3_live_object_cache.Make(S2)

  module Live_files = V3_live_object_cache.Make(
    struct
      type t = lwt
      type id = fid
      type a = per_file
    end)

  let root_did = 0

  let new_did = 
    let x = ref @@ sql_dir_ops.max_did () in
    incr x;
    fun () -> 
      let y = !x in
      incr x;
      y

  let resurrect did =
    assert(dont_log || line __LINE__);
    sql_dir_ops.get_meta ~did >>= fun (parent,times) -> 
    assert(dont_log || line __LINE__);
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
    assert(dont_log || line __LINE__);
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
    assert(dont_log || line __LINE__);
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
      assert(dont_log || line __LINE__);
      ensure_dir_is_live did >>= fun kref -> 
      assert(dont_log || line __LINE__);
      ops.kref_to_obj kref |> fun per_dir -> 
      assert(dont_log || line __LINE__);
      entries_cache_ops.insert per_dir.entries_cache k v >>= fun () -> 
      assert(dont_log || line __LINE__);
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
      assert(dont_log || line __LINE__);
      ensure_dir_is_live did >>= fun kref -> 
      assert(dont_log || line __LINE__);
      ops.kref_to_obj kref |> fun per_dir -> 
      assert(dont_log || line __LINE__);
      let times = R.(!(per_dir.times)) in
      assert(dont_log || line __LINE__);
      ops.put kref;
      assert(dont_log || line __LINE__);
      return times
    in
    let get_parent ~did =
      ensure_dir_is_live did >>= fun kref -> 
      ops.kref_to_obj kref |> fun per_dir -> 
      let parent = R.(!(per_dir.parent)) in
      ops.put kref;
      return parent
    in
    let sync ~lock_held ~did = 
      assert(dont_log || line __LINE__);
      ensure_dir_is_live did >>= fun kref -> 
      assert(dont_log || line __LINE__);
      ops.kref_to_obj kref |> fun per_dir -> 
      assert(dont_log || line __LINE__);
      (if lock_held then return () else lwt_mutex_ops.lock per_dir.lock) >>= fun () -> 
      assert(dont_log || line __LINE__);
      finalise [(did,per_dir)] >>= fun () -> 
      assert(dont_log || line __LINE__);
      (if lock_held then return () else lwt_mutex_ops.unlock per_dir.lock) >>= fun () ->
      assert(dont_log || line __LINE__);
      ops.put kref;
      assert(dont_log || line __LINE__);
      return ()
    in
    { find;insert;delete;set_times;get_times;get_parent;sync }


  (* NOTE this is filled in later *)
  let locks = 
    ref Lock_ops.{ lock=(fun ~tid:_ -> failwith ""); unlock=(fun ~tid:_ -> failwith "") }

  let dirs = 
    let delete did = return () in
    (* FIXME at the moment, we don't actually delete directories from the db *)

    let create_and_add_to_parent ~parent_locked:() ~parent ~name ~times =
      assert(dont_log || line __LINE__);      
      (* make new dir, sync; add to parent; sync parent *)
      (* FIXME since parent is locked, we know it is in the cache, so
         this is unnecessary *)
      let new_did = new_did () in      
      sql_dir_ops.pre_create ~note_does_not_touch_parent:() ~new_did ~parent ~times >>= fun () -> 
      assert(dont_log || line __LINE__);
      dir.insert ~did:parent name (Did new_did) >>= fun () -> 
      assert(dont_log || line __LINE__);
      (* FIXME probably not necessary to sync the parent here *)
      sync_dir_when_lock_held parent >>= fun () -> 
      assert(dont_log || line __LINE__);
      return ()
    in

    let remove_entry_from_cache ~did name = 
      ensure_dir_is_live did >>= fun kref -> 
      live_dirs_ops.kref_to_obj kref |> fun per_dir -> 
      entries_cache_ops.remove_clean_entry per_dir.entries_cache name;
      live_dirs_ops.put kref;
      return ()
    in

    let module Op = Sqlite_dir.Op in
    let rename_file rename_case =
      match rename_case with
      | Rename_file_missing { locks_held; times; src; dst } -> 
        assert(dont_log || line __LINE__);
        let (sp,sn,sfid) = src in
        let (dp,dn) = dst in
        (* one implementation is to sync src and dst (maybe just the
           relevant keys), clear the relevant cache entries (sn
           dn... dn may be "deleted" but not flushed to db), and make
           the change atomically in the db, then unlock and return *)
        dir.sync ~lock_held:true ~did:sp >>= fun () -> 
        assert(dont_log || line __LINE__);
        dir.sync ~lock_held:true ~did:dp >>= fun () -> 
        assert(dont_log || line __LINE__);        
        remove_entry_from_cache ~did:sp sn >>= fun () -> 
        assert(dont_log || line __LINE__);
        remove_entry_from_cache ~did:dp dn >>= fun () -> 
        assert(dont_log || line __LINE__);
        let ops = Sqlite_dir.Op.([Delete(sp,sn);Insert(dp,dn,Fid sfid)]) in
        sql_dir_ops.exec ops >>= fun () -> 
        assert(dont_log || line __LINE__);
        return ()
        
      | Rename_file_file { locks_held; times; src; dst } -> 
        let (sp,sn,sfid) = src in
        let (dp,dn,dfid) = dst in
        assert(sfid <> dfid);
        assert( (sp,sn) <> (dp,dn) );
        dir.sync ~lock_held:true ~did:sp >>= fun () -> 
        dir.sync ~lock_held:true ~did:dp >>= fun () -> 
        remove_entry_from_cache ~did:sp sn >>= fun () -> 
        remove_entry_from_cache ~did:dp dn >>= fun () -> 
        let ops = Sqlite_dir.Op.([Delete(sp,sn);Insert(dp,dn,Fid sfid)]) in
        sql_dir_ops.exec ops        
    in

    (* NOTE this is the only level 2 operation that locks objects via [!locks] *)
    let rename_dir = function
      | Rename_dir_missing { tid; locks_not_held; needs_ancestor_check; src; dst } ->
          (* We need to figure out which objs need locking; exit early
             if ancestor; otherwise lock locks; re-validate the objs
             are as before; exit early if concurrent mod; otherwise
             sync src and dst; clear cache entries for the affected
             dir entries; then atomically rename in the backend *)
          let (sp,sn,sdid) = src in
          let (dp,dn) = dst in
          (dp,[]) |> iter_k (fun ~k (did,acc) -> 
              ensure_dir_is_live did >>= fun kref -> 
              live_dirs_ops.kref_to_obj kref |> fun per_dir ->
              match did=sdid with 
              | true -> 
                (kref::List.map fst acc) |> List.iter live_dirs_ops.put;
                return `Src_is_ancestor_of_dest
              | false -> 
                match did=root_did with
                | true -> return @@ `Ok ((kref,did)::acc)
                | false -> 
                  dir.get_parent ~did >>= fun did' -> 
                  k (did',(kref,did)::acc))
          >>= function
          | `Src_is_ancestor_of_dest -> return (Error `Error_attempt_to_rename_to_subdir)
          | `Ok acc -> 
            (* acquire locks *)
            let objs = sdid::(List.map snd acc) |> List.map (fun did -> Did did) in
            (!locks).lock ~tid ~objs >>= fun () -> 
            (* revalidate the path to root *)
            (List.map snd acc) |> fun dids -> 
            dids |> iter_k (fun ~k dids -> 
                match dids with
                | [] -> failwith "impossible"
                | [x] -> 
                  assert(x=root_did && x=dp);
                  return true
                | d1::d2::rest -> 
                  dir.get_parent ~did:d2 >>= fun did -> 
                  match did = d1 with
                  | false -> return false
                  | true -> k (d2::rest)) >>= function
            | false -> return (Error eRROR_CONCURRENT_MODIFICATION)
            | true -> 
              (* OK, everything is fine, we can do the rename; NOTE
                 that we acquired the locks just above *)
              dir.sync ~lock_held:true ~did:sp >>= fun () -> 
              dir.sync ~lock_held:true ~did:dp >>= fun () -> 
              remove_entry_from_cache ~did:sp sn >>= fun () -> 
              remove_entry_from_cache ~did:dp dn >>= fun () -> 
              let ops = Op.([Delete(sp,sn);Insert(dp,dn,Did sdid);Set_parent(sdid,dp)]) in
              sql_dir_ops.exec ops >>= fun () -> 
              (* now release all the locks and the krefs *)
              (!locks).unlock ~tid ~objs >>= fun () -> 
              (List.map fst acc) |> List.iter live_dirs_ops.put;
              return (Ok ())
    in
    { delete; create_and_add_to_parent; rename_file; rename_dir }

  (* FIXME prefer krefs.get/put rather than having abstract types *)

  let fid_to_path fid = config.file_data_path ^"/" ^(string_of_int fid)

  let sid_to_path sid = config.file_data_path ^"/" ^(string_of_int sid)

  let live_files_ops = 
    let finalise1 (_fid,{fd}) = Lwt.(
      Lwt_unix.fsync fd >>= fun () -> 
      Lwt_unix.close fd >>= fun () -> 
      return ())
    in
    Live_files.make_cache_ops
      ~resurrect:(fun fid -> 
          Lwt_unix.openfile (fid_to_path fid) Unix.[O_CREAT;O_RDWR] 0o660
          |> from_lwt >>= fun fd -> 
          return {fd})
      ~finalise:(fun xs -> 
          xs |> List.map finalise1 |> Lwt.join |> from_lwt)

  let live_files = 
    live_files_ops.create 
      ~config:V3_live_object_cache.{
          cache_size=config.live_files_capacity; trim_delta=config.live_files_trim_delta}

  let ensure_file_is_live fid = live_files_ops.get fid live_files

  let ops = live_files_ops

  (* NOTE lwt operations require bytes, but we tend to prefer
     bigarray; at the moment we do a lot of allocating of temporary
     bytes objects, and then blit to the bigarray FIXME *)

  let file : file_ops = 
    let pread ~fid ~foff ~len ~(buf:Shared_ctxt.buf) ~boff = 
      ensure_file_is_live fid >>= fun kref -> 
      let {fd} = kref |> ops.kref_to_obj in
      let buf' = Bytes.create len in
      (* FIXME should take care of pread exceptions, and pwrite etc *)
      Lwt_unix.pread fd buf' ~file_offset:foff 0 (*boff*) len |> from_lwt >>= fun n -> 
      let _ : buf = Shared_ctxt.buf_ops.blit_bytes_to_buf ~src:buf' ~src_off:{off=0} ~src_len:{len=n} ~dst:buf ~dst_off:{off=boff} in
      ops.put kref;
      return (Ok n)
    in
    let pwrite ~fid ~foff ~len ~(buf:Shared_ctxt.buf) ~boff = 
      ensure_file_is_live fid >>= fun kref -> 
      let {fd} = kref |> ops.kref_to_obj in
      let buf' = Bytes.create len in
      Bigstringaf.blit_to_bytes buf.ba_buf ~src_off:boff buf' ~dst_off:0 ~len;
      Lwt_unix.pwrite fd buf' ~file_offset:foff 0(*boff*) len |> from_lwt >>= fun n -> 
      ops.put kref;
      return (Ok n)
    in
    let truncate ~fid n = 
      ensure_file_is_live fid >>= fun kref -> 
      let {fd} = kref |> ops.kref_to_obj in
      Lwt_unix.ftruncate fd n |> from_lwt >>= fun () -> 
      ops.put kref;
      return ()      
    in
    let get_sz ~fid =
      ensure_file_is_live fid >>= fun kref -> 
      let {fd} = kref |> ops.kref_to_obj in
      Lwt_unix.fstat fd |> from_lwt >>= fun st -> 
      ops.put kref;
      return st.st_size
    in
    let get_times ~fid =
      ensure_file_is_live fid >>= fun kref -> 
      let {fd} = kref |> ops.kref_to_obj in
      Lwt_unix.fstat fd |> from_lwt >>= fun st -> 
      ops.put kref;
      return Times.{atim=st.st_atime;mtim=st.st_mtime}
    in
    let set_times ~fid Times.{atim;mtim} =
      Lwt_unix.utimes (fid_to_path fid) atim mtim |> from_lwt >>= fun s -> 
      return ()
    in
    let sync ~fid = 
      ensure_file_is_live fid >>= fun kref -> 
      let {fd} = kref |> ops.kref_to_obj in
      Lwt_unix.fsync fd |> from_lwt >>= fun st -> 
      ops.put kref;
      return ()
    in
    { pread;pwrite;truncate;get_sz;set_times;get_times;sync }
    
  let max_fid () = 
    Unix.(
      let dh = opendir config.file_data_path in
      0 |> iter_k (fun ~k n -> 
          (try readdir dh with End_of_file -> closedir dh; "") |> function
          | "" -> n
          | s -> 
            s |> int_of_string_opt |> function
            | None -> k n
            | Some m -> 
              let n = max m n in
              k n))

  let new_fid =
    let x = ref (max_fid()) in
    incr x;
    fun () -> 
      let y = !x in
      incr x;
      y
      
  let files : files_ops = 
    let create_and_add_to_parent ~parent ~name ~times =
      let fid = new_fid () in
      let pth = fid_to_path fid in
      assert(not @@ file_exists pth);
      (* FIXME probably want to sync the config.file_data_path
         directory to ensure the file is actually created on disk *)
      Lwt.(Lwt_unix.(openfile pth [O_CREAT;O_RDWR] 0o660 >>= fun fd ->
                     close fd >>= fun () -> 
                     utimes pth times.Times.atim times.mtim)) |> from_lwt >>= fun () -> 
      (* now add to parent *)
      dir.insert ~did:parent name (Fid fid)
    in
    let create_symlink_and_add_to_parent ~parent ~name ~times ~(contents:str_256) =
      let sid = new_fid () in
      let pth = sid_to_path sid in
      assert(not @@ file_exists pth);
      (* FIXME probably want to sync the config.file_data_path
         directory to ensure the file is actually created on disk *)
      Lwt.(Lwt_unix.(
          (* FIXME again, probably want to sync config.file_data_path dir *)
          symlink (contents|>Str_256.to_string) pth >>= fun () ->
          (* FIXME? NOTE lwt_unix doesn't have lutimes, so times are ignored for symlinks *)
          return ())) |> from_lwt >>= fun () -> 
      (* now add to parent *)
      dir.insert ~did:parent name (Sid sid)
    in
    { create_and_add_to_parent; create_symlink_and_add_to_parent }

  let read_symlink ~sid =
    Lwt_unix.(readlink (sid|>sid_to_path)) |> from_lwt >>= fun s -> 
    return (Str_256.make s)
    


  (** {2 locks} *)

  (** NOTE locks mostly are handled at level 1, except for [Rename_dir_missing] where the
      locking is sufficiently intricate that we do it at level 2 *)

  type per_tid = { objs: dir_entry list; krefs:Live_dirs.kref list; locks: Lwt_mutex.t list }

  let live_locks : (tid,per_tid) Hashtbl.t = Hashtbl.create 100

  let locks' : (tid,dir_entry,t) Lock_ops.lock_ops = 
    let lock ~tid ~objs:objs0 =       
      let objs = List.sort_uniq Stdlib.compare objs0 in
      (* The objs can be files or directories, not symlinks; files
         only get locked during a rename currently; FIXME do we need
         file locks at all, since another rename can't interfere
         because the dirs are locked? Let's assume not for the time
         being *)
      let objs = objs |> List.filter_map (function | Did did -> Some did | _ -> None) in
      ([],[],objs) |> iter_k (fun ~k (krefs,lcks,objs) -> 
          match objs with 
          | [] -> return (krefs,lcks)
          | x::xs -> begin
              ensure_dir_is_live x >>= fun kref -> 
              live_dirs_ops.kref_to_obj kref |> fun per_dir -> 
              lwt_mutex_ops.lock per_dir.lock >>= fun () -> 
              k (kref::krefs, (per_dir.lock::lcks), xs)
            end)
      >>= fun (krefs,locks) -> 
      assert(not @@ Hashtbl.mem live_locks tid);
      Hashtbl.add live_locks tid {objs=objs0; krefs; locks};
      return ()
    in
    let unlock ~tid ~objs:objs0 =
      Hashtbl.find_opt live_locks tid |> function
      | None -> assert(false)
      | Some {objs;krefs;locks} -> 
        assert(objs = objs0); 
        (* threads can lock one set of objs at a time, and must release all at once *)
        (krefs,locks) |> iter_k (fun ~k (krefs,lcks) -> 
            match krefs,lcks with
            | [],[] -> return ()
            | x::xs,y::ys -> 
              lwt_mutex_ops.unlock y >>= fun () -> 
              live_dirs_ops.put x;
              k (xs,ys)
            | _ -> failwith "unlock: lists of differing lengths; impossible")
          (* FIXME just maintain a single list of pairs *)
    in
    { lock; unlock }

  let _ = locks := locks'

  let locks = locks'
      
  
  (** {2 Dir handles} *)
    
  type per_dir_handle = { 
    tid    : tid; (* for debugging resource usage *)
    ls_ops : (dir_k,dir_v,lwt) Sqlite_dir.Ls.ops }

  let live_dir_handles : (did,per_dir_handle)Hashtbl.t = Hashtbl.create 100

  let new_dir_handle = 
    let x = ref 0 in
    incr x;
    fun () -> 
      let y = !x in
      incr x;
      y
    
  let dir_handles: dir_handle_ops = 
    let opendir ~tid did = 
      (* FIXME performance: we flush any cached entries on the dir
         before reading; perfer a scheme where we patch up the sqldir
         list with the cache dirty entries *)
      dir.sync ~lock_held:false ~did >>= fun () -> 
      let dh = new_dir_handle () in
      sql_dir_ops.opendir ~did >>= fun ls_ops -> 
      assert(not @@ Hashtbl.mem live_dir_handles dh);
      let per_dh = { tid; ls_ops } in
      Hashtbl.add live_dir_handles dh per_dh;
      return dh
    in
    let readdir ~tid:_ dh = 
      Hashtbl.find live_dir_handles dh |> fun per_dh -> 
      per_dh.ls_ops.is_finished () >>= function
      | true -> return []
      | false -> 
        per_dh.ls_ops.kvs () >>= fun kvs -> 
        per_dh.ls_ops.step () >>= fun () -> 
        return (List.map fst kvs)
    in
    let closedir ~tid:_ dh = 
      Hashtbl.remove live_dir_handles dh;
      return ()
    in
    { opendir; readdir; closedir }


  (** {2 Path resolution} *)

  (** NOTE copied from v1_specific.ml *)

  let resolve_comp: did -> comp_ -> ((fid,did,sid)resolved_comp,t)m = 
    fun did comp ->
    assert(String.length comp <= 256);
    let comp = Str_256.make comp in
    dir.find ~did comp >>= function
    | None -> return RC_missing
    | Some x -> 
      match x with
      | Fid fid -> RC_file fid |> return
      | Did did -> RC_dir did |> return
      | Sid sid -> 
        read_symlink ~sid >>= fun s ->
        RC_sym (sid,Str_256.to_string s) |> return
        
  let fs_ops = {
    root=root_did;
    resolve_comp
  }

  (* NOTE for fuse we always resolve absolute paths *)
  let resolve = Tjr_path_resolution.resolve ~monad_ops ~fs_ops ~cwd:root_did
      
  let _ : 
    follow_last_symlink:follow_last_symlink ->
    comp_ -> 
    ((fid, did,sid) resolved_path_or_err, lwt) m = resolve
    
  let resolve_path : resolve_path = Path_resolution.{
      resolve_path=fun ~follow_last_symlink comp_ ->         
        resolve ~follow_last_symlink comp_ >>= function
        | Error e -> return (Error e)
        | Ok rp -> 
          (* Need to convert comp_ component to str_256 *)
          let Tjr_path_resolution.Intf.{ parent_id;comp;result;trailing_slash } = rp in
          return (Ok Path_resolution.{parent_id;comp=Str_256.make comp;result;trailing_slash})
    }
      
  let mk_stat_times () = 
    let t = Sys.time () in
    Times.{atim=t;mtim=t}
      
  let extra = 
    let internal_err ~tid s = 
      Printf.printf "INTERNAL ERROR THREAD %d!!! %s\n%!" tid s;
      failwith s
    in
    { internal_err }
      
end (* Stage2 *)


open struct
  (* verify that Stage2 does indeed produce a T2 *)
  module Check_stage2(Stage1:STAGE1) : T2 = Stage2(Stage1)
end
