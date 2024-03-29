(** V2 - compared to V1, this version implements files (via file_impl_v2).

We try to reuse V1_generic. Some of the following copied from v1.ml *)

[@@@warning "-33"]

open Tjr_monad.With_lwt
open Shared_ctxt
open Bin_prot.Std
open V2_intf
(* open V2_fs_impl *)
open V2_gom

module G = V2_generic

let add_logging_to_blk_dev ~(read_msg:blk_id -> unit) ~(write_msg:blk_id -> unit) ~blk_dev_ops = 
  let read ~blk_id = 
    read_msg blk_id;
    blk_dev_ops.read ~blk_id
  in
  let write ~blk_id ~blk = 
    write_msg blk_id;
    blk_dev_ops.write ~blk_id ~blk
  in
  { blk_dev_ops with read; write }

(** {2 Setup the generic instance} *)

(** Base types *)
module S0 = struct
  type t = lwt
  let monad_ops = monad_ops
  type fid = int[@@deriving bin_io]
  type did = int[@@deriving bin_io]
  type sid = int[@@deriving bin_io]
end
open S0

module Make = G.Make(S0)

(** derived types *)
module S1 = Make.S1
open S1

(** functor argument type holder *)
module S2 = Make.S2

(** what we need to provide to get a filesystem *)
module type T2 = S2.T2

(** functor to construct a filesystem (takes a module of type T2); the
   implementation is {!T2_impl}, towards the end of this file *)
module Make_2 = Make.Make_2

module Make_3 = Make.Make_3


(** {2 Some simple defns we can complete here} *)

let root_did = 0

let mk_stat_times () = 
  Unix.time () |> fun t -> (* 1s resolution? FIXME *)
  return Times.{ atim=t; mtim=t }

type dir_entry = S1.dir_entry


(** {2 Various marshallers} *)

let shared_mshlrs = Tjr_fs_shared.bp_mshlrs

let dir_entry_mshlr : dir_entry bp_mshlr = 
  (module 
    (struct 
      type t = dir_entry[@@deriving bin_io] 
      let max_sz = 10 (* FIXME check 9+1 *)
    end))

[@@@warning "-32"]


(** {2 Stage 1: we assume blk_dev and origin blkid known} *)

module Stage_1(S1:sig
    val blk_dev_ops : (r,blk,t)blk_dev_ops
    val fs_origin   : r Fs_origin_block.t
    (* FIXME replace with fl_examples#fl_params_1 *)
    val fl_params   : Tjr_freelist.Freelist_intf.params
  end) = struct
  open S1

  let _ = Printf.printf "%s: Stage_1 starts\n%!" __FILE__

  let barrier () = return ()
  let sync () = return ()

  (** {2 New identifiers} *)

  (** We use mutually exclusive subsets of int for identifiers; NOTE 0
      is reserved for the root directory *)

  let counter_factory = V2_counter.example

  let counter_ops = 
    let read_msg blk_id = Printf.printf "counter: read from %d\n%!" (B.to_int blk_id) in
    let write_msg blk_id = Printf.printf "counter: write to %d\n%!" (B.to_int blk_id) in
    let blk_dev_ops = add_logging_to_blk_dev ~read_msg ~write_msg ~blk_dev_ops in
    counter_factory#with_ ~blk_dev_ops ~sync |> fun o -> 
    o#init_from_disk fs_origin.counter_origin


  (** {2 The freelist } *)

  let _ = Printf.printf "%s: Stage_1 freelist\n%!" __FILE__

  (** We need to read the freelist origin block, and resurrect the freelist *)

  let fl_params = object
    method tr_upper=2000
    method tr_lower=1000
    method min_free_alloc_size=500
  end

  (* FIXME we need to initialize the freelist at some point of course;
     an init functor? *)
  let freelist = 
    let fact = Tjr_freelist.fl_examples#for_r in
    let read_msg blk_id = Printf.printf "freelist: read from %d\n%!" (B.to_int blk_id) in
    let write_msg blk_id = Printf.printf "freelist: write to %d\n%!" (B.to_int blk_id) in
    let blk_dev_ops = add_logging_to_blk_dev ~read_msg ~write_msg ~blk_dev_ops in
    let with_ = 
      fact#with_
        ~blk_dev_ops
        ~barrier
        ~sync
        ~params:fl_params
    in
    with_#restore ~autosync:true ~origin:fs_origin.fl_origin

  let fl_ops = freelist >>= fun x -> 
    return x#freelist_ops


  (** {2 The global object map (GOM) } *)
  
  let _ = Printf.printf "%s: Stage_1 GOM\n%!" __FILE__


  (* FIXME freelistops should have the same intf as blk_alloc? *)
  let gom_ops = fl_ops >>= fun fl_ops -> 
    let read_msg blk_id = Printf.printf "gom: read from %d\n%!" (B.to_int blk_id) in
    let write_msg blk_id = Printf.printf "gom: write to %d\n%!" (B.to_int blk_id) in
    let blk_dev_ops = add_logging_to_blk_dev ~read_msg ~write_msg ~blk_dev_ops in
    V2_gom.gom_example#with_
      ~blk_dev_ops
      ~barrier
      ~sync
      ~freelist_ops:fl_ops |> fun o -> 
    o#init_from_disk fs_origin.gom_origin

  (* NOTE we also have to make sure we flush the GOM root... *)

  let _ = Printf.printf "%s: Stage_1 post GOM\n%!" __FILE__


  (** {2 Stage_2: we assume freelist_ops, gom_ops and counter_ops are available} *)

  module Stage_2(S2:sig 
      val fl_ops      : (r,r,t)Freelist_intf.freelist_ops 
      val gom_ops     : (dir_entry,blk_id,blk_id,t) Gom_ops.t
      val counter_ops : t V2_counter.counter_ops
    end) = struct
    open S2

    let _ = Printf.printf "%s: Stage_2\n%!" __FILE__

    let new_id () = counter_ops.alloc ()

    let new_did () = new_id () 

    let new_fid () = new_id () 

    let new_sid () = new_id () 

(*

  let min_free_id_ref = ref fs_origin.counter

  let new_id () = 
    let r = !min_free_id_ref in
    incr min_free_id_ref;
    return r

*)

    let id_to_int = V2_dir_impl.Dir_entry.(function
        | Did did -> did
        | Fid fid -> fid
        | Sid sid -> sid)

    let root_did = root_did


    (** {2 Blk allocator using freelist} *)

(*
    let blk_alloc : _ blk_allocator_ops =     
      {
        blk_alloc=S2.fl_ops.alloc;
        blk_free=S2.fl_ops.free
      }
*)

    (* let freelist_ops' = blk_alloc *)

    (** {2 Dirs} *)

    let gom_find_opt = gom_ops.find

    let gom_find k = gom_find_opt k >>= function
      | Some r -> return r
      | None -> 
        Printf.printf "Error: gom key %d not found\n%!" (id_to_int k);
        failwith "gom: id did not map to an entry"

    let gom_insert = gom_ops.insert

    let gom_delete = gom_ops.delete


    let dir_impl = V2_dir_impl.dir_example
    let dir_impl' =             
      let read_msg blk_id = Printf.printf "dir_impl: read from %d\n%!" (B.to_int blk_id) in
      let write_msg blk_id = Printf.printf "dir_impl: write to %d\n%!" (B.to_int blk_id) in
      let blk_dev_ops = add_logging_to_blk_dev ~read_msg ~write_msg ~blk_dev_ops in
      dir_impl#with_
        ~blk_dev_ops
        ~barrier
        ~sync
        ~freelist_ops:fl_ops


    let _ = Printf.printf "%s: Stage_2 dirs\n%!" __FILE__

    (** Cache of live directories; see also {!Live_f} *)
    module Live_d : sig
(*
      val start_thread : unit -> (unit, t)m
      (** Start a thread that periodically attempts to sync dirty dirs *)
*)
      val find         : did -> (dir_ops,t)m
    end = struct
      module A = struct
        type k = did
        let cmp_k = Int.compare
        type v = dir_ops
        type nonrec t = t
        let monad_ops = monad_ops
      end
      module L = V2_live_object_cache.Make(A)
      
      (** $(CONFIG("params: capacity of the live_f cache")) *)
      let capacity = 20
        
      let empty = L.factory#empty ~capacity

      (** Resurrect file_ops from disk *)
      let lower_acquire did = 
        gom_find (Did did) >>= fun blk_id -> 
        dir_impl'#dir_from_origin blk_id >>= fun ops ->
        return (Some ops)
        
      (** When we evict from cache, we take care to call sync (this is
         blocking... possible performance issue... see
         {!Live_object_cache}. *)
      let lower_release (_fid, (dir_ops:dir_ops)) = 
        dir_ops.sync ()        

      let ref_ = ref empty

      let with_locked_state = Tjr_monad.With_lwt.with_locked_ref ref_

      let live_d = L.factory#with_ ~lower_acquire ~lower_release ~with_locked_state

      let _ : 
        < find_opt : fd -> (dir_ops option, S0.t) Tjr_monad.m;
          to_list : unit -> ((did * dir_ops) list, S0.t) Tjr_monad.m > 
        = live_d

      let find : did -> (dir_ops,t)m = fun did -> 
        live_d#find_opt did >>= function
        | None -> 
          (* $(FIXME("""make sure this case actually is impossible""")) *)
          failwith "impossible: attempted to find did, but did was not found"
        | Some d_ops -> return d_ops

      (** A file in the cache may be dirty; we want the file to be
         flushed periodically; so we set up a thread that just scans
         the cache periodically and issues a sync on those files that
         need it *)
      let rec live_d_thread () = 
        (* Sleep for a bit *)
        With_lwt.(from_lwt (sleep 1.0)) >>= fun () -> 
        (* $(FIXME("""need to add sleep and yield to the monad_ops; also
           With_lwt should have sleep and yield using our monad, not
           lwt's""")) *)

        (* At this point, we might want to sync/flush dirty
           directories; however, currently, there is nothing to flush
           because directories are implemented using a raw
           B-tree... and each dir origin should be updated
           automatically *)

        (* And repeat *)
        live_d_thread ()
        
      let start_thread () = 
        async live_d_thread

      let _ = start_thread
    end 

    

    let dirs : dirs_ops = 
      let find = Live_d.find in
      {
        find;
        delete = (fun did -> gom_delete (Did did));
        create_dir = (fun ~parent ~name ~times -> 
            new_did () >>= fun did ->
            dir_impl'#create_dir ~parent ~times >>= fun blk_id ->
            gom_insert (Did did) blk_id >>= fun () ->
            find parent >>= fun p ->
            p.insert name (Did did))
      }


    let _ = Printf.printf "%s: Stage_2 file_impl\n%!" __FILE__

    let file_impl = File_impl_v2.file_examples#example_1
    let file_impl' = 
      let read_msg blk_id = Printf.printf "file_impl: read from %d\n%!" (B.to_int blk_id) in
      let write_msg blk_id = Printf.printf "file_impl: write to %d\n%!" (B.to_int blk_id) in
      let blk_dev_ops = add_logging_to_blk_dev ~read_msg ~write_msg ~blk_dev_ops in
      file_impl#with_
        ~blk_dev_ops
        ~barrier
        ~sync
        ~freelist_ops:fl_ops

    let symlink_impl = V2_symlink_impl.example
    let symlink_impl' =
      let read_msg blk_id = Printf.printf "symlink_impl: read from %d\n%!" (B.to_int blk_id) in
      let write_msg blk_id = Printf.printf "symlink_impl: write to %d\n%!" (B.to_int blk_id) in
      let blk_dev_ops = add_logging_to_blk_dev ~read_msg ~write_msg ~blk_dev_ops in
      symlink_impl#with_
        ~blk_dev_ops
        ~freelist_ops:fl_ops

    let _ = Printf.printf "%s: Stage_2 files\n%!" __FILE__

       
    (** A cache of live files; see also {!Live_d} *)
    module Live_f : sig
      val start_thread : unit -> (unit, t)m
      (** Start a thread that periodically attempts to sync dirty files *)

      val find         : fid -> (file_ops,t)m
    end = struct
      module A = struct
        type k = fid
        let cmp_k = Int.compare
        type v = file_ops
        type nonrec t = t
        let monad_ops = monad_ops
      end
      module L = V2_live_object_cache.Make(A)
      
      (** $(CONFIG("params: capacity of the live_f cache")) *)
      let capacity = 20
        
      let empty = L.factory#empty ~capacity

      (** Resurrect file_ops from disk *)
      let lower_acquire fid = 
        gom_find (Fid fid) >>= fun blk_id -> 
        file_impl'#file_from_origin blk_id >>= fun ops ->
        return (Some ops)

      let _ = lower_acquire
        
      (** When we evict from cache, we take care to call sync (this is
         blocking... possible performance issue... see
         {!Live_object_cache}. *)
      let lower_release (_fid, (file_ops:file_ops)) = 
        file_ops.sync ()        

      let ref_ = ref empty

      let with_locked_state = Tjr_monad.With_lwt.with_locked_ref ref_

      let live_f = L.factory#with_ ~lower_acquire ~lower_release ~with_locked_state

      let _ : 
        < find_opt : fd -> (file_ops option, S0.t) Tjr_monad.m;
          to_list : unit -> ((fd * file_ops) list, S0.t) Tjr_monad.m > 
        = live_f

      let find : fid -> (file_ops,t)m = fun fid -> 
        live_f#find_opt fid >>= function
        | None -> 
          (* $(FIXME("""make sure this case actually is impossible""")) *)
          failwith "impossible: attempted to find fid, but fid was not found"
        | Some f_ops -> return f_ops

      (** A file in the cache may be dirty; we want the file to be
         flushed periodically; so we set up a thread that just scans
         the cache periodically and issues a sync on those files that
         need it *)
      let rec live_f_thread () = 
        (* Sleep for a bit *)
        With_lwt.(from_lwt (sleep 1.0)) >>= fun () -> 
        (* $(FIXME("""need to add sleep and yield to the monad_ops; also
           With_lwt should have sleep and yield using our monad, not
           lwt's""")) *)

        (* Get the files in the cache *)
        live_f#to_list () >>= fun xs -> 
        xs |> iter_k (fun ~k xs -> 
            match xs with
            | [] -> return ()
            | (fid,(f_ops:file_ops))::xs -> 
              (* $(FIXME("""we should be cleverer here... only sync if
                 the last synced time is more than a second or so
                 older than the last modified time""")) *)
              Printf.printf "%s: live_f thread syncing file %d\n%!" __FILE__ fid;
              f_ops.sync () >>= fun () -> 
              k xs) >>= fun () -> 

        (* And repeat *)
        live_f_thread ()
        
      let start_thread () = 
        async live_f_thread

      let _ = start_thread
    end 

    (* FIXME should this be here? what is the alternative? *)
    let _ = Live_f.start_thread ()
        
    let files : files_ops = 
(*
      let file_cache = Hashtbl.create 100 in
      let find fid = 
        match Hashtbl.find_opt file_cache fid with
        | Some x -> return x
        | None -> 
          gom_find (Fid fid) >>= fun blk_id -> 
          file_impl'#file_from_origin blk_id >>= fun ops ->
          Hashtbl.replace file_cache fid ops;
          return ops
      in
*)
      let find fid = Live_f.find fid in
      let create_file ~parent ~name ~times = 
        new_fid () >>= fun fid ->
        file_impl'#create_file times >>= fun blk_id ->
        gom_insert (Fid fid) blk_id >>= fun () ->
        dirs.find parent >>= fun p ->
        p.insert name (Fid fid)
      in
      let _ = assert(blk_sz|>Blk_sz.to_int >= 256) in
      let create_symlink ~parent ~name ~times:_ ~contents = 
        (* NOTE symlink times are currently ignored *)
        new_sid () >>= fun sid ->
        symlink_impl'#create_symlink contents >>= fun blk_id ->
        gom_insert (Sid sid) blk_id >>= fun () ->
        dirs.find parent >>= fun p ->
        p.insert name (Sid sid)
      in
      { find; create_file; create_symlink }

    let read_symlink : (sid -> (str_256,t)m) = fun sid -> 
      gom_find (Sid sid) >>= fun blk_id -> 
      symlink_impl#read_symlink ~blk_dev_ops ~blk_id

    open Tjr_path_resolution.Intf
    let resolve_comp: did -> comp_ -> ((fid,did,sid)resolved_comp,t)m = 
      fun did comp ->
      assert(String.length comp <= 256);
      let comp = Str_256.make comp in
      dirs.find did >>= fun dir_ops ->   
      dir_ops.find comp >>= fun vopt ->
      match vopt with
      | None -> return RC_missing
      | Some x -> 
        match x with
        | Fid fid -> RC_file fid |> return
        | Did did -> RC_dir did |> return
        | Sid sid -> 
          read_symlink sid >>= fun s ->
          RC_sym (sid, (s :> string)) |> return
    (* FIXME perhaps we also need to identify a symlink by id
       during path res? *)

    let fs_ops = {
      root=root_did;
      resolve_comp
    }

    (* NOTE for fuse we always resolve absolute paths *)
    let resolve = Tjr_path_resolution.resolve ~monad_ops ~fs_ops ~cwd:root_did

    let _ :
      follow_last_symlink:follow_last_symlink ->
      string ->
      ((fid, did,sid) resolved_path_or_err,t)m 
      = resolve

    let resolve_path = resolve

    let mk_stat_times = mk_stat_times

    (** {2 Extra ops} *)

    let _ = Printf.printf "%s: Stage_2 extra\n%!" __FILE__

    let extra : extra_ops = {
      internal_err=(fun s -> failwith s);  (* FIXME *)
      is_ancestor=(fun ~parent:_ ~child:_ -> return false) (* FIXME *)
    }

  end (* Stage_2 *)

  let make () : (_ Tjr_minifs.Minifs_intf.ops, t) m = 
    let _ = Printf.printf "%s: Stage_1 make() line %d\n%!" __FILE__  __LINE__ in
    fl_ops >>= fun fl_ops ->
    let _ = Printf.printf "%s: Stage_1 make() line %d\n%!" __FILE__  __LINE__ in
    gom_ops >>= fun gom_ops ->
    let _ = Printf.printf "%s: Stage_1 make() line %d\n%!" __FILE__  __LINE__ in
    counter_ops >>= fun counter_ops -> 
    let _ = Printf.printf "%s: Stage_1 make() line %d\n%!" __FILE__  __LINE__ in
    let module S2 = struct
      let fl_ops = fl_ops
      let gom_ops = gom_ops
      let counter_ops = counter_ops
    end
    in
    let _ = Printf.printf "%s: Stage_1 make() line %d\n%!" __FILE__  __LINE__ in
    let module X = Stage_2(S2) in
    let _ = Printf.printf "%s: Stage_1 make() line %d\n%!" __FILE__  __LINE__ in
    let module Y = Make_2(X) in
    let _ = Printf.printf "%s: Stage_1 make() line %d\n%!" __FILE__  __LINE__ in
    return Y.ops

  let _ = make
end


let make 
    ~blk_dev_ops
    ~fs_origin
    ~fl_params 
  : ( _ Tjr_minifs.Minifs_intf.ops,t)m
  = 
  let module S1 = struct
    let blk_dev_ops=blk_dev_ops
    let fs_origin = fs_origin
    let fl_params = fl_params
  end in
  let module X = Stage_1(S1) in
  X.make ()

let _ = make

(** What remains is to establish the blk_dev_ops and fs_origin (no
   need for origin syncing, since the 3 individual components have
   origin blocks whose location doesn't change) *)
