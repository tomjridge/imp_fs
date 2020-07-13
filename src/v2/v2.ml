(** V2 - compared to V1, this version implements files (via file_impl_v2).

We try to reuse V1_generic. Some of the following copied from v1.ml *)

[@@@warning "-33"]

open Tjr_monad.With_lwt
open Shared_ctxt
open Bin_prot.Std
open V2_intf
open V2_fs_impl
open V2_gom

module G = V2_generic


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

module Stage_1(S1:sig
    val blk_dev_ops : (r,blk,t)blk_dev_ops
    val fs_origin   : r Fs_origin_block.t
    (* FIXME replace with fl_examples#fl_params_1 *)
    val fl_params   : Tjr_plist_freelist.Freelist_intf.params
  end) = struct
  open S1

  let barrier () = return ()
  let sync () = return ()

  (** {2 New identifiers} *)

  (** We use mutually exclusive subsets of int for identifiers; NOTE 0
      is reserved for the root directory *)

  let counter_factory = V2_counter.example

  let counter_ops = 
    counter_factory#with_ ~blk_dev_ops ~sync |> fun o -> 
    o#init_from_disk fs_origin.counter_origin



  (** {2 The freelist } *)

  (** We need to read the freelist origin block, and resurrect the freelist *)

  let fl_params = object
    method tr_upper=2000
    method tr_lower=1000
    method min_free_alloc_size=500
  end

  (* FIXME we need to initialize the freelist at some point of course;
     an init functor? *)
  let freelist = 
    let fact = fl_examples#for_r in
    let with_ = 
      fact#with_
        ~blk_dev_ops
        ~barrier
        ~sync
        ~params:fl_params
    in
    with_#from_origin_with_autosync fs_origin.fl_origin

  let fl_ops = freelist >>= fun x -> 
    return x#freelist_ops


    (** {2 The global object map (GOM) } *)

  let gom_ops = fl_ops >>= fun fl_ops -> 
    (* FIXME freelistops should have the same intf as blk_alloc *)
    let blk_alloc : _ blk_allocator_ops =     
      {
        blk_alloc=fl_ops.alloc;
        blk_free=fl_ops.free
      }
    in
    V2_gom.gom_example#with_
      ~blk_dev_ops
      ~barrier
      ~sync
      ~freelist_ops:blk_alloc |> fun o -> 
    o#init_from_disk fs_origin.gom_origin

  (* let gom_ops = gom#map_ops_with_ls *)

  (* NOTE we also have to make sure we flush the GOM root... *)

  (** Stage_2: we assume freelist_ops, gom_ops and counter_ops are available *)
  module Stage_2(S2:sig 
      val fl_ops      : (r,r,t)Freelist_intf.freelist_ops 
      val gom_ops     : (dir_entry,blk_id,blk_id,t) Gom_ops.t
      val counter_ops : t V2_counter.counter_ops
    end) = struct
    open S2

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

    let id_to_int = Dir_impl.Dir_entry.(function
        | Did did -> did
        | Fid fid -> fid
        | Sid sid -> sid)

    let root_did = root_did


    (** {2 Blk allocator using freelist} *)

    let blk_alloc : _ blk_allocator_ops =     
      {
        blk_alloc=S2.fl_ops.alloc;
        blk_free=S2.fl_ops.free
      }

    let freelist_ops = blk_alloc

    (** {2 Dirs} *)

    let gom_find_opt = gom_ops.find

    let gom_find k = gom_find_opt k >>= function
      | Some r -> return r
      | None -> 
        Printf.printf "Error: gom key %d not found\n%!" (id_to_int k);
        failwith "gom: id did not map to an entry"

    let gom_insert = gom_ops.insert

    let gom_delete = gom_ops.delete


    let dir_impl = Dir_impl.dir_example
    let dir_impl' =             
      dir_impl#with_
        ~blk_dev_ops
        ~barrier
        ~sync
        ~freelist_ops


    let dirs : dirs_ops = 
      let find = (fun did -> 
          gom_find (Did did) >>= fun blk_id ->
          dir_impl'#dir_from_origin blk_id
        )
      in
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


    let file_impl = File_impl_v2.file_examples#example_1
    let file_impl' = 
      file_impl#with_
        ~blk_dev_ops
        ~barrier
        ~sync
        ~freelist_ops

    let symlink_impl = Symlink_impl.example
    let symlink_impl' =
      symlink_impl#with_
        ~blk_dev_ops
        ~freelist_ops

    let files : files_ops = 
      let find fid = 
        gom_find (Fid fid) >>= fun blk_id -> 
        file_impl'#file_from_origin blk_id
      in
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
    let resolve_comp: did -> comp_ -> ((fid,did)resolved_comp,t)m = 
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
          RC_sym (s :> string) |> return
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
      ((fid, did) resolved_path_or_err,t)m 
      = resolve

    let resolve_path = resolve

    let mk_stat_times = mk_stat_times

    (** {2 Extra ops} *)


    let extra : extra_ops = {
      internal_err=(fun s -> failwith s);  (* FIXME *)
      is_ancestor=(fun ~parent:_ ~child:_ -> return false) (* FIXME *)
    }

  end (* Stage_2 *)

  let make () : (_ Tjr_minifs.Minifs_intf.ops, t) m = 
    fl_ops >>= fun fl_ops ->
    gom_ops >>= fun gom_ops ->
    counter_ops >>= fun counter_ops -> 
    let module S2 = struct
      let fl_ops = fl_ops
      let gom_ops = gom_ops
      let counter_ops = counter_ops
    end
    in
    let module X = Stage_2(S2) in
    let module Y = Make_2(X) in
    return Y.ops
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

(** What remains is to establish the blk_dev_ops and fs_origin, and origin syncing *)
