(** V1 - implement directories and file metadata, with file data
   passed through to an underlying filesystem.

We have a single "global object map", which maps ids (ints) to either
   dir-roots, or file-meta, or symlinks.

NOTE much of this is based on {!Tjr_minifs.In_mem}, but with on-disk
   maps, rather than in-memory. *)

open Tjr_monad.With_lwt
open Std_types
open Bin_prot.Std
module G = V1_generic
(* open G *)

type ('k,'v,'t) uncached_btree = ('k,'v,'t)Tjr_btree.Make_3.uncached_btree

(* FIXME move to std_types *)
let buf_create () = buf_ops.create (Blk_sz.to_int blk_sz)

let s256_to_string (s:str_256) = (s :> string)

(** Free blocks *)
let min_free_blk = ref 0
let get_min_free_blk () = 
  let r = !min_free_blk in
  incr min_free_blk;
  r

let b0_system_root_blk = get_min_free_blk()
let b1_gom_map_root = get_min_free_blk()


(** {2 Setup the generic instance} *)

(** Base types *)
module S0 = struct
  type t = lwt
  let monad_ops = monad_ops
  type fid = {fid:int}[@@deriving bin_io]
  type did = {did:int}[@@deriving bin_io]
  type sid = {sid:int}[@@deriving bin_io]
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

(** {2 Some simple defns we can complete here} *)

let root_did = {did=0}

let mk_stat_times () = 
  Unix.time () |> fun t -> (* 1s resolution? FIXME *)
  return Times.{ atim=t; mtim=t }



(** {2 Implement definitions from T2, ie, the core filesystem} *)

type obj_root = blk_id  [@@deriving bin_io]

(*
module Dir_entry = struct
  (* open Str_256 *)
  (** NOTE again, could just use a plain blk_id/int and drop the constructors *)
  type dir_entry = 
    | F of obj_root
    | D of obj_root
    | S of obj_root
  [@@deriving bin_io]
end
open Dir_entry
*)

type dir_entry = S1.dir_entry
;;


(** {2 Various marshallers} *)

module Ms = struct
  open Tjr_btree.Make_3

(*
  let id_mshlr : id bin_mshlr = 
    (module 
      (struct 
        type t = id[@@deriving bin_io] 
        let max_sz = 10 (* FIXME check 9+1 *)
      end))
*)

  let int_mshlr : int bin_mshlr = 
    (module 
      (struct 
        type t = int[@@deriving bin_io] 
        let max_sz = 10 (* FIXME check 9+1 *)
      end))

  let blk_id_mshlr : blk_id bin_mshlr = 
    (module 
      (struct 
        type t = blk_id[@@deriving bin_io] 
        let max_sz = 10 (* FIXME check 9+1 *)
      end))

  let dir_entry_mshlr : dir_entry bin_mshlr = 
    (module 
      (struct 
        type t = dir_entry[@@deriving bin_io] 
        let max_sz = 10 (* FIXME check 9+1 *)
      end))

  open Str_256
  let s256_mshlr : str_256 bin_mshlr =
    (module 
      (struct 
        type t = str_256[@@deriving bin_io] 
        let max_sz = 259 (* FIXME check 256+3 *)
      end))
end



(** {2 Various runtime components that get filled in later} *)

let blk_dev_ref = ref None
let blk_dev_ops : (_,_,_) blk_dev_ops Lazy.t = lazy (
  match !blk_dev_ref with
  | None -> failwith __LOC__
  | Some x -> x)


let blk_alloc_ref = ref None  
let blk_alloc : (_,_)blk_allocator_ops Lazy.t = lazy (
  match !blk_alloc_ref with 
  | None -> failwith __LOC__
  | Some x -> x)


let root_ops_ref = ref None
let root_ops : (_,_)with_state Lazy.t = lazy (
  match !root_ops_ref with
  | None -> failwith __LOC__
  | Some x -> x)


(** We use mutually exclusive subsets of int for identifiers; NOTE 0
   is reserved for the root directory *)
let min_free_id_ref = ref 1
let new_id () = 
  let r = !min_free_id_ref in
  incr min_free_id_ref;
  return r

let new_did () = new_id () >>= fun did -> return {did}

let new_fid () = new_id () >>= fun fid -> return {fid}

let new_sid () = new_id () >>= fun sid -> return {sid}

let id_to_int = function
  | Did did -> did.did
  | Fid fid -> fid.fid
  | Sid sid -> sid.sid


(** {2 The global object map (GOM) } *)


module Gom = struct
  open Tjr_btree.Make_3
  open Ms

  (** Could just use disjoint subsets of int and drop the constructors *)

  type id = dir_entry
  [@@deriving bin_io]

  let id_mshlr : id bin_mshlr = 
    (module 
      (struct 
        type t = id[@@deriving bin_io] 
        let max_sz = 10 (* FIXME check 9+1 *)
      end))
  
  let consistent_entry id e = 
    match id,e with
    | Fid _,Fid _  | Did _,Did _ | Sid _, Sid _ -> true
    | _ -> false

  type k = id[@@deriving bin_io]

  let k_mshlr : k bin_mshlr = id_mshlr

  (** The gom maps an id to a root blk *)
  type v = blk_id

  let v_mshlr : v bin_mshlr = blk_id_mshlr

  let gom_args = object
    method k_cmp: id -> id -> int = Stdlib.compare
    method k_mshlr = k_mshlr
    method v_mshlr = v_mshlr
  end  
end
(* open Gom *)

let gom_empty_leaf_as_blk, (gom_btree : (Gom.k,Gom.v,t) uncached_btree Lazy.t) = 
  Tjr_btree.Make_4.make
    ~args:Gom.gom_args |> fun obj ->
  obj#empty_leaf_as_blk, 
  lazy (obj#rest 
          ~blk_dev_ops:(Lazy.force blk_dev_ops)
          ~blk_alloc:(Lazy.force blk_alloc)
          ~root_ops:(Lazy.force root_ops))

let _ = gom_btree



(** {2 Definitions after the GOM has been initialized} *)

(** NOTE Instantiate this after gom_btree has been initialized *)
module With_gom() = struct
  let blk_dev_ops = Lazy.force blk_dev_ops 
  let blk_alloc = Lazy.force blk_alloc
  let gom_btree = Lazy.force gom_btree 

  let gom_find_opt = gom_btree#find

  let gom_find k = gom_find_opt k >>= function
    | Some r -> return r
    | None -> 
      Printf.printf "Error: gom key %d not found\n%!" (id_to_int k);
      failwith "gom: id did not map to an entry"
    
  let gom_insert = gom_btree#insert

  let gom_delete = gom_btree#delete


  (** In order to incorporate path resolution, we need to be able to
     lookup an entry in a directory *)

  module Make_root_blk_ops(S:sig type rb[@@deriving bin_io] end) = struct
    include S
    let write_rb ~blk_id rb =
      (* FIXME have a single buf_create, specialized to std_type and buf_sz *)
      let buf = buf_create () in
      bin_write_rb buf ~pos:0 rb |> fun _ ->
      blk_dev_ops.write ~blk_id ~blk:buf
    let read_rb: blk_id:blk_id -> (rb,t)m = fun ~blk_id ->
      blk_dev_ops.read ~blk_id >>= fun blk ->
      bin_read_rb blk ~pos_ref:(ref 0) |> fun rb ->
      return rb
  end


  (* FIXME we could perhaps split dir_ops into the find/insert/delete
     operations on the map, and the other operations that work with
     the root block *)

  module Dir = struct
    open Ms
    open Tjr_btree.Make_3
    open Str_256

    module Rb = struct type rb = { map_root:blk_id; parent:did; times:Times.times }[@@deriving bin_io] end    
    include Make_root_blk_ops(Rb)

    type k = str_256
    type v = Gom.id

    let k_mshlr = s256_mshlr
    let v_mshlr = Gom.id_mshlr

    let dir_args = object
      method k_cmp: str_256 -> str_256 -> int = Stdlib.compare
      method k_mshlr = k_mshlr
      method v_mshlr = v_mshlr
    end

    let dir_empty_leaf_as_blk,dir_map_ops = 
      Tjr_btree.Make_4.make ~args:dir_args |> fun obj ->
      obj#empty_leaf_as_blk, fun ~root_ops -> obj#rest 
        ~blk_dev_ops
        ~blk_alloc
        ~root_ops

    let _ : root_ops:_ -> (k,v,t) uncached_btree = dir_map_ops

    let _ = gom_find

    let read_symlink: (sid -> (str_256,t)m) ref = ref (fun _sid -> 
      failwith "impossible: this is patched later")

    open Tjr_path_resolution.Intf
    let resolve_comp: did -> comp_ -> ((fid,did)resolved_comp,t)m = 
      fun did comp ->
      assert(String.length comp <= 256);
      let comp = Str_256.make comp in
      gom_find (Did did) >>= fun (blk_id:blk_id) ->   
      read_rb ~blk_id >>= fun rb ->
      let r = ref rb.map_root in
      (* FIXME this shares code with dirs below; here, we don't expect
         set_state to be called on root_ops *)
      let root_ops = with_ref r in
      let dir_map_ops = dir_map_ops ~root_ops in
      dir_map_ops#find comp >>= fun vopt ->
      match vopt with
      | None -> return RC_missing
      | Some x -> 
        match x with
        | Fid fid -> RC_file fid |> return
        | Did did -> RC_dir did |> return
        | Sid sid -> 
          (!read_symlink) sid >>= fun s ->
          RC_sym (s256_to_string s) |> return
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

    let write_empty_leaf ~blk_id =
      blk_dev_ops.write ~blk_id ~blk:(dir_empty_leaf_as_blk ())

  end
  let resolve_path = Dir.resolve

  (** The dirs ops, find and delete FIXME these need to be maintained
     in a pool, and only one instance per id *)
  let dirs : dirs_ops = {
    find = (fun did -> 
        gom_find (Did did) >>= fun blk_id ->
        Dir.read_rb ~blk_id >>= fun rb ->
        let rb = ref rb in
        let write_rb () = Dir.write_rb ~blk_id (!rb) in
        let root_ops = 
          let with_state f = 
            f ~state:(!rb.map_root) ~set_state:(fun map_root ->
                rb:={!rb with map_root};
                write_rb())
          in
          {with_state}
        in
        let dir_map_ops = Dir.dir_map_ops ~root_ops in
        let set_parent did = 
          rb:={!rb with parent=did};
          write_rb()
        in
        let get_parent () = (!rb).parent |> return in
        let set_times times =
          rb:={!rb with times};
          write_rb()
        in
        let get_times () = (!rb).times |> return in          
        let dir_ops = {
          find=dir_map_ops#find;
          insert=dir_map_ops#insert;
          delete=dir_map_ops#delete;
          ls_create=dir_map_ops#ls_create;
          set_parent;
          get_parent;
          set_times;
          get_times;
        }
        in
        return dir_ops);                           
    delete = (fun did -> gom_delete (Did did))
  }


  module File = struct

    (* FIXME we really want sz to be stored in the btree, not in the
       underlying file *)
    (* FIXME of course, there should be a pool, and fds should be
       closed when files expunged from pool *)
    module Rb = struct
      type rb = { times: Times.times }[@@deriving bin_io]
    end
    include Make_root_blk_ops(Rb)

    let fn fid = Printf.sprintf "./tmp/v1_files/%d" fid.fid

    let get_fd : fid:fid -> foff:int -> (Lwt_unix.file_descr,t)m = fun ~fid ~foff ->
      let default_file_perm = Tjr_file.default_create_perm in
      (from_lwt Lwt_unix.(openfile (fn fid) [O_RDWR] default_file_perm)) >>= fun fd ->
      (from_lwt Lwt_unix.(lseek fd foff SEEK_SET)) >>= fun (_:int) ->
      return fd

    let close fd = (from_lwt Lwt_unix.(close fd))

    let pread ~fid ~foff ~len ~buf ~boff = 
      get_fd ~fid ~foff >>= fun fd ->
      let bs = Bytes.create (buf_ops.len buf - boff) in
      (from_lwt Lwt_unix.(read fd bs boff len)) >>= fun (n:int) ->
      Bigstring.blit_of_bytes bs 0 buf boff n;
      close fd >>= fun () ->
      return (Ok n)

    let pwrite ~fid ~foff ~len ~buf ~boff = 
      Printf.printf "pwrite 1: fid=%d foff=%d len=%d boff=%d \n%!" fid.fid foff len boff;
      get_fd ~fid ~foff >>= fun fd ->
      Printf.printf "pwrite 2\n%!";
      let bs = Bigstring.to_bytes buf in  (* FIXME don't need whole buf *)
      Printf.printf "pwrite 3\n%!";
      (from_lwt Lwt_unix.(write fd bs boff len)) >>= fun n ->
      Printf.printf "pwrite 4\n%!";
      assert(n=len); (* FIXME *)
      Bigstring.blit_of_bytes bs boff buf boff n;
      close fd >>= fun () ->
      Printf.printf "pwrite 5\n%!";
      return (Ok n)

    let truncate ~fid len = 
      (from_lwt Lwt_unix.(truncate (fn fid) len))
      
    let get_sz ~fid () =
      (from_lwt Lwt_unix.(stat (fn fid))) >>= fun st -> 
      return st.st_size

    let file_ops ~fid = 
      gom_find (Fid fid) >>= fun blk_id ->
      read_rb ~blk_id >>= fun rb ->
      let rb = ref rb in
      let set_times times = 
        rb:={times};
        write_rb ~blk_id !rb
      in
      let get_times () =
        (!rb).times |> return
      in
      return { pread=pread ~fid;
               pwrite=pwrite ~fid;
               truncate=truncate ~fid;
               get_sz=get_sz ~fid;
               set_times;
               get_times
             }          
  end

  (** The files ops *)
  module Files = struct
    open File
    let files = {
      find=(fun fid -> File.file_ops ~fid);
      create=(fun fid times -> 
          let perm = Tjr_file.default_create_perm in
          (* FIXME we assume it doesn't already exist *)
          from_lwt Lwt_unix.(openfile (fn fid) [O_CREAT;O_RDWR] perm) >>= fun fd ->
          from_lwt Lwt_unix.(close fd) >>= fun () ->
          (* we also have to create a root block *)
          blk_alloc.blk_alloc () >>= fun blk_id ->
          write_rb ~blk_id { times } >>= fun () ->
          (* and insert into gom *)
          gom_insert (Fid fid) blk_id
        );
      (* delete=(fun fid -> gom_delete (Fid fid)) (\** NOTE no gc *\) *)
    }

  end
  let files: files_ops = Files.files

  (** Extra ops *)
  let extra : extra_ops = {
    internal_err=(fun s -> failwith s);  (* FIXME *)
    is_ancestor=(fun ~parent:_ ~child:_ -> return false) (* FIXME *)
  }

  (** create_dir, create_file and create_symlink *)
      
  [@@@warning "-27"]

  let create_file ~parent ~name ~times = 
    new_fid () >>= fun fid -> 
    files.create fid times >>= fun () ->
    dirs.find parent >>= fun dir ->
    dir.insert name (Fid fid)
      
  let create_dir_ ~(is_root:bool) ~parent ~name ~times = 
    (* the name argument is ignored if we are creating the root dir *)
    assert( if is_root then name=(Str_256.make "") else true);
    (match is_root with
     | true -> return root_did
     | false -> new_did ()) >>= fun did ->
    blk_alloc.blk_alloc () >>= fun blk_id ->
    blk_alloc.blk_alloc () >>= fun map_root ->
    (* initialize the empty leaf blk *)
    Dir.write_empty_leaf ~blk_id:map_root >>= fun () ->
    (* write the root block itself *)
    Dir.(write_rb ~blk_id { map_root; parent; times }) >>= fun () ->
    (* if we are creating the root, we don't insert it into itself *)
    (match is_root with
       | true -> return ()
       | false -> 
         (* add new dir to parent *)
         dirs.find parent >>= fun dir ->
         dir.insert name (Did did)) >>= fun () ->
    (* make sure to insert into gom *)
    Printf.printf "%s: adding Did %d to gom\n%!" __FILE__ (id_to_int (Did did));
    gom_insert (Did did) blk_id >>= fun () ->
    return ()

  let create_dir ~parent ~name ~times = 
    create_dir_ ~is_root:false ~parent ~name ~times

  let create_root_dir ~times = 
    create_dir_ ~is_root:true ~parent:root_did ~name:(Str_256.make "") ~times

  module Symlink = struct
    module Rb = struct open Str_256 type rb={contents:str_256}[@@deriving bin_io] end
    include Make_root_blk_ops(Rb)
  end

  let create_symlink ~parent ~name ~times ~contents = 
    new_sid () >>= fun sid ->
    blk_alloc.blk_alloc () >>= fun blk_id ->
    Symlink.write_rb ~blk_id {contents} >>= fun () ->
    (* add new symlink to parent *)
    dirs.find parent >>= fun dir ->
    dir.insert name (Sid sid) >>= fun () ->
    gom_insert (Sid sid) blk_id >>= fun () ->
    return ()
  (* FIXME we also need readlink to be implemented properly - see path res *)
      
  let read_symlink sid = 
    gom_find (Sid sid) >>= fun blk_id ->
    Symlink.read_rb ~blk_id >>= fun rb ->
    return rb.contents

  (* patch the prior ref at runtime, to avoid reordering adn mutual
     dependency between create_symlink and Dir *)
  let _ = Dir.read_symlink := read_symlink


  (** {2 Instantiate filesystem} *)
  module X = struct
    let root_did = root_did
    let dirs = dirs
    let files = files
    let resolve_path = resolve_path
    let mk_stat_times = mk_stat_times
    let extra = extra
    let create_dir = create_dir
    let create_file = create_file
    let create_symlink = create_symlink
  end
  module The_filesystem = Make_2(X)

end
