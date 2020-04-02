(** V1 - implement directories and file metadata, with file data
   passed through to an underlying filesystem.

We have a single "global object map", which maps ids (ints) to either
   dir-roots, or file-meta, or symlinks.

NOTE much of this is based on {!Tjr_minifs.In_mem}, but with on-disk
   maps, rather than in-memory. *)

open Tjr_monad.With_lwt
open Std_types
open Bin_prot.Std
module G = Generic
open G

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

(** derivied types *)
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
  { st_atim=t; st_mtim=t }



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


(** We use mutually exclusive subsets of int for identifiers *)
let min_free_id_ref = ref 0
let new_id () = 
  let r = !min_free_id_ref in
  incr min_free_id_ref;
  return r

let new_did = new_id

let new_fid = new_id


(** {2 The global object map (GOM) } *)


module Gom = struct
  open Tjr_btree.Make_3
  open Ms

  (** Could just use disjoint subsets of int and drop the constructors *)

  type id = 
    | Gom_fid of int
    | Gom_did of int
    | Gom_sid of int
  [@@deriving bin_io]

  let id_mshlr : id bin_mshlr = 
    (module 
      (struct 
        type t = id[@@deriving bin_io] 
        let max_sz = 10 (* FIXME check 9+1 *)
      end))
  
  let consistent_entry id e = 
    match id,e with
    | Gom_fid _,Fid _  | Gom_did _,Did _ | Gom_sid _, Sid _ -> true
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
open Gom

type ('k,'v,'t) uncached_btree = ('k,'v,'t)Tjr_btree.Make_3.uncached_btree

let gom_btree : (Gom.k,Gom.v,t) uncached_btree Lazy.t = lazy (
  Tjr_btree.Make_4.make
    ~args:gom_args
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
    | None -> failwith "gom: id did not map to an entry"
    
  let gom_insert = gom_btree#insert

  (** In order to incorporate path resolution, we need to be able to
     lookup an entry in a directory *)

  module Dir_root_blk = struct
    type rb = { map_root:blk_id; parent:did; times:stat_times }[@@deriving bin_io]
    let write_to: blk_id:blk_id -> rb -> (unit,t)m = 
      failwith "FIXME"

    let read_from: blk_id:blk_id -> (rb,t)m = failwith "FIXME"

    (* FIXME we could perhaps split dir_ops into the
       find/insert/delete operations on the map, and the other
       operations that work with the root block *)
  end

  module Dir = struct
    open Ms
    open Tjr_btree.Make_3
    open Str_256
    type k = str_256
    type v = id
      
    let k_mshlr = s256_mshlr
    let v_mshlr = id_mshlr
    
    let dir_args = object
      method k_cmp: str_256 -> str_256 -> int = Stdlib.compare
      method k_mshlr = k_mshlr
      method v_mshlr = v_mshlr
    end

    let dir_ops ~root_ops = 
      Tjr_btree.Make_4.make 
        ~args:dir_args
        ~blk_dev_ops
        ~blk_alloc
        ~root_ops

    let _ : root_ops:_ -> (k,v,t) uncached_btree = dir_ops

    let _ = gom_find

    open Tjr_path_resolution.Intf
    let resolve_comp: did -> comp_ -> ((fid,did)resolved_comp,t)m = 
      fun {did} comp ->
      assert(String.length comp <= 256);
      let comp = Str_256.make comp in
      gom_find (Gom_did did) >>= fun (r:blk_id) -> 
      let r = ref r in
      let root_ops = with_ref r in
      let dir_ops = dir_ops ~root_ops in
      dir_ops#find comp >>= fun vopt ->
      match vopt with
      | None -> return RC_missing
      | Some x -> 
        match x with
        | Gom_fid fid -> RC_file {fid} |> return
        | Gom_did did -> RC_dir {did} |> return
        | Gom_sid _i -> 
          (* FIXME perhaps we also need to identify a symlink by id
             during path res? *)
          failwith "FIXME need to lookup the contents of the sid"[@@warning "-8"]
                      
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
    
  end
  let resolve = Dir.resolve
end




(** {2 Instantiate T2 - T2_impl} *)

(*
module T2_impl : T2 = struct
  let root_did = {did=0}
  let dirs : dirs_ops = {
  }
end
*)




