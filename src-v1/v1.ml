(** V1 - implement directories and file metadata, with file data
   passed through to an underlying filesystem.

We have a single "global object map", which maps ids (ints) to either
   dir-roots, or file-meta, or symlinks.

NOTE much of this is based on {!Tjr_minifs.In_mem}, but with on-disk
   maps, rather than in-memory. *)

open Tjr_monad.With_lwt
open Std_types

open Bin_prot.Std


module F_meta = struct
  (** NOTE we store the file data in a file "n", where n is the id of the file. *)
  type f_meta = {
    atim: float;
    mtim: float;
  }[@@deriving bin_io]
end
open F_meta

type obj_root = blk_id  [@@deriving bin_io]

module Dir_entry = struct
  (* open Str_256 *)
  type dir_entry = 
    | F of obj_root
    | D of obj_root
    | S of obj_root
  [@@deriving bin_io]
end
open Dir_entry

type id = 
  | Fid of int
  | Did of int
  | Sid of int
[@@deriving bin_io]

let consistent_entry id e = 
  match id,e with
  | Fid _,F _  | Did _,D _ | Sid _, S _ -> true
  | _ -> false
  

(** {2 The global object map (GOM) } *)


module Pvt = struct
  open Tjr_btree.Make_3
       
  type k = id

  let k_mshlr : k bin_mshlr = 
    (module 
      (struct 
        type t = id[@@deriving bin_io] 
        let max_sz = 10 (* FIXME check 9+1 *)
      end))

  type v = dir_entry

  let v_mshlr : v bin_mshlr = 
    (module 
      (struct 
        type t = dir_entry[@@deriving bin_io] 
        let max_sz = 10 (* FIXME check 9+1 *)
      end))    

end
open Pvt


let gom_args = object
  method k_cmp: id -> id -> int = Stdlib.compare
  method k_mshlr = k_mshlr
  method v_mshlr = v_mshlr
end  


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

type ('k,'v,'t) uncached_btree = ('k,'v,'t)Tjr_btree.Make_3.uncached_btree

let gom_btree : (k,v,t) uncached_btree Lazy.t = lazy (
  Tjr_btree.Make_4.make
    ~args:gom_args
    ~blk_dev_ops:(Lazy.force blk_dev_ops)
    ~blk_alloc:(Lazy.force blk_alloc)
    ~root_ops:(Lazy.force root_ops))

let _ = gom_btree

(** Instantiate this after gom_btree has been initialized *)
module With_gom() = struct
  let blk_dev_ops = Lazy.force blk_dev_ops 
  let gom_btree = Lazy.force gom_btree 

  let find = gom_btree#find

  let insert = gom_btree#insert

  (** NOTE this checks that the entries are consistent, and drop the option type *)
  let find : id -> (dir_entry,t) m = fun i -> 
    find i >>= function 
    | None -> failwith (Printf.sprintf "%s: attempt to deref unknown identifier" __FILE__)
    | Some e -> 
      assert(consistent_entry i e);
      return e

  let lookup_file: int -> (f_meta,t)m = fun i ->
    find (Fid i) >>= function
    | F r -> 
      blk_dev_ops.read ~blk_id:r >>= fun blk ->
      F_meta.bin_read_f_meta blk ~pos_ref:(ref 0) |> fun f_meta ->
      return f_meta
    | _ -> failwith "impossible"
 
end

(* FIXME this is repeating a lot of stuff from in_mem; perhaps we should abstract? *)


