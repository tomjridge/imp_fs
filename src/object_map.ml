(* global object map ------------------------------------------------ *)

open Tjr_btree
open Btree_api
open Block
open Page_ref_int


(* object ids *)

type object_id = int 
type oid = object_id


(* file entries *)

type sz = int  (* 32 bits suffices? *)

type f_ent = blk_id * sz 

type d_ent = blk_id 

(* CHOICE do we want to have two maps (one for files, one for dirs) or
   just one (to sum type)? for simplicity have one for the time being *)

module Entries = struct
  open Bin_prot.Std
  (* these types should match f_ent and d_ent; don't want derivings
     everywhere *)

  type t = F of int * int | D of int [@@deriving bin_io]  
  type omap_entry = t 

  let sz = 1 (* tag*) + 2*Bin_prot.Size.Maximum.bin_size_int
end
module E = Entries


(* "omap" is the "global" object map from oid to Ent.t *)

(* global state TODO *)
module S = struct
  type t = {

    (* We always allocate new blocks. *)
    free: page_ref;

    (* The omap is represented by a pointer to the B-tree *)
    omap_root: blk_id;

    (* The omap is cached. *)
    omap_cache: unit; (* TODO *)

    (* This is the "global transaction log", which records synced
       objects without requiring a sync of the object map. Represented
       using a B-tree or (better?) a persistent on-disk list. *)
    omap_additional_object_roots: blk_id; 

    (* The B-tree backing each file also has a cache. *)
    file_caches: oid -> unit;

    (* Ditto directories *)
    dir_caches: oid -> unit;

    (* TODO other layers of caching *)
  }
end

type omap_ops = (oid,E.t,S.t) map_ops


(* TODO instantiate the omap with cache *)

module Int_ = Examples_common.Bin_prot_int

module Dir = struct
  open Small_string
  open Bin_prot_max_sizes
  type k = SS.t
  type v = oid
  
  let ps = 
    Binprot_marshalling.mk_ps ~blk_sz:4096 
      ~cmp:SS.compare ~k_size:bin_size_ss ~v_size:bin_size_int
      ~read_k:SS.bin_reader_t ~write_k:SS.bin_writer_t
      ~read_v:Bin_prot.Std.bin_reader_int ~write_v:Bin_prot.Std.bin_writer_int

  open Examples_common
  let x = mk_example ~ps
  let dir_ops = map_ops x
end


module File = struct

  (* int -> blk_id map *)
  let map_ops = Map_int_int.map_ops

  (* files are implemented using the (index -> blk) map *)
  let mk_file_ops ~write_blk ~read_blk = 
    Map_int_blk.mk_int_blk_map ~write_blk ~read_blk ~map_ops

end


module Omap = struct
  
  type k = oid
  type v = E.t
  let v_size = E.sz

  let ps = 
    Binprot_marshalling.mk_ps ~blk_sz:4096 
      ~cmp:Int_.compare ~k_size:Int_.size ~v_size
      ~read_k:Int_.bin_reader_t ~write_k:Int_.bin_writer_t
      ~read_v:E.bin_reader_t ~write_v:E.bin_writer_t

  open Examples_common
  let x = mk_example ~ps
      
  let map_ops = map_ops x

end





let omap_ops : omap_ops = failwith ""


