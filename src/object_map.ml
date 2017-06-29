(* global object map ------------------------------------------------ *)

open Tjr_btree
open Btree_api
open Block
open Page_ref_int

let blk_sz = 4096

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

  type entry = F of int * int | D of int [@@deriving bin_io]  
  type t = entry
  type omap_entry = entry

  let sz = 1 (* tag*) + 2*Bin_prot_util.bin_size_int
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



module Disk = struct
  open Btree_api
  let disk_ops : S.t disk_ops = failwith "TODO"
end
include Disk




(* store ops are specific to type: file_store_ops, dir_store_ops,
   omap_store_ops; all stores share the same free space map *)
module Free = struct

  open S
  open Monad
  let free_ops : (blk_id,S.t) mref = {
    get=(fun () -> (fun t -> (t,Ok t.free)));
    set=(fun free -> fun t -> ({t with free}, Ok ()));
  }
end
include Free




module Dir = struct

  open Small_string
  open Bin_prot_util
  type k = SS.t
  type v = oid
  
  let ps = 
    Binprot_marshalling.mk_ps ~blk_sz 
      ~cmp:SS.compare ~k_size:bin_size_ss ~v_size:bin_size_int
      ~read_k:bin_reader_ss ~write_k:bin_writer_ss
      ~read_v:bin_reader_int ~write_v:bin_writer_int

  let dir_store_ops : (k,v,page_ref,S.t) store_ops = 
    Disk_to_store.disk_to_store ~ps ~disk_ops ~free_ops

  let dir_map_ops = Store_to_map.store_ops_to_map_ops ~ps ~store_ops:dir_store_ops
end




(* store_ops for file *)
module File = struct

  open Small_string
  open Bin_prot_util
  open Monad

  (* int -> blk_id map *)
  module Map_int_blk_id = struct
    let ps = Map_int_int.ps' ~blk_sz
    let store_ops = Disk_to_store.disk_to_store ~ps ~disk_ops ~free_ops
    let map_ops ~page_ref_ops = Store_to_map.store_ops_to_map_ops ~ps ~store_ops 
        ~page_ref_ops
  end
  include Map_int_blk_id

  (* write_blk and read_blk based on disk_ops *)
  let write_blk blk = (
    free_ops.get () |> bind (fun blk_id -> 
        disk_ops.write blk_id blk |> bind (fun _ -> 
            free_ops.set (blk_id+1) |> bind (fun () -> 
                return blk_id))))

  let read_blk blk_id = (
    disk_ops.read blk_id |> bind (fun blk ->
        return (Some blk)))
      
  (* files are implemented using the (index -> blk) map *)
  let mk_file_ops ~page_ref_ops = 
    let map_ops = map_ops ~page_ref_ops in
    Map_int_blk.mk_int_blk_map ~write_blk ~read_blk ~map_ops
  
end





module Omap = struct
  
  open Bin_prot_util
  open E
  type k = oid
  type v = entry
  let v_size = E.sz

  let ps = 
    Binprot_marshalling.mk_ps ~blk_sz
      ~cmp:Int_.compare ~k_size:bin_size_int ~v_size
      ~read_k:bin_reader_int ~write_k:bin_writer_int
      ~read_v:bin_reader_entry ~write_v:bin_writer_entry

  let store_ops = Disk_to_store.disk_to_store ~ps ~disk_ops ~free_ops

  let map_ops ~page_ref_ops = Store_to_map.store_ops_to_map_ops ~ps ~store_ops 
        ~page_ref_ops

end





(* we have to implement pread and pwrite on top of omap, since size is
   stored in omap *)
module File_ops = struct

  

end


let omap_ops : omap_ops = failwith ""


(* TODO instantiate the omap with cache *)


