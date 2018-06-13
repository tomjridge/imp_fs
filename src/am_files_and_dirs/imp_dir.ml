(** Implementation of directories as maps via the B-tree *)

(* directories ------------------------------------------------------ *)

open Imp_pervasives
open Tjr_btree.Store_to_map
open Tjr_btree.Small_string
open Tjr_btree.Bin_prot_util

open Imp_state
open Imp_disk_ops 
open Imp_free_ops
(* open Page_ref_ops *)

(* store ops are specific to type: file_store_ops, dir_store_ops,
   omap_store_ops; *)

type dir_entry = Fid of fid | Did of did [@@deriving bin_io]
let bp_size_dir_entry = 1+bp_size_int  (* FIXME check *)

type k = ss
type v = dir_entry


let ps = 
  Tjr_btree.Binprot_marshalling.mk_binprot_ps ~blk_sz 
    ~cmp:SS.compare ~k_size:bp_size_ss ~v_size:bp_size_dir_entry
    ~read_k:bin_reader_ss ~write_k:bin_writer_ss
    ~read_v:bin_reader_dir_entry ~write_v:bin_writer_dir_entry

let _ = ps

(* FIXME remove dbg_ps from ps *)

let dir_store_ops = Tjr_btree.Disk_to_store.disk_to_store 
    ~monad_ops:imp_monad_ops ~ps ~disk_ops ~free_ops

(* each directory has its own page ref of course *)
let map_ops ~page_ref_ops = 
  store_ops_to_map_ops
    ~monad_ops:imp_monad_ops
    ~constants:(ps#constants)
    ~cmp:(ps#cmp)
    ~page_ref_ops 
    ~store_ops:dir_store_ops

let _ = map_ops

open Tjr_btree.Leaf_stream_ops
(* FIXME produce at same time as map_ops, via poly_rec *)
let ls_ops ~page_ref_ops : ('k,'v,'r,'t) leaf_stream_ops = 
  Tjr_btree.Store_to_map.store_ops_to_ls_ops
    ~monad_ops:imp_monad_ops
    ~constants:(ps#constants)
    ~cmp:(ps#cmp)
    ~store_ops:dir_store_ops 
