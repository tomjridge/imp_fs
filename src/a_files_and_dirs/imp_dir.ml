(* directories ------------------------------------------------------ *)


open Imp_pervasives
open X.Store_to_map
open X.Small_string
open X.Bin_prot_util

open Imp_state
open Disk_ops  (* FIXME? nameclash with tjr_btree *)
open Free_ops
open Page_ref_ops

(* store ops are specific to type: file_store_ops, dir_store_ops,
   omap_store_ops; *)

type dir_entry = Fid of fid | Did of did [@@deriving bin_io]
let bp_size_dir_entry = 1+bp_size_int  (* FIXME check *)

type k = ss
type v = dir_entry


let ps = 
  X.Binprot_marshalling.mk_binprot_ps ~blk_sz 
    ~cmp:SS.compare ~k_size:bp_size_ss ~v_size:bp_size_dir_entry
    ~read_k:bin_reader_ss ~write_k:bin_writer_ss
    ~read_v:bin_reader_dir_entry ~write_v:bin_writer_dir_entry

let dir_store_ops : [<`Store_ops of 'a] = 
  X.Disk_to_store.disk_to_store ~ps ~disk_ops ~free_ops

let map_ops ~page_ref_ops : [< `Map_ops of 'a ] = 
  store_ops_to_map_ops ~ps ~page_ref_ops ~store_ops:dir_store_ops

(* FIXME produce at same time as map_ops, via poly_rec *)
let ls_ops ~page_ref_ops : [< `Ls_ops of 'a] = 
  make_ls_ops ~ps ~store_ops:dir_store_ops ~page_ref_ops
