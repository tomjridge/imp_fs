(* directories ------------------------------------------------------ *)
open Tjr_btree
open Small_string
open Bin_prot_util
open Imp_pervasives


(* store ops are specific to type: file_store_ops, dir_store_ops,
   omap_store_ops; *)
type dir_entry = fid_did [@@deriving bin_io]

type k = SS.t
type v = dir_entry

let bin_size_dir_entry = 

let ps = 
  Binprot_marshalling.mk_ps ~blk_sz 
    ~cmp:SS.compare ~k_size:bin_size_ss ~v_size:Omap_entry.bin_size_entry
    ~read_k:bin_reader_ss ~write_k:bin_writer_ss
    ~read_v:bin_reader_dir_entry ~write_v:bin_writer_dir_entry

let dir_store_ops : (k,v,page_ref,omap_state) store_ops = 
  Disk_to_store.disk_to_store ~ps ~disk_ops ~free_ops

let map_ops = Store_to_map.store_ops_to_map_ops ~ps ~store_ops:dir_store_ops
    ~kk:(fun ~map_ops ~find_leaf -> map_ops)

(* FIXME produce at same time as map_ops, via poly_rec *)
let ls_ops = Store_to_map.make_ls_ops ~ps ~store_ops:dir_store_ops
