open Tjr_btree
open Btree_api
(* store_ops for file *)
open Small_string
open Bin_prot_util
open Monad

open Imp_pervasives
open Disk_ops
open Imp_state

(* int -> blk_id map *)
module Map_int_blk_id = struct
  let ps = Map_int_int.ps' ~blk_sz
  let store_ops = Disk_to_store.disk_to_store ~ps ~disk_ops ~free_ops
  let map_ops ~page_ref_ops = Store_to_map.store_ops_to_map_ops ~ps 
      ~store_ops  ~page_ref_ops ~kk:(fun ~map_ops ~find_leaf -> map_ops)
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
let mk_map_int_blk_ops ~page_ref_ops = 
  let map_ops = map_ops ~page_ref_ops in
  Map_int_blk.mk_int_blk_map ~write_blk ~read_blk ~map_ops
