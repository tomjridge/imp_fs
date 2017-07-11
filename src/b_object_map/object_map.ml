(* global object map ------------------------------------------------ *)

(** We maintain a single global object map, from oid (int) to entry. *)

open Tjr_btree
open Btree_api
open Block
open Page_ref_int
open Bin_prot_util
open Omap_entry
open Imp_pervasives
open Imp_state

(* invariant: fid maps to file, did maps to dir; package as two ops? *)
type omap_ops = (oid,omap_entry,imp_state) map_ops

type size = int

type omap_fid_ops = (fid,blk_id*size,imp_state) map_ops

let omap_fid_ops = failwith "TODO"

type omap_did_ops = (did,blk_id,imp_state) map_ops

let omap_did_ops = failwith "TODO"



(* object map ------------------------------------------------------- *)

module Omap = struct
  
  open Bin_prot_util
  open Omap_entry
  open Disk_ops
  type k = oid
  type v = omap_entry
  let v_size = bp_size_omap_entry

  let ps = 
    Binprot_marshalling.mk_ps ~blk_sz
      ~cmp:Int_.compare ~k_size:bp_size_int ~v_size
      ~read_k:bin_reader_int ~write_k:bin_writer_int
      ~read_v:bin_reader_omap_entry ~write_v:bin_writer_omap_entry

  let store_ops = Disk_to_store.disk_to_store ~ps ~disk_ops ~free_ops

  let page_ref_ops = failwith "TODO"

  let map_ops = Store_to_map.store_ops_to_map_ops ~ps ~store_ops 
      ~page_ref_ops ~kk:(fun ~map_ops ~find_leaf -> map_ops)

  open Monad

  (* return get/set for a particular oid *)
  let did_to_page_ref_ops ~did = 
    did |> did2oid |> fun oid ->
    { 
      get=(fun () -> map_ops.find oid |> bind @@ fun ent_opt ->
        match ent_opt with
        | None -> failwith __LOC__ (* TODO impossible? oid has been deleted? *)
        | Some ent -> 
          let open Omap_entry in 
          match ent with
          | File_blkid_sz(r,sz) -> failwith __LOC__ (* TODO invariant did is a dir *)
          | Dir_blkid(r) -> return r);
      set=(fun r -> map_ops.insert oid (Dir_blkid(r)))
    }

  let lookup_did ~did =     
    did |> did2oid |> fun oid ->
    map_ops.find oid |> bind @@ fun ent_opt ->
    match ent_opt with
    | None -> failwith __LOC__  (* TODO old reference no longer valid? *)
    | Some ent -> 
      match ent with
      | File_blkid_sz (r,sz) -> failwith __LOC__
      | Dir_blkid r -> return r
                         
  let did_to_map_ops ~did = 
    lookup_did ~did |> bind @@ fun r ->
    let page_ref_ops = did_to_page_ref_ops ~did in
    return (Imp_dir.map_ops ~page_ref_ops)
      (* TODO caching? *)

  let did_to_ls_ops ~did = 
    lookup_did ~did |> bind @@ fun r ->
    let page_ref_ops = did_to_page_ref_ops ~did in
    return (Imp_dir.ls_ops ~page_ref_ops)

end




let omap_ops : omap_ops = failwith ""


(* TODO instantiate the omap with cache *)

