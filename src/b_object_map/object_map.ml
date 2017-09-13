(* global object map ------------------------------------------------ *)

(** We maintain a single global object map, from oid (int) to entry. *)
open Imp_pervasives
open X.Bin_prot_util
open Imp_state
open Free_ops
open X.Map_ops

(* omap entries ------------------------------------------------- *)

(** An entry in the object map is either a file (a pointer to a block,
    and a file size) or a directory (a pointer to a block). *)

(* CHOICE do we want to have two maps (one for files, one for dirs) or
   just one (to sum type)? for simplicity have one for the time being *)

open Bin_prot.Std

(* these types should match f_ent and d_ent; don't want derivings
   everywhere *)
type omap_entry = 
  | File_blkid_sz of int * int 
  | Dir_blkid of int [@@deriving bin_io]

let bp_size_omap_entry = 1 (* tag*) + 2*bp_size_int


(* invariant: fid maps to file, did maps to dir; package as two ops? *)
type ('k,'v,'t) map_ops  (* FIXME at the moment this is just a placeholder *)
type omap_ops = (oid,omap_entry,imp_state) map_ops


(* don't use omap_ops; use omap_fid_ops, or omap_did_ops *)
type omap_fid_ops = (fid,X.Block.blk_id*int,imp_state) map_ops

let omap_fid_ops = failwith "TODO"

type omap_did_ops = (did,X.Block.blk_id,imp_state) map_ops

let omap_did_ops = failwith "TODO"


(* object map ------------------------------------------------------- *)

open Disk_ops

type k = oid
type v = omap_entry
let v_size = bp_size_omap_entry

let ps = 
  X.Binprot_marshalling.mk_binprot_ps ~blk_sz
    ~cmp:X.Int_.compare ~k_size:bp_size_int ~v_size
    ~read_k:bin_reader_int ~write_k:bin_writer_int
    ~read_v:bin_reader_omap_entry ~write_v:bin_writer_omap_entry

let store_ops = X.Disk_to_store.disk_to_store ~ps ~disk_ops ~free_ops

let page_ref_ops = failwith "TODO"

let map_ops : [<`Map_ops of 'a] = 
  X.Store_to_map.store_ops_to_map_ops ~ps ~store_ops 
    ~page_ref_ops


(* looking up directories and files --------------------------------- *)

open X.Monad

let find,insert,delete,insert_many = 
  dest_map_ops map_ops @@ fun ~find ~insert ~delete ~insert_many ->
  (find,insert,delete,insert_many)

let lookup_did ~did =     
  did |> did2oid |> fun oid ->
  find oid |> bind @@ fun ent_opt ->
  match ent_opt with
  | None -> failwith __LOC__  (* TODO old reference no longer valid? *)
  | Some ent -> 
    match ent with
    | File_blkid_sz (r,sz) -> failwith __LOC__
    | Dir_blkid r -> return r

let lookup_fid ~fid =     
  fid |> fid2oid |> fun oid ->
  find oid |> bind @@ fun ent_opt ->
  match ent_opt with
  | None -> failwith __LOC__  (* TODO old reference no longer valid? *)
  | Some ent -> 
    match ent with
    | File_blkid_sz (r,sz) -> return (r,sz)
    | Dir_blkid r -> failwith __LOC__


(* return get/set for a particular oid *)
let did_to_page_ref_ops ~did = 
  did |> did2oid |> fun oid ->
  { 
    get=(fun () -> lookup_did ~did);
    set=(fun r -> insert oid (Dir_blkid(r)))
  }

let fid_to_page_ref_x_size_ops ~fid = 
  fid |> fid2oid |> fun oid ->
  { 
    get=(fun () -> lookup_fid ~fid);
    set=(fun (r,sz) -> insert oid (File_blkid_sz(r,sz)))
  }

let did_to_map_ops ~did = 
  let page_ref_ops = did_to_page_ref_ops ~did in
  return (Imp_dir.map_ops ~page_ref_ops)

let did_to_ls_ops ~did = 
  let page_ref_ops = did_to_page_ref_ops ~did in
  return (Imp_dir.ls_ops ~page_ref_ops)

(* let fid_to_map_ops  *)



(* TODO instantiate the omap with cache *)

