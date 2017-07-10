open Tjr_btree
open Page_ref_int
open Block
open Omap_pervasives


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
type omap_state = t


(* omap operations from oid/fid/did --------------------------------- *)

(* "omap" is the "global" object map from oid to Ent.t *)

open Tjr_btree
open Btree_api
open Omap_entry

(* invariant: fid maps to file, did maps to dir; package as two ops? *)
type omap_ops = (oid,omap_entry,omap_state) map_ops

type size = int

type omap_fid_ops = (fid,blk_id*size,omap_state) map_ops

let omap_fid_ops = failwith "TODO"

type omap_did_ops = (did,blk_id,omap_state) map_ops

let omap_did_ops = failwith "TODO"


(* free space ------------------------------------------------------- *)

(* all stores share the same free space map *)
module Free = struct
  open Monad
  let free_ops : (blk_id,omap_state) mref = {
    get=(fun () -> (fun t -> (t,Ok t.free)));
    set=(fun free -> fun t -> ({t with free}, Ok ()));
  }
end
include Free


