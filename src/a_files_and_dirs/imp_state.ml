(* Full ImpFS state ------------------------------------------------- *)

open Tjr_btree
open Btree_api
open Page_ref_int
open Block
open Monad
open Imp_pervasives

(* we need this early because the monad require this type *)

type imp_state = {

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
  file_caches: int (* fid *) -> unit;

  (* Ditto directories *)
  dir_caches: int (* did *) -> unit;

  (* TODO other layers of caching *)
}


(* free space ------------------------------------------------------- *)

(* all stores share the same free space map *)

let free_ops = {
  get=(fun () -> fun t -> (t,Ok t.free));
  set=(fun free -> fun t -> ({t with free}, Ok ()));
}

let page_ref_ops = {
  get=(fun () -> fun t -> (t,Ok t.omap_root));
  set=(fun omap_root -> fun t -> ({t with omap_root},Ok ()));
}


