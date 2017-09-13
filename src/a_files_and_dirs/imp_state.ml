(** The ImpFS state type *)

(* Full ImpFS state ------------------------------------------------- *)

open Imp_pervasives
open X.Page_ref_int
open X.Block
open X.Monad

(* we need this early because the monad require this type *)

type imp_state = {

  (* TODO this is to demo the filesystem *)
  fd: X.Disk_on_fd.fd;

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


