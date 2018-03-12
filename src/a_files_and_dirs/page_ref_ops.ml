(** Object map page ref FIXME rename module *)

open Imp_pervasives
open Imp_state
open X.Base_types
open Tjr_fs_shared

let page_ref_ops = {
  get=(fun () -> fun t -> (t,Ok t.omap_root));
  set=(fun omap_root -> fun t -> ({t with omap_root},Ok ()));
}


