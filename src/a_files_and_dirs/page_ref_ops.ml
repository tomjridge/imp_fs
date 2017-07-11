open Imp_state
open Tjr_btree.Monad

let page_ref_ops = {
  get=(fun () -> fun t -> (t,Ok t.omap_root));
  set=(fun omap_root -> fun t -> ({t with omap_root},Ok ()));
}


