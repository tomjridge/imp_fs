(** Operations to get a free block; currently just a counter *)

(* free space ------------------------------------------------------- *)

(* all stores share the same free space map *)

open Imp_pervasives
open Tjr_btree.Base_types  (* mref *)

open Imp_state

let with_world = Tjr_monad.State_passing_instance.with_world

let free_ops = {
  get=(fun () -> with_world (fun t -> t.free,t));
  set=(fun free -> with_world (fun t -> ((), {t with free})));
}
