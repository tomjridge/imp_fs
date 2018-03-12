(** Operations to get a free block; currently just a counter *)

(* free space ------------------------------------------------------- *)

(* all stores share the same free space map *)

open Imp_pervasives
open X.Base_types

open Imp_state

let free_ops = {
  get=(fun () -> fun t -> (t,Ok t.free));
  set=(fun free -> fun t -> ({t with free}, Ok ()));
}
