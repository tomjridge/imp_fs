(* free space ------------------------------------------------------- *)

(* all stores share the same free space map *)

open Imp_pervasives
open X.Monad

open Imp_state

let free_ops = {
  get=(fun () -> fun t -> (t,Ok t.free));
  set=(fun free -> fun t -> ({t with free}, Ok ()));
}
