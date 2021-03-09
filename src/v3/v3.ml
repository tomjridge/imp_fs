(** This version focuses on the front end caching and locking, and
   uses SQLite as the backend for the metadata, and some other
   filesystem as a backend for file data. 

*)

[@@@warning "-33"]

open V3_abstract

module S0 : S0 = struct
  open Bin_prot.Std

  type t = lwt
  let monad_ops = lwt_monad_ops

  type fid = int[@@deriving bin_io]
  type did = int[@@deriving bin_io]
  type sid = int[@@deriving bin_io]

  type dh  = int[@@deriving bin_io]

  type tid = int[@@deriving bin_io]
end
open S0

module S1 = V3_abstract.S1(S0)
open S1


module S2 = V3_abstract.S2(S0)

(** What we have to implement *)
module type T2 = S2.T2

(* FIXME todo 
module T2 : T2 = struct

  let root_did = 0

  (* dir  - implemented using a 2-gen LRU with a dirty flag *)

  module Dir = struct

    (* NOTE we already have ../common/dv3.ml, which does not have
       parameterization over did; this makes use of wbc_ops, which is
       not directly implemented by the 2genlru *)
  
    type k = did
    type v = (

  let flush_m2 

  let lru = Tjr_lib_core.Lru_two_gen.create_imperative ~monad_ops ~max_sz:100 ~flush_m2:

  let dir : dir_ops = 
    let open struct
      let find ~did name = failwith ""
    end
    in

end
*)
