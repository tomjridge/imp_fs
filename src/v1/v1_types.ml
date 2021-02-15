(** Concrete types; this comes after v1_generic *)

open Tjr_monad.With_lwt
open Shared_ctxt
open Bin_prot.Std
module G = V1_generic
(* open G *)

[@@@warning "-33"]


(* type ('k,'v,'t) uncached_btree = ('k,'v,'t)Tjr_btree.Make_3.uncached_btree *)

(*
(** Free blocks *)
let min_free_blk = ref 0
let get_min_free_blk () = 
  let r = !min_free_blk in
  incr min_free_blk;
  r

let b0_system_root_blk = get_min_free_blk()
let b1_gom_map_root = get_min_free_blk()
*)


(** {2 Setup the generic instance} *)

(** Base types *)
module S0 = struct
  type t = lwt
  let monad_ops = monad_ops
  type fid = {fid:int}[@@deriving bin_io]
  type did = {did:int}[@@deriving bin_io]
  type sid = {sid:int}[@@deriving bin_io]
end
open S0
    

module Make = G.Make(S0)

(** derived types *)
module S1 = Make.S1
open S1

(** functor argument type holder *)
module S2 = Make.S2

(** what we need to provide to get a filesystem *)
module type T2 = S2.T2

(** functor to construct a filesystem (takes a module of type T2); the
    implementation is {!T2_impl}, towards the end of this file *)
module Make_2 = Make.Make_2


