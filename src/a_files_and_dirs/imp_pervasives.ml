
(* don't open Tjr_btree - module clashes :(; use open X.Yzw *)
module X = Tjr_btree

open Bin_prot.Std

let blk_sz = 4096

(* object ids -------------------------------------------------------- *)

type object_id = int [@@deriving bin_io]
type oid = object_id [@@deriving bin_io]

module Fid : sig 
  type fid [@@deriving bin_io ] 
  val fid2oid : fid -> oid
end = struct
  type fid = oid [@@deriving bin_io]
  let fid2oid x = x 
end
include Fid


module Did : sig 
  type did [@@deriving bin_io ] 
  val did2oid : did -> oid
end = struct
  type did = oid [@@deriving bin_io]
  let did2oid x = x 
end
include Did

let bp_size_oid = X.Bin_prot_util.bp_size_int


let option_case ~_None ~_Some x = (match x with
  | None -> _None
  | Some x -> _Some x)
