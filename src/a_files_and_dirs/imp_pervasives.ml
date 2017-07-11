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

let bp_size_oid = Tjr_btree.Bin_prot_util.bp_size_int


