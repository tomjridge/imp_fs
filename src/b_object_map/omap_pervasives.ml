open Bin_prot.Std

let blk_sz = 4096

(* object ids -------------------------------------------------------- *)

type object_id = int [@@deriving bin_io]
type oid = object_id [@@deriving bin_io]

module Fid : sig 
  type fid [@@deriving bin_io ] 
  val i2fid : int -> fid
  val fid2i : fid -> int
end = struct
  type fid = oid [@@deriving bin_io]
  let i2fid x = x
  let fid2i x = x 
end
include Fid


module Did : sig 
  type did [@@deriving bin_io ] 
  val i2did : int -> did
  val did2i : did -> int
end = struct
  type did = oid [@@deriving bin_io]
  let i2did x = x
  let did2i x = x 
end
include Did

type fid_did = [ `Fid of fid | `Did of did ] [@@deriving bin_io]

let bin_size_fid_did_ = Tjr_btree.Bin_prot_util.bin_size_int * 2  (* FIXME check *)
(* FIXME rename to bin_size_int_ *)

let dest_fid (`Fid x) = x
let dest_did (`Did x) = x

