open Tjr_monad.With_lwt

open Bin_prot.Std

type t = lwt
let monad_ops = lwt_monad_ops

type fid = int[@@deriving bin_io, yojson]
type did = int[@@deriving bin_io, yojson]
type sid = int[@@deriving bin_io, yojson]

type dh  = int[@@deriving bin_io]

type tid = int[@@deriving bin_io]
