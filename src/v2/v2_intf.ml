(** Types for V2 *)


(** Object ids *)

type ('fid,'did,'sid) dir_entry' = 
  ('fid,'did,'sid) Dir_impl.Dir_entry.dir_entry'[@@deriving bin_io]
type dir_entry = Dir_impl.Dir_entry.dir_entry[@@deriving bin_io]

let dir_entry_to_int = Dir_impl.Dir_entry.dir_entry_to_int






(* type ('fid,'did,'sid) dir_entry' = Fid of 'fid | Did of 'did | Sid of 'sid[@@deriving bin_io] *)




(*
module Dir_entry = struct
  open Bin_prot.Std

  type t = (fid,did,sid)dir_entry'[@@deriving bin_io]
  let max_sz = 9
  let to_int = function
    | Did did -> did.did
    | Fid fid -> fid.fid
    | Sid sid -> sid.sid
end

let dir_entry_mshlr : _ bp_mshlr = (module Dir_entry)
*)


(** The origin block for the whole system (typically stored in block 0) *)
module Fs_origin_block = struct
  (* open Bin_prot.Std *)

  type 'blk_id t = {
    fl_origin      : 'blk_id;
    gom_origin     : 'blk_id;    
    counter_origin : 'blk_id;
  }[@@deriving bin_io, sexp]
(** freelist origin; root of GOM; object id counter (numbers >=
    counter are free to be used as object identifiers) *)

  type t' = Shared_ctxt.r t[@@deriving bin_io, sexp]
end
(* open Fs_origin_block *)

