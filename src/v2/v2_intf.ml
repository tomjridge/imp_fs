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
