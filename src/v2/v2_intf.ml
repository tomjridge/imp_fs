(** Types for V2 *)


(** Object ids *)
type ('fid,'did,'sid) dir_entry' = Fid of 'fid | Did of 'did | Sid of 'sid[@@deriving bin_io]

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

(** The origin block for the whole system; currently this is stored in block 0 *)
module Fs_origin_block = struct

  type 'blk_id t = {
    fl_origin          : 'blk_id;
    gom_root           : 'blk_id;    
    min_free_object_id : int;
  }
  (** freelist origin; root of GOM; object id counter *)

end

(** The data we keep in memory, in addition to the data held by the
   GOM and the freelist *)
module Filesystem_im = struct
  type dir_pool
  type file_fool

end
