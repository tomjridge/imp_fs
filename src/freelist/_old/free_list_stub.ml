(** A stub for the free list, pending proper implementation. *)

module Make(S : sig include Map.OrderedType type blk end) = struct
  open S

  type blk_id = S.t

  module M = Map.Make(S)

  type free_list = blk M.t

  let make ~filename = 
    match Tjr_file.file_exists filename with 
    | true -> 
      let fl : free_list = Marshal.from_channel (open_in filename) in
      fl
    | false -> 
      failwith "" (* FIXME *)

end
