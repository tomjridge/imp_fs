(** Implementation of the on-disk filesystem, which is essentially
   on-disk pointers to the freelist and the GOM *)


(** The origin block for the whole system (typically stored in block 0) *)
module Fs_origin_block = struct
  open Bin_prot.Std

  type 'blk_id t = {
    fl_origin : 'blk_id;
    gom_origin  : 'blk_id;    
    counter   : int;
  }[@@deriving bin_io]
(** freelist origin; root of GOM; object id counter (numbers >=
    counter are free to be used as object identifiers) *)
end

module Pvt = struct
  
  module Bp = struct
    type t = Shared_ctxt.r Fs_origin_block.t[@@deriving bin_io]
    let max_sz = 3*9 (* FIXME check *)
  end
      
  let bp_mshlr : _ bp_mshlr = (module Bp)

  let ba_mshlr = bp_mshlrs#ba_mshlr ~mshlr:bp_mshlr ~buf_sz:(Shared_ctxt.blk_sz |> Blk_sz.to_int)
      
  include (val ba_mshlr)

end


(** The data we keep in memory, in addition to the data held by the
   GOM and the freelist. FIXME to avoid repeated syncs, we should
   append updates to an on-disk log *)
module Filesystem_im = struct
  
  
  

end
