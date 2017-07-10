(* directory entries ------------------------------------------------- *)

(** An entry in the object map is either a file (a pointer to a block,
    and a file size) or a directory (a pointer to a block). *)

open Tjr_btree
open Block
open Bin_prot_util

type f_size = int  (* 32 bits suffices? *)

type f_ent = blk_id * f_size 

type d_ent = blk_id 

(* CHOICE do we want to have two maps (one for files, one for dirs) or
   just one (to sum type)? for simplicity have one for the time being *)

include struct 
  open Bin_prot.Std

  (* these types should match f_ent and d_ent; don't want derivings
     everywhere *)
  type omap_entry = Fid_sz of int * int | Did of int [@@deriving bin_io]  
end


let bin_size_entry = 1 (* tag*) + 2*bin_size_int

