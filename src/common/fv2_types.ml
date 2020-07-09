(** Types for {!File_impl_v2} *)



(*

$(ABBREV("im = in memory"))

$(ABBREV("fim = file in memory"))

$(ASSUME("""All operations for an object go via a single blkdev; thus, a
   sync on an object can be accomplished by a flush (or barrier)
   followed by a sync on the underlying blkdev"""))

$(CONVENTION("""We try to use the term "flush" for objects and
   "barrier" for blk_devs, although we believe these are the same sort
   of operation; a barrier on an object effectively tells it to push
   cached ops to the blkdev, and issue a blkdev barrier; sometimes we
   """))

*)


(* open Int_like *)
(* open Buffers_from_btree *)

type 'fid file_id = { fid:'fid }

type stat_times = Minifs_intf.times[@@deriving bin_io]

module Usedlist = Usedlist_impl.Usedlist

(*
module Freelist = struct
  (* FIXME this type is almost the same as blk_allocator *)
  type ('blk_id,'t) ops = {
    alloc: unit -> ('blk_id,'t)m;
    free: 'blk_id -> (unit,'t)m;
  }
end
*)

module Btree_ops = struct
  type ('k,'v,'r,'t) t = {
    find         : 'k -> ('v option,'t) m;
    insert       : 'k -> 'v -> (unit,'t) m;
    delete       : 'k -> (unit,'t) m;

    delete_after : 'k -> (unit,'t) m; 
    (** delete all entries for keys AFTER (>) k; used for truncate *)

    flush        : unit -> (unit,'t)m;
    get_root     : unit -> ('r,'t)m;
  }
end



