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


open Int_like
(* open Buffers_from_btree *)

type 'fid file_id = { fid:'fid }

(** NOTE the in-memory state of the usedlist is opaque to us; after
   operations, we check the origin to see if it has changed and then
   possibly flush/barrier/sync *)
module Usedlist = struct

  type 'blk_id origin = 'blk_id Plist_intf.Pl_origin.pl_origin[@@deriving bin_io]

  (* $(PIPE2SH("""sed -n '/usedlist_ops:[ ]/,/}/p' >GEN.usedlist_ops.ml_""")) *)
  (** usedlist_ops: The operations provided by the usedlist; in
     addition we need to integrate the freelist with the usedlist:
     alloc_via_usedlist

      NOTE a sync is just a flush followed by a sync of the underlying
     blkdev, since we assume all object operations are routed to the
     same blkdev *)      
  type ('blk_id,'t) usedlist_ops = {
    add        : 'blk_id -> (unit,'t)m;    
    get_origin : unit -> ('blk_id origin,'t)m;
    flush      : unit -> (unit,'t)m;
  }

  type ('blk_id,'t) ops = ('blk_id,'t) usedlist_ops
end

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



(** We reconstruct the file from the contents of the file's origin
   blk; this includes the size, the [blk->index] map root, and the
   pointers for the used list (which is used to implement alloc in
   conjunction with the freelist) *)
module File_origin_block = struct

  open Bin_prot.Std

  (* NOTE Tjr_fs_shared has size but no deriving bin_io; FIXME perhaps it should? *)
  (* type size = {size:int} [@@deriving bin_io] *)

  (* $(PIPE2SH("""sed -n '/type[ ].*file_origin_block =/,/}/p' >GEN.file_origin_block.ml_""")) *)
  type 'blk_id file_origin_block = {
    file_size        : int; (* in bytes of course *)
    blk_idx_map_root : 'blk_id;
    usedlist_origin  : 'blk_id Usedlist.origin;
  }[@@deriving bin_io]

  type 'blk_id t = 'blk_id file_origin_block
  

end
module Fo = File_origin_block




(** State we hold in memory for a particular file *)
module File_im = struct

  type t = {
    file_size: int;
  }
  (** The usedlist and blk-idx map are fixed for the lifetime of the
     file, so should be parameters on creation. Other than that, we
     just have a field for file size and a field for the location of
     the origin blk. *)

end


type pread_error = Pread_error of string

type pwrite_error = Pwrite_error of string

(* $(FIXME("for pwrite, perhaps return unit")) *)

(* $(PIPE2SH("""sed -n '/Standard[ ]file operations/,/^}/p' >GEN.file_ops.ml_""")) *)
(** Standard file operations, pwrite, pread, size and truncate.

NOTE we expect buf to be string for the functional version; for
   mutable buffers we may want to pass the buffer in as a parameter?

NOTE for pwrite, we always return src_len since all bytes are written
   (unless there is an error of course). 

For pread, we always return a buffer of length len (assuming off+len <= file size).

*)
type ('buf,'t) file_ops = {
  size     : unit -> (int,'t)m;
  pwrite   : src:'buf -> src_off:offset -> src_len:len -> 
    dst_off:offset -> ((int (*n_written*),pwrite_error)result,'t)m;
  pread    : off:offset -> len:len -> (('buf,pread_error)result,'t)m;
  truncate : size:int -> (unit,'t)m;
  flush    : unit -> (unit,'t)m;
  sync     : unit -> (unit,'t)m;
}


