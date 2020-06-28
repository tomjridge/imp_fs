(** Types for {!File_impl_v2} *)



(*

$(ABBREV("im = in memory"))

$(ABBREV("fim = file in memory"))

$(ASSUME("All operations for an object go via a single blkdev; thus, a
   sync on an object can be accomplished by a flush (or barrier)
   followed by a sync on the underlying blkdev"))

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

  (** The raw operations provided by the plist; in addition we need to
     integrate the freelist with the usedlist: alloc_via_usedlist *)
  type ('blk_id,'t) ops = {
    add        : 'blk_id -> (unit,'t)m;    
    get_origin : unit -> 'blk_id origin;
    flush      : unit -> (unit,'t)m;
  }
  (** NOTE a sync is just a flush followed by a sync of the underlying
     blkdev, since we assume all object operations are routed to the
     same blkdev *)

end


module Freelist = struct
  type ('blk_id,'t) ops = {
    alloc: unit -> ('blk_id,'t)m;
  }
end


module Btree_ops = struct
  type ('k,'v,'r,'t) t = {
    find         : 'k -> ('v option,'t) m;
    insert       : 'k -> 'v -> (unit,'t) m;
    delete       : 'k -> (unit,'t) m;

    delete_after : 'k -> (unit,'t) m; 
    (** delete all entries for keys > r; used for truncate *)

    flush        : unit -> (unit,'t)m;
    get_root     : unit -> 'r;
  }

    (* $(FIXME(""" btree should have a get_root method, flush/barrier and sync""")) *)
end



(** We reconstruct the file from the contents of the file's origin
   blk; this includes the size, the [blk->index] map root, and the
   pointers for the used list (which is used to implement alloc in
   conjunction with the freelist) *)
module File_origin_block = struct

  open Bin_prot.Std

  (* NOTE Tjr_fs_shared has size but no deriving bin_io; FIXME perhaps it should? *)
  type size = {size:int} [@@deriving bin_io]

  type 'blk_id t = {
    file_size          : size; (* in bytes of course *)
    blk_index_map_root : 'blk_id;
    usedlist_origin    : 'blk_id Usedlist.origin;
  }[@@deriving bin_io]

  

end
module Fo = File_origin_block



(** State we hold in memory for a particular file *)
module File_im = struct

  type 'blk_id t = {
    origin_info  : 'blk_id File_origin_block.t;
    origin_dirty : bool;
    (* blk_dev_ops; barrier; sync *)
    (* btree cache *)
    (* data cache *)
  }
  (**
     origin_dirty - whether we need to write out the origin (ie any of the
     relevant fields have changed)  *)

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

For pread, we always return a buffer of length len.  *)
type ('buf,'t) file_ops = {
  size     : unit -> (size,'t)m;
  pwrite   : src:'buf -> src_off:offset -> src_len:len -> 
    dst_off:offset -> ((size,pwrite_error)result,'t)m;
  pread    : off:offset -> len:len -> (('buf,pread_error)result,'t)m;
  truncate : size:size -> (unit,'t)m;
  flush    : unit -> (unit,'t)m;
  sync     : unit -> (unit,'t)m;
}


type ('buf,'blk,'blk_id,'t) file_factory = <

  read_origin: 
    blk_dev_ops : ('blk_id,'blk,'t) blk_dev_ops -> 
    blk_id : 'blk_id -> 
    ('blk_id File_origin_block.t,'t)m;

  write_origin:
    blk_dev_ops : ('blk_id,'blk,'t) blk_dev_ops -> 
    blk_id : 'blk_id -> 
    origin: 'blk_id File_origin_block.t -> 
    (unit,'t)m;

  origin_to_fim: 'blk_id File_origin_block.t -> 'blk_id File_im.t;

  usedlist_origin : 'blk_id File_origin_block.t -> 'blk_id Usedlist.origin;

  with_: 
    blk_dev_ops  : ('blk_id,'blk,'t) blk_dev_ops -> 
    barrier      : (unit -> (unit,'t)m) -> 
    sync         : (unit -> (unit,'t)m) -> 
    freelist_ops : ('blk_id,'t) Freelist.ops -> 
    <    
      usedlist_ops : 'blk_id Usedlist.origin -> (('blk_id,'t) Usedlist.ops,'t)m;

      alloc_via_usedlist : 
        ('blk_id,'t) Usedlist.ops ->         
        < alloc_via_usedlist: unit -> ('blk_id,'t)m>;
      (** Allocate and record in usedlist *)

      blk_index_map_ops : 'blk_id -> (int,'blk_id,'blk_id,'t)Btree_ops.t;
      
      file_ops: 
        usedlist_ops       : ('blk_id,'t) Usedlist.ops -> 
        alloc_via_usedlist : (unit -> ('blk_id,'t)m) ->         
        blk_index_map_ops  : (int,'blk_id,'blk_id,'t)Btree_ops.t -> 
        with_fim           : ('blk_id File_im.t,'t) with_state -> 
        ('buf,'t)file_ops;
    >
>
