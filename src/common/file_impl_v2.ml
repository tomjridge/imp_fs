(** A simple file implementation; supersedes v1. 

Terminology: (file) block-index map: the map from blk index to blk_id

We implement a file as a map from blk_index to blk. The map is
typically implemented by a B-tree or similar.

The inode contains the file size and the root of the blk_index map.

New blocks are allocated as needed (by default the map is empty, and a
block is only allocated when that block is written to; so files are
"sparse").

The "iterated block read/write" component has been abstracted out
because it appears in a lot of other places.

The components we require are as follows (see "make" function below; NOTE missing free, since
this is not currently implemented for file.truncate).

{[
 let make (type fid blk blk_id t) 
      ~monad_ops
      ~(blk_ops       : blk blk_ops)
      ~(blk_dev_ops   : (blk_id,blk,t) blk_dev_ops)
      ~(blk_index_map : (int,blk_id,blk_id,t)pre_map_ops)
      ~(with_inode    : ((fid,blk_id)inode,t)with_state)
      ~(alloc         : unit -> (blk_id,t)m)
  =
]}


Concurrency: The backing B-tree is itself a persistent object.

GOM interaction: We treat a file as a separate object; the GOM maps file id to origin block. 


TODO:
- b-tree to implement delete_after (or some alternative approach)
- validate against "normal" file semantics
- ensure the tests are run by some top-level test executable
- implement freeing of blocks after truncate
- maybe worth distinguishing the blk_id used by the map from the blk_id used to store data
*)

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
open Buffers_from_btree

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


(* $(PIPE2SH("""sed -n '/because[ ]the file root/,/^end/p' >GEN.pre_map_ops.ml_""")) *)
(** [Pre_map_ops]: because the file root is stored in the inode, we need to access
   the B-tree using explicit root passing (and then separately update
   the inode); this is to ensure that the inode update is atomic (via
   with_inode). We could store the root in a separate block but this
   is a bit too inefficient. *)
module Pre_map_ops = struct
  (* we don't expose leaf and frame here *)
  type ('k,'v,'r,'t) pre_map_ops = {
    find         : r:'r -> k:'k -> ('v option,'t) m;
    insert       : r:'r -> k:'k -> v:'v -> ('r option,'t) m;
    delete       : r:'r -> k:'k -> ('r,'t) m;

    delete_after : r:'r -> k:'k -> ('r,'t) m; 
    (** delete all entries for keys > r; used for truncate *)

    flush        : unit -> (unit,'t)m;
    get_root     : unit -> 'r;
  }
end
open Pre_map_ops




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

      blk_index_map_ops : 'blk_id -> (int,'blk_id,'blk_id,'t)pre_map_ops;
      
      file_ops: 
        usedlist_ops       : ('blk_id,'t) Usedlist.ops -> 
        alloc_via_usedlist : (unit -> ('blk_id,'t)m) ->         
        blk_index_map_ops  : (int,'blk_id,'blk_id,'t)pre_map_ops -> 
        with_fim           : ('blk_id File_im.t,'t) with_state -> 
        ('buf,'t)file_ops;
    >
>

type ('a,'b) iso = {
  a_to_b: 'a -> 'b;
  b_to_a: 'b -> 'a
}

(** Check the arguments to pread *)
let pread_check = File_impl_v1.pread_check

(** Check arguments to pwrite *)
let pwrite_check = File_impl_v1.pwrite_check

module Iter_block_blit = File_impl_v1.Iter_block_blit

(* FIXME at the moment, this assumes that we can rewrite file blocks as we wish *)

(** NOTE The pread,pwrite functions that result will lock the file
    whilst executing, in order to udpate the B-tree root *)

module type S = sig
  type blk = ba_buf
  type buf = ba_buf
  type blk_id = Shared_ctxt.r
  type t
  val monad_ops   : t monad_ops
  val buf_ops     : buf buf_ops  (* FIXME create_zeroed? *)
  val blk_dev_ops : (blk_id,blk,t) blk_dev_ops
  val barrier     : unit -> (unit,'t)m
  val sync        : unit -> (unit,'t)m

  type a = blk_id
  val plist_factory : (a,blk_id,blk,buf,t) Plist_intf.plist_factory

  val file_origin_mshlr: blk_id File_origin_block.t ba_mshlr
end

module type T = sig
  module S : S
  open S
  val file_factory : (buf,blk,blk_id,t) file_factory
end

module Make(S:S) : T with module S = S = struct
  module S = S
  open S

  let ( >>= ) = monad_ops.bind
  let return = monad_ops.return 
                 
  (* NOTE unlike a buffer, a blk typically has a fixed size eg 4096 bytes *)
  let blk_sz = blk_dev_ops.blk_sz |> Blk_sz.to_int

  module File_origin_mshlr = (val file_origin_mshlr)

  let read_origin ~blk_dev_ops ~blk_id =
    blk_dev_ops.read ~blk_id >>= fun buf -> 
    File_origin_mshlr.unmarshal buf |> return

  let write_origin ~blk_dev_ops ~blk_id ~origin =
    origin |> File_origin_mshlr.marshal |> fun buf -> 
    assert( (buf_ops.buf_size buf).size = blk_sz); 
    blk_dev_ops.write ~blk_id ~blk:buf

  let origin_to_fim (fo: _ File_origin_block.t) : (_ File_im.t) = 
    File_im.{
      origin_info=fo;
      origin_dirty=false;
    }
    
  let usedlist_origin (fo: _ Fo.t) = 
    fo.usedlist_origin

  module With_(S2:sig
      val blk_dev_ops  : ('blk_id,'blk,'t) blk_dev_ops
      val barrier      : (unit -> (unit,'t)m)
      val sync         : (unit -> (unit,'t)m)
      val freelist_ops : ('blk_id,'t) Freelist.ops
    end) = struct
    open S2

    let usedlist_ops (uo:_ Usedlist.origin) =
      plist_factory#with_blk_dev_ops ~blk_dev_ops ~barrier |> fun x -> 
      x#init#from_endpts uo >>= fun pl -> 
      x#with_ref pl |> fun y -> 
      x#with_state y#with_plist  |> fun plist_ops -> 
      return plist_ops
      
    let alloc_via_usedlist (ul_ops: _ Usedlist.ops) =
      let alloc () = 
        freelist_ops.alloc () >>= fun blk_id -> 
        ul_ops.add blk_id >>= fun () -> 
        barrier() >>= fun () ->
        return blk_id
      in
      object method alloc_via_usedlist=alloc end

    let blk_index_map_ops _r : (int,_,_,_)pre_map_ops = 
      failwith "" (* just instantiate a B-tree with an in-mem ref say,
                     and alloc via usedlist *)

    [@@@warning "-27"]

    let file_ops
        (usedlist_ops       : ('blk_id,'t) Usedlist.ops)
        (alloc_via_usedlist : (unit -> ('blk_id,'t)m))
        (blk_index_map_ops  : (int,'blk_id,'blk_id,'t)pre_map_ops)
        (with_fim           : ('blk_id File_im.t,'t) with_state)
        : (_,_)file_ops 
        =
      failwith "" (* base on file_impl_v1 *)

  end    

  let file_factory = failwith ""
end

