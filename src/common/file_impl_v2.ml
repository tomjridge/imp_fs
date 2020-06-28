(** A simple file implementation; supersedes v1. 

Terminology: (file) block-index map: the map from blk index to blk_id

We implement a file as a map from blk_index to blk. The map is
typically implemented by a B-tree or similar.

New blocks are allocated as needed (by default the map is empty, and a
block is only allocated when that block is written to; so files are
"sparse").

The "iterated block read/write" component has been abstracted out
because it appears in a lot of other places.

Concurrency: The backing B-tree is itself a persistent object.

GOM interaction: We treat a file as a separate object; the GOM maps file id to origin block. 


TODO:
- b-tree to implement delete_after (or some alternative approach)
- validate against "normal" file semantics
- ensure the tests are run by some top-level test executable
- implement freeing of blocks after truncate
- maybe worth distinguishing the blk_id used by the map from the blk_id used to store data
*)


open Int_like
open Buffers_from_btree
open Fv2_types


(** Check the arguments to pread *)
let pread_check = File_impl_v1.pread_check

(** Check arguments to pwrite *)
let pwrite_check = File_impl_v1.pwrite_check


module Iter_block_blit = Fv2_iter_block_blit


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

  (** For the freelist *)
  type a = blk_id
  val plist_factory : (a,blk_id,blk,buf,t) Plist_intf.plist_factory

  val file_origin_mshlr: blk_id File_origin_block.t ba_mshlr
end


module type T = sig
  module S : S
  open S
  val file_factory : (buf,blk,blk_id,t) file_factory
end


[@@@warning "-32"]

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

    let blk_index_map_ops _r : (int,_,_,_)Btree_ops.t = 
      failwith "" (* just instantiate a B-tree with an in-mem ref say,
                     and alloc via usedlist *)

    [@@@warning "-27"]

    let file_ops
        (usedlist_ops       : ('blk_id,'t) Usedlist.ops)
        (alloc_via_usedlist : (unit -> ('blk_id,'t)m))
        (blk_index_map_ops  : (int,'blk_id,'blk_id,'t)Btree_ops.t)
        (with_fim           : ('blk_id File_im.t,'t) with_state)
        : (_,_)file_ops 
        =
      failwith "" (* base on file_impl_v1 *)

  end    

  let file_factory = failwith ""
end

