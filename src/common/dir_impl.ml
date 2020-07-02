(** A simple directory implementation.

Directories are one of the main objects we deal with.

We implement a directory as a map from name (string, max length 256
   bytes) to entry (file, directory or symlink). Of course, this uses
   an underlying B-tree.

Any used blocks are recorded in a per-directory-object used
   list. Currently, blocks are only reclaimed when the directory is
   deleted. Later, we plan to implement cleaning of directories that
   are still live, to free up blocks that are no longer used.

We reuse the usedlist implementation from {!File_impl_v2}.

A directory is really a thin layer over a B-tree, interfacing with the
   used list, and handling the origin block.

*)

module Usedlist = Fv2_types.Usedlist
open Usedlist_impl

module Dir_origin = struct
  (* $(PIPE2SH("""sed -n '/type[ ].*dir_origin =/,/}/p' >GEN.dir_origin.ml_""")) *)
  type 'blk_id dir_origin = {
    dir_map_root: 'blk_id;
    usedlist_origin: 'blk_id Usedlist.origin;
  }[@@deriving bin_io]

  type 'blk_id t = 'blk_id dir_origin[@@deriving bin_io]
end

module Dir = struct
  (* $(PIPE2SH("""sed -n '/type[ ].*dir_ops =/,/}/p' >GEN.dir_ops.ml_""")) *)
  type ('k,'v,'r,'t) dir_ops = {
    find     : 'k -> ('v option,'t) m;
    insert   : 'k -> 'v -> (unit,'t) m;
    delete   : 'k -> (unit,'t) m;
    flush    : unit -> (unit,'t)m;
    sync     : unit -> (unit,'t)m;
  }
  type ('k,'v,'r,'t) t = ('k,'v,'r,'t) dir_ops
end



(* $(PIPE2SH("""sed -n '/NOTE[ ].de stands for/,/^>/p' >GEN.dir_factory.ml_""")) *)
(** NOTE 'de stands for dir_entry *)
type ('blk_id,'blk,'de,'t) dir_factory = <
  read_origin: 
    blk_dev_ops : ('blk_id,'blk,'t) blk_dev_ops -> 
    blk_id : 'blk_id -> 
    ('blk_id Dir_origin.t,'t)m;

  write_origin:
    blk_dev_ops : ('blk_id,'blk,'t) blk_dev_ops -> 
    blk_id : 'blk_id -> 
    origin: 'blk_id Dir_origin.t -> 
    (unit,'t)m;
    
  with_: 
    blk_dev_ops  : ('blk_id,'blk,'t) blk_dev_ops -> 
    barrier      : (unit -> (unit,'t)m) -> 
    sync         : (unit -> (unit,'t)m) -> 
    freelist_ops : ('blk_id,'t) blk_allocator_ops -> 
    <    
      usedlist_ops : 'blk_id Usedlist.origin -> (('blk_id,'t) Usedlist.ops,'t)m;

      alloc_via_usedlist : 
        ('blk_id,'t) Usedlist.ops ->         
        ('blk_id,'t)blk_allocator_ops;
      
      mk_dir  : 
        usedlist     : ('blk_id,'t) Usedlist.ops ->        
        dir_map_root : 'blk_id -> 
        origin       : 'blk_id ->
        (str_256,'de,'blk_id,'t)Dir.t;

      (* Convenience *)
      
      dir_from_origin_blk: 'blk_id Dir_origin.t -> ((str_256,'de,'blk_id,'t)Dir.t,'t)m;

      dir_from_origin: 'blk_id -> ((str_256,'de,'blk_id,'t)Dir.t,'t)m;
    >;  
>


module type S = sig
  type blk = ba_buf
  type blk_id = Shared_ctxt.r
  type r = Shared_ctxt.r
  type t
  val monad_ops : t monad_ops
  (* FIXME two different versions of buf_ops *)

  val dir_origin_mshlr: blk_id Dir_origin.t ba_mshlr

  type dir_entry
  val de_mshlr: dir_entry bp_mshlr

  val usedlist_factory : (blk_id,blk,t) usedlist_factory

  type ls

  (** NOTE this is the type for btree_factory#uncached *)
  val uncached : 
    blk_dev_ops     : (r, blk, t) blk_dev_ops -> 
    blk_alloc       : (r, t) blk_allocator_ops -> 
    init_btree_root : r -> 
    <
      get_btree_root  : unit -> (r,t) m;
      map_ops_with_ls : (str_256,dir_entry,r,ls,t) Tjr_btree.Btree_intf.map_ops_with_ls
    >
end

module type T = sig
  module S : S
  open S
  val dir_factory : (blk_id,blk,dir_entry,t) dir_factory
end


(** Make with full sig *)
module Make_v1(S:S) = struct
  module S = S
  open S

  let ( >>= ) = monad_ops.bind
  let return = monad_ops.return 

  let buf_ops = ba_buf_ops

  module Dir_origin_mshlr = (val dir_origin_mshlr)

  
  let read_origin ~blk_dev_ops ~blk_id =
    blk_dev_ops.read ~blk_id >>= fun buf -> 
    Dir_origin_mshlr.unmarshal buf |> return
  
  let write_origin ~blk_dev_ops ~blk_id ~origin =
    origin |> Dir_origin_mshlr.marshal |> fun buf -> 
    assert( (buf_ops.len buf) = (blk_dev_ops.blk_sz|>Blk_sz.to_int)); 
    blk_dev_ops.write ~blk_id ~blk:buf

  [@@@warning "-32"]
  [@@@warning "-27"]

  module With(S2:sig
      val blk_dev_ops  : (blk_id,blk,t) blk_dev_ops
      val barrier      : (unit -> (unit,t)m)
      val sync         : (unit -> (unit,t)m)
      val freelist_ops : (blk_id,t) blk_allocator_ops      
    end) 
  = 
  struct
    open S2
    let usedlist_factory' = usedlist_factory#with_ 
        ~blk_dev_ops
        ~barrier
        ~freelist_ops

    let usedlist_ops  : (_ Usedlist.origin) -> ((r,t)Usedlist.ops,t)m =
      usedlist_factory'#usedlist_ops
        
    let alloc_via_usedlist = usedlist_factory'#alloc_via_usedlist

    let mk_dir 
        ~(usedlist     : ('blk_id,'t) Usedlist.ops)        
        ~(dir_map_root : 'blk_id)
        ~(origin       : 'blk_id)
      : _ Dir.t
      =
      failwith ""
  end

end
