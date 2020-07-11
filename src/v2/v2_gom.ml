(** The "global object map" GOM

This is just an instance of Tjr_btree, mapping object id (which we
   identify with dir_entry) to blk_id 

NOTE this is responsible for handling its own synchronization

FIXME this should use allocation via a usedlist

*)
open V2_intf

module Usedlist = Usedlist_impl.Usedlist

module Gom_origin_block = struct
  open Bin_prot.Std

  type 'blk_id t = {
    gom_root        : 'blk_id;
    usedlist_origin : 'blk_id Usedlist.origin;
  }[@@deriving bin_io]

end

(* Local abbrev; not exposed *)
open (struct
  type 'blk_id origin = 'blk_id Gom_origin_block.t
end)


module Gom_ops = struct
  type ('k,'v,'r,'t) t = {
    find     : 'k -> ('v option,'t) m;
    insert   : 'k -> 'v -> (unit,'t) m;
    delete   : 'k -> (unit,'t) m;
    get_root : unit -> ('r,'t)m;
    sync     : unit -> (unit,'t)m;
  }
  (** NOTE sync is just for tidy shutdown *)
end

open (struct
  type ('k,'v,'r,'t) gom_ops = ('k,'v,'r,'t) Gom_ops.t
end)

type ('blk_id,'blk,'t,'de,'gom_ops) gom_factory = <
  (* NOTE 'gom_ops is just an abbrev; don't actually call this function *)
  note_type_abbrev: 'gom_ops -> ('de,'blk_id,'blk_id,'t) gom_ops -> unit;

  read_origin: 
    blk_dev_ops : ('blk_id,'blk,'t) blk_dev_ops -> 
    blk_id : 'blk_id -> 
    ('blk_id origin,'t)m;

  write_origin:
    blk_dev_ops : ('blk_id,'blk,'t) blk_dev_ops -> 
    blk_id : 'blk_id -> 
    origin: 'blk_id origin -> 
    (unit,'t)m;

  with_: 
    blk_dev_ops : ('blk_id,'blk,'t) blk_dev_ops -> 
    barrier      : (unit -> (unit,'t)m) -> 
    sync         : (unit -> (unit,'t)m) -> 
    freelist_ops : ('blk_id,'t) blk_allocator_ops -> 
    <
      usedlist_ops : 'blk_id Usedlist.origin -> (('blk_id,'t) Usedlist.ops,'t)m;

      alloc_via_usedlist : 
        ('blk_id,'t) Usedlist.ops ->         
        ('blk_id,'t) blk_allocator_ops;
            
      create         : unit -> ('gom_ops,'t)m;
      init_from_origin    : 'blk_id -> 'blk_id origin -> ('gom_ops,'t)m;      
      init_from_disk : 'blk_id -> ('gom_ops,'t)m;
    >
>


module Make(S:sig
    type blk_id 
    type r = blk_id
    type blk
    type t = lwt
    type dir_entry

    type nonrec gom_ops = (dir_entry,blk_id,blk_id,t)gom_ops

    val origin_mshlr : blk_id origin ba_mshlr

    type _ls (* we don't use leafstreams for the GOM *)
        
    (* NOTE this is from btree_factory *)
    val uncached : 
      blk_dev_ops     : (r, blk, t) blk_dev_ops ->
      blk_alloc       : (r, t) blk_allocator_ops ->
      init_btree_root : r ->
      < get_btree_root  : unit -> (r, t) Tjr_monad.m;
        map_ops_with_ls : 
          (dir_entry, blk_id, r, _ls, t) Tjr_btree.Btree_intf.map_ops_with_ls >

    val usedlist_factory : (blk_id,blk,t) Usedlist_impl.usedlist_factory
    
  end) = struct
  open S

  open Tjr_monad.With_lwt

  let note_type_abbrev: gom_ops -> (dir_entry,r,r,t)Gom_ops.t -> unit = 
    fun x y -> 
    (* just note that x and y have the same type *)
    let r = ref x in r:=y; ()

  module Origin_mshlr = (val origin_mshlr)

  let read_origin ~blk_dev_ops ~blk_id = 
    blk_dev_ops.read ~blk_id >>= fun blk -> 
    Origin_mshlr.unmarshal blk |> return
    
  let write_origin ~blk_dev_ops ~blk_id ~origin = 
    Origin_mshlr.marshal origin |> fun blk ->
    blk_dev_ops.write ~blk_id ~blk

  module With_(W:sig
      val blk_dev_ops  : (blk_id,blk,t) blk_dev_ops
      val barrier      : (unit -> (unit,t)m)
      val sync         : (unit -> (unit,t)m)
      val freelist_ops : (blk_id,t) blk_allocator_ops
    end) 
  = 
  struct
    open W
        
    let usedlist_ops = ()

    let alloc_via_usedlist = ()
                             
    let create = ()
    let init_from_origin = ()
    let init_from_disk = ()

    (* FIXME implement the above *)

    let export = ()
  end (* With_ *)

  let with_ ~blk_dev_ops ~barrier ~sync ~freelist_ops =
    let module X = struct
      let blk_dev_ops = blk_dev_ops
      let barrier = barrier
      let sync = sync
      let freelist_ops = freelist_ops
    end
    in
    let module W = With_(X) in
    W.export


  

  
end
  
  



module Dir_entry = struct
  open Bin_prot.Std
  type t = (int,int,int) dir_entry'[@@deriving bin_io]
  let max_sz = 9
end


module Pvt = struct
  let blk_sz = Shared_ctxt.blk_sz 

  module Bp = struct
    type t = Shared_ctxt.r Gom_origin_block.t[@@deriving bin_io]
    let max_sz = 9
  end

  let bp_mshlr : _ bp_mshlr = (module Bp)

  let ba_mshlr = bp_mshlrs#ba_mshlr ~mshlr:bp_mshlr ~buf_sz:(blk_sz |> Blk_sz.to_int)

  include (val ba_mshlr)

end
open Pvt


module S = struct
  let k_mshlr : _ bp_mshlr = (module Dir_entry)
  type k = Dir_entry.t[@@deriving bin_io]
  type v = Shared_ctxt.r
  type r = Shared_ctxt.r
  type t = Shared_ctxt.t
  let k_cmp: k -> k -> int = Stdlib.compare
  let monad_ops = Shared_ctxt.monad_ops
  (* let k_mshlr = dir_entry_mshlr *)
  let v_mshlr = bp_mshlrs#r_mshlr
  let r_mshlr = bp_mshlrs#r_mshlr

  let k_size = let module X = (val k_mshlr) in X.max_sz
  let v_size = let module X = (val v_mshlr) in X.max_sz
  let cs = Tjr_btree.Bin_prot_marshalling.make_constants ~blk_sz ~k_size ~v_size
  let r_cmp = Shared_ctxt.r_cmp
end

module T = Tjr_btree.Make_6.Make_v2(S)

(** Use the uncached method from the following to implement the GOM *)
let gom_factory : (Dir_entry.t, S.v, S.r, S.t, T.leaf, T.node,
 (T.node, T.leaf) Isa_btree.dnode, T.ls, T.blk, T.wbc)
Tjr_btree.Make_6.btree_factory
= T.btree_factory

let write_empty_leaf = gom_factory#write_empty_leaf

(** Currently we used the uncached B-tree *)
let uncached = gom_factory#uncached

