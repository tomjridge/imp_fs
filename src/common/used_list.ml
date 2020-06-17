(** A used list, a complement to a freelist that also can function as
   a freelist.

This is essentially the same as a freelist, with the exception that we
   also record the origin ptr.


{%html: <img
   src="https://docs.google.com/drawings/d/e/2PACX-1vR3nHzNU8Nl6nffcW2R8KoUcol1HcAMNoefBtt_gleyVKNtc_315MWHdWrX3ToPgKu6Btzni84mq7KI/pub?w=1284&amp;h=501">
   %}


*)
open Tjr_plist_freelist
open Tjr_plist_freelist.Freelist_intf

[@@@warning "-27"]

type ('blk_id,'blk,'t) used_list_in_mem = {
  blk0_ptr   : 'blk_id;
  hd_ptr     : 'blk_id;
  tl_ptr     : 'blk_id;
  alloc_size : int; (* in bytes *)
  transient  : 'blk_id list;
  alloc      : (unit -> ('blk_id,'t)m)
}

type 'blk_id used_list_origin_blk = {
  blk0_ptr   :'blk_id;
  hd_ptr     :'blk_id;
  tl_ptr     :'blk_id;
  blk_len    : int;
  alloc_size : int
}
(* $(FIXME("We need to make sure that the plist doesn't assume that tl
   is the real tl - it may have successors if we delay syncing")) *)


type ('blk_id,'t) allocator = {
  alloc   : unit -> ('blk_id,'t)m;

  alloc_n : int -> ('blk_id list,'t)m; 
  (** returns a non-empty list of length <= n *)

  delete  : unit -> 'blk_id used_list_origin_blk;
  (** ignore alloc_size *)
}
(* $(FIXME("make sure alloc_n has the same semantics as freelist_ops")) *)
          
let make (type blk_id blk(*buf*) t) 
    ~(monad_ops: t monad_ops)
    ~(blk_dev_ops: (blk_id,blk,t)blk_dev_ops)
    ~(sync_blk_dev: (unit -> (unit,t)m))
    ~(ctxt_fl_ops: (blk_id,t)freelist_ops)
    ~(origin_mshlr: blk_id used_list_origin_blk bp_mshlr)
  : (blk_id,t)allocator
  =
  let open (struct
    let ( >>= ) = monad_ops.bind
    let return = monad_ops.return

    let alloc () = 
      
      
  end)
  in
  {alloc;alloc_n;delete}

