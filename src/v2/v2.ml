(** V2 - compared to V1, this version implements files (via file_impl_v2).

We try to reuse V1_generic. Some of the following copied from v1.ml *)

[@@@warning "-33"]

open Tjr_monad.With_lwt
open Shared_ctxt
open Bin_prot.Std
open V2_intf
module G = V2_generic


(** {2 Setup the generic instance} *)

(** Base types *)
module S0 = struct
  type t = lwt
  let monad_ops = monad_ops
  type fid = {fid:int}[@@deriving bin_io]
  type did = {did:int}[@@deriving bin_io]
  type sid = {sid:int}[@@deriving bin_io]
end
open S0

module Make = G.Make(S0)

(** derived types *)
module S1 = Make.S1
open S1

(** functor argument type holder *)
module S2 = Make.S2

(** what we need to provide to get a filesystem *)
module type T2 = S2.T2

(** functor to construct a filesystem (takes a module of type T2); the
   implementation is {!T2_impl}, towards the end of this file *)
module Make_2 = Make.Make_2


(** {2 Some simple defns we can complete here} *)

let root_did = {did=0}

let mk_stat_times () = 
  Unix.time () |> fun t -> (* 1s resolution? FIXME *)
  return Times.{ atim=t; mtim=t }

type dir_entry = S1.dir_entry


(** {2 Various marshallers} *)

let shared_mshlrs = Tjr_fs_shared.bp_mshlrs

let dir_entry_mshlr : dir_entry bp_mshlr = 
  (module 
    (struct 
      type t = dir_entry[@@deriving bin_io] 
      let max_sz = 10 (* FIXME check 9+1 *)
    end))

[@@@warning "-32"]

module Stage_1(S1:sig
    val blk_dev_ops : (r,blk,t)blk_dev_ops
    val origin : r Fs_origin_block.t
    val fl_params : Tjr_plist_freelist.Freelist_intf.params
  end) = struct
  open S1
  
  let barrier () = return ()
  let sync () = return ()

  (** {2 The freelist } *)
  
  (** We need to read the freelist origin block, and resurrect the freelist *)

  module Fl_origin = Tjr_plist_freelist.Freelist_intf.Fl_origin
  module Pl_origin = Tjr_plist.Plist_intf.Pl_origin

  let freelist = 
    let open (struct 
      let fl_examples = Tjr_plist_freelist.fl_examples
      let fl_factory = fl_examples#freelist_factory 
      let fl_origin_ops = 
        fl_factory#origin_ops
          ~blk_dev_ops
          ~barrier
          ~sync
          ~blk_id:origin.fl_origin

      let with_ = 
        fl_factory#with_
          ~blk_dev_ops
          ~barrier
          ~sync
          ~origin_ops:fl_origin_ops
          ~params:fl_params

      end)
    in
    fl_origin_ops.read () >>= fun fl_origin -> 
    let pl_origin = fl_examples#origin_to_pl fl_origin in
    with_#plist_ops pl_origin >>= fun plist_ops -> 
    with_#with_plist_ops plist_ops >>= fun o -> 
    let min_free : _ Freelist_intf.min_free_ops = 
      match fl_origin.min_free with
      | None -> assert(false)
      | Some min_free -> Freelist_intf.{
          min_free_alloc=(fun r i -> 
              
        (min_free,(fun r -> 
);
    o#with_locked_ref (
      fl_factory#empty_freelist
        ~min_free:(fl_origin.min_free
                         
    

      
      let plist_ops () = with_#plist_ops (Plist_intf.Pl_origin.{hd;tl;blk_len})

  end

  (** {2 The global object map (GOM) } *)

        let gom = V2_gom.uncached
              ~blk_dev_ops
              ~blk_alloc
              ~init_btree_root:origin.gom_root
              
end

(*

module Gom = struct

  (** Could just use disjoint subsets of int and drop the constructors *)

  type id = dir_entry
  [@@deriving bin_io]

  let v_size = 10 (* FIXME check 9+1 *)

  let id_mshlr : id bp_mshlr = 
    (module 
      (struct 
        type t = id[@@deriving bin_io] 
        let max_sz = v_size
      end))
  
  let consistent_entry id e = 
    match id,e with
    | Fid _,Fid _  | Did _,Did _ | Sid _, Sid _ -> true
    | _ -> false

  type k = id[@@deriving bin_io]

  let k_mshlr : k bp_mshlr = id_mshlr

  (** The gom maps an id to a root blk *)
  type v = blk_id

  let v_mshlr : v bp_mshlr = bp_mshlrs#r_mshlr

  let k_cmp: id -> id -> int = Stdlib.compare

  (*
  let gom_args = object
    method k_cmp: id -> id -> int = Stdlib.compare
    method k_mshlr = k_mshlr
    method v_mshlr = v_mshlr
  end  
*)
  type r = Shared_ctxt.r
  let r_cmp = Shared_ctxt.r_cmp
  let r_mshlr = bp_mshlrs#r_mshlr  (* FIXME put in Shared_ctxt *)

  let cs = Shared_ctxt.(Tjr_btree.Bin_prot_marshalling.make_constants ~blk_sz ~k_size:r_size ~v_size)

  type t = Shared_ctxt.t
  let monad_ops = Shared_ctxt.monad_ops

end
(* open Gom *)

module Gom_btree = Tjr_btree.Pvt.Make_5.Make(Gom)

let gom_factory = lazy (
  Gom_btree.btree_factory 
    ~blk_dev_ops:(Lazy.force blk_dev_ops)
    ~blk_allocator_ops:(Lazy.force blk_alloc)
    ~blk_sz:Shared_ctxt.blk_sz)

(* FIXME move bt_1 etc to a top-level module *)
let (* gom_empty_leaf_as_blk,*) (gom_btree : (Gom.k,Gom.v,_,_,t) Tjr_btree.Pvt.Make_5.Btree_factory.bt_1 Lazy.t) = 
  lazy(
    (Lazy.force gom_factory)#make_uncached (Lazy.force root_ops))

let _ = gom_btree
*)
