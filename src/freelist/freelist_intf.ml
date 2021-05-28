(** Main interfaces used by the freelist *)


open Tjr_plist.Plist_intf


(** 

There are two versions of the freelist:

- For_blkids: to implement the "usual" blk freelist, managing on-disk
  blocks; the elts are actually blk_ids

- For_arbitrary_elts: which allocates and frees arbitrary elements;
  some other freelist of the first type provides the alloc and free
  blk functionality

*)
(* $(PIPE2SH("""sed -n '/type[ ].*version/,/^$/p' >GEN.version.ml_""")) *)
type ('elt,'blk_id,'t) version = 
  | For_blkids of ('elt,'blk_id) for_blk_ids
  | For_arbitrary_elts of 
      { alloc: unit -> ('blk_id,'t)m; free: 'blk_id -> (unit,'t)m }
      (** For arbitrary elts, we need a way to allocate and free blocks *)
and ('elt,'blk_id) for_blk_ids = 
  { e2b:'elt -> 'blk_id; b2e: 'blk_id -> 'elt } 



module Fl_origin = struct
  open Bin_prot.Std
  open Sexplib.Std
  (* $(PIPE2SH("""sed -n '/type[ ][^=]*fl_origin/,/}/p' >GEN.fl_origin.ml_""")) *)
  type ('a,'blk_id) fl_origin = {
    hd: 'blk_id;
    tl: 'blk_id;
    blk_len: int;
    min_free: 'a option
  }[@@deriving bin_io, sexp]

  type ('a,'blk_id) t = ('a,'blk_id) fl_origin[@@deriving bin_io, sexp]
end


type fIXME

(* $(PIPE2SH("""sed -n '/type[ ].*freelist_ops/,/^}$/p' >GEN.freelist_ops.ml_""")) *)
type ('a,'blk_id,'t) freelist_ops = {
  alloc      : unit -> ('a,'t)m;
  alloc_many : int -> (fIXME,'t)m;
  free       : 'a -> (unit,'t)m;
  free_many  : fIXME -> (unit,'t)m;
  get_origin : unit -> (('a,'blk_id)Fl_origin.t,'t)m;
  sync       : unit -> (unit,'t)m;
  (** NOTE the freelist already ensures it is crash safe; this sync is
     really for tidy shutdown *)
}
(** alloc_many: int is the number of blk_ids although this sort-of
   forces us to unmarshal the whole block; perhaps prefer also storing
   the number of elements in the block, precisely for this reason;
   maybe something to $(CONSIDER())

free_many: free an entire list of blk_ids, by appending to this
   freelist *)

let freelist_to_blk_allocator freelist_ops : _ blk_allocator_ops = 
  let { alloc; free; _ } = freelist_ops in
  { blk_alloc=alloc; blk_free=free }


(* $(PIPE2SH("""sed -n '/^type[ ].*min_free_ops/,/^}$/p' >GEN.min_free_ops.ml_""")) *)
type 'a min_free_ops = {
  min_free_alloc: 'a -> int -> 'a list * 'a
}
(** Working with a min_free element, we provide a method that takes
   the min_free element, and a count of the number of frees required,
   and returns a list of free elements (of the same length as
   requested) and the new min_free *)


(* $(PIPE2SH("""sed -n '/In-memory[ ]state for the freelist /,/^}$/p' >GEN.freelist_im.ml_""")) *)
(** In-memory state for the freelist *)
type 'a freelist_im = {
  transient          : 'a list; 
  min_free           : 'a option; 
  
  waiting            : ('a event list);
  disk_thread_active : bool;
}
(** The freelist state, in addition to the plist.

- transient: a non-persistent list of free elts that can be allocated

- min_free: free elts are stored in transient, or in the on-disk plist;
usually we also have an "upper bound", beyond which we can still
allocate elts freely (an elt is either in transient, allocated to some
object, in the plist, or geq min_free)

- waiting: a list of threads waiting for a disk process to return

NOTE: we assume that the state is accessed via with_state (ie with a
mutex), so only one thread at a time modifies state

*)

let empty_freelist ~min_free = {
  transient=[];
  min_free;
  waiting=[];
  disk_thread_active=false
}

let _ = empty_freelist


type params = <
  tr_upper:int;
  tr_lower:int;
  min_free_alloc_size:int;
  (* debug: ('a freelist_im -> string)option;    *)
>
      
(* NOTE 'a is 'blk_id when working with the standard freelist *)

(* $(PIPE2SH("""sed -n '/type[ ].*freelist_factory/,/^>/p' >GEN.freelist_factory.ml_""")) *)
type ('a,'buf,'blk_id,'blk,'t) freelist_factory = <
  version       : ('a, 'blk_id) for_blk_ids; 
  (** NOTE this is for freelist only, not arbitrary elts *)

  empty_freelist : min_free:'a option -> 'a freelist_im;
  (** [min_free] depends on the nature of 'a; for 'a = blk_id, we can
     use the origin blk_id and incr to implement min_free *)

  read_origin:
    blk_dev_ops : ('blk_id,'blk,'t)blk_dev_ops ->
    blk_id      : 'blk_id -> 
    (('a,'blk_id) Fl_origin.t,'t)m;

  write_origin:
    blk_dev_ops : ('blk_id,'blk,'t)blk_dev_ops ->
    blk_id      : 'blk_id -> 
    origin      : ('a,'blk_id) Fl_origin.t -> 
    (unit,'t)m;

  fl_origin_to_pl : ('a,'blk_id) Fl_origin.t -> 'blk_id Pl_origin.t;

  with_: 
    blk_dev_ops : ('blk_id,'blk,'t)blk_dev_ops ->
    barrier     : (unit -> (unit,'t)m) -> 
    sync        : (unit -> (unit,'t)m) -> 
    params      : params ->
    <          
      plist_ops : 'a Pl_origin.t -> (('a,'buf,'blk_id,'t) plist_ops,'t)m;
      (** The freelist uses a persistent list. This is provided for convenience. *)

      with_plist_ops : ('a,'buf,'blk_id,'t) plist_ops -> 
        <
          with_state : 
            ('a freelist_im,'t)with_state -> ('a,'blk_id,'t)freelist_ops;
          (** General version *)

          with_locked_ref : 'a freelist_im -> 
            < freelist_ops: ('a,'blk_id,'t)freelist_ops;
              freelist_ref: 'a freelist_im ref;
            >;
          (** Use an imperative ref to hold the state; lock for concurrency *)
        >;
      (** NOTE these operations do not involve an origin *)

      add_origin_autosync: 
        origin_blk_id:'blk_id -> 
        ('a,'blk_id,'t)freelist_ops -> 
        ('a,'blk_id,'t)freelist_ops;
      (** A wrapper for freelist_ops. This automatically syncs the
         origin block when origin data changes; $(CONSIDER()) more efficient
         would be to sync only when hd advances. It is assumed that
         initially the origin blk_id is synced *) 

      initialize: origin:'blk_id -> free_blk: 'blk_id -> min_free:'a option -> (unit,'t)m;
      (** Create a new freelist; the free_blk is used to hold an empty
         plist *)

      (* Convenience *)
      
      (* was "from_origin" *)
      restore: autosync:bool -> origin:'blk_id -> 
        (< freelist_ops: ('a,'blk_id,'t)freelist_ops;
           freelist_ref: 'a freelist_im ref;
         >,'t)m;
    >    
>


