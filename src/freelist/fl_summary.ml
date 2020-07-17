(** Summary of main types *)

(**

{[
  type ('a,'blk_id) fl_origin = {
    hd: 'blk_id;
    tl: 'blk_id;
    blk_len: int;
    min_free: 'a option
  }[@@deriving bin_io]

type ('a,'buf,'blk_id,'t) freelist_factory = <
  version       : ('a, 'blk_id) for_blk_ids; 
  (** NOTE this is for freelist only, not arbitrary elts *)

  empty_freelist : min_free:'a option -> 'a freelist_im;
  (** [min_free] depends on the nature of 'a; for 'a = blk_id, we can
     use the origin blk_id and incr to implement min_free *)

  read_origin:
    blk_dev_ops : ('blk_id,'buf,'t)blk_dev_ops ->
    blk_id      : 'blk_id -> 
    (('a,'blk_id) Fl_origin.t,'t)m;

  write_origin:
    blk_dev_ops : ('blk_id,'buf,'t)blk_dev_ops ->
    blk_id      : 'blk_id -> 
    origin      : ('a,'blk_id) Fl_origin.t -> 
    (unit,'t)m;

  fl_origin_to_pl : ('a,'blk_id) Fl_origin.t -> 'blk_id Pl_origin.t;

  with_: 
    blk_dev_ops : ('blk_id,'buf,'t)blk_dev_ops ->
    barrier     : (unit -> (unit,'t)m) -> 
    sync        : (unit -> (unit,'t)m) -> 
    params      : params ->
    <          
      plist_ops : 'a Pl_origin.t -> (('a,'buf,'blk_id,'t) plist_ops,'t)m;

      with_plist_ops : ('a,'buf,'blk_id,'t) plist_ops -> 
        <
          with_state : 
            ('a freelist_im,'t)with_state -> ('a,'blk_id,'t)freelist_ops;

          with_locked_ref : 'a freelist_im -> 
            < freelist_ops: ('a,'blk_id,'t)freelist_ops;
              freelist_ref: 'a freelist_im ref;
            >
        (** use an imperative ref to hold the state; lock for concurrency *)
        >;


      add_origin_autosync: 
        blk_id:'blk_id -> 
        freelist_ops:('a,'blk_id,'t)freelist_ops -> 
        ('a,'blk_id,'t)freelist_ops;
      (** This automatically syncs the origin block when origin data
         changes; FIXME more efficient would be to sync only when hd
         advances. *) 


      (* Convenience *)

      from_origin: 'blk_id -> 
        (< freelist_ops: ('a,'blk_id,'t)freelist_ops;
           freelist_ref: 'a freelist_im ref;
         >,'t)m;
          
      from_origin_with_autosync: 'blk_id -> 
        (< freelist_ops: ('a,'blk_id,'t)freelist_ops;
           freelist_ref: 'a freelist_im ref;
         >,'t)m;
    >    
>

(** In-memory state for the freelist *)
type 'a freelist_im = {
  transient          : 'a list; 
  min_free           : 'a option; 
  
  waiting            : ('a event list);
  disk_thread_active : bool;
}

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

type 'a min_free_ops = {
  min_free_alloc: 'a -> int -> 'a list * 'a
}

type ('elt,'blk_id,'t) version = 
  | For_blkids of ('elt,'blk_id) for_blk_ids
  | For_arbitrary_elts of 
      { alloc: unit -> ('blk_id,'t)m; free: 'blk_id -> (unit,'t)m }
      (** For arbitrary elts, we need a way to allocate and free blocks *)
and ('elt,'blk_id) for_blk_ids = 
  { e2b:'elt -> 'blk_id; b2e: 'blk_id -> 'elt } 


]}

*)

