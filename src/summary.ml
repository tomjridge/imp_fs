(** Summary of main interfaces *)

(** {2 File interface} 

{[
(** Construct file operations.

Sequence of steps:

- (1) Read the file origin block; this contains: file size; the
  blk-idx map root; the usedlist roots
- (2) Construct the usedlist
- (2.1) Get the usedlist origin information
- (2.2) And this provides the usedlist operations
- (2.3) And allocation should indirect via the usedlist (to record
  which blk_ids have been allocated)
- (3) Construct the blk-idx map
- (3.1) Construct the ops from the blk-idx root
- (4) Finally construct the file operations

*)
type ('buf,'blk,'blk_id,'t) file_factory = <

  (* (1) *)
  read_origin: 
    blk_dev_ops : ('blk_id,'blk,'t) blk_dev_ops -> 
    blk_id : 'blk_id -> 
    ('blk_id File_origin_block.t,'t)m;

  write_origin:
    blk_dev_ops : ('blk_id,'blk,'t) blk_dev_ops -> 
    blk_id : 'blk_id -> 
    origin: 'blk_id File_origin_block.t -> 
    (unit,'t)m;

  (* origin_to_fim: 'blk_id File_origin_block.t -> 'blk_id File_im.t; *)

  usedlist_origin : 'blk_id File_origin_block.t -> 'blk_id Usedlist.origin;
  (** (2.1) *)


  with_: 
    blk_dev_ops  : ('blk_id,'blk,'t) blk_dev_ops -> 
    barrier      : (unit -> (unit,'t)m) -> 
    sync         : (unit -> (unit,'t)m) -> 
    freelist_ops : ('blk_id,'t) blk_allocator_ops -> 
    <    
      usedlist_ops : 'blk_id Usedlist.origin -> (('blk_id,'t) Usedlist.ops,'t)m;
      (** (2.2) *)

      alloc_via_usedlist : 
        ('blk_id,'t) Usedlist.ops ->         
        ('blk_id,'t)blk_allocator_ops;
      (** (2.3) Allocate and record in usedlist *)
      
      mk_blk_idx_map  : 
        usedlist: ('blk_id,'t) Usedlist.ops ->        
        btree_root:'blk_id -> 
        (int,'blk_id,'blk_id,'t)Btree_ops.t;
      (** (3.1) *)
      
      file_ops: 
        usedlist           : ('blk_id,'t) Usedlist.ops -> 
        alloc_via_usedlist : (unit -> ('blk_id,'t)m) ->         
        blk_idx_map        : (int,'blk_id,'blk_id,'t)Btree_ops.t -> 
        file_origin        : 'blk_id ->         
        file_size          : int -> 
        ('buf,'t)file_ops;
      (** (4) *)

      (* Convenience *)

      file_from_origin_blk : 
        ('blk_id * 'blk_id File_origin_block.t) -> (('buf,'t)file_ops,'t)m;
      
      file_from_origin : 'blk_id -> (('buf,'t)file_ops,'t)m;
    >
>

(** Standard file operations, pwrite, pread, size and truncate.

NOTE we expect buf to be string for the functional version; for
   mutable buffers we may want to pass the buffer in as a parameter?

NOTE for pwrite, we always return src_len since all bytes are written
   (unless there is an error of course). 

For pread, we always return a buffer of length len.  *)
type ('buf,'t) file_ops = {
  size     : unit -> (int,'t)m;
  pwrite   : src:'buf -> src_off:offset -> src_len:len -> 
    dst_off:offset -> ((int (*n_written*),pwrite_error)result,'t)m;
  pread    : off:offset -> len:len -> (('buf,pread_error)result,'t)m;
  truncate : size:int -> (unit,'t)m;
  flush    : unit -> (unit,'t)m;
  sync     : unit -> (unit,'t)m;
}

  type 'blk_id file_origin_block = {
    file_size        : int; (* in bytes of course *)
    blk_idx_map_root : 'blk_id;
    usedlist_origin  : 'blk_id Usedlist.origin;
  }[@@deriving bin_io]

  (** usedlist_ops: The operations provided by the usedlist; in addition we need to
     integrate the freelist with the usedlist: alloc_via_usedlist 

      NOTE a sync is just a flush followed by a sync of the underlying
     blkdev, since we assume all object operations are routed to the
     same blkdev
  *)      
  type ('blk_id,'t) usedlist_ops = {
    add        : 'blk_id -> (unit,'t)m;    
    get_origin : unit -> ('blk_id origin,'t)m;
    flush      : unit -> (unit,'t)m;
  }

]}

*)


(** {2 V1 interfaces}


{[
(** Very basic types *)
module type S0 = sig
  type t
  val monad_ops: t monad_ops

  type fid  [@@deriving bin_io]
  type did  [@@deriving bin_io]
  type sid  [@@deriving bin_io] (** symlink id *)

end

(** Types created from basic types *)
module S1(S0:S0) = struct
  open S0

  (** We expect an implentation which maps an id to a root block *)

  type dir_entry = Fid of fid | Did of did | Sid of sid[@@deriving bin_io]                  

  type dir_k = str_256

  type dir_v = dir_entry

  type dh = (dir_k,dir_v,t)Tjr_btree.Btree_intf.ls (** dir handle *)
  (* NOTE since dh is supposed to not change whilst we traverse the directory, we can't just identify dh with ls *)


  type dir_ops = {
    find      : str_256 -> (dir_entry option,t)m;
    insert    : str_256 -> dir_entry -> (unit,t)m;
    delete    : str_256 -> (unit,t)m;
    ls_create : unit -> (dh,t)m;
    set_parent: did -> (unit,t)m;
    get_parent: unit -> (did,t)m;
    set_times : stat_times -> (unit,t)m;
    get_times : unit -> (stat_times,t)m;
  }

  (* val read_symlink: sid -> (str_256,t)m *)
  (* FIXME we probably want path res to return an sid, which could be
     just a string of course *)

  (* NOTE lookup failures for did and fid are dealt with in the monad *)
  type dirs_ops = {
    find   : did -> (dir_ops,t)m;    
    delete : did -> (unit,t)m;    
    (* create : did -> (unit,t)m; we can't create a dir without rb, which requires the parent *)
  }
  (** NOTE for create, use create_dir; this needs to add the did to the gom *)

  type fd = fid

  type buf = ba_buf

  (* FIXME open minifs automatically? or at least some of the
     submodules within minifs_intf? *)
  open Call_specific_errors

  type file_ops = {
    pread: foff:int -> len:int -> buf:buf -> boff:int -> 
      ((int,pread_err)result, t) m;
    pwrite: foff:int -> len:int -> buf:buf -> boff:int -> 
      ((int,pwrite_err)result, t) m;
    truncate: int -> (unit,t)m;
    get_sz: unit -> (int,t)m;
    set_times: stat_times -> (unit,t)m;
    get_times: unit -> (stat_times,t)m;
  }

  type files_ops = {
    find: fid -> (file_ops,t)m;
    create: fid -> stat_times -> (unit,t)m;
    (* delete: fid -> (unit,t)m; *)
  }
  (** NOTE create just creates a new file in the gom; it doesn't link
      it into a parent etc *)

  type resolved_path_or_err = (fid,did)Tjr_path_resolution.resolved_path_or_err

  type path = string

  type extra_ops = {
    internal_err: 'a. string -> ('a,t) m;
    is_ancestor: parent:did -> child:did -> (bool,t) m
  }
end

(** The values we expect to be present *)
module S2(S0:S0) = struct
  open S0 
  open S1(S0)

  module type T2 = sig
    val root_did: did

    val dirs: dirs_ops

    val files: files_ops

    val resolve_path: 
      follow_last_symlink:follow_last_symlink -> path -> (resolved_path_or_err,t)m

    val mk_stat_times: unit -> (stat_times,t)m

    val extra: extra_ops

    (* in a later step, we refine this further *)
    val create_dir: parent:did -> name:str_256 -> times:stat_times -> (unit,t)m

    val create_file: parent:did -> name:str_256 -> times:stat_times -> (unit,t)m

    val create_symlink: parent:did -> name:str_256 -> times:stat_times -> contents:str_256 -> (unit,t)m
  end
end

(** The resulting filesystem (fd is a file identifier, dh is a
   directory handle supporting leaf_stream operations) *)
module type T = sig
  type t
  type fd
  type dh
  val ops: (fd,dh,t) Minifs_intf.ops
end

]}

See also {!V1_generic.Make.Make_2} (abstract ops) and {!V1_generic.Make.Make_3} (with restricted sig) and {!V1.With_gom.The_filesystem} (the actual impl we use)
*)
