(** Abstract development of V3.

See {!module:V3}.

Initial path resolution is carried out without locks. After, we lock
   objects that we need to, validate the entries, and then go ahead
   with the modifications. If we detect changes between path res and
    execution, we backout with a concurrent modification error.

*)

type times = Minifs_intf.times


module Errors = struct
  (** Typical errors we encounter *)
  type exn_ = [ 
    | `Error_no_entry
    | `Error_not_directory
    | `Error_not_file
    | `Error_not_symlink
    | `Error_attempt_to_rename_dir_over_file
    | `Error_attempt_to_rename_root
    | `Error_attempt_to_rename_to_subdir
    | `Error_no_src_entry
    | `Error_path_resolution
    | `Error_not_empty
    | `Error_exists
    | `Error_is_directory
    | `Error_concurrent_modification (* new, compared to minifs *)
    | `Error_other
  ] [@@deriving bin_io, yojson]
end


(** NOTE The following are copied from v1_generic.ml *)

module Path_resolution = struct

  (* FIXME we need path res to take account of symlink id (which can
     just be the string contents of course, or unit *)
  type ('fid,'did,'sid) resolved_component =
      File of 'fid | Dir of 'did | Sym of 'sid*string | Missing

  type ('fid,'did,'sid) resolved_path = {
    parent_id      : 'did;
    comp           : str_256;
    result         : ('fid,'did,'sid) resolved_component;
    trailing_slash : bool
  }

  type ('fid,'did) err = ('fid,'did) Tjr_path_resolution.Intf.resolved_err

  type ('fid,'did,'sid) resolved_path_or_err =
    ( ('fid,'did,'sid) resolved_path, ('fid,'did) err) result

  type follow_last_symlink = Tjr_path_resolution.Intf.follow_last_symlink

  (** Alias this type *)
  type ('fid,'did,'sid,'t) resolve_path = {
    resolve_path:
      follow_last_symlink:follow_last_symlink ->
      path ->
      (('fid,'did,'sid) resolved_path_or_err,'t)m
  }
end

module Lock_ops = struct
(*
  (** Use raw ops to implement the per-object locks *)
  type ('tid,'lck,'t) raw_lock_ops = {
    create : unit -> ('lck,'t)m;
    lock   : tid:'tid -> lck:'lck -> (unit,'t)m;
    unlock : tid:'tid -> lck:'lck -> (unit,'t)m;
  }
*)

  type ('tid,'id,'t) lock_ops = {
    lock       : tid:'tid -> objs:'id list -> (unit,'t)m;
    unlock     : tid:'tid -> objs:'id list -> (unit,'t)m;
    (* unlock_all : tid:'tid -> (unit,'t)m; *)
  }
end
open Lock_ops

module type S0 = sig
  type t
  val monad_ops: t monad_ops

  type fid  [@@deriving bin_io]
  type did  [@@deriving bin_io]
  type sid  [@@deriving bin_io] (** symlink id *)

  type dh [@@deriving bin_io] (** dir handle FIXME bin_io? *)

  type tid [@@deriving bin_io] (** thread id FIXME do we need bin_io? *)
end


(* $ (PIPE2SH("""sed -n '/Types[ ]created/,/^end/p' >GEN.S1.ml_""")) *)
(** Types created from basic types *)
module S1(S0:S0) = struct
  open S0

  (** {2 Directory types} *)

  type dir_entry = Fid of fid | Did of did | Sid of sid[@@deriving bin_io]

  type dir_k = str_256

  type dir_v = dir_entry

  type dir_ops = {
    find      : did:did -> str_256 -> (dir_entry option,t)m;
    insert    : did:did -> str_256 -> dir_entry -> (unit,t)m;
    delete    : 
      did_locked: unit -> 
      did:did -> name:str_256 -> obj:dir_entry -> reduce_obj_nlink:unit -> (unit,t)m;

    set_times : did:did -> times -> (unit,t)m;
    get_times : did:did -> (times,t)m;

    (* set_parent: did:did -> parent:did -> (unit,t)m; (\** NOTE inter-object link *\) *)
    get_parent: did:did -> (did,t)m;

    sync: did:did -> (unit,t)m; (** used in rename *)
  }
  (** NOTE the parent field is an inter-object link and so needs to be
     updated atomically ; NOTE no nlink for dirs, but POSIX probably
     requires it? *)

  type rename_case =
    | Rename_file_missing of {
        times : times;
        src   : did*str_256*fid;
        dst   : did*str_256 }
    (** Assumes src and dst are locked and src_name,fid in src; src
        may equal dst; if so, src_name<>dst_name FIXME check this in
        impl; tid enable check of locked-ness and contents of src and
        dst *)

    | Rename_file_file of {
        times : times;
        src   : did*str_256*fid;
        dst   : did*str_256*fid }
      (** fid1<>fid2, so (did1,name1) <> (did2,name2) *)

    | Rename_dir_missing of {
        times : times;
        src   : did*str_256*did;
        dst   : did*str_256 }
      (** Assumes all locks held, ancestor check completed etc *)


  (* NOTE lookup failures for did and fid are dealt with in the monad *)
  type dirs_ops = {
    (* find   : did -> (dir_ops,t)m; *)
    delete : did -> (unit,t)m;

    create_and_add_to_parent : 
      parent_locked:unit -> 
      parent:did -> name:str_256 -> times:times -> (unit,t)m; (* was create_dir *)

    rename: locks_held:unit -> rename_case -> (unit,t)m;

  }


  (** {2 Dir handles} *)

  type dir_handle_ops = {
    opendir  : tid:tid -> did -> (dh,t)m;
    readdir  : dh -> (str_256 list,t) m;
    closedir : dh -> (unit,t)m;
  }


  (** {2 Types related to files} *)

  type fd = fid

  type buf = ba_buf

  open struct
    open Call_specific_errors
    type nonrec pread_err = pread_err
    type nonrec pwrite_err = pwrite_err
  end

  type file_ops = {
    pread     : fid:fid -> foff:int -> len:int -> buf:buf -> boff:int -> ((int,pread_err)result, t) m;
    pwrite    : fid:fid -> foff:int -> len:int -> buf:buf -> boff:int -> ((int,pwrite_err)result, t) m;
    truncate  : fid:fid -> int -> (unit,t)m;
    get_sz    : fid:fid -> (int,t)m;
    set_times : fid:fid -> times -> (unit,t)m;
    get_times : fid:fid -> (times,t)m;
    (* get_nlink : fid:fid -> (int,t)m; *)
    (* set_nlink : fid:fid -> int -> (unit,t)m;  (\** NOTE inter-object *\) *)

    sync      : fid:fid -> (unit,t)m;
  }
  (** NOTE the nlink field is derived from inter-object links, and so
      needs to be updated atomically *)

  type files_ops = {
    (* delete: fid -> (unit,t)m; - use ref counting and impl at level 2 *)

    create_and_add_to_parent: parent:did -> name:str_256 -> times:times -> (unit,t)m;
    (* NOTE files.create: parent is a dir, and we don't have nlinks for dirs *)

    create_symlink_and_add_to_parent:
      parent:did -> name:str_256 -> times:times -> contents:str_256 -> (unit,t)m
  }


  (** {2 Path resolution types} *)

  type nonrec resolve_path = (fid,did,sid,t) Path_resolution.resolve_path

  type path = string

  type extra_ops = {
    internal_err : 'a. string -> ('a,t) m;
    is_ancestor  : parent:did -> child:did -> (bool,t) m
  }

  (* type id = dir_entry *)
(*
  type kref_ops = {
    get: tid:tid -> id list -> unit;
    put: tid:tid -> id list -> unit;
  }
*)
end

(* $ (PIPE2SH("""sed -n '/The[ ]values/,/^end/p' >GEN.S2.ml_""")) *)
(** The values we expect to be present at level 2 *)
module Level2_provides(S0:S0) = struct
  open S0
  open S1(S0)

  module type T2 = sig
    val root_did: did

    val dir: dir_ops

    val dirs: dirs_ops

    val file: file_ops

    val files: files_ops

    val dir_handles: dir_handle_ops

    val locks: (tid,dir_entry,t)lock_ops

    val resolve_path: resolve_path

    val mk_stat_times: unit -> (times,t)m

    val extra: extra_ops
  end
end

(* $ (PIPE2SH("""sed -n '/The[ ]resulting/,/^end/p' >GEN.T.ml_""")) *)
(** The resulting filesystem (fd is a file identifier, dh is a
   directory handle supporting leaf_stream operations) *)
module type Level1_provides = sig
  type t
  type fd
  type dh
  val ops: (fd,dh,t) Minifs_intf.ops
end



(** {2 Implementation types} *)

module Refs_with_dirty_flags = struct
  type 'a ref = {
    mutable value: 'a;
    mutable dirty: bool
  }

  (** values start off clean *)
  let ref value = {value;dirty=false}

  let is_dirty x = x.dirty

  let clean x = x.dirty <- false

  (** assigning sets the dirty flag *)
  let ( := ) r value = r.value <-value; r.dirty <- true

  let (!) r = r.value
end
open struct module R = Refs_with_dirty_flags end


(** For this version, we implement a file using an underlying
   filesystem. NOTE we also store sz in the database; we should check
   that the on-disk version agrees with the DB when resurrecting (and
   maybe times too). *)
type per_file = {
  filename       : string;
  file_descr     : Lwt_unix.file_descr;
  lock           : Lwt_mutex.t;  
  times          : times R.ref;
  sz             : int R.ref;
}
