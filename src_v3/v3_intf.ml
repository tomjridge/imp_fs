(** Interfaces describing the abstract development of V3.

Levels/layers are the layers from standard software engineering. For us, level 0 is the
topmost, and level 1 sits underneath etc.

Stages are related to time: stage 1 occurs before stage 2 etc. This recognizes that some
values are available only at later points in time.

Initially path resolution is carried out without locks. Afterwards, we lock objects that
we need to, validate the entries, and then go ahead with the modifications. If we detect
changes between path res and execution, we backout with a concurrent modification error.

*)

(** {1 Preamble} *)

open struct
  type times = Tjr_fs_shared.Times.times
end

module Errors = Minifs_intf.Error_

(** To avoid adding an error that is not in POSIX, we introduce this pseudo error
    constructor which just default to [`Error_other] *)
let eRROR_CONCURRENT_MODIFICATION = `Error_other


(** NOTE The following are copied from v1_generic.ml *)

module Path_resolution = struct

  (* FIXME we need path res to take account of symlink id, which can
     just be the string contents of course *)
  type ('fid,'did,'sid) resolved_component 
    = ('fid,'did,'sid) Tjr_path_resolution.Intf.simplified_result' 
    = File of 'fid | Dir of 'did | Sym of 'sid*string | Missing

  type ('fid,'did,'sid) resolved_path 
    (* = ('fid,'did,'sid) Tjr_path_resolution.Intf.simplified_result  *)
    = {
    parent_id      : 'did;
    comp           : str_256; (* NOTE string in tjr_path_resolution *)
    result         : ('fid,'did,'sid) resolved_component;
    trailing_slash : bool
  }

  type ('fid,'did) resolved_err = ('fid,'did) Tjr_path_resolution.Intf.resolved_err

  type ('fid,'did,'sid) resolved_path_or_err =
    ( ('fid,'did,'sid) resolved_path, ('fid,'did) resolved_err) result

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

  (** NOTE tid (thread id) is really "request id", similar to requests
     in http servers *)
  type ('tid,'id,'t) lock_ops = {
    lock       : tid:'tid -> objs:'id list -> (unit,'t)m;
    unlock     : tid:'tid -> objs:'id list -> (unit,'t)m;
    (* unlock_all : tid:'tid -> (unit,'t)m; *)
  }
end
open Lock_ops


(** {1 Basic types, extended types} 

From a basic set of types, we construct further types.

*)

(** Very basic types: monadic t, fid, did, sid... *)
module type S0 = sig
  type t
  val monad_ops: t monad_ops

  type fid  [@@deriving bin_io, yojson]
  type did  [@@deriving bin_io, yojson]
  type sid  [@@deriving bin_io, yojson] (** symlink id *)

  type dh [@@deriving bin_io] (** dir handle FIXME bin_io? *)

  type tid [@@deriving bin_io] (** thread id FIXME do we need bin_io? *)
end


(* $ (PIPE2SH("""sed -n '/Types[ ]created/,/^end/p' >GEN.S1.ml_""")) *)
(** Types created from basic types *)
module S1(S0:S0) = struct
  open S0

  (** {2 Directory types} *)

  type dir_entry = Fid of fid | Did of did | Sid of sid[@@deriving bin_io, yojson]

  type dir_k = str_256

  type dir_v = dir_entry

  (** Operations on a single directory *)
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

    sync: lock_held:bool -> did:did -> (unit,t)m; (** used in rename *)
  }
  (** NOTE the parent field is an inter-object link and so needs to be
     updated atomically ; NOTE no nlink for dirs, but POSIX probably
     requires it? *)

  type rename_file =
    | Rename_file_missing of {
        locks_held: unit;
        times : times;
        src   : did*str_256*fid;
        dst   : did*str_256 }
    (** Assumes src and dst are locked and src_name,fid in src; src
        may equal dst; if so, src_name<>dst_name FIXME check this in
        impl; tid enable check of locked-ness and contents of src and
        dst *)

    | Rename_file_file of {
        locks_held: unit;
        times : times;
        src   : did*str_256*fid;
        dst   : did*str_256*fid }
      (** fid1<>fid2, so (did1,name1) <> (did2,name2) *)

  type rename_dir_missing = 
    | Rename_dir_missing of {
        tid : tid;
        locks_not_held    : unit;
        needs_ancestor_check : unit;
        (* times             : times; *)
        src               : did*str_256*did;
        dst               : did*str_256 }
    (** Implementation should incorporate ancestor check,locking etc etc *)


  (* NOTE lookup failures for did and fid are dealt with in the monad *)
  (** Operations to create/delete a directory, and operate on more than one directory (eg
      cross-directory rename) *)
  type dirs_ops = {
    (* find   : did -> (dir_ops,t)m; *)
    delete : did -> (unit,t)m;

    create_and_add_to_parent : 
      parent_locked:unit -> 
      parent:did -> name:str_256 -> times:times -> (unit,t)m; (* was create_dir *)

    rename_file : rename_file -> (unit,t)m;
    rename_dir  : rename_dir_missing -> ((unit,Errors.exn_)result,t)m;
    
  }


  (** {2 Dir handles} *)

  type dir_handle_ops = {
    opendir  : tid:tid -> did -> (dh,t)m;
    readdir  : tid:tid -> dh -> (str_256 list,t) m;
    closedir : tid:tid -> dh -> (unit,t)m;
  }


  (** {2 Types related to files} *)

  type fd = fid

  type buf = Shared_ctxt.buf

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
  (** NOTE the nlink field is derived from inter-object links, and so needs to be updated
      atomically *)

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

  (* type 'a is_ancestor_rtype = { krefs:'a; locked_objs: dir_entry list } *)

  type extra_ops = {
    internal_err : 'a. tid:tid -> string -> ('a,t) m;
  }

end


(** {1 Abstract types describing implementation in terms of levels and stages} *)

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

    val mk_stat_times: unit -> times

    val extra: extra_ops
  end
end

(* $ (PIPE2SH("""sed -n '/module[ ]Level1_provides/,/^end/p' >GEN.Level1_provides.ml_""")) *)
(** Level 1 provides a single "ops" record of operations, parameterized by thread id [tid] *)
module Level1_provides = struct
  type buf = Shared_ctxt.buf
  open Minifs_intf.Call_specific_errors
  type ('tid,'fd,'dh,'t) ops = {
    root     : path;
    unlink   : tid:'tid -> path -> ((unit,unlink_err)r_, 't) m;
    mkdir    : tid:'tid -> path -> ((unit,mkdir_err)r_,'t) m;
    opendir  : tid:'tid -> path -> (('dh,opendir_err)r_, 't) m;
    readdir  : tid:'tid -> 'dh  -> ((string list,readdir_err)r_, 't) m;
    (** NOTE . and .. are returned *)

    closedir : tid:'tid -> 'dh  -> ((unit,closedir_err)r_,  't) m;
    create   : tid:'tid -> path -> ((unit,create_err)r_,  't) m;
    (** create is to create a file; use mkdir for a dir *)

    open_    : tid:'tid -> path -> (('fd,open_err)r_,  't) m;

    pread    : 
      tid:'tid -> fd:'fd -> foff:int -> len:int -> buf:buf -> boff:int -> 
      ((int,pread_err)r_, 't) m; 

    pwrite   : 
      tid:'tid -> fd:'fd -> foff:int -> len:int -> buf:buf -> boff:int -> 
      ((int,pwrite_err)r_, 't) m;

    close    : tid:'tid -> 'fd  -> ((unit,close_err)r_,  't) m;

    rename   : tid:'tid -> path -> path -> ((unit,rename_err)r_,  't) m;

    truncate : tid:'tid -> path -> int -> ((unit,truncate_err)r_,  't) m;
    (** truncate a file to a given len *)

    stat     : tid:'tid -> path -> ((stat_record,stat_err)r_,  't) m;
    symlink  : tid:'tid -> path -> path -> ((unit,symlink_err)r_, 't) m;
    readlink : tid:'tid -> path -> ((string,readlink_err)r_,'t) m;
    reset    : unit -> (unit,  't) m;
  }
end

(** Level 0 provides a single "ops" record of operations, as Minifs_intf.Ops_type.ops;
    note that we convert Shared_ctxt.buf to ba_buf *)
module Level0_provides = struct
  type ('fd,'dh,'t) ops = ('fd,'dh,'t) Minifs_intf.Ops_type.ops    
end


