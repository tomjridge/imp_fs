(** V2 generic development based on V1 generic; this layer deals with
   path resolution, times and a few other things; assumes create_dir
   and create_file. *)


(** $(INDEX("time,times,timestamps"))

Short note on times:

POSIX has an informal spec. SibylFS has a formal spec of timestamps,
but here I want a very short description of atime, mtime and ctime,
suitable for a rough implementation of times.

Sources:

- https://www.howtogeek.com/517098/linux-file-timestamps-explained-atime-mtime-and-ctime/
- https://www.unixtutorial.org/atime-ctime-mtime-in-unix-filesystems/

atime: last access time (read? or read/write? what is the meaning of
"access"? probably "read of file data") for file or directory; often
disabled (since updating causes a disk write)

ctime: last (metadata) change (ownership, access perms, ...); also
updated if contents change; probably doesn't affect us since we don't
currently have ownership or access perms

mtime: last change to file contents; does not change when eg
ownership/perms change

Since "file size" is typically counted as metadata (!), it is clear
that parts of metadata are considered differently from others (ctime
identifies some subset of file metadata... ctime doesn't change when
file size changes I expect).

Personally, I would consider file data (and the related file size) as
independent of anything else about the file (ownership, permissions,
timestamp). Then it makes sense to talk about when the file data (not
the metadata!) was last read, and when it was last written. So, for
us, let's define atime as "last read time", ignore ctime (identify
with mtime), and mtime as the time the data was last modified
(including truncate of course).

Since atime is usually ignored/disabled anyway, we end up with just
mtime. For files, this likely changes on pwrite and truncate
(set_times confusingly doesn't itself automatically update mtime,
since the purpose of set_times is precisely to allow updating the
mtime to some specific value).

For directories, we should update mtime on insert and delete. The
slight issue is that we may want a cross-directory rename to appear to
happen at a single time. For a cross directory rename, we should call
"set_times" on both the source and destination directory, with the
same timestamp, AFTER the rename has inserted and deleted.

Should files/directories be responsible for updating their own
timestamps? As above, this makes sense, except in cases like rename
where we may want the src and target dirs to have identical
mtimes. (And even in this case, does anyone program applications to
work only if this behaviour is implemented by the filesystem?). So we
DON'T explicitly update times in this generic code (unless eg for
rename case)

*)


type exn_ = Tjr_minifs.Minifs_intf.exn_

type stat_times = Minifs_intf.times

type stat_record = Minifs_intf.stat_record
(*
{
  sz:int;
  kind:[`F | `D | `S];
  times: stat_times
}
*)

(* $(PIPE2SH("""sed -n '/Very[ ]basic types/,/end/p' >GEN.S0.ml_""")) *)
(** Very basic types *)
module type S0 = sig
  type t
  val monad_ops: t monad_ops

  type fid  [@@deriving bin_io]
  type did  [@@deriving bin_io]
  type sid  [@@deriving bin_io] (** symlink id *)

end

(* $(PIPE2SH("""sed -n '/Types[ ]created/,/^end/p' >GEN.S1.ml_""")) *)
(** Types created from basic types *)
module S1(S0:S0) = struct
  open S0

  (** We expect an implementation which maps an id to a root block *)

  type dir_entry = (fid,did,sid) V2_intf.dir_entry'[@@deriving bin_io]                  

  type dir_k = str_256

  type dir_v = dir_entry

  type dh = (dir_k,dir_v,t)Tjr_btree.Btree_intf.ls (** dir handle *)
  (* NOTE since dh is supposed to not change whilst we traverse the
     directory, we can't just identify dh with ls *)

  (* In the generic development, this is the only place where blk_id
     appears; arguably it is part of the internal interface; however,
     I am reluctant to remove the get_origin field from the dir_ops
     type FIXME just have another type which omits the get_origin
     field, surely? *)
  type blk_id' = Shared_ctxt.r
  type dir_ops = (str_256,dir_entry,blk_id',t,did) Dir_impl.Dir_ops.t

  (* val read_symlink: sid -> (str_256,t)m *)
  (* FIXME we probably want path res to return an sid, which could be
     just a string of course *)

  (* NOTE lookup failures for did and fid are dealt with in the monad *)
  type dirs_ops = {
    find   : did -> (dir_ops,t)m;    
    delete : did -> (unit,t)m;    
    create_dir: parent:did -> name:str_256 -> times:stat_times -> (unit,t)m;
  }
  (** NOTE for dir.create, use create_dir; this needs to:
      allocate a new did;
      initialize a new directory;
      add the did to the GOM;
      add the did to the parent *)

  type fd = fid

  type buf = ba_buf

  (* FIXME open minifs automatically? or at least some of the
     submodules within minifs_intf? *)
  (* open Call_specific_errors *)

(*
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
*)

  type file_ops = (blk_id',buf,t) File_impl_v2.file_ops

  type files_ops = {
    find: fid -> (file_ops,t)m;
    (* create_raw: fid -> stat_times -> (unit,t)m; FIXME not needed? *)
    create_file: parent:did -> name:str_256 -> times:stat_times -> (unit,t)m;
    create_symlink: parent:did -> name:str_256 -> times:stat_times -> contents:str_256 -> (unit,t)m;
  }

  (* NOTE create_raw just creates a new file in the gom; it doesn't link
      it into a parent etc *)

  type resolved_path_or_err = (fid,did,sid)Tjr_path_resolution.resolved_path_or_err

  type path = string

  type extra_ops = {
    internal_err: 'a. string -> ('a,t) m;
    is_ancestor: parent:did -> child:did -> (bool,t) m
  }
end

(* $(PIPE2SH("""sed -n '/The[ ]values/,/^end/p' >GEN.S2.ml_""")) *)
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
  end
end


(* $(PIPE2SH("""sed -n '/The[ ]resulting/,/^end/p' >GEN.T.ml_""")) *)
(** The resulting filesystem (fd is a file identifier, dh is a
   directory handle supporting leaf_stream operations) *)
module type T = sig
  type t
  type fd
  type dh
  val ops: (fd,dh,t) Minifs_intf.ops
end

module Make(S0:S0) = struct
  open S0
  module S1 = S1(S0)
  open S1
  module S2 = S2(S0)
  open S2

  (** Make step 2, assuming values (from T2) *)
  module Make_2(X:T2) = struct
    open X
    let ( >>= ) = monad_ops.bind
    let return = monad_ops.return

    (** propagate errors *)
    let ( >>=| ) a b = a >>= function
      | Ok a -> b a
      | Error e -> return (Error e)

    let _ = ( >>=| )

    let err (e:exn_) = return (Error e)

    let ok x = return (Ok x)


    (* pull these create functions from X *)
    let create_file = files.create_file
    let create_symlink = files.create_symlink
    let create_dir = dirs.create_dir


    let resolve_path ~follow_last_symlink path =
      resolve_path ~follow_last_symlink path >>= function
      | Ok x -> return (Ok x)
      | Error _e ->
        (* e is a path resolution error, so we need to map it to a
           standard error FIXME this probably belongs with the
           path_resolution code *)
        return (Error `Error_other) (* FIXME *)      

    (* FIXME not sure about Always in following two funs *)
    let resolve_file_path (path:path) : ((fid,exn_)result,t) m = 
      resolve_path ~follow_last_symlink:`Always path >>= function
      | Ok { result=(File fid); _ } -> return (Ok fid)
      | _ -> err `Error_not_file

    let resolve_dir_path (path:path) : ((did,exn_)result,t) m = 
      resolve_path ~follow_last_symlink:`Always path >>= function 
      | Ok { result=(Dir did);_ } -> return (Ok did)
      | _ -> err `Error_not_directory

    let root : path = "/" 

    open Tjr_path_resolution.Intf


    (* FIXME or just allow unlink with no expectation of the kind? FIXME
       add a "follow" flag? optional? FIXME how does parent/name interact
       with symlinks? FIXME perhaps keep parent/name, but allow another
       layer ot deal with follow; FIXME not sure about `If_trailing_slash  *)
    let unlink path = 
      resolve_path ~follow_last_symlink:`If_trailing_slash path >>=| fun rpath ->
      let { parent_id=pid; comp=name; result; _ } = rpath in
      match result with
      | Missing -> err `Error_no_entry
      | _ -> 
        dirs.find pid >>= fun dir_ops ->
        let name = Str_256.make name in
        dir_ops.delete name >>= fun () ->
        ok ()
    (* NOTE dir_ops.delete automatically updates times *)
    

    (* FIXME meta changes for parent and child should occur via create_dir? *)
    let mkdir path : ((unit,'e5)result,'t) m = 
      resolve_path ~follow_last_symlink:`If_trailing_slash path >>=| fun rpath ->
      let { parent_id=parent; comp=name; result; trailing_slash=_ } = rpath in
      match result with 
      | Missing -> 
        mk_stat_times () >>= fun times -> 
        (* perhaps adjust resolve so that it returns a str_256 name? no *)
        let name = Str_256.make name in
        create_dir ~parent ~name ~times >>= fun () ->
        (* FIXME should create_dir alter the parent timestamp? or
           should be explicitly alter it here? probably we should at
           least allow a flag arg for create_dir to update_parent_times *)
        ok ()
      | _ -> err `Error_exists

    (* FIXME update atim? or just ignore atim? *)
    let opendir path = 
      resolve_dir_path path >>=| fun did ->
      dirs.find did >>= fun dir ->
      dir.ls_create () >>= fun dh ->
      ok dh

    let readdir (dh:dh) = 
      dh#ls_kvs () |> fun kvs -> 
      (* FIXME add str_256 conversion to_string to str_256 intf; this is ugly *)
      (* FIXME move finished to fs_shared *)
      let kvs = List.map (fun ( (k:str_256),_v) -> (k :> string) ) kvs in 
      dh#ls_step () >>= fun {finished} ->
      ok (kvs,Tjr_minifs.Minifs_intf.{finished}) (* FIXME move to fs_shared? *)

    let closedir _ = ok ()
    (* FIXME should we record which dh are valid? ie not closed *)

    let create path = 
      resolve_path ~follow_last_symlink:`If_trailing_slash path >>=| fun rpath ->
      let { parent_id=parent; comp=name; result; trailing_slash=_ } = rpath in
      match result with 
      | Missing ->
        mk_stat_times () >>= fun times -> 
        (* FIXME create_file should perhaps alter the timestamp for
           the parent *)
        create_file ~parent ~name:(Str_256.make name) ~times >>= fun () ->
        ok ()
      | _ -> err `Error_exists

    (* FIXME atim *)
    let open_ path = 
      resolve_file_path path >>=| fun fid -> 
      ok fid

    (* FIXME must account for reading beyond end of file; FIXME atim *)
    let pread ~fd ~foff ~len ~buf ~boff = 
      files.find fd >>= fun file ->
      file.pread ~off:{off=foff} ~len:{len} >>=| fun buf' ->
      (* FIXME do we really assert this?
      assert(Bigstring.size buf' = len); *)
      (* FIXME unnecessary blit between buffers *)
      Bigstring.blit buf' 0 buf boff (ba_buf_ops.buf_length buf');
      return (Ok len)

    (* NOTE file.pwrite should automatically update the timestamps *)
    let pwrite ~fd ~foff ~len ~buf ~boff =
      files.find fd >>= fun file ->
      (* file.pwrite ~foff ~len ~buf ~boff *)
      file.pwrite ~src:buf ~src_off:{off=boff} ~src_len:{len} ~dst_off:{off=foff}
      
    let close _fd = ok ()  (* FIXME record which are open? *)

    (* FIXME ddir and sdir may be the same, so we need to be careful to
       always use indirection via did *)
    (* NOTE times are handled explicitly, to ensure that the src and
       dst directories appear to be modified at the same time *)
    let rename spath dpath = begin
      let follow_last_symlink = `Always in
      resolve_path ~follow_last_symlink spath >>=| fun spath ->
      resolve_path ~follow_last_symlink dpath >>=| fun dpath ->          
      dirs.find spath.parent_id >>= fun sdir ->
      (* NOTE sdir is the container of the spath component *)
      match spath.result with 
      | Missing -> err `Error_no_src_entry
      | _ -> 
        let scomp = spath.comp |> Str_256.make in
        dirs.find dpath.parent_id >>= fun ddir ->
        (* NOTE ddir is the container of the dpath component *)
        let dcomp = dpath.comp |> Str_256.make in
        let insert_and_remove id =
          (* this is the "default" behaviour *)
          match spath.parent_id = dpath.parent_id with
          | true -> 
            assert(scomp != dcomp);
            ddir.insert dcomp id >>= fun () ->
            ddir.delete scomp >>= fun () ->
            (* update meta on ddir (and sdir!) *)
            mk_stat_times () >>= fun times ->
            ddir.set_times times
          | false -> 
            ddir.insert dcomp id >>= fun () ->
            mk_stat_times () >>= fun times ->
            ddir.set_times times >>= fun () -> 
            sdir.delete scomp >>= fun () ->
            sdir.set_times times
        in
        match spath.result,dpath.result with
        (* FIMXE following for symlinks *)
        | Sym _,_ | _,Sym _ -> (
            Printf.printf "impossible %s\n%!" __LOC__;
            exit_1 "FIXME shouldn't happen - follow=`Always")
        | File fid,Missing -> 
          insert_and_remove (Fid fid) >>= fun () ->
          ok ()
        | File fid1,File fid2 -> 
          if fid1=fid2 then ok () 
          else
            insert_and_remove (Fid fid1) >>= fun () -> ok ()
        | File _fid,Dir _did ->
          extra.internal_err "FIXME rename f to d, d should be empty?"          
        | Dir sdid,Missing -> 
          (* check not root *)
          if sdid=root_did then err `Error_attempt_to_rename_root 
          else 
            (* check not subdir *)
            extra.is_ancestor ~parent:sdid ~child:dpath.parent_id >>= (function
                | true -> err `Error_attempt_to_rename_to_subdir
                | false -> 
                  (* FIXME other checks *)
                  dirs.find sdid >>= fun sdir -> 
                  mk_stat_times () >>= fun times ->
                  sdir.set_times times >>= fun () ->
                  (* we want to call insert_and_remove, but we should
                     remember to update the parent afterwards (and this
                     really should happen atomically) FIXME *)
                  (* new directory id *)
                  insert_and_remove (Did sdid) >>= fun () ->
                  sdir.set_parent dpath.parent_id >>= fun () ->
                  ok ())
        | Dir _sdid,File _fid -> 
          err `Error_attempt_to_rename_dir_over_file  (* FIXME correct ? *)
        | Dir sdid,Dir ddid ->
          if sdid=ddid then ok () 
          else 
            extra.internal_err "FIXME rename d to d, dst should be empty?"
        | Missing ,_ -> (
            Printf.printf "impossible %s\n%!" __LOC__;
            exit_1 "impossible")
    end (* rename *)


    (* FIXME truncate parent name; FIXME also stat *)
    (* NOTE file.truncate automatically updates times *)
    let truncate path length = 
      resolve_file_path path >>=| fun fid ->
      files.find fid >>= fun file ->
      file.truncate ~size:length >>= fun () ->
(*      mk_stat_times () >>= fun times -> 
      file.set_times times >>= fun () -> *)
      ok ()


    (* FIXME add times to symlinks? *)
    let dummy_times = Times.{ atim=0.0; mtim=0.0 }

    (* FIXME here and elsewhere atim is not really dealt with *)
    let stat path = 
      resolve_path ~follow_last_symlink:`If_trailing_slash path >>=| fun rpath ->
      let { parent_id=_pid; comp=_name; result; trailing_slash=_ } = rpath in
      let open Stat_record in
      match result with
      | Missing -> err `Error_no_entry
      | File fid ->
        files.find fid >>= fun file ->
        file.size () >>= fun sz ->
        file.get_times () >>= fun times ->
        ok { sz;kind=`File; times }
      | Dir did -> 
        dirs.find did >>= fun dir ->
        dir.get_times() >>= fun times ->
        ok { sz=1; kind=`Dir; times }   (* sz for dir? FIXME size of dir *)
      | Sym (_sid,s) -> 
        ok {sz=String.length s; times=dummy_times; kind=`Symlink}

    let symlink contents path =
      resolve_path ~follow_last_symlink:`If_trailing_slash path >>=| fun rpath ->
      let { parent_id=parent; comp=name; result; trailing_slash=_ } = rpath in
      match result with
      | Missing -> 
        mk_stat_times () >>= fun times -> 
        (* FIXME add a generic conversion from string, which raises an error in monad?  *)
        let contents = Str_256.make contents in 
        create_symlink ~parent ~name:(Str_256.make name) ~times ~contents >>= fun () ->
        ok ()
      | _ -> err `Error_exists

    let readlink path = 
      resolve_path ~follow_last_symlink:`If_trailing_slash path >>=| fun rpath ->
      let { result; _ } = rpath in
      match result with
      | Sym (_sid,s) -> ok s
      | _ -> err `Error_not_symlink

    let reset () = return () (* FIXME? *)

    (** Export the filesystem operations as a single value *)
    let ops : (_,_,_) Minifs_intf.ops = {
      root;
      unlink;
      mkdir;
      opendir;
      readdir;
      closedir;
      create;
      open_;
      pread;
      pwrite;
      close;
      rename;
      truncate;
      stat;
      symlink;
      readlink;
      reset
    }

    let _ : (fd,
 < ls_kvs : unit -> (dir_k * dir_entry) list;
   ls_step : unit -> (Tjr_btree.Btree_intf.finished, S0.t) Tjr_monad.m >,
 S0.t)
Tjr_minifs.Minifs_intf.Ops_type.ops = ops

    (* include these types to allow Make_3 below *)
    type nonrec t = t
    type nonrec dh = dh
    type nonrec fd = fd
  end

  (** Make_2, but with a restriction on result sig *)
  module Make_3(X:T2): T = Make_2(X:T2)
end

module X = Make
