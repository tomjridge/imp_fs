(** Abstract development of V3.

See {!module:V3}. *)

type times = Minifs_intf.times


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
open Path_resolution

module Lock_ops = struct

  (** Use raw ops to implement the per-object locks *)
  type ('tid,'lck,'t) raw_lock_ops = {
    create : unit -> ('lck,'t)m;
    lock   : tid:'tid -> lck:'lck -> (unit,'t)m;
    unlock : tid:'tid -> lck:'lck -> (unit,'t)m;
  }

  type ('tid,'id,'t) lock_ops = {
    lock       : tid:'tid -> objs:'id list -> (unit,'t)m;
    unlock     : tid:'tid -> objs:'id list -> (unit,'t)m;
    unlock_all : tid:'tid -> (unit,'t)m;
  }
end

module type S0 = sig
  type t
  val monad_ops: t monad_ops

  type fid  [@@deriving bin_io]
  type did  [@@deriving bin_io]
  type sid  [@@deriving bin_io] (** symlink id *)

  type dh [@@deriving bin_io] (** dir handle *)

  type tid [@@deriving bin_io] (** thread id *)
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
    find      : str_256 -> (dir_entry option,t)m;
    insert    : str_256 -> dir_entry -> (unit,t)m;

    (* unsafe_delete    : str_256 -> (unit,t)m;  *)
    (* for when we are sure nothing changes in the dir, ie the dir is locked *)

    delete    : name:str_256 -> obj:dir_entry -> (unit,t)m;

    ls_create : unit -> (dh,t)m;
    (* FIXME we probably need to track dhs; probably move this to dhs_ops? *)

    set_times : times -> (unit,t)m;
    get_times : unit -> (times,t)m;

    set_parent: did -> (unit,t)m; (** NOTE inter-object link *)
    get_parent: unit -> (did,t)m;

    sync: unit -> (unit,t)m; (** used in rename *)
  }
  (** NOTE the parent field is an inter-object link and so needs to be
     updated atomically *)
  (** NOTE no nlink for dirs, but POSIX probably requires it? *)


  (* val read_symlink: sid -> (str_256,t)m *)
  (* FIXME we probably want path res to return an sid, which could be
     just a string of course *)

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
    find   : did -> (dir_ops,t)m;
    delete : did -> (unit,t)m;

    create : parent:did -> name:str_256 -> times:times -> (unit,t)m; (* was create_dir *)

    rename: rename_case -> (unit,t)m;

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
    pread     : foff:int -> len:int -> buf:buf -> boff:int -> ((int,pread_err)result, t) m;
    pwrite    : foff:int -> len:int -> buf:buf -> boff:int -> ((int,pwrite_err)result, t) m;
    truncate  : int -> (unit,t)m;
    get_sz    : unit -> (int,t)m;
    set_times : times -> (unit,t)m;
    get_times : unit -> (times,t)m;
    get_nlink : unit -> (int,t)m;
    set_nlink : int -> (unit,t)m;  (** NOTE inter-object *)

    sync      : unit -> (unit,t)m;
  }
  (** NOTE the nlink field is derived from inter-object links, and so
      needs to be updated atomically *)

  type files_ops = {
    find        : fid -> (file_ops,t)m;
    (* create_raw  : fid -> times -> (unit,t)m; (\* was create *\) *)
    delete      : fid -> (unit,t)m;
    create : parent:did -> name:str_256 -> times:times -> (unit,t)m;
    (* FIXME rename to create *)

    create_symlink:
      parent:did -> name:str_256 -> times:times -> contents:str_256 -> (unit,t)m
  }
  (** NOTE create just creates a new file in the gom; it doesn't link
      it into a parent etc, so nlink is 0 *)



  (** {2 Path resolution types} *)

  type nonrec resolve_path = (fid,did,sid,t) Path_resolution.resolve_path

  type path = string

  type extra_ops = {
    internal_err : 'a. string -> ('a,t) m;
    is_ancestor  : parent:did -> child:did -> (bool,t) m
  }

end

(* $ (PIPE2SH("""sed -n '/The[ ]values/,/^end/p' >GEN.S2.ml_""")) *)
(** The values we expect to be present *)
module S2(S0:S0) = struct
  open S0
  open S1(S0)

  module type T2 = sig
    val root_did: did

    val dirs: dirs_ops

    val files: files_ops

    val resolve_path: resolve_path

    val mk_stat_times: unit -> (times,t)m

    val extra: extra_ops
  end
end

(* $ (PIPE2SH("""sed -n '/The[ ]resulting/,/^end/p' >GEN.T.ml_""")) *)
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

    let resolve_path = resolve_path.resolve_path

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

    (** Map a resolved component to an object, assuming not missing *)
    let resolved_component_to_object = function
      | File fid -> Fid fid
      | Dir did -> Did did
      | Sym (sid,_contents) -> Sid sid
      | Missing -> failwith "resolved_component_to_object"

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
        let obj = resolved_component_to_object result in
        dirs.find pid >>= fun dir_ops ->
        dir_ops.delete ~name ~obj >>= fun () ->
        (* FIXME need to atomically reduce nlink by 1 for obj... or assume dir_ops does it *)
        ok ()

    (* FIXME meta changes for parent and child *)
    let mkdir path : ((unit,'e5)result,'t) m =
      resolve_path ~follow_last_symlink:`If_trailing_slash path >>=| fun rpath ->
      let { parent_id=parent; comp=name; result; trailing_slash=_ } = rpath in
      match result with
      | Missing ->
        mk_stat_times () >>= fun times ->
        dirs.create ~parent ~name ~times >>= fun () ->
        ok ()
      | _ -> err `Error_exists

    (* FIXME update atim *)
    let opendir path =
      resolve_dir_path path >>=| fun did ->
      dirs.find did >>= fun dir ->
      dir.ls_create () >>= fun dh ->
      ok dh

    let readdir (_dh:dh) = failwith ""
(*
      dh#ls_kvs () |> fun kvs ->
      (* FIXME add str_256 conversion to_string to str_256 intf; this is ugly *)
      (* FIXME move finished to fs_shared *)
      let kvs = List.map (fun ( (k:str_256),_v) -> (k :> string) ) kvs in
      dh#ls_step () >>= fun {finished} ->
      ok (kvs,Tjr_minifs.Minifs_intf.{finished}) (* FIXME move to fs_shared *)
*)
    let closedir _ = ok ()
    (* FIXME should we record which dh are valid? ie not closed *)

    let create path =
      resolve_path ~follow_last_symlink:`If_trailing_slash path >>=| fun rpath ->
      let { parent_id=parent; comp=name; result; trailing_slash=_ } = rpath in
      match result with
      | Missing ->
        mk_stat_times () >>= fun times ->
        files.create ~parent ~name ~times >>= fun () ->
        (* FIXME this should update parent.nlink, or assume files.create does this *)
        ok ()
      | _ -> err `Error_exists

    (* FIXME atim *)
    let open_ path =
      resolve_file_path path >>=| fun fid ->
      ok fid

    (* FIXME must account for reading beyond end of file; FIXME atim *)
    let pread ~fd ~foff ~len ~buf ~boff =
      files.find fd >>= fun file ->
      file.pread ~foff ~len ~buf ~boff

    let pwrite ~fd ~foff ~len ~buf ~boff =
      files.find fd >>= fun file ->
      file.pwrite ~foff ~len ~buf ~boff

    let close _fd = ok ()  (* FIXME record which are open? *)

    (* FIXME ddir and sdir may be the same, so we need to be careful to
       always use indirection via did *)
    let rename spath dpath = begin
      let follow_last_symlink = `Always in
      resolve_path ~follow_last_symlink spath >>=| fun spath ->
      resolve_path ~follow_last_symlink dpath >>=| fun dpath ->
      (* dirs.find spath.parent_id >>= fun sdir -> *)
      (* NOTE sdir is the container of the spath component *)
      match spath.result with
      | Missing -> err `Error_no_src_entry
      | _ ->
        (* let scomp = spath.comp in  *)
        (* dirs.find dpath.parent_id >>= fun ddir -> *)
        (* NOTE ddir is the container of the dpath component *)
        (* let dcomp = dpath.comp in *)
(*
        (* this looks like it should be an atomic operation, since it affects two objects *)
        let insert_and_remove id =
          (* this is the "default" behaviour *)
          match spath.parent_id = dpath.parent_id with
          | true ->
            assert(scomp != dcomp);
            ddir.insert dcomp id >>= fun () ->
            ddir.unsafe_delete scomp >>= fun () ->
            (* update meta on ddir (and sdir!) *)
            mk_stat_times () >>= fun times ->
            ddir.set_times times
          | false ->
            ddir.insert dcomp id >>= fun () ->
            mk_stat_times () >>= fun times ->
            ddir.set_times times >>= fun () ->
            sdir.unsafe_delete scomp >>= fun () ->
            sdir.set_times times
        in
*)
        match spath.result,dpath.result with
        (* FIMXE following for symlinks *)
        | Sym _,_ | _,Sym _ -> (
            Printf.printf "impossible %s\n%!" __LOC__;
            exit_1 "FIXME shouldn't happen - follow=`Always")

        | File fid,Missing ->
          mk_stat_times () >>= fun times ->
          (* FIXME locks, validate pathres;  *)
          (* FIXME dirs.rename should sync src and dst then do the update atomically *)
          dirs.rename (Rename_file_missing {
              times;
              src=(spath.parent_id,spath.comp,fid);
              dst=(dpath.parent_id,dpath.comp)
            }) >>= fun () ->
          ok ()

        | File fid1,File fid2 -> begin match fid1=fid2 with
            | true -> ok ()
            | false ->
              (* FIXME locks, validate pathres *)
              (* FIXME dirs.rename should sync src and dst then do the update atomically *)
              (* FIXME dirs.rename should update nlink etc; and
                 presumably to keep this consistent we need to sync
                 the meta for the objs to disk *)
              mk_stat_times () >>= fun times ->
              dirs.rename (Rename_file_file {
                  times;
                  src=(spath.parent_id,spath.comp,fid1);
                  dst=(dpath.parent_id,dpath.comp,fid2)
                }) >>= fun () ->
              ok ()
          end

        | File _fid,Dir _did ->
          extra.internal_err "FIXME rename f to d, d should be empty?"

        | Dir sdid,Missing -> begin
            (* check not root *)
            match sdid=root_did with
            | true -> err `Error_attempt_to_rename_root
            | false ->
              (* FIXME locks, validate pathres *)
              (* check not subdir *)
              (* FIXME dirs.rename should sync src and dst then do the update atomically *)
              (* NOTE if we have locked all objects and verified path
                 resolution result hasn't changed, we should be able to
                 answer this based on locked objects *)
              extra.is_ancestor ~parent:sdid ~child:dpath.parent_id >>= (function
                  | true -> err `Error_attempt_to_rename_to_subdir
                  | false ->
                    mk_stat_times () >>= fun times ->
                    dirs.rename (Rename_dir_missing {
                        times;
                        src=(spath.parent_id,spath.comp,sdid);
                        dst=(dpath.parent_id,dpath.comp)
                      }) >>= fun () ->
                    ok())

(*
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
*)
          end

        | Dir _sdid,File _fid ->
          err `Error_attempt_to_rename_dir_over_file  (* FIXME correct ? *)
        | Dir sdid,Dir ddid -> (
            match sdid=ddid with
            | true -> ok ()
            | false -> extra.internal_err "FIXME rename d to d, dst should be empty?")
        | Missing ,_ -> (
            Printf.printf "impossible %s\n%!" __LOC__;
            exit_1 "impossible")
    end (* rename *)


    (* FIXME truncate parent name; FIXME also stat *)
    let truncate path length =
      resolve_file_path path >>=| fun fid ->
      files.find fid >>= fun file ->
      (* FIXME probably lock file before truncate? so the lower level
         doesn't have to worry *)
      file.truncate length >>= fun () ->
      mk_stat_times () >>= fun times ->
      file.set_times times >>= fun () ->
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
        file.get_sz () >>= fun sz ->
        file.get_times () >>= fun times ->
        ok { sz;kind=`File; times }
      | Dir did ->
        dirs.find did >>= fun dir ->
        dir.get_times() >>= fun times ->
        ok { sz=1; kind=`Dir; times }   (* sz for dir? FIXME size of dir *)
      | Sym (_sid,contents) ->
        ok {sz=String.length contents; times=dummy_times; kind=`Symlink}

    let symlink contents path =
      resolve_path ~follow_last_symlink:`If_trailing_slash path >>=| fun rpath ->
      let { parent_id=parent; comp=name; result; trailing_slash=_ } = rpath in
      match result with
      | Missing ->
        mk_stat_times () >>= fun times ->
        (* FIXME add a generic conversion from string, which raises an error in monad?  *)
        let contents = Str_256.make contents in
        files.create_symlink ~parent ~name ~times ~contents >>= fun () ->
        ok ()
      | _ -> err `Error_exists

    let readlink path =
      resolve_path ~follow_last_symlink:`If_trailing_slash path >>=| fun rpath ->
      let { result; _ } = rpath in
      match result with
      | Sym (_sid,contents) -> ok contents
      | _ -> err `Error_not_symlink

    let reset () = return () (* FIXME? *)

    (* FIXME what about sync_dir, sync_file ? *)

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

    let _ : (fd,dh,S0.t) Tjr_minifs.Minifs_intf.Ops_type.ops = ops

    (* include these types to allow Make_3 below *)
    type nonrec t = t
    type nonrec dh = dh
    type nonrec fd = fd
  end

  (** Make_2, but with a restriction on result sig *)
  module Make_3(X:T2): T = Make_2(X:T2)
end

module X = Make
