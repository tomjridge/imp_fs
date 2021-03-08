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
open Errors


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
      did:did -> name:str_256 -> obj:dir_entry -> reduce_obj_nlink:unit -> (unit,t)m;

    set_times : did:did -> times -> (unit,t)m;
    get_times : did:did -> (times,t)m;

    set_parent: did:did -> parent:did -> (unit,t)m; (** NOTE inter-object link *)
    get_parent: did:did -> (did,t)m;

    sync: did:did -> (unit,t)m; (** used in rename *)
  }
  (** NOTE the parent field is an inter-object link and so needs to be
     updated atomically *)
  (** NOTE no nlink for dirs, but POSIX probably requires it? *)

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

    create : parent:did -> name:str_256 -> times:times -> (unit,t)m; (* was create_dir *)

    rename: rename_case -> (unit,t)m;

  }


  (** {2 Dir handles} *)

  type dhs_ops = {
    opendir  : did -> (dh,t)m;
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
    get_nlink : fid:fid -> (int,t)m;
    set_nlink : fid:fid -> int -> (unit,t)m;  (** NOTE inter-object *)

    sync      : fid:fid -> (unit,t)m;
  }
  (** NOTE the nlink field is derived from inter-object links, and so
      needs to be updated atomically *)

  type files_ops = {
    delete: fid -> (unit,t)m;
    create: 
      parent:did -> name:str_256 -> times:times -> (unit,t)m;
    (* NOTE files.create: parent is a dir, and we don't have nlinks for dirs *)

    create_symlink:
      parent:did -> name:str_256 -> times:times -> contents:str_256 -> (unit,t)m
  }


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

    val dir: dir_ops

    val dirs: dirs_ops

    val file: file_ops

    val files: files_ops

    val dir_handles: dhs_ops

    val locks: (tid,dir_entry,t)lock_ops

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
           standard error FIXME ? this probably belongs with the
           path_resolution code *)
        return (Error `Error_path_resolution)

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
    let unlink ~tid path =
      resolve_path ~follow_last_symlink:`If_trailing_slash path >>=| fun rpath ->
      let { parent_id=pid; comp=name; result; _ } = rpath in
      match result with
      | Missing -> err `Error_no_entry
      | _ ->
        (* lock parent and validate pthres *)
        let objs = [Did pid] in
        locks.lock ~tid ~objs >>= fun () -> 
        let obj = resolved_component_to_object result in
        dir.find ~did:pid name >>= fun v -> 
        match v=Some obj with 
        | false -> 
          (* something changed... back out *)
          locks.unlock ~tid ~objs >>= fun () ->
          err `Error_concurrent_modification
        | true ->           
          dir.delete ~did:pid ~name ~obj ~reduce_obj_nlink:() >>= fun () ->
          locks.unlock ~tid ~objs >>= fun () ->
          ok ()

    (* FIXME meta changes for parent and child *)
    let mkdir ~tid path : ((unit,'e5)result,'t) m =
      resolve_path ~follow_last_symlink:`If_trailing_slash path >>=| fun rpath ->
      let { parent_id=parent; comp=name; result; trailing_slash=_ } = rpath in
      match result with
      | Missing -> begin
          (* lock parent, validate missing *)
          let objs = [Did parent] in
          locks.lock ~tid ~objs >>= fun () -> 
          dir.find ~did:parent name >>= function
          | None ->         
            mk_stat_times () >>= fun times ->
            dirs.create ~parent ~name ~times >>= fun () ->
            locks.unlock ~tid ~objs >>= fun () ->
            ok ()
          | _ -> 
            (* concurrent modification *)
            locks.unlock ~tid ~objs >>= fun () ->
            err `Error_concurrent_modification
        end
      | _ -> err `Error_exists

    (* FIXME update atim *)
    let opendir path =
      resolve_dir_path path >>=| fun did ->
      dir_handles.opendir did >>= fun dh ->
      ok dh

    let readdir (dh:dh) = dir_handles.readdir dh >>= fun xs -> ok xs

    let closedir dh = dir_handles.closedir dh >>= fun () -> ok ()

    let create path =
      resolve_path ~follow_last_symlink:`If_trailing_slash path >>=| fun rpath ->
      let { parent_id=parent; comp=name; result; trailing_slash=_ } = rpath in
      match result with
      | Missing ->
        mk_stat_times () >>= fun times ->
        files.create ~parent ~name ~times >>= fun () ->
        ok ()
      | _ -> err `Error_exists

    (* FIXME atim *)
    let open_ path =
      resolve_file_path path >>=| fun fid ->
      ok fid

    (* FIXME must account for reading beyond end of file; FIXME atim *)
    let pread ~fd ~foff ~len ~buf ~boff =
      file.pread ~fid:fd ~foff ~len ~buf ~boff

    let pwrite ~fd ~foff ~len ~buf ~boff =
      (* NOTE no locks *)
      file.pwrite ~fid:fd ~foff ~len ~buf ~boff

    let close _fd = ok ()  (* FIXME record which are open? *)

    (* FIXME ddir and sdir may be the same, so we need to be careful to
       always use indirection via did *)
    let rename ~tid spath dpath = begin
      let follow_last_symlink = `Always in
      resolve_path ~follow_last_symlink spath >>=| fun spath ->
      resolve_path ~follow_last_symlink dpath >>=| fun dpath ->
      (* NOTE sdir is the container of the spath component *)
      match spath.result with
      | Missing -> err `Error_no_src_entry
      | _ ->
        match spath.result,dpath.result with
        (* FIMXE following for symlinks *)
        | Sym _,_ | _,Sym _ -> (
            Printf.printf "impossible %s\n%!" __LOC__;
            exit_1 "FIXME shouldn't happen - follow=`Always")

        | File fid,Missing -> begin
            let objs = [Fid fid; Did spath.parent_id;Did dpath.parent_id] in
            locks.lock ~tid ~objs >>= fun () -> 
            dir.find ~did:spath.parent_id spath.comp >>= fun r1 -> 
            dir.find ~did:dpath.parent_id dpath.comp >>= fun r2 -> 
            match (r1 = Some (Fid fid)) && (r2 = None) with
            | false -> 
              locks.unlock ~tid ~objs >>= fun () ->
              err `Error_concurrent_modification
            | true -> 
              mk_stat_times () >>= fun times ->
              (* FIXME dirs.rename should sync src and dst then do the update atomically *)
              dirs.rename (Rename_file_missing {
                  times;
                  src=(spath.parent_id,spath.comp,fid);
                  dst=(dpath.parent_id,dpath.comp)
                }) >>= fun () ->
              locks.unlock ~tid ~objs >>= fun () ->
              ok ()
          end

        | File fid1,File fid2 -> begin 
            match fid1=fid2 with
            | true -> ok ()
            | false ->
              let objs = [Fid fid1; Fid fid2; Did spath.parent_id; Did dpath.parent_id] in
              locks.lock ~tid ~objs >>= fun () ->
              dir.find ~did:spath.parent_id spath.comp >>= fun r1 -> 
              dir.find ~did:dpath.parent_id dpath.comp >>= fun r2 -> 
              match (r1 = Some (Fid fid1)) && (r2 = Some (Fid fid2)) with
              | false -> 
                locks.unlock ~tid ~objs >>= fun () ->
                err `Error_concurrent_modification
              | true -> 

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
                locks.unlock ~tid ~objs >>= fun () ->
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
    let truncate ~tid path length =
      resolve_file_path path >>=| fun fid ->
      (* FIXME probably lock file before truncate? so the lower level
         doesn't have to worry *)
      let objs = [Fid fid] in
      mk_stat_times () >>= fun times ->
      locks.lock ~tid ~objs >>= fun () -> 
      file.truncate ~fid length >>= fun () ->
      file.set_times ~fid times >>= fun () ->
      locks.unlock ~tid ~objs >>= fun () ->
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
        (* NOTE no locks *)
        file.get_sz ~fid >>= fun sz ->
        file.get_times ~fid >>= fun times ->
        ok { sz;kind=`File; times }
      | Dir did ->
        dir.get_times ~did >>= fun times ->
        ok { sz=1; kind=`Dir; times }   (* sz for dir? FIXME size of dir *)
      | Sym (_sid,contents) ->
        ok {sz=String.length contents; times=dummy_times; kind=`Symlink}

    let symlink contents path =
      resolve_path ~follow_last_symlink:`If_trailing_slash path >>=| fun rpath ->
      let { parent_id=parent; comp=name; result; trailing_slash=_ } = rpath in
      match result with
      | Missing ->
        mk_stat_times () >>= fun times ->
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

(*
    (** Export the filesystem operations as a single value *)
    let ops : (_,_,_) Minifs_intf.ops = {
      root;
      unlink;
      mkdir;
      opendir;
      readdir=failwith "";  (* FIXME readdir should just return a list surely *)
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
*)
    (* include these types to allow Make_3 below *)
    type nonrec t = t
    type nonrec dh = dh
    type nonrec fd = fd
  end

  (** Make_2, but with a restriction on result sig *)
  (* module Make_3(X:T2): T = Make_2(X:T2) *)
end

module X = Make


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
