(** An abstract generic development; this layer deals with path resolution, times and a few others things; assumes create_dir and create_file. *)

type exn_ = Tjr_minifs.Minifs_intf.exn_


include struct
  open Bin_prot.Std
  type stat_times = {
    st_atim: float;
    st_mtim: float;
  }[@@deriving bin_io]
end

type stat_record = {
  sz:int;
  kind:[`F | `D | `S];
  times: stat_times
}

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

  (** we expect an implentation which maps an id to a root block *)

  type dir_entry = Fid of fid | Did of did | Sid of sid[@@deriving bin_io]                  

  type dir_k = str_256
    
  type dir_v = dir_entry

  type dh = (dir_k,dir_v,t)Tjr_btree.Make_3.ls (** dir handle *)

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
    find        : did -> (dir_ops,t)m;    
    delete      : did -> (unit,t)m;    
  }

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
    create: fid -> (unit,t)m;
    delete: fid -> (unit,t)m;
  }

  (* open Tjr_path_resolution *)

  type resolved_path_or_err = (fid,did)Tjr_path_resolution.resolved_path_or_err

  type path = string

  type extra_ops = {
    internal_err: 'a. string -> ('a,t) m;
    is_ancestor: parent:did -> child:did -> (bool,t) m
  }
end

(** the values we expect to be present *)
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

module Make(S0:S0) = struct
  open S0
  module S1 = S1(S0)
  open S1
  module S2 = S2(S0)
  open S2
  
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

    (* FIXME meta changes for parent and child *)
    let mkdir path : ((unit,'e5)result,'t) m = 
      resolve_path ~follow_last_symlink:`If_trailing_slash path >>=| fun rpath ->
      let { parent_id=parent; comp=name; result; trailing_slash=_ } = rpath in
      match result with 
      | Missing -> 
        mk_stat_times () >>= fun times -> 
        (* FIXME perhaps adjust resolve so that it returns a str_256 name *)
        let name = Str_256.make name in
        create_dir ~parent ~name ~times >>= fun () ->
        ok ()
      | _ -> err `Error_exists

    (* FIXME update atim *)
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
      dh#ls_step () >>= fun Tjr_btree.Make_3.{finished} ->
      ok (kvs,Tjr_minifs.Minifs_intf.{finished}) (* FIXME move to fs_shared *)

    let closedir _ = ok ()
    (* FIXME should we record which rd are valid? ie not closed *)

    let create path = 
      resolve_path ~follow_last_symlink:`If_trailing_slash path >>=| fun rpath ->
      let { parent_id=parent; comp=name; result; trailing_slash=_ } = rpath in
      match result with 
      | Missing ->
        mk_stat_times () >>= fun times -> 
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
    let truncate path length = 
      resolve_file_path path >>=| fun fid ->
      files.find fid >>= fun file ->
      file.truncate length >>= fun () ->
      mk_stat_times () >>= fun times -> 
      file.set_times times >>= fun () ->
      ok ()


    (* FIXME add times to symlinks? *)
    let dummy_times = { st_atim=0.0; st_mtim=0.0 }

    (* FIXME here and elsewhere atim is not really dealt with *)
    let stat path = 
      resolve_path ~follow_last_symlink:`If_trailing_slash path >>=| fun rpath ->
      let { parent_id=_pid; comp=_name; result; trailing_slash=_ } = rpath in
      match result with
      | Missing -> err `Error_no_entry
      | File fid ->
        files.find fid >>= fun file ->
        file.get_sz () >>= fun sz ->
        file.get_times () >>= fun times ->
        ok { sz;kind=`F; times }
      | Dir did -> 
        dirs.find did >>= fun dir ->
        dir.get_times() >>= fun times ->
        ok { sz=1; kind=`D; times }   (* sz for dir? FIXME size of dir *)
      | Sym s -> 
        ok {sz=String.length s; times=dummy_times; kind=`S}

    let symlink contents path =
      resolve_path ~follow_last_symlink:`If_trailing_slash path >>=| fun rpath ->
      let { parent_id=parent; comp=name; result; trailing_slash=_ } = rpath in
      match result with
      | Missing -> 
        mk_stat_times () >>= fun times -> 
        let contents = Str_256.make contents in (* FIXME add a generic
                                                   conversion from
                                                   string, which raises
                                                   an error in monad *)
        create_symlink ~parent ~name:(Str_256.make name) ~times ~contents >>= fun () ->
        ok ()
      | _ -> err `Error_exists

    let readlink path = 
      resolve_path ~follow_last_symlink:`If_trailing_slash path >>=| fun rpath ->
      let { result; _ } = rpath in
      match result with
      | Sym s -> ok s
      | _ -> err `Error_not_symlink

    let reset () = ok () (* FIXME? *)

  end
end

module X = Make
