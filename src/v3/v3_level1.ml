(** Abstract development of V3.

See {!module:V3}.

Initial path resolution is carried out without locks. After, we lock
   objects that we need to, validate the entries, and then go ahead
   with the modifications. If we detect changes between path res and
    execution, we backout with a concurrent modification error.

*)

open V3_intf

open V3_intf.Errors

open V3_intf.Path_resolution

open V3_intf.Lock_ops


module Make(S0:S0) = struct
  open S0
  module S1 = S1(S0)
  open S1
  module S2 = Level2_provides(S0)
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
          (* NOTE we don't hold a kref on obj here FIXME do we need
             to? Maybe if we need to reduce the nlinks ie touch the
             object itself *)
          dir.delete ~did_locked:() ~did:pid ~name ~obj ~reduce_obj_nlink:() >>= fun () ->
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
            (* FIXME dirs.create needs to make sure the new dir is
               persistent on disk before returning.... or else ensure
               that if the parent flushes, the child is flushed first
               *)
            dirs.create_and_add_to_parent ~parent_locked:() ~parent ~name ~times >>= fun () ->
            locks.unlock ~tid ~objs >>= fun () ->
            ok ()
          | _ -> 
            (* concurrent modification *)
            locks.unlock ~tid ~objs >>= fun () ->
            err `Error_concurrent_modification
        end
      | _ -> err `Error_exists

    (* FIXME update atim *)
    let opendir ~tid path =
      (* NOTE live dhs are just maintained in a table and dropped when
         closed; thus, we need a mapping dh -> did; and we never reuse
         dh (so, dh is NOT just did); NOTE the dh certainly outlives
         the request, so the "resource" dh is not tied to tid *)
      resolve_dir_path path >>=| fun did ->
      dir_handles.opendir ~tid did >>= fun dh ->
      ok dh

    (* FIXME note we could maintain a kref with the dh; but this
       effectively pins the dir for the duration of the readdir, which
       is bad since it may be for ever; so as an alternative readdir
       must obtain a kref afresh each time; but then the directory may
       not exist the next time it is accessed via dh *)
    let readdir (dh:dh) = dir_handles.readdir dh >>= fun xs -> ok xs

    let closedir dh = dir_handles.closedir dh >>= fun () -> ok ()

    (* FIXME need to lock and validate the entry is nonexist *)
    let create path =
      resolve_path ~follow_last_symlink:`If_trailing_slash path >>=| fun rpath ->
      let { parent_id=parent; comp=name; result; trailing_slash=_ } = rpath in
      match result with
      | Missing ->
        mk_stat_times () >>= fun times ->
        (* FIXME again we must make sure that if the link to the new
           file is persisted, then the new file is persisted
           beforehand, or "at the same time" *)
        files.create_and_add_to_parent ~parent ~name ~times >>= fun () ->
        ok ()
      | _ -> err `Error_exists

    (* FIXME atim *)
    let open_ path =
      resolve_file_path path >>=| fun fid ->
      ok fid

    (* FIXME must account for reading beyond end of file; FIXME atim *)
    let pread ~tid:_ ~fd ~foff ~len ~buf ~boff =
      file.pread ~fid:fd ~foff ~len ~buf ~boff

    let pwrite ~tid:_ ~fd ~foff ~len ~buf ~boff =
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
              (* NOTE dirs.rename always takes place in a situation
                 where the relevant objects are locked *)
              (* FIXME dirs.rename should sync src and dst then do the update atomically *)
              dirs.rename_file (Rename_file_missing {
                  locks_held=();
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
                dirs.rename_file (Rename_file_file {
                    locks_held=();
                    times;
                    src=(spath.parent_id,spath.comp,fid1);
                    dst=(dpath.parent_id,dpath.comp,fid2)
                  }) >>= fun () ->
                locks.unlock ~tid ~objs >>= fun () ->
                ok ()
          end

        | File _fid,Dir _did ->
          extra.internal_err ~tid "FIXME rename f to d, d should be empty?"

        | Dir sdid,Missing -> begin
            (* check not root *)
            match sdid=root_did with
            | true -> err `Error_attempt_to_rename_root
            | false ->
              dirs.rename_dir (Rename_dir_missing {
                  tid;
                  locks_not_held=();
                  needs_ancestor_check=();
                  src=(spath.parent_id,spath.comp,sdid);
                  dst=(dpath.parent_id,dpath.comp) })
          end

        | Dir _sdid,File _fid ->
          err `Error_attempt_to_rename_dir_over_file  (* FIXME correct ? *)
        | Dir sdid,Dir ddid -> (
            match sdid=ddid with
            | true -> ok ()
            | false -> 
              (* FIXME this is allowed if dest is empty *)
              extra.internal_err ~tid "FIXME rename d to d, dst should be empty?")
        | Missing ,_ -> (
            Printf.printf "impossible %s\n%!" __LOC__;
            exit_1 "impossible")
    end (* rename *)


    (* FIXME truncate parent name; FIXME also stat *)
    let truncate ~tid:_ path length =
      resolve_file_path path >>=| fun fid ->
      (* let objs = [Fid fid] in *)
      mk_stat_times () >>= fun times ->
      (* locks.lock ~tid ~objs >>= fun () ->  *)
      file.truncate ~fid length >>= fun () ->
      file.set_times ~fid times >>= fun () ->
      (* locks.unlock ~tid ~objs >>= fun () -> *)
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
        (* NOTE no locks; is stat supposed to be atomic wrt metadata?
           perhaps have a "get_meta" call? *)
        file.get_sz ~fid >>= fun sz ->
        file.get_times ~fid >>= fun times ->
        ok { sz;kind=`File; times }
      | Dir did ->
        (* FIXME do we really need a kref for getting a single field?
           or even for just read-only access? No, read only access
           doesn't need to be protected with krefs; FIXME are there
           other places in this file that we only use read operations,
           so don't need a kref? Perhaps the point is that we don't
           know the actual implementation of these operations, which
           perhaps do cause updates? So we always need a kref? *)
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
        files.create_symlink_and_add_to_parent ~parent ~name ~times ~contents >>= fun () ->
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
