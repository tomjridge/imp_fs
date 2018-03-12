(** POSIX operations on top of the object map *)

(* imp_posix, various posix ops ------------------------------------ *)

(* create and delete files and dirs; rename files and dirs; truncate
   files; read and write to files *)

open Imp_pervasives
open Tjr_fs_shared.Monad

let file_create 
    ~name 
    ~parent_map_ops 
    ~(create_empty_file:unit -> ('r,'t)m)
    ~_File
  = (
    let parent_map_ops_insert = failwith "FIXME" in
    create_empty_file () |> bind @@ fun r ->
    (* NOTE storing the size in the parent dir prevents implementation
       of hardlinks *)
    parent_map_ops_insert name (_File ~root:r ~sz:0) 
    (* also need the parent of the parent? no because we store object
       ids in the grandparent, not references to the parent *)
  )

let file_or_dir_delete
    ~parent_map_ops
    ~name
    ~oid
    ~file_or_dir
    ~mark_garbage
  = (
    let parent_map_ops_delete = failwith "FIXME" in
    parent_map_ops_delete name |> bind @@ fun () ->
    (* assume no links, so oid is garbage and can be collected *)
    mark_garbage oid 
    (* obviously if a dir we may delete all the oids of things in the
       dir; assume this happens in the step above *)
  )

let dir_create 
    ~parent 
    ~name 
    ~dir_map_ops 
    ~(create_empty_dir:unit -> ('r,'t)m)
    ~_Dir
  = (
    create_empty_dir () |> bind @@ fun r ->
    dir_map_ops parent |> bind @@ fun map_ops ->
    let map_ops_insert = failwith "FIXME" in
    map_ops_insert name (_Dir ~root:r)
  )


let readdir ~dir = 
  (dir#!ls_ops) |> fun ls_ops ->
  X.Leaf_stream_util.all_kvs ~ls_ops 

  
let rename' ~root_oid ~src ~dst =
  (* https://github.com/libfuse/libfuse/wiki/Invariants - we can
     ASSUME maybe that checks have already been carried out *)
  let p_map_ops = failwith "FIXME" in (* (p#!map_ops) *)
  let p_insert,p_delete = failwith "FIXME" in (* of p_map_ops *)
  match src,dst with
  | `File(src),`Missing(dst) -> (
      dst#!parent |> (fun p -> p_insert (dst#!name) (src#!entry))
      |> bind @@ fun () ->
      src#!parent |> fun p -> p_delete (src#!name))
  | `File(src),`File(dst) -> (
      match dst#!oid = src#!oid with
      | true -> assert false  (* FUSE checks this already? *)
      | false -> 
        (dst#!parent) 
        |> (fun p -> p_insert (dst#!name) (src#!entry))
        |> bind @@ fun () ->
        src#!parent |> fun p -> p_delete (src#!name))
  | `File(_),`Dir(_) -> assert false  (* ASSUME caught by fuse *)
  | `Dir(src),`Missing(dst) -> (
      (* ASSUME cannot be root; ASSUME other checks hold *)
      assert (src#!oid <> root_oid);
      (dst#!parent) |> (fun p -> p_insert (dst#!name) (src#!entry))
      |> bind @@ fun () ->
      (src#!parent) |> (fun p -> p_delete (src#!name)))
  | `Dir(src),`Dir(dst) -> (
      assert (src#!oid <> dst #!oid);
      (* ASSUME other conditions hold eg dst empty *)
      (dst#!parent) |> (fun p -> p_insert (dst#!name) (src#!entry))
      |> bind @@ fun () -> 
      (src#!parent) |> fun p -> p_delete (src#!name))
  | `Dir(src),`File(dst) -> 
    Monad.err (__LOC__ ^ ": attempt to rename directory onto file ENOTDIR")

let _ = rename'

let truncate () = failwith "TODO"

