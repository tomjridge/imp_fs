(* impfs ------------------------------------------------------------ *)

(* create and delete files and dirs; rename files and dirs; truncate
   files; read and write to files *)

open Tjr_btree
open Btree_api
open Monad

let file_create 
    ~name 
    ~parent_map_ops 
    ~(create_empty_file:unit -> ('r,'t)m)
    ~_File
  = (
    create_empty_file () |> bind @@ fun r ->
    (* NOTE storing the size in the parent dir prevents implementation
       of hardlinks *)
    parent_map_ops.insert name (_File ~root:r ~sz:0) 
    (* also need the parent of the parent? no because we store object
       ids in the grandparent, not references to the parent *)
  )

let file_or_dir_delete
    ~parent_map_ops
    ~name
    ~oid
    ~mark_garbage
  = (
    parent_map_ops.delete name |> bind @@ fun () ->
    (* assume no links, so oid is garbage and can be collected *)
    mark_garbage#oid oid 
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
    map_ops.insert name (_Dir ~root:r)
  )


  
