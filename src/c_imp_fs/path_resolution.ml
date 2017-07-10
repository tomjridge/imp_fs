(* simple resolution of fuse absolute paths *)

open Tjr_string

open Tjr_btree
open Btree_api
open Monad
open Omap_pervasives
open Object_map

let string_to_components s kk = (
  assert(starts_with ~prefix:"/" s);
  s 
  |> split_on_all ~sub:"/"
  |> List.filter (fun x-> x<>"")
  |> List.map Small_string.of_string  (* NOTE each component <= 256 bytes *)
  |> fun s' -> kk ~cs:s' ~ends_with_slash:(ends_with ~suffix:"/" s)
)

open Omap_entry

(* we want to identify either a file or a directory by object id *)
let resolve' 
    ~root_did
    ~(dir_map_ops: did -> (('k,'v,'t)map_ops,'t)m)
    ~cs 
    ~ends_with_slash 
    ~_Error
    ~_Missing
    ~_Dir
    ~_File
  = (
    let rec loop ~(parent:did) (* of dir *) ~cs = (
      match cs with
      | [] -> failwith __LOC__
      | c::cs -> 
        dir_map_ops parent |> bind @@ fun map_ops ->
        map_ops.find c |> bind @@ fun vopt ->
        (* vopt is entry in parent dir, not in object map *)
        match vopt with
        | None -> (
            if cs = [] then 
              return @@ _Missing ~parent ~c ~ends_with_slash
            else 
              return @@ _Error (`Error_no_directory(parent,c,cs)))
        | Some (`Fid(fid)) -> (
            if cs = [] && not ends_with_slash then
              return @@ _File ~parent ~fid
            else
              return @@ _Error (`Error_file_present(c,cs,ends_with_slash)))
        | Some (`Dif(did)) -> (
            if cs = [] then 
              return @@ _Dir ~parent ~did
            else
              loop ~parent:did ~cs) )
    in  
    if cs = [] then 
      return @@ _Dir ~parent:root_did ~did:root_did
    else 
      loop ~parent:root_did ~cs)

let _ = resolve'
