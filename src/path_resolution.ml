(* simple resolution of fuse absolute paths *)

open Tjr_string

open Tjr_btree
open Btree_api
open Monad


let string_to_components s kk = (
  assert(starts_with ~prefix:"/" s);
  s 
  |> split_on_all ~sub:"/"
  |> List.filter (fun x-> x<>"")
  |> fun s' -> kk ~cs:s' ~ends_with_slash:(ends_with ~suffix:"/" s)
)

(* we want to identify either a file or a directory by object id *)
let resolve' 
    ~root_oid
    ~(dir_map_ops: 'oid -> (('k,'v,'t)map_ops,'t)m)
    ~cs 
    ~ends_with_slash 
  = (
    let rec loop ~parent (* of dir *) ~cs = (
      match cs with
      | [] -> failwith __LOC__
      | c::cs -> 
        dir_map_ops parent |> bind (fun map_ops ->
          map_ops.find c |> bind (fun vopt ->
            match vopt with
            | None -> (
                if cs = [] then 
                  return @@ `Missing_entry(parent,c,ends_with_slash)
                else 
                  return @@ `Error_no_directory(parent,c,cs))
            | Some (`File(oid)) -> (
                if cs = [] && not ends_with_slash then
                  return @@ `File(parent,oid)
                else
                  return @@ `Error_file_present(c,cs,ends_with_slash))
            | Some (`Dir(oid)) -> (
                if cs = [] then 
                  return @@ `Dir(parent,oid)
                else
                  loop ~parent:oid ~cs)
          )
        ))
    in  
    if cs = [] then 
      return @@ `Dir(root_oid,root_oid) 
    else 
      loop ~parent:root_oid ~cs)
