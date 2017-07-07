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
  |> List.map Small_string.of_string  (* NOTE each component <= 256 bytes *)
  |> fun s' -> kk ~cs:s' ~ends_with_slash:(ends_with ~suffix:"/" s)
)

open Entry

(* we want to identify either a file or a directory by object id *)
let resolve' 
    ~root_oid
    ~(dir_map_ops: 'oid -> (('k,'v,'t)map_ops,'t)m)
    ~cs 
    ~ends_with_slash 
    ~_Error
    ~_Missing
    ~_Dir
    ~_File
  = (
    let rec loop ~parent (* of dir *) ~cs = (
      match cs with
      | [] -> failwith __LOC__
      | c::cs -> 
        dir_map_ops parent |> bind @@ fun map_ops ->
        map_ops.find c |> bind @@ fun vopt ->
        match vopt with
        | None -> (
            if cs = [] then 
              return @@ _Missing ~parent ~c ~ends_with_slash
            else 
              return @@ _Error (`Error_no_directory(parent,c,cs)))
        | Some (F(oid,sz)) -> (
            if cs = [] && not ends_with_slash then
              return @@ _File ~parent ~oid ~sz
            else
              return @@ _Error (`Error_file_present(c,cs,ends_with_slash)))
        | Some (D(oid)) -> (
            if cs = [] then 
              return @@ _Dir ~parent ~oid
            else
              loop ~parent:oid ~cs) )
    in  
    if cs = [] then 
      return @@ _Dir ~parent:root_oid ~oid:root_oid
    else 
      loop ~parent:root_oid ~cs)

let _ = resolve'
