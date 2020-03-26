(** The oft-repeated-almost-verbatim path resolution code *)

(* FIXME replace with path_resolution from the separate gh repo 

(* simple resolution of fuse absolute paths *)
open Tjr_string
open Tjr_fs_shared.Monad
open Imp_pervasives
open Imp_dir

let string_to_components s kk = 
  assert(starts_with ~prefix:"/" s);
  s 
  |> split_on_all ~sub:"/"
  |> List.filter (fun x-> x<>"")
  |> List.map X.Small_string.of_string  
  (* ASSUMES each component <= 256 bytes *)
  |> fun s' -> kk ~cs:s' ~ends_with_slash:(ends_with ~suffix:"/" s)


(* we want to identify either a file or a directory by object id *)
let resolve' 
    ~root_did
    ~(did_to_map_ops: did:did -> (('k,'v,'t)X.Map_ops.map_ops,'t)m)
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
        did_to_map_ops ~did:parent |> bind @@ fun map_ops ->
        X.Map_ops.dest_map_ops map_ops @@ fun ~find ~insert ~delete ~insert_many ->
        find c |> bind @@ fun vopt ->
        (* vopt is entry in parent dir, not in object map *)
        match vopt with
        | None -> (
            if cs = [] then 
              return @@ _Missing ~parent ~c ~ends_with_slash
            else 
              return @@ _Error (`Error_no_directory(parent,c,cs)))
        | Some (Fid(fid)) -> (
            if cs = [] && not ends_with_slash then
              return @@ _File ~parent ~fid
            else
              return @@ _Error (`Error_not_dir(c,cs,ends_with_slash)))
        | Some (Did(did)) -> (
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


let resolve ~root_did ~did_to_map_ops ~_Error ~_Missing ~_Dir ~_File s = 
  string_to_components s (fun ~cs ~ends_with_slash ->
    resolve' ~root_did ~did_to_map_ops ~_Error ~_Missing ~_Dir ~_File ~cs ~ends_with_slash)
*)
