(** Use Persistent_list to implement a "chunked" list. Store multiple
    items in a node, and automatically create a new node when the
    current fills up. 


    We want to avoid repeated serialization. So the representation of the
    kv is present already as a separate type. We need a function:

    'repr -> 'kv -> 'repr

    which extends the repr type with another 'kv; we also need a way
    to check that 'repr fits in the block.

    An alternative is just to allocate a range of blocks contiguously,
    and write operations into these blocks consecutively. But this is
    a bit horrible.


*)

open Tjr_fs_shared.Monad
open Persistent_list


(* state we maintain; this is for the current chunk *)
type ('e,'repr) pcl_state = {
  elts: 'e list;
  elts_repr: 'repr
  
}

(* what we need from marshalling *)
type ('e,'repr) repr_ops = {
  nil: 'repr;
  snoc: 'e -> 'repr -> [ `Ok of 'repr | `Error_too_large ];  
  (* may not be able to snoc an element if it won't fit in the node *)
}


(* we also need persistent_list ops *)

type inserted_type = 
  Inserted_in_current_node | Inserted_in_new_node

let make_persistent_chunked_list 
    ~list_ops 
    ~repr_ops 
    ~(pcl_state_ref : (('e,'repr) pcl_state,'t) mref)
    : (insert:('e -> (inserted_type, 't) m) -> 'a) -> 'a
  =
  let read_state,write_state = pcl_state_ref.get, pcl_state_ref.set in
  let { replace_last; new_node } = list_ops in
  let { nil; snoc } = repr_ops in
  let insert (e:'e) = 
    read_state () |> bind @@ fun s ->
    let { elts; elts_repr } = s in
    snoc e elts_repr |> function
    | `Ok new_elts_repr -> 
      let s = { elts=s.elts@[e]; elts_repr = new_elts_repr } in
      write_state s |> bind @@ fun () ->
      (* we can write the new contents into the list *)
      replace_last new_elts_repr |> bind @@ fun () ->
      return Inserted_in_current_node
    | `Error_too_large ->
      (* we can't fit this new elt; so make a new node and try again *)
      snoc e nil |> function 
      | `Error_too_large -> 
        (* FIXME ASSUMES we need to be sure that any singleton list
           [elt] can fit in a Persistent_list node *)
        failwith __LOC__
      | `Ok new_elts_repr ->
        let s = { elts=[e]; elts_repr=new_elts_repr } in
        write_state s |> bind @@ fun () ->
        new_node new_elts_repr |> bind @@ fun () ->
        return Inserted_in_new_node
  in
  fun f -> f ~insert


let _ : 
  list_ops:('repr, 't) list_ops -> 
  repr_ops:('e, 'repr) repr_ops -> 
  pcl_state_ref:(('e, 'repr) pcl_state, 't) mref 
  -> (insert:('e -> (inserted_type, 't) m) -> 'a) -> 'a
  = 
  make_persistent_chunked_list
