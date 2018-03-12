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


(* state we maintain *)
type ('e,'repr) state = {
  elts: 'e list;
  elts_repr: 'repr
  
}

(* what we need from marshalling *)
type ('e,'repr) repr_ops = {
  nil: 'repr;
  snoc: 'e -> 'repr -> 'repr;
  wf_size: 'repr -> bool  (* whether we can fit repr in a Persistent_list node *)
}


(* we also need persistent_list ops *)

let make_persistent_chunked_list 
    ~list_ops 
    ~repr_ops 
    ~(read_state : unit -> (('e,'repr) state,'t) m)
  =
  let { replace_last; new_node } = list_ops in
  let { nil; snoc; wf_size } = repr_ops in
  let insert (e:'e) = 
    read_state () |> bind @@ fun s ->
    let { elts; elts_repr } = s in
    snoc e elts_repr |> fun new_elts_repr -> 
    wf_size new_elts_repr |> function
    | true -> 
      (* we can write the new contents into the list *)
      replace_last new_elts_repr
    | false ->
      (* we can't fit this new elt; so make a new node and try again *)
      snoc e nil |> fun new_elts_repr ->
      (* FIXME ASSUMES we need to be sure that any singleton list
         [elt] can fit in a Persistent_list node *)
      assert(wf_size new_elts_repr);
      new_node new_elts_repr
  in
  fun f -> f ~insert
  

let _ : 
  list_ops:('repr, 't) list_ops -> 
  repr_ops:('e, 'repr) repr_ops -> 
  read_state:(unit -> (('e, 'repr) state, 't) m) 
  -> (insert:('e -> (unit, 't) m) -> 'a) -> 'a
  = 
  make_persistent_chunked_list
