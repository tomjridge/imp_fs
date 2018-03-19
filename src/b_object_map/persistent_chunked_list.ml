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

  repr_to_list: 'repr -> 'e list  (* inverse of marshalling *)
}


type 'ptr inserted_type = 
  Inserted_in_current_node | Inserted_in_new_node of 'ptr

type ('e,'ptr,'t) pcl_ops = {
  insert:'e -> ('ptr inserted_type,'t) m
}


let make_persistent_chunked_list 
    ~list_ops 
    ~repr_ops 
    ~(pcl_state_ref : (('e,'repr) pcl_state,'t) mref)
    : ('e,'ptr,'t) pcl_ops
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
        (* NOTE the following allocates a new node and updates the
           pointer in the old node *)
        new_node new_elts_repr |> bind @@ fun ptr ->
        return (Inserted_in_new_node ptr)
  in
  { insert }


let _ : 
  list_ops:('repr, 'ptr, 't) list_ops -> 
  repr_ops:('e, 'repr) repr_ops -> 
  pcl_state_ref:(('e, 'repr) pcl_state, 't) mref 
  -> ('e,'ptr,'t) pcl_ops
  = 
  make_persistent_chunked_list




(* debugging -------------------------------------------------------- *)

(* we use the plist debug code, but map usign repr_to_list *)

let pclist_to_nodes 
    ~(repr_to_list:'repr -> 'e list) 
    ~(plist_to_nodes : ptr:'ptr -> 't -> ('ptr * ('ptr,'repr)list_node)list)
    ~(ptr:'ptr) 
    s 
  : ('ptr * 'e list) list 
  =
  plist_to_nodes ptr s
  |> List.map (fun (ptr,n) -> (ptr,n.contents |> repr_to_list))

let _ = pclist_to_nodes


(* test  ---------------------------------------------------------- *)

(* now we want to use the Persistent_chunked_list, which rests on
   persistent_list *)


module Test = struct

  module Pl = Persistent_list

  type ('k,'v) op = Insert of 'k * 'v | Delete of 'k  [@@deriving yojson]

  (* on-disk representation ----------------------------------------- *)
  (* we have to fix on a representation for the list of ops; for the
     time being, let's just keep this abstract; eventually it will be
     bytes *)

  module Repr : sig 
    type ('k,'v) repr
    val repr_ops : int -> ( ('k,'v)op, ('k,'v)repr) repr_ops
  end = struct
    type ('k,'v) repr = ('k,'v) op  list
    let repr_ops n = 
      {
      nil=[];
      snoc=(fun e es -> 
          if List.length es + 1 <= n 
          then `Ok(es@[e]) 
          else `Error_too_large);
      repr_to_list=(fun r -> r)
    }
  end
  open Repr




  (* In order to test, we need a state which contains the plist state
     and the pclist state. *)

  type ptr = int
  type ('k,'v) list_node = (ptr,('k,'v)repr) Pl.list_node


  (* system state *)
  type ('k,'v) state = {
    map: (ptr * ('k,'v)list_node) list;  (* association list *)
    free: int;  (* iso to ptr *)

    (* plog_state: ('k,'v) plog_state *)
    plist_state: (int,('k,'v)repr) Pl.plist_state;
    pclist_state: (('k,'v)op,('k,'v)repr) pcl_state
  }

  let init_state ~repr_ops = 
    let start_block = Persistent_list.Test.start_block in
    (* NOTE we can't reuse Pl.Test.init_state because the values on
       disk are of a different type *)
    (* let i = Persistent_list.Test.init_state in *)
    let elts = [] in
    let elts_repr = repr_ops.nil in
    let current_node= Pl.{ next=None; contents=elts_repr } in
    {    
      map=[(start_block,current_node)]; 
      free=(start_block+1);
      plist_state={
        current_ptr=start_block;
        current_node
      }; 
      pclist_state={ elts; elts_repr };
    }


  (* list ops ------------------------------------------------------- *)

  let list_ops () : (('k, 'v) repr, 'ptr, ('k, 'v) state) Pl.list_ops = 
    Pl.make_persistent_list
      ~write_node:(fun ptr node -> fun s -> ({ s with map=(ptr,node)::s.map },Ok ()))
      ~plist_state_ref:{
        get=(fun () -> fun s -> (s,Ok s.plist_state));
        set=(fun plist_state -> fun s -> ({s with plist_state},Ok ()))
      }
      ~alloc:(fun () -> fun s -> ({ s with free=s.free+1 },Ok s.free))

  let _ = list_ops


  (* chunked list ops ----------------------------------------------- *)


  let chunked_list ~repr_ops =
    make_persistent_chunked_list
      ~list_ops:(list_ops ())
      ~repr_ops
      ~pcl_state_ref:{
        get=(fun () -> fun s -> (s,Ok s.pclist_state));
        set=(fun pclist_state -> fun s -> ({s with pclist_state},Ok ()));
      }

  let _ : repr_ops:('e,'repr)repr_ops -> ('e,'ptr,'t) pcl_ops
    = chunked_list



  (* main ----------------------------------------------------------- *)

  (* run some tests *)
  let main () = 
    let repr_ops = repr_ops 2 in
    let init_state = init_state ~repr_ops in
    chunked_list ~repr_ops |> function { insert } ->
    let cmds = 
      Tjr_list.from_to 0 20 |> List.map (fun x -> (x,2*x)) |> fun xs ->
      let rec f xs = 
        match xs with 
        | [] -> return ()
        | (k,v)::xs -> 
          insert (Insert(k,v)) |> bind @@ fun _ ->
          f xs
      in
      f xs
    in  
    cmds init_state |> fun (s,Ok x) -> 
    assert(x=());
    s


  (* to test interactively:

     FIXME remove this thread dependency

     #thread;;  
     #require "imp_fs";;

     open Imp_fs;;
     open Persistent_log;;
     Test.main();;


     - : (int, int) Imp_fs.Persistent_log.Test.state =
     {Imp_fs.Persistent_log.Test.map =
     [(2, {Imp_fs.Persistent_log.Pl.next = None; contents = <abstr>});
     (1, {Imp_fs.Persistent_log.Pl.next = Some 2; contents = <abstr>});
     (1, {Imp_fs.Persistent_log.Pl.next = None; contents = <abstr>});
     (1, {Imp_fs.Persistent_log.Pl.next = None; contents = <abstr>});
     (1, {Imp_fs.Persistent_log.Pl.next = None; contents = <abstr>});
     (1, {Imp_fs.Persistent_log.Pl.next = None; contents = <abstr>});
     (1, {Imp_fs.Persistent_log.Pl.next = None; contents = <abstr>});
     (1, {Imp_fs.Persistent_log.Pl.next = None; contents = <abstr>});
     (1, {Imp_fs.Persistent_log.Pl.next = None; contents = <abstr>});
     (1, {Imp_fs.Persistent_log.Pl.next = None; contents = <abstr>});
     (1, {Imp_fs.Persistent_log.Pl.next = None; contents = <abstr>});
     (1, {Imp_fs.Persistent_log.Pl.next = None; contents = <abstr>});
     (0, {Imp_fs.Persistent_log.Pl.next = Some 1; contents = <abstr>});
     (0, {Imp_fs.Persistent_log.Pl.next = None; contents = <abstr>});
     (0, {Imp_fs.Persistent_log.Pl.next = None; contents = <abstr>});
     (0, {Imp_fs.Persistent_log.Pl.next = None; contents = <abstr>});
     (0, {Imp_fs.Persistent_log.Pl.next = None; contents = <abstr>});
     (0, {Imp_fs.Persistent_log.Pl.next = None; contents = <abstr>});
     (0, {Imp_fs.Persistent_log.Pl.next = None; contents = <abstr>});
     (0, {Imp_fs.Persistent_log.Pl.next = None; contents = <abstr>});
     (0, {Imp_fs.Persistent_log.Pl.next = None; contents = <abstr>});
     (0, {Imp_fs.Persistent_log.Pl.next = None; contents = <abstr>});
     (0, {Imp_fs.Persistent_log.Pl.next = None; contents = <abstr>});
     (0, {Imp_fs.Persistent_log.Pl.next = None; contents = <abstr>})];
     free = 3;
     plist_state =
     {Imp_fs.Persistent_log.Pl.current_ptr = 2;
     current_node = {Imp_fs.Persistent_log.Pl.next = None; contents = <abstr>}};
     pclist_state =
     {Imp_fs.Persistent_log.Pcl.elts = [Insert (20, 40)]; elts_repr = <abstr>}}

     This looks OK. The current elts is a singleton [(20,40)] and we
     have just started writing to node 2.

  *)


end



