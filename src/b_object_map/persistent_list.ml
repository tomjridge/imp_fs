(* A persistent list of values, implemented as a persistent
   singly-linked list. *)

(*
#thread;;
#require "tjr_btree";;
#require "tjr_lib";;
*)

open Tjr_fs_shared.Monad


(* nodes in the list have an optional next pointer, and contents *)
type ('ptr,'a) list_node = {
  next: 'ptr option;
  contents: 'a;
}

(* the cursor state is in memory; make sure to write the current_node to disk *)
type ('ptr,'a) cursor_state = {
  current_ptr: 'ptr;  (* block we are currently updating *)
  current_node: ('ptr,'a) list_node;  (* stored in mem FIXME? *)
}

type ('a,'t) list_ops = {
  replace_last: 'a -> (unit,'t) m;
  new_node: 'a -> (unit,'t) m;
}



let make_persistent_list 
    ~(write_node : 'ptr -> ('ptr,'a) list_node -> (unit,'t) m) 
    ~(read_state : unit -> (('ptr,'a) cursor_state,'t) m)
    ~(write_state : ('ptr,'a) cursor_state -> (unit,'t) m)
    ~(alloc : unit -> ('ptr,'t) m)
  =
(*
  let create ~ptr ~contents = 
    let node = {
      next=None;
      contents
    }
    in
    write_node ptr node
  in
*)
  let replace_last contents = 
    read_state () |> bind @@ fun s ->
    let current_node = { s.current_node with contents } in
    write_node s.current_ptr current_node |> bind @@ fun () ->
    write_state { s with current_node } |> bind @@ fun () -> 
    return ()
  in
  let new_node contents = 
    alloc () |> bind @@ fun new_ptr ->
    read_state () |> bind @@ fun { current_ptr; current_node } ->
    (* write the current block with a valid next ptr *)
    let next = Some new_ptr in
    {current_node with next } |> write_node current_ptr |> bind @@ fun () ->
    (* construct new node *)
    { next=None; contents } |> fun new_node ->
    write_node new_ptr new_node |> bind @@ fun () ->
    { current_ptr=new_ptr; current_node=new_node } |> fun s ->
    write_state s
  in
  { replace_last; new_node }


let _ = make_persistent_list


let rec plist_to_list ~read_node ~ptr =
  let acc = ref [] in
  let rec loop ptr = 
    read_node ptr |> bind @@ fun { next; contents } ->
    acc:=contents::!acc;
    match next with
    | None -> return (List.rev !acc)
    | Some ptr -> loop ptr
  in
  loop ptr



let _ = plist_to_list



(* testing ---------------------------------------------------------- *)


(* the state of the whole system *)
type ('ptr,'a) t = {
  map: ('ptr*('ptr,'a)list_node) list;  (* association list *)
  cursor_state: ('ptr,'a) cursor_state;
  free: int;  (* iso to ptr *)
}

let write_node ptr n = fun s -> 
  { s with map=(ptr,n)::s.map } |> fun s ->
  (s,Ok ())

let tap = ref []

let read_node ptr = fun s ->
  Printf.printf "ptr is: %d\n" ptr;
  tap:=s.map;
  (s, Ok (List.assoc ptr s.map))  (* ASSUMES ptr in map *)


let read_state () = fun s -> 
  (s,Ok s.cursor_state)

let write_state cursor_state = fun s ->
  { s with cursor_state } |> fun s ->
  (s, Ok ())

let alloc ~int_to_ptr = fun () -> fun s -> 
  ({s with free=s.free+1},Ok (s.free |> int_to_ptr))


(* Write some new nodes, update some, and finally print out the list *)
let main () = 
  let init = 
    let root = 0 in
    let current_node={ next=None; contents="Start" } in
    {    
      map=[(root,current_node)]; 
      cursor_state={ current_ptr=root; current_node };
      free=root+1
    }
  in
  let int_to_ptr x = x in
  let alloc = alloc ~int_to_ptr in
  let ops = make_persistent_list ~write_node ~read_state ~write_state ~alloc in
  let cmds = 
    ops.replace_last "New start" |> bind @@ fun () ->
    ops.new_node "second node" |> bind @@ fun () ->
    ops.new_node "third node" |> bind @@ fun () ->
    ops.replace_last "alternative third node" |> bind @@ fun () ->
    plist_to_list ~read_node ~ptr:0
  in
  cmds init |> fun (s,Ok xs) ->
  assert(xs = ["New start";"second node";"alternative third node"]);
  xs |> Tjr_string.concat_strings ~sep:";" |> fun str ->
  print_endline str;
  s  (* actually return the state *)