(** A persistent log, used to reduce traffic via the B-tree. NOTE
    append-only logs are quickest when data must be stored persistently *)

(*

TODO:

- need an in-memory map of current and past operations

- need API functions to query the union of the current and past maps,
  and to get the past map as a list



*)

open Tjr_map

open Tjr_fs_shared.Monad
open Imp_pervasives
open X.Block


(* we construct on top of a persistent_chunked_list *)

module Pl = Persistent_list
module Pcl = Persistent_chunked_list
open Pcl



(* actions ---------------------------------------------------------- *)

(* we need a concrete representation of actions; these are the
   elements that get written to disk *)


(* FIXME perhaps to avoid marshalling issues and type vars we should
   work with bytes? *)

(* FIXME need insert_many *)
type ('k,'v) op = Insert of 'k * 'v | Delete of 'k

let op2k = function
  | Insert (k,v) -> k
  | Delete k -> k


(* we have to decide what information we need to keep for the "current
   chunk" *)

(* FIXME this doesn't work; why?
type ('k,'v,'repr) chunk_state = (('k,'v)op,'repr) pcl_state = {
  elts: (('k,'v) op) list;
  elts_repr: 'repr
}

*)


(* FIXME needed? type ('k,'v,'repr) chunk_state = (('k,'v)op,'repr) pcl_state *)


(* in-mem map; NOTE 'v is ('k,'v)op  *)
type ('k,'v,'map) map_ops = ('k,('k,'v)op,'map) Tjr_map.map_ops


type ('k,'v,'map,'ptr,'t) plog_ops = {
  find: 'k -> (('k,'v) op option,'t) m;  
  (* should execute in mem but to control concurrency we put in the
     monad FIXME? something better can be done? *)

  add: ('k,'v)op -> (unit,'t) m;  (* add rather than insert, to avoid confusion *)
  
  detach: unit -> 'ptr * 'map * 'ptr  
  (* 'ptr to first block in list; map upto current node; 'ptr to current node *)
}


type ('map,'ptr) plog_state = {
  start_block: 'ptr;
  current_block: 'ptr;
  map_past: 'map;  (* in reverse order *)
  map_current: 'map;
}


(* a map built from two other maps; prefer m2 *)
let map_find_union ~map_ops ~m1 ~m2 k = 
  let open Tjr_map in
  map_ops.map_find k m2 |> function
  | Some _ as x -> x
  | None -> 
    map_ops.map_find k m1

(* moved to tjr_map       
let map_union ~map_ops ~m1 ~m2 = 
  let { map_add; map_bindings } = map_ops in
  Tjr_list.with_each_elt
    ~step:(fun ~state:m1' (k,op) -> map_add k op m1')
    ~init_state:m1
    (map_bindings m2)
*)

(* FIXME what about initialization? *)

let make_plog
    ~map_ops
    ~insert
    ~plog_state_ref
(*  : ('k,'v,'map,'ptr,'t) plog_ops *)
  =
  let { map_find; map_add; map_empty } = map_ops in
  let map_union m1 m2 = Tjr_map.map_union ~map_ops ~m1 ~m2 in
  let {get;set} = plog_state_ref in
  (* ASSUME start_block is initialized and consistent with pcl_state *)
  let find k : (('k,'v) op option,'t) m =
    get () |> bind @@ fun s ->
    let map_find = map_find_union ~map_ops ~m1:s.map_past ~m2:s.map_current in    
    let r = map_find k in
    return r
  in    
  let add op =
    insert op |> bind @@ function
    | Inserted_in_current_node ->
      get () |> bind @@ fun s ->
      set { s with map_current=map_ops.map_add (op2k op) op s.map_current } 
    | Inserted_in_new_node ptr ->
      get () |> bind @@ fun s ->
      set { s with 
            map_past=map_union s.map_past s.map_current;
            map_current=map_empty }      
  in
  let detach () = () in
  failwith ""
  

  
  
  
  



(* test  ---------------------------------------------------------- *)

(* now we want to use the Persistent_chunked_list, which rests on
   persistent_list *)


module Test = struct

  (* on-disk representation ----------------------------------------- *)
  (* we have to fix on a representation for the list of ops; for the
     time being, let's just keep this abstract; eventually it will be
     bytes *)

  module Repr : sig 
    type ('k,'v) repr
    val repr_ops : ( ('k,'v)op, ('k,'v)repr) repr_ops
  end = struct
    type ('k,'v) repr = ('k,'v) op  list
    let repr_ops = {
      nil=[];
      snoc=(fun e es -> if List.length es < 10 then `Ok (es@[e]) else `Error_too_large);
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

  let init_state = 
    let root = 0 in
    let elts = [] in
    let elts_repr = repr_ops.nil in
    let current_node= Pl.{ next=None; contents=elts_repr } in
    {    
      map=[(root,current_node)]; 
      free=root+1;
      plist_state=Pl.{
        current_ptr=0;
        current_node
      };
      pclist_state=Pcl.{ elts; elts_repr };
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


  let chunked_list () =
    Pcl.make_persistent_chunked_list
      ~list_ops:(list_ops ())
      ~repr_ops
      ~pcl_state_ref:{
        get=(fun () -> fun s -> (s,Ok s.pclist_state));
        set=(fun pclist_state -> fun s -> ({s with pclist_state},Ok ()));
      }

  let _ : unit -> (insert:(('a, 'b) op -> ('ptr inserted_type, ('a, 'b) state) m) -> 'c) -> 'c 
    = chunked_list
    


  (* main ----------------------------------------------------------- *)

  (* run some tests *)
  let main () = 
    chunked_list () @@ fun ~insert ->
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



