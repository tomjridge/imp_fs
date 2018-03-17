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
type ('k,'v) op = ('k,'v) Pcl.Test.op = Insert of 'k * 'v | Delete of 'k

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
  
  detach: unit -> ('ptr * 'map * 'ptr,'t) m
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
  : ('k,'v,'map,'ptr,'t) plog_ops 
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
  (* FIXME be clear about concurrency here: detach happens in memory,
     almost instantly, but other operations cannot interleave with it
     even if it is in the monad FIXME perhaps we prefer a "single
     step" detach without the use of bind, which introduces
     non-atomicity *)
  let detach () =  
    get () |> bind @@ fun s ->
    let r = (s.start_block,s.map_past,s.current_block) in
    set { s with start_block=s.current_block; map_past=map_empty } |> bind @@ fun () ->
    return r
  in
  { find; add; detach }  


let _ = make_plog


(* test  ---------------------------------------------------------- *)


module Test = struct

  include struct
    type ptr = int
    open Pl
    open Pcl.Test.Repr

    type ('k,'v) list_node = (ptr,('k,'v)repr) Pl.list_node

    type ('k,'v,'map) state = {
      map: (ptr * ('k,'v)list_node) list;  (* association list *)
      free: ptr;  (* iso to ptr *)

      plist_state: (int,('k,'v)repr) plist_state;
      pclist_state: (('k,'v)op,('k,'v)repr) pcl_state;
      plog_state: ('map,ptr) plog_state;
    }
  end

  (* NOTE FIXME copied from pcl *)
  let list_ops () = Pl.make_persistent_list
      ~write_node:(fun ptr node -> fun s -> ({ s with map=(ptr,node)::s.map },Ok ()))
      ~plist_state_ref:{
        get=(fun () -> fun s -> (s,Ok s.plist_state));
        set=(fun plist_state -> fun s -> failwith "" (* ({s with plist_state},Ok ()) *))
      }
      ~alloc:(fun () -> fun s -> ({ s with free=s.free+1 },Ok s.free))

  let _ = list_ops

  let repr_ops = Pcl.Test.Repr.repr_ops

  let chunked_list () =
    make_persistent_chunked_list
      ~list_ops:(list_ops ())
      ~repr_ops
      ~pcl_state_ref:{
        get=(fun () -> fun s -> (s,Ok s.pclist_state));
        set=(fun pclist_state -> fun s -> ({s with pclist_state},Ok ()));
      }

  let _ = chunked_list
  
  let plog ~map_ops = 
    chunked_list () @@ fun ~insert -> 
    make_plog
      ~map_ops
      ~insert
      ~plog_state_ref:{
        get=(fun () -> fun s -> (s,Ok s.plog_state));
        set=(fun plog_state -> fun s -> ({s with plog_state}, Ok ()))
      }

  let _ : 
    map_ops:('k, ('k, 'v) op, 'map) Tjr_map.map_ops -> 
    ('k, 'v, 'map, ptr, ('k, 'v, 'map) state) plog_ops 
    = plog


  (* fix types of 'k 'v and 'map *)
  (* test with an int -> int map *)

  open Tjr_map

  module Map_ = Tjr_map.Make(Int_ord)

  let map_ops = Map_.map_ops

  let plog () = plog ~map_ops

  let _ : unit ->
    (ptr, 'a, (ptr, 'a) op Map_int.t, ptr,
     (ptr, 'a, (ptr, 'a) op Map_int.t) state)
      plog_ops
    = plog
  

  (* FIXME exhaustive testing? *)
  let main () = ()
    

end



