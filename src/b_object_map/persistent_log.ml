(** A persistent log, used to reduce traffic via the B-tree. NOTE
    append-only logs are quickest when data must be stored persistently *)

(*

TODO:

- need an in-memory map of current and past operations

- need API functions to query the union of the current and past maps,
  and to get the past map as a list



*)


(*

Interactive:

#thread;;
#require "imp_fs";;

open Imp_fs;;

*)

open Tjr_map

open Tjr_monad.Monad
open Imp_pervasives
open Tjr_btree.Block
open Tjr_btree.Base_types  (* mref *)

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

let make_plog_ops
    ~monad_ops
    ~map_ops
    ~insert
    ~plog_state_ref
  : ('k,'v,'map,'ptr,'t) plog_ops 
  =
  let ( >>= ) = monad_ops.bind in
  let return = monad_ops.return in  
  let { map_find; map_add; map_empty } = map_ops in
  let map_union m1 m2 = Tjr_map.map_union ~map_ops ~m1 ~m2 in
  (* NOTE get and set are for the plog_state component of the state *)
  let {get;set} = plog_state_ref in
  (* ASSUME start_block is initialized and consistent with pcl_state *)
  let find k : (('k,'v) op option,'t) m =
    get () >>= fun s ->
    let map_find = map_find_union ~map_ops ~m1:s.map_past ~m2:s.map_current in    
    let r = map_find k in
    return r
  in    
  let add op =
    get () >>= fun s ->
    insert op >>= function
    | Inserted_in_current_node ->
      get () >>= fun s' ->
      set { s' with map_current=map_ops.map_add (op2k op) op s'.map_current } 
    | Inserted_in_new_node ptr ->
      get () >>= fun s' ->
      set { s' with 
            current_block=ptr;
            map_past=map_union s.map_past s.map_current;
            map_current=map_ops.map_add (op2k op) op map_empty }
  in
  (* FIXME be clear about concurrency here: detach happens in memory,
     almost instantly, but other operations cannot interleave with it
     even if it is in the monad FIXME perhaps we prefer a "single
     step" detach without the use of bind, which introduces
     non-atomicity *)
  let detach () =  
    get () >>= fun s ->
    let r = (s.start_block,s.map_past,s.current_block) in
    (* we need to adjust the start block and the map_past - we are
       forgetting everything in previous blocks *)
    set { s with start_block=s.current_block; map_past=map_empty } >>= fun () ->
    return r
  in
  { find; add; detach }  


let _ = make_plog_ops




(* debug ------------------------------------------------------------ *)

module Dbg = struct

  (* use the pclist_to_nodes debug function *)

  (* The debug state is a pair of lists representing the past and
     current maps (the lists are derived from the on-disk contents). *)
(*
type ('k,'v) dbg = {
  dbg_current: ('k,'v) op list;
  dbg_past: ('k,'v) op list
} 
*)

  (* specialize for yojson *)
  let op_to_yojson a b op : Yojson.Safe.json = match op with
      Insert(k,v) -> `String (Printf.sprintf "Insert(%d,%d)" k v)
    | Delete k -> `String (Printf.sprintf "Delete(%d)" k)

  let op_of_yojson a b op = failwith __LOC__

  type find_result = (int,int) op option [@@deriving yojson]

  type dbg = {
    dbg_current: (int,int) op list;
    dbg_past: (int,int) op list
  } [@@deriving yojson]

  let init_dbg = {
    dbg_current=[];
    dbg_past=[]
  }

  (* note this has a separate ptr FIXME needed? *)
  let plog_to_dbg ~pclist_to_nodes ~get_plog_state (s:'t) : (* ('k,'v) *) dbg =
    let plog_state = s|>get_plog_state in
    pclist_to_nodes ~ptr:plog_state.start_block s
    |> List.map (fun (ptr,es) -> es)
    |> fun ess ->
    match plog_state.start_block = plog_state.current_block with
    | true -> 
      assert(List.length ess=1); (* ptr is s.start_block *)
      let dbg_current=List.concat ess in
      (* FIXME are we sure dbg_current is wellformed? *)
      {dbg_past=[]; dbg_current}
    | false -> 
      assert(ess <> []);
      {dbg_past=(Tjr_list.butlast ess |> List.concat); dbg_current=(Tjr_list.last ess) }

  let _ = plog_to_dbg


  (* for an association list, we need new entries to be at the front *)
  let dbg2list {dbg_current; dbg_past} = 
    (List.rev dbg_current @ List.rev dbg_past)

  let dbg2assoc_list dbg = dbg |> dbg2list |> List.map (fun op -> (op2k op,op))

  let find k dbg = 
    dbg |> dbg2assoc_list |> fun xs ->
    match List.assoc k xs with
    | exception _ -> None
    | v -> Some v 


  type tmp = Yojson.Safe.json option [@@deriving yojson]

  let with_world = Tjr_monad.State_passing_instance.with_world


  (* take an existing plog ops, and add testing code based on the dbg state *)
  let make_checked_plog_ops ~monad_ops ~plog_ops ~plog_to_dbg ~set_dbg ~get_dbg 
    : ('k,'v,'map,'ptr,'t) plog_ops
    = 
    let ( >>= ) = monad_ops.bind in
    let return = monad_ops.return in  
    let get_state () = with_world (fun s -> (s,s)) in
    let set_state s' = with_world (fun s -> ((),s')) in
    let find k = 
      get_state () >>= fun s ->
      let expected = find k (get_dbg s) in
      plog_ops.find k >>= fun v ->
      Imp_pervasives.log_ops.Tjr_log.log_lazy (fun () ->
          expected |> find_result_to_yojson |> fun expected ->
          v |> find_result_to_yojson |> fun v ->
          Printf.sprintf "%s:\n    expected(%s)\n    actual(%s)"
            "make_checked_plog_ops.find"
            (expected |> Yojson.Safe.pretty_to_string)
            (v |> Yojson.Safe.pretty_to_string));
      assert(v=expected);
      return v
    in
    let add op = 
      get_state () >>= fun s ->
      plog_ops.add op >>= fun () ->
      get_state () >>= fun s' ->
      get_dbg s |> fun dbg ->
      plog_to_dbg (* ~ptr:(s'|>start_block) *) s' |> fun dbg' ->
      Imp_pervasives.log_ops.Tjr_log.log_lazy (fun () ->
          Printf.sprintf "%s: %s %s"
            "make_checked_plog_ops.add"
            (dbg_to_yojson dbg |> Yojson.Safe.pretty_to_string)
            (dbg_to_yojson dbg' |> Yojson.Safe.pretty_to_string));
      assert(dbg2list dbg' = (op::(dbg2list dbg)));
      (* now need to update the dbg state *)
      set_state (s' |> set_dbg dbg') >>= fun () ->
      return ()
    in
    let detach () = 
      plog_ops.detach () >>= fun r ->
      get_state () >>= fun s' ->
      (* set dbg state *)
      plog_to_dbg (* ~ptr:(s'|>start_block) *) s' |> fun dbg' ->
      set_dbg dbg' s' |> fun s' ->
      set_state s' >>= fun () ->
      return r
    in  
    { find; add; detach }


  let _ = make_checked_plog_ops

end


(* test  ---------------------------------------------------------- *)


module Test : sig val test : depth:int -> unit end = struct

  open Dbg

  include struct
    type ptr = int
    open Pl
    open Pcl.Test.Repr

    type ('k,'v) list_node = (ptr,('k,'v)repr) Pl.list_node

    type ('k,'v,'map,'dbg) state = {
      map: (ptr * ('k,'v)list_node) list;  (* association list *)
      free: ptr;  (* iso to ptr *)

      plist_state: (int,('k,'v)repr) plist_state;
      pclist_state: (('k,'v)op,('k,'v)repr) pcl_state;
      plog_state: ('map,ptr) plog_state;
      dbg: 'dbg; (* debug state *)
    }
  end


  open Tjr_monad
  open Tjr_monad.Monad

  let monad_ops : ('k,'v,'map,'dbg) state state_passing monad_ops = 
    Tjr_monad.State_passing_instance.monad_ops ()
  

  (* NOTE FIXME copied from pcl *)
  let list_ops () = Pl.make_persistent_list
      ~monad_ops
      ~write_node:(fun ptr node -> with_world (fun s -> ((),{ s with map=(ptr,node)::s.map })))
      ~plist_state_ref:{
        get=(fun () -> with_world (fun s -> (s.plist_state,s)));
        set=(fun plist_state -> with_world (fun s -> ((),{s with plist_state})))
      }
      ~alloc:(fun () -> with_world (fun s -> (s.free,{ s with free=s.free+1 })))

  let _ = list_ops

  (* this should ensure no more than 2 items in each block FIXME but
     it seems that this is not enforced BUG *)
  let repr_ops = Pcl.Test.Repr.repr_ops 2 (* FIXME parameterize tests by this *)

  let chunked_list () =
    make_persistent_chunked_list
      ~monad_ops
      ~list_ops:(list_ops ())
      ~repr_ops
      ~pcl_state_ref:{
        get=(fun () -> with_world (fun s -> (s.pclist_state,s)));
        set=(fun pclist_state -> with_world (fun s -> ((),{s with pclist_state})));
      }

  let _ = chunked_list
  
  let plog ~map_ops = 
    chunked_list () |> fun { insert } -> 
    make_plog_ops
      ~monad_ops
      ~map_ops
      ~insert
      ~plog_state_ref:{
        get=(fun () -> with_world (fun s -> (s.plog_state,s)));
        set=(fun plog_state -> with_world (fun s -> ((),{s with plog_state})))
      }

  let _ : 
    map_ops:('k, ('k, 'v) op, 'map) Tjr_map.map_ops -> 
    ('k, 'v, 'map, ptr, ('k, 'v, 'map,'dbg) state state_passing) plog_ops 
    = plog


  (* fix types of 'k 'v and 'map *)
  (* test with an int -> int map *)

  open Tjr_map

  module Map_ = Tjr_map.Make(Int_ord)

  let map_ops = Map_.map_ops

  let plog () = plog ~map_ops


  let _ : unit ->
    (ptr, 'a, (ptr, 'a) op Map_int.t, ptr,
     (ptr, 'a, (ptr, 'a) op Map_int.t,'dbg) state state_passing)
      plog_ops
    = plog


  let checked_plog () : ('k,'v,'map,'ptr,'t) plog_ops = 
    let read_node ptr s = List.assoc ptr s.map in
    let plist_to_nodes ~(ptr:ptr) (s:('k,'v,'map,'dbg)state) = 
      Pl.plist_to_nodes ~read_node ~ptr s 
    in
    let repr_to_list = repr_ops.repr_to_list in
    let pclist_to_nodes ~ptr s = 
      Pcl.pclist_to_nodes ~repr_to_list ~plist_to_nodes ~ptr s
    in
    let _ = pclist_to_nodes in
    let plog_to_dbg s = 
      plog_to_dbg
        ~pclist_to_nodes
        ~get_plog_state:(fun s -> s.plog_state)
        s
    in
    let plog_ops = plog () in
    let set_dbg = fun dbg s -> {s with dbg} in
    let get_dbg = fun s -> s.dbg in
    make_checked_plog_ops
      ~monad_ops
      ~plog_ops
      ~plog_to_dbg
      ~set_dbg
      ~get_dbg



  let _ : (int,'v,'map,ptr,(int,'v,'map,'dbg)state state_passing)plog_ops = 
    checked_plog ()


  (* testing ------------------------------------------------------ *)

  

  let init_state = 
    let start_block = Pl.Test.start_block in
    let i = Pcl.Test.init_state ~repr_ops in
    {
      map=i.map;
      free=i.free;
      plist_state=i.plist_state;
      pclist_state=i.pclist_state;
      plog_state={
        start_block;
        current_block=start_block;
        map_past=map_ops.map_empty;
        map_current=map_ops.map_empty
      };
      dbg=init_dbg
    }
    [@@ocaml.warning "-40"]
      

  open Tjr_log

  let test ~depth = 
    let num_tests = ref 0 in
    let plog_ops = checked_plog () in
    (* let plog_ops = plog () in *)
    (* the operations are: find k; add op; detach 

       given some finite range for k, we want to attempt each
       operation in each state; we are not too bothered about
       repeating work at this point *)
    let ks = [1;2;3] in
    let ops = 
      `Detach :: 
      (ks |> List.map (fun k -> [`Find(k);`Insert(k,2*k);`Delete(k)]) |> List.concat) 
    in
    (* we exhaustively test these operations up to a maximum depth;
       the test state is a decreasing count paired with the system
       state *)
    let run = State_passing_instance.run in
    let rec step (count,s) =
      let f op = 
        num_tests:=!num_tests+1;
        match op with
        | `Detach -> 
          Imp_pervasives.log_ops.log "detach";
          run ~init_state:s (plog_ops.detach ()) |> fun (_,s') -> s'
        | `Delete k -> 
          Printf.sprintf "delete(%d)" k |> Imp_pervasives.log_ops.log;
          run ~init_state:s (plog_ops.add (Delete(k))) |> fun (_,s') -> s'
        | `Find k ->
          Printf.sprintf "find(%d)" k |> Imp_pervasives.log_ops.log;
          run ~init_state:s (plog_ops.find k) |> fun (_,s') -> s'
        | `Insert(k,v) -> 
          Printf.sprintf "insert(%d,%d)" k (k*2) |> Imp_pervasives.log_ops.log;
          run ~init_state:s (plog_ops.add (Insert(k,v))) |> fun (_,s') -> s'
      in
      let f op = 
        f op |> fun s' ->        
        Imp_pervasives.log_ops.Tjr_log.log_lazy (fun () ->
        Printf.sprintf "%s: %s"
          "test, post op"
          (dbg_to_yojson s'.dbg |> Yojson.Safe.pretty_to_string));
        step (count-1,s')
      in
      if count <= 0 then () else ops |> List.iter f
    in
    Printf.printf "%s: tests starting...\n%!" __FILE__;
    step (depth,init_state);
    Printf.printf "%s: ...tests finished\n" __FILE__;
    Printf.printf "%s: %d tests executed in total\n" __FILE__ !num_tests
  [@@ocaml.warning "-8"]


    

end



