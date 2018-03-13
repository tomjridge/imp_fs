(** A persistent log, used to reduce traffic via the B-tree. NOTE
    append-only logs are quickest when data must be stored persistently *)

open Tjr_fs_shared.Monad
open Imp_pervasives
open X.Block


(* we construct on top of a persistent_chunked_list *)

module Pl = Persistent_list
module Pcl = Persistent_chunked_list
open Pcl


(* we need a concrete representation of actions; these are the
   elements that get written to disk *)


(* FIXME perhaps to avoid marshalling issues and type vars we should
   work with bytes? *)

(* FIXME need insert_many *)
type ('k,'v) op = Insert of 'k * 'v | Delete of 'k

(* we have to decide what information we need to keep for the "current
   chunk" *)

(* FIXME this doesn't work; why?
type ('k,'v,'repr) chunk_state = (('k,'v)op,'repr) pcl_state = {
  elts: (('k,'v) op) list;
  elts_repr: 'repr
}

*)

type ('k,'v,'repr) chunk_state = (('k,'v)op,'repr) pcl_state

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


(* test  ---------------------------------------------------------- *)

(* now we want to use the Persistent_chunked_list, which rests on
   persistent_list *)


module Test = struct

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
    

  
  let list_ops () : (('k, 'v) repr, ('k, 'v) state) Pl.list_ops = 
    Pl.make_persistent_list
      ~write_node:(fun ptr node -> fun s -> ({ s with map=(ptr,node)::s.map },Ok ()))
      ~plist_state_ref:{
        get=(fun () -> fun s -> (s,Ok s.plist_state));
        set=(fun plist_state -> fun s -> ({s with plist_state},Ok ()))
      }
      ~alloc:(fun () -> fun s -> ({ s with free=s.free+1 },Ok s.free))

  let _ = list_ops


  let chunked_list () =
    Pcl.make_persistent_chunked_list
      ~list_ops:(list_ops ())
      ~repr_ops
      ~pcl_state_ref:{
        get=(fun () -> fun s -> (s,Ok s.pclist_state));
        set=(fun pclist_state -> fun s -> ({s with pclist_state},Ok ()));
      }

  let _ : unit -> (insert:(('a, 'b) op -> (unit, ('a, 'b) state) m) -> 'c) -> 'c 
    = chunked_list
    

  (* run some tests *)
  let main () = 
    chunked_list () @@ fun ~insert ->
    let cmds = 
      Tjr_list.from_to 0 20 |> List.map (fun x -> (x,2*x)) |> fun xs ->
      let rec f xs = 
        match xs with 
        | [] -> return ()
        | (k,v)::xs -> 
          insert (Insert(k,v)) |> bind @@ fun () ->
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



