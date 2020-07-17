(** A too-simple hacky implementation of a free list FIXME

We use the Ke library for functional queues; in production these
   should be replaced by imperative queues of course.

FIXME we should check for double free

NOTE this is superseded by an implementation in imp_fs

 *)

module Fke = Ke.Fke

type ('a,'q) free_list_ops = {
  alloc: 'q -> ('a * 'q) option;
  free: 'a -> 'q -> 'q
}

module Internal = struct

  (* functional queue interface *)
  type ('a,'q) fq_ops = {
    empty    :'q;
    is_empty : 'q -> bool;
    enqueue  : 'a -> 'q -> 'q;
    dequeue  : 'q -> ('a * 'q)option;
    peek     : 'q -> 'a option;
    to_list  : 'q -> 'a list;
    of_list  : 'a list -> 'q
  }

  let fq = 
    let empty=Fke.empty in
    let enqueue=(fun a q -> Fke.push q a) in
    {
      empty;
      is_empty=Fke.is_empty;
      enqueue;
      dequeue=Fke.pop;
      peek=Fke.peek;
      to_list=(fun q -> Fke.fold (fun xs x -> x::xs) [] q);
      of_list=(fun xs -> (xs,Fke.empty) |> iter_k (fun ~k -> function
          | [],q -> q
          | x::xs,q -> k (xs,enqueue x q)))
    }

  let _ = fq

  (* elements in the queue are either:
     Free of int - for a free index
     Free_from i - to indicate that all blocks above i are free
     
     As an implementation detail, we only break down Free_from if
     there are no other entries in the queue
     
     Actually, our free list is a pair of free_from and the free list
  *)
  
  type ('a,'q) free_list = {
    free_from : 'a;
    queue     : 'q;
  } [@@deriving bin_io, sexp]
     
  let alloc ~succ (x:('a,'q)free_list) = 
    match fq.is_empty x.queue with
    | true -> Some(x.free_from,{x with free_from=succ x.free_from})
    | false -> 
      x.queue |> fq.dequeue |> function
      | None -> failwith "impossible"
      | Some (a,q) -> 
        Some(a,{x with queue=q})

  let free a (x:('a,'q)free_list) = { x with queue=fq.enqueue a x.queue }

  let alloc = alloc ~succ:(fun x -> x+1)

  let free_list_ops = { alloc; free }

  let _ 
: (int, (int, int Fke.t) free_list) free_list_ops
= free_list_ops
end
open Internal



  (* type free_list_impl = (int, int Fke.t) free_list *)
  (* val to_free_list   : abstract_free_list -> free_list_impl  *)
  (* val from_free_list : free_list_impl -> abstract_free_list *)

(** Generative functor *)
module Make(): sig
  type abstract_free_list
  val create_free_list : queue:int list -> free_from:int -> abstract_free_list
  val free_list_ops    : (int,abstract_free_list) free_list_ops
  module Free_list_with_bin_prot : sig
    type t = (int,int list)free_list [@@deriving bin_io, sexp]
    val to_t   : abstract_free_list -> t
    val from_t : t -> abstract_free_list
  end
end = struct
  type abstract_free_list = (int,int Fke.t) free_list
  let free_list_ops = free_list_ops
  (* let to_free_list = fun x -> x *)
  (* let from_free_list = fun x -> x *)

  module Free_list_with_bin_prot = struct
    open Bin_prot.Std
    open Sexplib.Std
    type t = (int,int list)free_list [@@deriving bin_io, sexp]
    let to_t = fun {free_from;queue} -> {free_from;queue=queue |> fq.to_list}
    let from_t = fun {free_from;queue} -> {free_from;queue=queue|>fq.of_list}
  end

  let create_free_list ~queue ~free_from = 
    { free_from;queue} |> Free_list_with_bin_prot.from_t
end

module Internal3 = Make()

include Internal3

let test () = 
  Printf.printf "Free_list: running tests...\n";
  let { free; alloc } = free_list_ops in
  create_free_list ~queue:[1;2] ~free_from:5 |> fun fl ->
  (alloc fl |> function 
      | (Some(1,fl)) -> fl
      | _ -> failwith __LOC__) |> fun fl ->
  (alloc fl |> function 
    | (Some(2,fl)) -> fl
    | _ -> failwith __LOC__) |> fun fl -> 
  (alloc fl |> function
    | Some(5,fl) -> fl
    | _ -> failwith __LOC__) |> fun fl ->
  (free 1 fl) |> fun fl -> 
  (alloc fl |> function
    | Some(1,fl) -> fl
    | _ -> failwith __LOC__) |> fun fl ->
  (alloc fl |> function
    | Some(6,fl) -> fl
    | _ -> failwith __LOC__) |> fun fl ->
  (free 2 fl) |> fun fl -> 
  (free 1 fl) |> fun fl -> 
  Printf.printf "Example free list: %s\n" 
    (Sexplib.Sexp.to_string_hum Free_list_with_bin_prot.(fl|>to_t|>sexp_of_t));
  Printf.printf "Free_list: all tests pass!\n";
  ()


