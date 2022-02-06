(** Utils; probably move elsewhere *)

module type Deriving_sexp = sig
  type t[@@deriving sexp]
end

type 'a deriving_sexp = (module Deriving_sexp with type t = 'a)



let split_at n xs = Base.List.split_n xs n

let take n xs = Base.List.take xs n

let write_string_to_file ~fn s = 
  (* Extlib. *)ExtLib.output_file ~filename:fn ~text:s

let read_file fn = (* Extlib. *)Std.input_file fn

(** f l .. f (h-1) *)
let rec map_range ~f l h = 
  if l >= h then [] else (f l)::(map_range ~f (l+1) h)

(** min, min+step, ... max-1  FIXME inefficient;  *)
let mk_range ~min ~max ~step = 
  let xs = ref [] in
  let n = ref min in
  while !n < max do
    xs:=!n::!xs;
    n:=!n+step
  done;
  List.rev !xs 

module Set_int = Set.Make(Int)

let dont_log = ref true

(** Essentially the Y combinator; useful for anonymous recursive
    functions. The k argument is the recursive callExample:

    {[
      iter_k (fun ~k n -> 
          if n = 0 then 1 else n * k (n-1))

    ]}


*)
let iter_k f (x:'a) =
  let rec k x = f ~k x in
  k x



module List = struct
  include List
  let hd_opt xs = if xs = [] then None else Some (List.hd xs)
end
