(** An LRU cache of live objects; used for files and dirs.

This code is a bit naive - it blocks when going to the lower layer,
   whereas a cleverer version might allow read-only access to in-cache
   entries whilst going to lower layer. Whether this is actually a
   bottleneck in practice I don't know.
*)


(** factory type; NOTE 'c tyvar is short for 'cache *)
type ('k,'v,'c,'t) factory = <
  empty: capacity:int -> 'c;
  with_: 
    lower_acquire     : ('k -> ('v option,'t)m) -> 
    lower_release     : ('k*'v -> (unit,'t)m) -> 
    with_locked_state : ('c,'t) with_state -> 
    <
      find_opt: 'k -> ('v option,'t)m;
      (** Look up in cache, then in lower map; possibly adjust cache
          and return *)

      to_list : unit -> (('k*'v)list,'t)m;
      (** Used to periodically scan entries and eg sync them if they
         are file objects and they haven't been synced for a while *)
    >
>
(** NOTE lower_acquire is used to look up k in the lower map;
   lower_release is used when dropping a LRU entry from the cache *)


module type S = sig
  type k
  val cmp_k: k -> k -> int
  type v
  type t
  val monad_ops: t monad_ops
end

module type T = sig
  module S:S
  open S
  type c
  val factory : (k,v,c,t) factory
end

(** With full sig *)
module Make_v1(S:S) = struct
  module S=S
  open S

  let ( >>= ) = monad_ops.bind
  let return = monad_ops.return


  (** Use pqwy LRU *)

  module K = struct
    type t = S.k
    let compare: k -> k -> int = cmp_k
  end
  module V = struct
    type t = S.v
    let weight (_:v) = 1
  end
  module L = Lru.F.Make(K)(V)
  type c = L.t

  let empty ~capacity = L.empty capacity

  module With_(W:sig
      val lower_acquire: k -> (v option,t)m
      val lower_release: k*v -> (unit,t)m
      val with_locked_state: (c,t) with_state
    end) = struct
    open W
        
    let find_opt k = 
      with_locked_state.with_state (fun ~state:s ~set_state -> 
          L.find k s |> function
          | None -> (
              lower_acquire k >>= function
              | None -> return None
              | Some v -> 
                (* add to LRU cache *)
                s |> L.add k v |> fun s -> 
                (* maybe trim *)
                assert(L.size s <= L.capacity s +1);
                match L.size s = L.capacity s +1 with
                | true -> (
                    (* need to trim *)
                    s |> L.pop_lru |> function
                    | None -> failwith "impossible; just added to state, so cannot be empty"
                    | Some(kv,s) ->
                      lower_release kv >>= fun () -> 
                      set_state s >>= fun () -> 
                      return (Some v))
                | false -> (
                    (* don't need to trim *)
                    set_state s >>= fun () -> 
                    return (Some v)))
          | Some v -> 
            s |> L.promote k |> fun s -> set_state s >>= fun () -> 
            return (Some v))

    let to_list () = 
      with_locked_state.with_state (fun ~state:s ~set_state:_ -> 
          L.to_list s |> return)

    let obj = object
      method find_opt=find_opt
      method to_list=to_list
    end            
  end (* With_ *)

  let with_ ~lower_acquire ~lower_release ~with_locked_state =
    let module A = struct
      let lower_acquire=lower_acquire
      let lower_release=lower_release
      let with_locked_state=with_locked_state
    end
    in
    let module B = With_(A) in
    B.obj

  let factory : _ factory = object
    method empty=empty
    method with_=with_
  end

end

(** Version with restricted sig *)
module Make_v2(S:S) : T with module S=S = struct
  include Make_v1(S)
end

module Make = Make_v2

module Examples = struct
  
  module Int_int = struct
    module S = struct
      type k = int
      let cmp_k = Int.compare
      type v = int
      type t = lwt
      let monad_ops = lwt_monad_ops
    end
    module Make' = Make(S)
    let factory = Make'.factory
  end

end

let examples = object
  method for_int_int=Examples.Int_int.factory
end
