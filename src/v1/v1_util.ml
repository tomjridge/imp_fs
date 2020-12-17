
(* FIXME move Map_to_set and owndership map to tjr_lib *)

module Map_to_set(S: sig
    type a
    val compare_a: a -> a -> int
    type b
    val compare_b: b -> b -> int
  end) = struct

  open S

  module S1 = struct
    type t = a
    let compare = compare_a
  end
  module M_a = Map.Make(S1)

  module S2 = struct
    type t = b
    let compare = compare_b
  end
  module S_b = Set.Make(S2)

  type t = S_b.t M_a.t

  let empty : t = M_a.empty

  let add_elt_to_set ~a ~b t = 
    M_a.update a (function
        | None -> Some(S_b.singleton b)
        | Some s -> Some(S_b.add b s))
      t

  let mem ~a ~b t = 
    t |> M_a.find_opt a |> function
    | None -> false
    | Some bs -> 
      S_b.mem b bs

  let remove_elt_from_set ~a ~b t = 
    M_a.update a (function
        | None -> None
        | Some s -> Some(S_b.remove b s))
      t

  let find ~a t = 
    t |> M_a.find_opt a |> function
    | None -> S_b.empty
    | Some x -> x
end

(** Each a can own many b; each b is owned by at most one a. If a
   wants to acquire b, we check b is not already acquired. No locking
   or anything like that. This simply records the ownership
   relation. Purely functional. *)
module Ownership_map(S: sig
    type a
    val compare_a: a -> a -> int
    type b
    val compare_b: b -> b -> int
  end)
= struct
  open S

  module M_a_bs = Map_to_set(S)
  module S_b = M_a_bs.S_b

  let is_empty = S_b.is_empty

  module X = struct
    type t = b
    let compare = compare_b
  end
  module M_b_a = Map.Make(X)

  type m_b_a = a M_b_a.t

  type t = { a_bs:M_a_bs.t; b_a:m_b_a }

  let empty = { a_bs=M_a_bs.empty; b_a=M_b_a.empty }

  let add_elt_to_set ~a ~b t = 
    (* ownership, so it makes sense to check this here *)
    assert(M_b_a.find_opt b t.b_a = None); 
    { a_bs=M_a_bs.add_elt_to_set ~a ~b t.a_bs;
      b_a=M_b_a.add b a t.b_a }

  let remove_elt_from_set ~a ~b t = 
    assert(M_b_a.find_opt b t.b_a = Some a);
    { a_bs=M_a_bs.remove_elt_from_set ~a ~b t.a_bs;
      b_a=M_b_a.remove b t.b_a }

  let wf_at_b ~b t = 
    M_b_a.find_opt b t.b_a |> function
    | None -> true (* assume b is not in range of any a *)
    | Some a -> 
      (* check b in range of a *)
      M_a_bs.mem ~a ~b t.a_bs


  let find_b ~b t = 
    assert(wf_at_b ~b t);
    M_b_a.find_opt b t.b_a

  let _ = find_b

  let find_a ~a t = M_a_bs.find ~a t.a_bs

  let mem_elt_set ~a ~b t = M_a_bs.mem ~a ~b t.a_bs

  module Thread_ops = M_a_bs.M_a

  module Resource_ops = M_b_a 

  let get_resources ~tid t = 
    Thread_ops.find_opt tid t.a_bs |> function
    | None -> S_b.empty
    | Some xs -> xs

  let get_owner ~oid t = find_b ~b:oid t

  let owns ~tid ~oid t = 
    mem_elt_set ~a:tid ~b:oid t

  let owns_nothing ~tid t = is_empty (get_resources ~tid t)

  let acquire ~tid ~oid t = add_elt_to_set ~a:tid ~b:oid t      

  (* note this acquires in order - do we want to enforce that here or elsewhere? *)
  let acquire_all ~tid ~oids t = 
    (oids|>List.sort compare_b,t) |> iter_k (fun ~k (oids,map) -> 
        match oids with
        | [] -> map
        | oid::oids -> 
          acquire ~tid ~oid map |> fun map' -> 
          k (oids,map'))

  let release ~tid ~oid t = remove_elt_from_set ~a:tid ~b:oid t

  let release_all ~tid (t:t) = 
    get_resources ~tid t |> fun bs -> 
    bs |> M_a_bs.S_b.elements |> fun bs -> 
    (bs,t) |> iter_k (fun ~k (bs,t) -> 
        match bs with 
        | [] -> t
        | oid::bs -> k (bs,release ~tid ~oid t))

end
