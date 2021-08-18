(** Various utils *)

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

