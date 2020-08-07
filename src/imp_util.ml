(** Utils; probably move elsewhere *)

module type Deriving_sexp = sig
  type t[@@deriving sexp]
end

type 'a deriving_sexp = (module Deriving_sexp with type t = 'a)


