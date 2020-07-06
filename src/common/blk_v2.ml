(** Another type for blks; discrimnate more at the type level between
   differently-sized blks.

    For a given type t1, we construct another type t2 which is
   intended to be the subset of the original; we also construct a
   singleton type with a value that gives information about the
   restricted type t2; further, we tie these together by linking the
   singleton type with t2 *)

(**/**)

module Pvt = struct
  (** 't2 is the new type; 't2 blk_info (ie a third type t3) is a
      phantom type standing for a singleton type which contains the
     blk_info for the type t2 *)
  type 't2 blk_info = {
    blk_sz: int;
  }

  module type S = sig
    type t1
    val blk_sz: int
    (* val blk_ops: t1 blk_ops *)
  end

  module Make(S:S) = struct
    open S
    type t2 = t1
    let blk_info: t2 blk_info = S.{blk_sz}
    (* let blk_ops = failwith "FIXME" *)
  end
end

module X = Pvt

module type X = sig
  type 't2 blk_info = private { blk_sz : int; }
  module type S = sig type t1 val blk_sz : int end
  module Make :
    functor (S : S) -> sig type t2 = private S.t1 val blk_info : t2 blk_info end
end

module Pvt_2 = (Pvt : X)

(**/**)

include Pvt_2

(** Now we can make a blk type with a given size, freely use it as the
    underlying type (via coercion) and also use blk_info to get
   information about the restricted type. However, we can't create new
   values of this type *)

(*
module type T1 = sig
  type t1
  type t2 = private t1
  val blk_ops: t2 blk_ops
  val blk_info: t2 blk_info
end

let ba_blk_factory = object
  method with_ = fun ~blk_sz -> 
    let module X : T1 with type t1=ba_buf = struct
      type t1 = ba_buf
      type t2 = private t1
      let blk_ops : t2 blk_ops = failwith ""
      let blk_info : t2 blk_info = failwith "" 
    end
    in
    (module X)
end
*)

(* an alternative: we can have a finite set of sizes (size_2,
   size_4096 etc) and use these as phantom parameters; not sure this
   is better (actually probably worse - why pick out size
   particularly?)

ultimately we want to ensure that all components agree on the size of
   the blk and buf, and preferably enforce this via types

a problem here might be the fact that (for bufs, say) we have plenty of conversions to/from string and bytes, which makes it difficult to avoid runtime checks; but do we actually need these conversions? for testing it makes things easier, but maybe elsewhere we don't
*)
