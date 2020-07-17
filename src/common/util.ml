(** Various utils *)

open Tjr_freelist

open Shared_ctxt

let add_tracing_to_freelist ~(freelist_ops:_ freelist_ops) = 
  let Freelist_intf.{ alloc; _ } = freelist_ops in
  let alloc () = 
    Printf.printf "%s: freelist alloc starts\n%!" __FILE__;
    alloc () >>= fun r -> 
    Printf.printf "%s: freelist alloc about to return %d\n%!" __FILE__ (B.to_int r);
    return r
  in
  {freelist_ops with alloc}

    
