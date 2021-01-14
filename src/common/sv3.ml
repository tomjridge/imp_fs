(** This is a cache-aware implementation of symlinks, for v3.

Don't open - too many types with names duplicated elsewhere. *)

(* See also: 

tjr_fs_shared/.../write_back_cache_v3.ml

*)

let dont_log = false

module Flag = Fv3.Flag
module Maybe_dirty = Fv3.Maybe_dirty
open Maybe_dirty

type ('k,'v,'cache) cache_ops = ('k,'v,'cache) wbc_ops
  
(** Per-sym cache entries *)
type  s_cache = {         
  contents : str_256 maybe_dirty
}

(** We need a way to write to the lower non-cached file implementation *)
type 't lower_ops = {
  set_contents : str_256 -> (unit,'t)m;
  get_contents : unit -> (str_256,'t)m;
  flush        : unit -> (unit,'t)m;
  sync         : unit -> (unit,'t)m;
}

type 't s_ops = 't lower_ops

module Make_v1(S: sig
    type t
    val monad_ops: t monad_ops

    val with_cache: (s_cache,t) with_state 

    val lower_ops: t lower_ops
  end) = struct

  open S

  let ( >>= ) = monad_ops.bind
  let return = monad_ops.return

  let set_contents contents = 
    with_cache.with_state (fun ~state:_ ~set_state -> 
        set_state {contents=(contents,Flag.dirty)})

  let get_contents () = 
    with_cache.with_state (fun ~state ~set_state:_ -> 
        state.contents |> fst |> return)

  let flush () =
    with_cache.with_state (fun ~state ~set_state -> 
        let contents,flg = state.contents in
        match flg with
        | x when x=Flag.clean -> return ()
        | x when x=Flag.dirty -> 
          lower_ops.set_contents contents >>= fun () -> 
          set_state {contents=(contents,Flag.clean)}
        | _ -> failwith "impossible")

  let sync () = 
    flush () >>= fun () ->
    lower_ops.sync ()    

  let symlink_ops = { set_contents;get_contents;flush;sync }

end


(** {2 common instance} *)

module With_lwt = struct
  open Shared_ctxt
      
  let make_symlink_cache ~contents : s_cache = { contents }

  let symlink_factory = 
    object
      method make_symlink_cache = make_symlink_cache
      method with_ ~lower_ops ~with_cache = 
        let module A = struct
          module S1 = struct
            include Shared_ctxt
            let with_cache = with_cache
            let lower_ops = lower_ops
          end
          module V1 = Make_v1(S1)
          include V1
        end
        in
        A.symlink_ops
    end

  type symlink_factory = < 
    make_symlink_cache : contents:str_256 maybe_dirty -> s_cache;
    with_ : 
      lower_ops:lwt s_ops ->
      with_cache:(s_cache, lwt) with_state -> 
      lwt s_ops 
  >

  let symlink_factory : symlink_factory = symlink_factory
      
end


