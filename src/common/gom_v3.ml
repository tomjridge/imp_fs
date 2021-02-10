(** The GOM for v3 is similar to a dir: a map backed by sqlite. *)

module Flag = Fv3.Flag
open Fv3.Maybe_dirty

type ('k,'v,'t) gom_ops = {
  find       : 'k -> ('v option,'t) m;
  insert     : 'k -> 'v -> (unit,'t) m;
  delete     : 'k -> (unit,'t) m;
  sync_list  : 'k list -> (unit,'t)m;
  sync       : unit -> (unit,'t)m
}

(* cache for the map entries *)
type 'a cache_entry = 'a Dv3.cache_entry = Some_ of 'a | Deleted | Not_present

type ('k,'v,'cache) cache_ops = ('k,'v,'cache) Dv3.cache_ops

type ('k,'v,'cache) gom_cache = {
  min_free_k: 'k maybe_dirty;
  cache: 'cache;
}

module Op = struct
  type ('k,'v) op =
    | Insert of 'k*'v
    | Delete of 'k

  type ('k,'v) exec_ops = ('k,'v) op list
end
open Op
  
type ('k,'v,'t) lower_ops = {
(*  get_min_free : unit -> ('k,'t) m;
  (** For initialization and subsequent gensym *) *)

  find : 'k -> ('v option,'t) m;

  exec : ('k,'v) exec_ops -> (unit,'t) m;
  (** exec is assumed synchronous - all caching done in the layer above *)
}

module Make(S:sig
    type t
    val monad_ops: t monad_ops
        
    type k
    type v
    type cache
    val cache_ops: (k,v cache_entry,cache) cache_ops
    val lower_ops: (k,v,t) lower_ops
    val with_cache: ((k,v,cache) gom_cache,t) with_state
  end) = struct
  open S
      
  let ( >>= ) = monad_ops.bind
  let return = monad_ops.return

  (* begin copy from dv3 *)
  let find k = 
    with_cache.with_state (fun ~state ~set_state -> 
        let cache = state.cache in
        cache_ops.find k cache |> fun (vopt,cache) -> 
        match (vopt: (v cache_entry * bool)option) with
        | None -> (
            lower_ops.find k >>= function
            | None -> (
                cache_ops.insert k (Not_present,Flag.clean) cache |> fun cache ->
                set_state {state with cache} >>= fun () -> 
                return None)
            | Some v -> (
                cache_ops.insert k (Some_ v, Flag.clean) cache |> fun cache -> 
                set_state {state with cache} >>= fun () -> 
                return (Some v)))
        | Some (v,_b) -> 
          set_state {state with cache} >>= fun () -> 
          match v with
          | Some_ v -> return (Some v)
          | Deleted -> return None
          | Not_present -> return None)

  let to_op (k, (v: _ cache_entry)) =
    match (v:_ cache_entry) with
    | Some_ v -> Op.Insert (k,v) 
    | Deleted -> Op.Delete k
    | Not_present ->
      (* These entries are always clean, so can't be returned by a trim *)
      failwith "impossible: cache_ops.clean should not return Not_present"
  

  let lower_insert_sync (xs: (k * v cache_entry) list) = 
    xs |> List.map to_op |> fun xs -> 
    lower_ops.exec xs

  let maybe_trim ~state ~set_state ~cache = 
      match cache_ops.needs_trim cache with
      | false -> 
        set_state {state with cache} >>= fun () -> 
        return ()
      | true -> 
        (* flush entries to lower layer *)
        cache_ops.trim cache |> fun (xs,cache) -> 
        lower_insert_sync xs >>= fun () -> 
        set_state {state with cache} >>= fun () -> 
        return ()

  let insert k v = with_cache.with_state (fun ~state ~set_state -> 
      let cache = state.cache in
      cache_ops.insert k (Some_ v,Flag.dirty) cache |> fun cache -> 
      maybe_trim ~state ~set_state ~cache)

  let delete k = 
    with_cache.with_state (fun ~state ~set_state -> 
        let cache = state.cache in
        cache_ops.delete k cache |> fun cache -> 
        maybe_trim ~state ~set_state ~cache)
  (* end copy from dv3 *)

  let clean x = (fst x,Flag.clean) 

  let sync_list _ks = failwith "FIXME"

  let sync () = 
    with_cache.with_state (fun ~state ~set_state ->         
        let cache = state.cache in
        cache_ops.clean cache |> fun (xs,cache) -> 
        let ops = xs |> List.map to_op in
        lower_ops.exec ops >>= fun () -> 
        set_state { min_free_k=(clean state.min_free_k); cache })


  let ops = { find; insert; delete; sync_list; sync }
  
end

