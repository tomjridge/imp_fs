(** The GOM for v3 is similar to a dir: a map backed by sqlite. If we
   are using fixed mutable origin blocks, then the GOM values don't
   change after creation until the GOM entry is deleted. *)

module Flag = Fv3_UNUSED.Flag
(* open Fv3.Maybe_dirty *)

(* FIXME this is really a combination of a syncable map and an
   allocator; prefer to use separate implementations for each of
   these, since we already have allocator? *)

type ('k,'v,'t) gom_ops = {
  alloc      : unit -> ('k,'t)m;
  find       : 'k -> ('v option,'t) m;
  insert     : 'k -> 'v -> (unit,'t) m;
  delete     : 'k -> (unit,'t) m;
  sync_list  : 'k list -> (unit,'t)m;
  sync       : unit -> (unit,'t)m  
  (** sync all keys, and min_free *)
}

module Op = struct
  type ('k,'v) op =
    | Insert of 'k*'v
    | Delete of 'k

  type ('k,'v) exec_ops = ('k,'v) op list
end
open Op

(* FIXME a cleverer approach would async alloc_n ahead of time, so as
   not to block when frees become empty; however, in our case the
   lower ops are implemented by sqlite, which we expect to be very
   fast to find the max free id on the relevant table; so no need to
   complicate the implementation with async and locks at this point *)
type ('k,'v,'t) lower_ops = {
  alloc_n: int -> ('k list,'t)m;
  (** blocking *)

  find : 'k -> ('v option,'t) m;

  exec : ('k,'v) exec_ops -> (unit,'t) m;
  (** exec is assumed synchronous - all caching done in the layer above *)
}

(* worth noting that the pattern is: upper layer caches updates, find
   is passed to lower; lower layer implements find, and batch updates;
   so the lower non-cached layer does not implement the same interface
   as the higher in general; of course, we could use a "transactional"
   (batch start, batch end) interface on the lower layer, which would
   preserve the insert/delete calls from the upper layer, but this
   would probably be pretty much iso to what we have here, ie a list
   of batched operations *)


(* cache for the map entries *)
type 'a cache_entry = 'a Dv3_UNUSED.cache_entry = Some_ of 'a | Deleted | Not_present

type ('k,'v,'cache) cache_ops = ('k,'v,'cache) Dv3_UNUSED.cache_ops

type ('k,'v,'cache) gom_cache = {
  frees: 'k list;
  cache: 'cache;
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

    val default_alloc_n : int
  end) = struct
  open S
      
  let ( >>= ) = monad_ops.bind
  let return = monad_ops.return

  let _ = assert(default_alloc_n > 0)

  let alloc () = 
    let n = default_alloc_n in
    with_cache.with_state (fun ~state ~set_state -> 
        match state.frees with 
        | [] -> 
          lower_ops.alloc_n n >>= fun ks -> 
          begin match ks with
          | [] -> failwith "impossible: default_alloc_n > 0"
          | k::ks -> 
            set_state {state with frees=ks} >>= fun () -> 
            return k end
        | k::frees -> 
          set_state {state with frees} >>= fun () -> 
          return k)

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

  (* let clean x = (fst x,Flag.clean)  *)

  let sync_list _ks = failwith "FIXME"

  let sync () = 
    with_cache.with_state (fun ~state ~set_state ->         
        let cache = state.cache in
        cache_ops.clean cache |> fun (xs,cache) -> 
        let ops = xs |> List.map to_op in
        lower_ops.exec ops >>= fun () -> 
        set_state { frees=state.frees; cache })

  let ops = { alloc; find; insert; delete; sync_list; sync }
  
end

(** As a function *)
let make (type t k v cache) ~monad_ops ~cache_ops ~lower_ops ~with_cache ~default_alloc_n = 
  let module A = struct
    type nonrec t = t
    let monad_ops = monad_ops
    type nonrec k = k
    type nonrec v = v
    type nonrec cache = cache
    let cache_ops = cache_ops
    let lower_ops = lower_ops
    let with_cache = with_cache
    let default_alloc_n = default_alloc_n
  end
  in
  let module B = Make(A) in
  B.ops

let _ = make    

(* FIXME make_with_lwt, with standard wbc cache_ops *)
