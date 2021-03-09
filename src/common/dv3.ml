(** This is a cache-aware implementation of directories, for v3.

Don't open - too many types with names duplicated elsewhere.

We model a dir as a map from name to dir entry, together with the parent and times.

The cache maps a name to [ `New _ | `Deleted _ | `Not_present _ ]

Find, insert, delete are straightforward (as are parent and times).

For listing a directory, we take the contents of the cache (held in memory) and combine it with the underlying list operation. The operations for listing are:

{[
type ('k,'v,'r,'ls_state,'t) leaf_stream_ops = {
    make_leaf_stream : 'r -> ('ls_state,'t) m;
    ls_step          : 'ls_state -> ('ls_state option,'t) m;
    ls_kvs           : 'ls_state -> ('k*'v)list;
  }
]}

If these operations are supported by the lower layer, then we just make sure ls_kvs returns the appropriate set of kvs, taking into account the contents of the cache.

 *)

(* See also: 

tjr_fs_shared/.../write_back_cache_v3.ml

*)

open Fv3.Maybe_dirty
module Flag = Fv3.Flag

module List = struct
  include List

  (** take an 'a list, and a function char from 'a to bool (which
     indicates which "class" the element lies in), and a function from
     'a to 'b and a function from 'a to 'c and produce two lists. The
     first is for elements where char is true.  *)
  let partition_map ~char ~f ~g xs = 
    ([],[],xs) |> iter_k (fun ~k (xs,ys,zs) -> 
        match zs with 
        | [] -> (xs,ys)
        | x::zs -> 
          match char x with
          | true -> k ((f x)::xs,ys,zs)
          | false -> k (xs,(g x)::ys,zs))

  let _ : char:('a -> bool) -> f:('a -> 'b) -> g:('a -> 'c) -> 'a t -> 'b t * 'c t = partition_map

  (** quick membership; assumes elts are comparable *)
  let mem_via_set (type k) ~compare_k ~elts () =
    let module K = struct
      type t = k
      let compare = compare_k
    end
    in
    let module S = Set.Make(K) in
    let set = S.of_list elts in
    fun k -> S.mem k set

  (** quick membership;  assumes elts are hashable *)
  let mem_via_hashtbl ~elts () = 
    let tbl = Hashtbl.create (List.length elts) in
    List.iter (fun x -> Hashtbl.add tbl x ()) elts;
    fun k -> Hashtbl.mem tbl k

end


let dont_log = false

type times = Minifs_intf.times[@@deriving bin_io]

(* As dir_impl.ml, but without get_origin *)
type ('k,'v,'t,'did) dir_ops = {
  find       : 'k -> ('v option,'t) m;
  insert     : 'k -> 'v -> (unit,'t) m;
  delete     : 'k -> (unit,'t) m;

  ls_create  : unit -> (('k,'v,'t)Tjr_btree.Btree_intf.ls,'t)m;
  set_parent : 'did -> (unit,'t)m;
  get_parent : unit -> ('did,'t)m;
  set_times  : times -> (unit,'t)m;
  get_times  : unit -> (times,'t)m;    

  flush      : unit -> (unit,'t)m;
  sync       : unit -> (unit,'t)m;    
}

(* NOTE if the lower layer is sqlite, there is no difference between flush and sync - they both force all pending changes to db; so we use lower_ops.sync rather than lower_ops.flush *)

type ('did,'cache) d_cache = {
  times: times maybe_dirty;
  parent: 'did maybe_dirty;
  cache: 'cache; (* cache of entries *)
}

type 'a cache_entry = Some_ of 'a | Deleted | Not_present

(* type ('k,'v,'cache) cache_ops = ('k,'v,'cache) wbc_ops *)

(* based on wbc_ops, from write_back_cache_v3; we need to bridge this
   to the lru2gen; the interface below is pure, but lru is monadic *)
type ('k,'v,'t) cache_ops = {
  find       : 'k -> 't -> ('v * bool) option * 't;
  insert     : 'k -> 'v * bool -> 't -> 't;
  delete     : 'k -> 't -> 't;
  needs_trim : 't -> bool;
  trim       : 't -> ('k * 'v) list * 't;
  clean      : 't -> ('k * 'v) list * 't;
  bindings   : 't -> ('k * ('v*bool)) list
}

(* Make_v1 in OLD *)


(** {2 Make_v2 - assumes lower layer implements batch operations} *)

module Op = struct
  type ('k,'v,'did) op' =
    | Insert of 'k*'v
    | Delete of 'k
    | Set_parent of 'did
    | Set_times of times

  (** With the current impl, the spill will also cause the database to
     update, but perhaps we could consider having another layer of
     caching here FIXME perhaps we want this just with_sync *)
  type exec_type = Spill | With_sync

  type ('k,'v,'t,'did) exec_ops = exec_type * ('k,'v,'did) op' list
  
end
open Op

(** A potentially nicer interface to list operations? *)
module Ls = struct

  type ('k,'v,'t) ops = {
    kvs: unit -> (('k*'v) list,'t)m; 
    (** NOTE if we return [], then we are not necessarily finished! *)

    step: unit -> (unit,'t)m; 

    is_finished: unit -> (bool,'t)m;

    (* close: unit -> (unit,'t)m; for explicit freeing of resources? *)
  }

end



module Dir = struct
  type ('k,'v,'t,'did) dir_ops = {
    find       : 'k -> ('v option,'t) m;
    insert     : 'k -> 'v -> (unit,'t) m;
    delete     : 'k -> (unit,'t) m;

    opendir    : unit -> (('k,'v,'t)Ls.ops,'t)m;
    set_parent : 'did -> (unit,'t)m;
    get_parent : unit -> ('did,'t)m;
    set_times  : times -> (unit,'t)m;
    get_times  : unit -> (times,'t)m;    

    flush      : unit -> (unit,'t)m;
    sync       : unit -> (unit,'t)m;    
  }
end
open Dir
  
type ('k,'v,'t,'did) lower_ops = {
  get_meta  : unit -> ('did * times,'t)m;
  (** returns parent and times *)

  find      : 'k -> ('v option,'t) m;

  exec      : ('k,'v,'t,'did) exec_ops -> (unit,'t)m;

  opendir   : unit -> (('k,'v,'t)Ls.ops,'t)m;

  (* sync : unit -> (unit,'t)m; NOTE we assume the lower layer already
     handles these synchronously *)
}

(** Like Make_v1, but assume that the lower layer supports an
   exec(ops) operation, rather than individual operations, since we
   typically flush batches of operations *)
module Make_v2(S:sig
    type t
    val monad_ops: t monad_ops

    type did

    type k (* str_256 *)
    (* val compare_k : k -> k -> int *)

    type v (* dir entry *)
    type cache
    val cache_ops : (k,v cache_entry,cache) cache_ops    

    val lower_ops : (k,v,t,did) lower_ops

    val with_cache : ((did,cache) d_cache, t) with_state
  end) = struct
  open S

  let ( >>= ) = monad_ops.bind
  let return = monad_ops.return

  let get_times () = 
    with_cache.with_state (fun ~state ~set_state:_ -> 
        state.times |> fst |> return)

  let set_times times = 
    with_cache.with_state (fun ~state ~set_state -> 
        set_state {state with times=(times,Flag.dirty) })


  let get_parent () = 
    with_cache.with_state (fun ~state ~set_state:_ -> 
        state.parent |> fst |> return)

  let set_parent parent = 
    with_cache.with_state (fun ~state ~set_state -> 
        set_state {state with parent=(parent,Flag.dirty) })

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

  let lower_insert_sync (xs: (k * v cache_entry) list) = 
    xs |> List.map (fun (k,v) -> 
        match (v:_ cache_entry) with
        | Some_ v -> Op.Insert (k,v) 
        | Deleted -> Op.Delete k
        | Not_present ->
            (* These entries are always clean, so can't be returned by a trim *)
          assert(false)) 
    |> fun xs -> 
    (Op.With_sync,xs) |> 
    lower_ops.exec

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

  let flush () = 
    with_cache.with_state (fun ~state ~set_state ->         
        let clean x = (fst x,Flag.clean) in
        (* parent and times first *)
        let parent = state.parent in
        let ops1 = 
          match parent|>snd with
          | x when x=Flag.clean -> []
          | x when x=Flag.dirty -> [Op.Set_parent (parent|>fst)]
          | _ -> failwith "impossible"
        in
        let times = state.times in
        let ops2 = 
          match times |> snd with
          | x when x=Flag.clean -> []
          | x when x=Flag.dirty -> [Op.Set_times (times|>fst)]
          | _ -> failwith "impossible"
        in
        let cache = state.cache in
        cache_ops.clean cache |> fun (xs,cache) -> 
        let ops3 = 
          xs |> List.map (function
              | (k,Some_ v) -> Op.Insert(k,v)
              | (k,Deleted) -> Op.Delete(k)
              | (_,Not_present) -> 
                failwith "impossible: cache_ops.clean should not return Not_present")
        in
        lower_ops.exec (Op.With_sync,ops1@ops2@ops3) >>= fun () -> 
        set_state {parent=clean state.parent; times=clean state.times; cache})

  let sync () = flush () >>= fun () -> 
    (* no need for lower_ops.sync, since we use With_sync in the impl above *)
    return ()

      

  (**

The ls operation ("leaf stream") is used to implement readdir. We have
a lower uncached implementation, the cache contents, and we need to
implement the cached version.

Cache entries are: Some _ | Deleted | Not_present

These entries can be dirty or clean (Not_present is always clean).

A stupid implementation would just flush to lower, and then use the
lower operations (since the cache is now clean).

  let ls_create () = 
    flush () >>= fun () -> 
    lower_ops.ls_create ()

This is clearly suboptimal, since we shouldn't HAVE to flush to lower,
and this operation is potentially costly.

How to implement the cached version of ls? We can ignore
Not_present. For deleted cache entries, we must make sure to filter
the relevant (k,v) entries from the lower ls.

What to do with new Some entries? There are two obvious
implementations:

1) Return all new entries in the first call to ls_kvs(); ls_step()
   then reverts to the (filtered) entries from lower
2) Attempt to return new entries as we iterate over lower entries via
   lower.ls_step()

Option 1 is ugly because it reveals the cache contents, and
potentially destroys whatever order the lower layer may provide (eg
lexicographic on names... not that POSIX requires such an order of
course).

Option 2 is somewhat complicated. The issue is where to put new
entries that might appear in one of two possible chunks. For example,
suppose the first chunk contains keys [1,2,3] and the next (after
ls_step()) contains [5,6,7]. Where should we put an entry with key 4?
To include in the first chunk we need to know that the next chunk does
not contain a key 4, which requires reading the next chunk in advance,
which is a bit ugly. To include in the second chunk is possible, and
avoids the reading ahead. Potentially this requires an extra chunk at
the end.

An alternative, if we knew the bounds for a particular chunk (k1 <=
... < k2), would be straightforward - we just include the new entry in
the correct chunk.

Ideally we would alter the ls operations to return the bound, and
implement the straightforward approach. Rather than go back and alter
the ls interface (again!) we hack something together here based on
option 1.
*)

  let opendir () =     
    with_cache.with_state (fun ~state ~set_state:_ -> 
        return state.cache) >>= fun cache -> 
    let bs = cache_ops.bindings cache in
    let bs = bs |> List.filter_map (fun (k,v) -> 
        (* do we care about the difference between clean and dirty
           here? *)
        match snd v with
        (* remove clean entries from consideration *)
        | x when x=Flag.clean -> None
        | _ -> 
          (* for dirty entries *)
          match fst v with
          | Some_ v -> Some (k,`Some v)
          | Deleted -> Some (k,`Deleted)
          | Not_present -> failwith "impossible: Not_present entries are always clean")
    in
    let bs : (k * [`Some of v | `Deleted]) list = bs in
    let new_,deleted = 
      List.partition_map ~char:(function (_,`Some _) -> true | (_,`Deleted) -> false)
        ~f:(function (k,`Some x) -> k,x | _ -> failwith "impossible")
        ~g:(function (k,`Deleted) -> k | _ -> failwith "impossible")
        bs
    in
    (* we need to quickly determine whether an element is in deleted *)
    let deleted = List.mem_via_hashtbl ~elts:deleted () in
    (* we track two chunks *)
    lower_ops.opendir () >>= fun ls -> 
    (* NOTE not ref transparent *)
    let stage = ref 1 in
    let kvs () = 
      match !stage with
      | 1 -> return new_
      | _ -> 
        ls.kvs () >>= fun kvs -> 
        kvs |> List.filter (fun (k,_) -> not (deleted k)) |> return
    in
    let step () =
      match !stage with
      | 1 -> begin
          incr(stage);
          return ()
        end
      | _ -> ls.step ()
    in
    let is_finished () = if !stage > 0 then ls.is_finished () else return false in
    (* let close () = ls.close () in *)
    return Ls.{
      kvs;
      step;
      is_finished;
      (* close; *)
    }

  let dir_ops = { 
    get_times; 
    set_times; 
    get_parent; 
    set_parent; 
    find; insert; delete; 
    opendir; 
    flush; sync 
  }
  
end

