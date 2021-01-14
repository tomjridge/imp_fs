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

  (* quick membership *)
  let mem_via_set (type k) ~compare_k ~elts () =
    let module K = struct
      type t = k
      let compare = compare_k
    end
    in
    let module S = Set.Make(K) in
    let set = S.of_list elts in
    fun k -> S.mem k set

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

type ('did,'cache) d_cache = {
  times: times maybe_dirty;
  parent: 'did maybe_dirty;
  cache: 'cache; (* cache of entries *)
}

type 'a cache_entry = Some_ of 'a | Deleted | Not_present

type ('k,'v,'cache) cache_ops = ('k,'v,'cache) wbc_ops


module Make_v1(S:sig
    type t
    val monad_ops: t monad_ops

    type did

    type k (* str_256 *)
    val compare_k : k -> k -> int

    type v (* dir entry *)
    type cache
    val cache_ops : (k,v cache_entry,cache) cache_ops    

    val lower_ops : (k,v,t,did) dir_ops

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

  let lower_insert xs = 
    xs |> iter_k (fun ~k:kont xs -> 
        match xs with 
        | [] -> return ()
        | (k,v)::xs -> 
          match (v:_ cache_entry) with
          | Some_ v -> 
            lower_ops.insert k v >>= fun () -> 
            kont xs
          | Deleted -> 
            lower_ops.delete k >>= fun () -> 
            kont xs
          | Not_present ->
            (* These entries are always clean, so can't be returned by a trim *)
            assert(false))


  let maybe_trim ~state ~set_state ~cache = 
      match cache_ops.needs_trim cache with
      | false -> 
        set_state {state with cache} >>= fun () -> 
        return ()
      | true -> 
        (* flush entries to lower layer *)
        cache_ops.trim cache |> fun (xs,cache) -> 
        lower_insert xs >>= fun () -> 
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
        let cache = state.cache in
        cache_ops.clean cache |> fun (xs,cache) -> 
        lower_insert xs >>= fun () -> 
        set_state {state with cache})

  let sync () = 
    flush () >>= fun () -> 
    lower_ops.sync ()    

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

  let ls_create () =     
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
    let deleted = List.mem_via_set ~compare_k ~elts:deleted () in
    (* we track two chunks *)
    lower_ops.ls_create () >>= fun ls -> 
    let stage = ref 1 in
    let ls_kvs () = 
      match !stage with
      | 1 -> new_
      | _ -> 
        ls#ls_kvs () |> fun kvs -> 
        kvs |> List.filter (fun (k,_) -> not (deleted k))        
    in 
    let ls_step () =
      match !stage with
      | 1 -> begin
          incr(stage);
          return Tjr_btree.Btree_intf.{finished=false}
        end
      | _ -> ls#ls_step ()
    in
    return @@ object
      method ls_kvs=ls_kvs
      method ls_step=ls_step
    end


  let dir_ops = { 
    get_times; 
    set_times; 
    get_parent; 
    set_parent; 
    find; insert; delete; 
    ls_create; 
    flush; sync 
  }
  
end
    

(** {2 Common instance} *)

(** We don't know the type of directory entries yet, so parameterize over the type *)
module With_lwt(S:sig 
    type did
    type dir_entry
end) = struct
  (* open Shared_ctxt *)

  (* $(FIXME("""functional cache - probably prefer imperative lru2gen for real impl""")) *)

  module K = struct type t = str_256 let compare = Str_256.compare end
  module V = struct type t = S.dir_entry cache_entry end
  module Wbc = Write_back_cache.Make(K)(V)
  include Wbc

  (* $(CONFIG("dv3 wbc_params")) *)
  let wbc_params = object method cap=100 method delta=10 end
  let cap,delta = wbc_params#cap, wbc_params#delta
  let wbc_ops = wbc_factory#make_wbc ~cap ~delta
      
  type cache = Wbc.wbc
  let cache_ops = wbc_ops#ops

  let empty_cache = wbc_ops#empty

  let make_dir_cache ~times ~(parent:S.did maybe_dirty) : _ d_cache = 
    { times; parent; cache=empty_cache } 

  let dir_factory = 
    object
      method make_dir_cache = make_dir_cache
      method with_ ~lower_ops ~with_cache = 
        let module A = struct
          module S1 = struct
            include Shared_ctxt
            include S
            type k = str_256
            let compare_k = Str_256.compare
            type v = S.dir_entry 
            type nonrec cache = cache
            let cache_ops = cache_ops
            let lower_ops = lower_ops
            let with_cache = with_cache
          end
          module V1 = Make_v1(S1)
          include V1
        end
        in
        A.dir_ops
    end

  type dir_factory = < 
    make_dir_cache : 
      times  : times maybe_dirty ->
      parent : S.did maybe_dirty -> 
      (S.did, cache) d_cache;
    with_ : 
      lower_ops  : (str_256, S.dir_entry, lwt, S.did) dir_ops ->
      with_cache : ((S.did, cache) d_cache, lwt) with_state ->
      (str_256, S.dir_entry, lwt, S.did) dir_ops 
  >

  let dir_factory : dir_factory = dir_factory

end
