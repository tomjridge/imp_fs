(** This code takes care of the various resources that need to be
   correctly locked in a concurrent setting. *)

(* This is a rough implementation using imperative state. *)


open V1_util
open Shared_ctxt

(* thread id *)
type tid = { tid:int }

let compare_tid = Stdlib.compare

let new_tid =
  let x = ref 1 in
  fun () -> 
    let y = !x in
    incr x;
    return {tid=y}
    
type live_thread_ops = {
  add: tid -> unit;
  remove: tid -> unit;
  size: unit -> int;
  size_below_bound: unit -> bool;
}

let thread_max = 100

let live_threads = 
  let s = ref Set_int.empty in
  let size = fun () -> Set_int.cardinal !s in
  { add=(fun {tid} -> s:=Set_int.add tid !s);
    remove=(fun {tid} -> s:=Set_int.remove tid !s);
    size;
    size_below_bound=(fun () -> size() <= thread_max)
  }

(* object id; tids dealt with separately as above *)
type oid = Gom_id | Fid of int | Did of int

let compare_oid = Stdlib.compare

module Map_oid = Map.Make(struct type t = oid let compare = compare_oid end)

(** acquire and acquire_all require that the thread does not own any
   resources; this is so that we can guarantee that the resources are
   obtained in order so we avoid deadlock *)
type resource_ops = {
  owns_nothing : tid -> bool;
  acquire      : tid:tid -> oid:oid -> (unit,t)m;
  acquire_all  : tid:tid -> oids:oid list -> (unit,t)m;
  release      : tid:tid -> oid:oid -> (unit,t)m;
  release_all  : tid -> (unit,t)m;
}


(* Per-object data; typically kept in an LRU, with acquired
   objects exempt from trim; trimmed objects should also dispose of
   entries in the resource map (remove oid from the res map, assuming
   oid not held, which it shouldn't be). For the time being, we just
   hold in a map. *)

module Per_object_data = struct
  
  type file_cache
  type fid
  type did
  type sid

  type blk_id

  type lock = mutex

  (* how do we want to record which parts of the state need to be
     synced? with bools, or by comparing? *)

  type 'a maybe_dirty = 'a * bool

  (* perhaps every entry has a lock, in which case, move lock out *)
  type unlocked_entry = 
    | Gom of { btree_root:blk_id; cache:unit (* etc *) }
    | File of { 
        fid        : fid; 
        origin     : blk_id;
        times      : Times.times maybe_dirty; 
        fd         : Unix.file_descr
      }
    | Dir of { 
        did        : did; 
        origin     : blk_id; 
        parent     : did maybe_dirty; 
        btree_root : blk_id maybe_dirty; 
        times      : Times.times maybe_dirty;
        cache      : unit (* etc *)
      }  (* ops? *)
    | Symlink of { 
        sid        : sid; 
        origin     : blk_id;
        contents   : str_256 maybe_dirty; 
      }

  type entry = unlocked_entry * lock
    
  (* really we want to do this generically, with a "finalize" for
     trimming an old obj *)

  type t = {
    entries: entry Map_oid.t;
    super_lock: lock  
    (** this has to be held if a thread wants to take a particular
       object lock; this is so that we can pause all thread lock
       operations on objects, in order to GC without worrying about
       other threads trying to take locks *)
  }

  


end





let resource_ops = 
  let open (struct
    module S = struct
      type a = tid
      let compare_a = compare_tid
      type b = oid
      let compare_b = compare_oid
    end

    module Ownership_map = Ownership_map(S)
    module O = Ownership_map
  end)
  in
  let map = ref Ownership_map.empty in
  (* FIXME following should wait on the oid lock if already held *)
  (* FIXME we want to ensure that the oids are acquired in id
     order... perhaps we can only acquire if we don't have anylocks
     already? or is this too strong? *)
  let owns_nothing tid = O.owns_nothing ~tid !map in
  let acquire ~tid ~oid = 
    (* threads can only acquire resources if they don't have any already *)
    assert(owns_nothing tid);
    (* FIXME then the acquire should wait on each object lock in turn *)
    map := O.acquire ~tid ~oid !map; return () 
  in
  let acquire_all ~tid ~oids = 
    assert(owns_nothing tid);
    map := O.acquire_all ~tid ~oids !map; return () in
  let release ~tid ~oid = 
    assert(O.(owns ~tid ~oid !map));
    map := O.release ~tid ~oid !map; return () 
  in
  let release_all tid = map := 
      O.release_all ~tid !map; return () 
  in
  { owns_nothing; acquire; acquire_all; release; release_all}


(* for use by code outside the resource manager; does this mean we
   have to pass tid through everywhere? would prefer to provide
   specialized monad ops with a get_tid method? this is equivalent to
   parameterizing everything by a get_tid, ie tid; perhaps that is
   ok... we can instantiate the per-thread api via a functor taking
   the thread id *)
module A : sig
  type tid 
  val create_tid: unit -> (tid,t)m

  val destroy_tid: tid -> (unit,t)m
  (** tids can only be destroyed if all their resources have been released *)
end = struct
  type nonrec tid = tid
  let create_tid () = new_tid () >>= fun tid -> 
    live_threads.add tid |> fun () -> 
    return tid
  let destroy_tid tid =
    assert(resource_ops.owns_nothing tid);
    live_threads.remove tid;
    return ()
end


(* operations on resources, per thread *)
module B(S:sig val tid:tid end) = struct 
  open S
  let acquire oid = resource_ops.acquire ~tid ~oid
  let acquire_all oids = resource_ops.acquire_all ~tid ~oids
  let release oid = resource_ops.release ~tid ~oid
  let release_all () = resource_ops.release_all tid
end
