(** Live object cache.

A cache of "live" objects. Expunging an object may take some
   time. Resurrecting an object may take some time. We use reference
   counting to prevent objects from being expunged while references
   remain live.

We are maintaining a cache of objects by id. We can maintain the
   counter with the object. A kref is an abstract wrapper round the
   object. Essentially it provides: kref_to_obj: kref -> obj. It also
   includes put: kref -> unit which destroys the kref and reduces the
   underlying count.

Don't open.  *)

let dont_log = V3_intf.dont_log

let line s = Printf.printf "%s: Reached line %d\n%!" "V3_live_object_cache" s; true

(** User view of krefs; the user must call krelease when finished with
   the object. *)
type ('obj,'kref) kref_ops = {
  kref_to_obj : 'kref -> 'obj;
  krelease     : 'kref -> unit;
}

module Private_kref_impl = struct
  type 'a kref = {
    obj: 'a;
    mutable valid: bool; (** whether this kref is valid *)
    counter: int ref; (** backing counter *)
  }

  let kref_to_obj: 'a kref -> 'a = 
    fun x -> 
    assert(x.valid);
    x.obj
        
  let create ~counter obj = 
    let kref = {obj; valid=true; counter} in
    (* Add a finalizer to ensure that the obj is not GC'ed in valid state *)
    assert(
      kref |> Gc.finalise (fun x -> 
          if x.valid then Printf.printf "WARNING! kref object GC'ed but valid flag was set\n%!");
      true);
    kref

  let krelease kref = 
    kref.valid <- false;
    Stdlib.decr kref.counter

  let kref_ops = { kref_to_obj; krelease }
end
type 'a kref = 'a Private_kref_impl.kref
let kref_ops = Private_kref_impl.kref_ops

module Kref = Private_kref_impl


(** A cache of live objects is just an LRU of ref counted objects *)

type 'a counted = {
  count: int ref;
  obj: 'a
}

type ('a,'t) entry = [
  | `Resurrecting of (unit,'t)m
  | `Present of 'a counted
  | `Finalising of (unit,'t)m
]

type config = {
  cache_size: int;
  trim_delta: int;
}

type ('lru,'t) cache = {
  lru        : 'lru;
  config     : config;
  mutable gc_thread  : (unit,'t)m;
}

type ('id,'a,'kref,'cache,'t) cache_ops = {
  create      : config:config -> 'cache;
  get         : 'id -> 'cache -> ('kref,'t)m;
  put         : 'kref -> unit;
  kref_to_obj : 'kref -> 'a;
}

module type S = sig
  type t = lwt
  type id 
  type a
end

module type T = sig
  type t = lwt
  type id
  type a
  type kref
  type cache

  (** resurrect: slow operation to resurrect an object from a
     persistent store; finalise:called when removing objects from the
     cache; no outstanding references remain; as such, the object
     should certainly not be locked *)
  val make_cache_ops:
    resurrect : (id -> (a,t)m) -> 
    finalise  : ( (id*a) list -> (unit,t)m) -> 
    (id,a,kref,cache,t)cache_ops
end

module Make(S:S) : T with type id = S.id and type a = S.a = struct
  include S
  open Tjr_monad.With_lwt

  module Tmp = struct
    module S' = struct
      type k = id
      type v = (a,t)entry
    end
    module Lru = Tjr_lru.Mutable.Make_with_pervasives(S')    
  end
  type lru = Tmp.Lru.t
  let lru_ops : (id,(a,t)entry,lru) Tjr_lru.Mutable.lru_ops = Tmp.Lru.lru_ops

  module Lru = (val lru_ops)

  type nonrec kref = a Private_kref_impl.kref

  type nonrec cache = (lru,lwt)cache

  exception Exit_early

  let make_cache_ops ~resurrect ~finalise = 
    let open (struct

      (* FIXME we want this to run only when necessary, or at least, to
         run often if there is a lot of pressure, but not often if less
         pressure *)
      (** A thread that scans the Lru to remove entries that are no longer
          referenced *)
      let rec gc_thread cache = 
        let overflow = Lru.size cache.lru - cache.config.cache_size in
        match overflow > 0 with
        | false -> from_lwt (sleep 1.) >>= fun () -> gc_thread cache
        | true -> 
          let n_to_remove = overflow+cache.config.trim_delta in
          let to_trim = ref [] in
          let n_removed = ref 0 in (* number of removed entries *)
          (* NOTE we modify the lru state as we iterate over it; this is
               safe with the current mutable lru implementation I believe
          *)
          (* a promise to signal the end of finalising *)
          let (p,signal_p) = Lwt.wait () in
          begin try 
              cache.lru |> Lru.iter (fun k v -> 
                  match !n_removed >= n_to_remove with
                  | true -> raise Exit_early
                  | false -> 
                    match v with 
                    | `Finalising _ -> ()
                    | `Resurrecting _ -> ()
                    | `Present counted -> (
                        match !(counted.count) with 
                        | 0 -> (
                            (* can remove; so replace existing binding *)   
                            Lru.add k (`Finalising (from_lwt p)) cache.lru;
                            to_trim := (k,counted.obj) :: !to_trim;
                            incr n_removed;
                            ())
                        | _ -> ()))

            with Exit_early -> () end;
          begin
            (* Some warnings *)
            if !n_removed < n_to_remove then 
              Printf.printf 
                "WARNING! Trimmed %d entries, but this is less than %d \
                 (amount we aim to trim). Possibly too many live entries?\n%!"
                !n_removed
                n_to_remove;

            if Lru.size cache.lru > cache.config.cache_size then 
              Printf.printf 
                "WARNING! The LRU size %d exceeds the capacity %d and we \
                 were unable to remove more entries\n%!" 
                (Lru.size cache.lru) 
                cache.config.cache_size
                (* FIXME what is the reasonable thing to do in this
                   situation?  maybe timeout??? or block and wait for
                   finds to complete? Or maybe mark cache as full, so that
                   no new entries get added... possibly this is simplest
                *)
          end;
          finalise !to_trim >>= fun () -> 
          (* now remove the finalising entries from the map *)
          (List.map fst !to_trim) |> List.iter (fun k -> Lru.remove k cache.lru);
          Lwt.wakeup_later signal_p ();
          gc_thread cache


      let create ~config = 
        let lru = Lru.create config.cache_size in
        let cache = {lru; config; gc_thread=(return ()) } in
        let gc_thread = gc_thread cache in
        cache.gc_thread <- gc_thread;
        cache


      (* FIXME a bit worried about thrashing eg a thread finds a
         resurrecting entry, and whilst waiting the entry is resurrected
         then removed from cache, so the thread has to wait again *)
      let rec get id cache = 
        assert(dont_log || line __LINE__);
        match Lru.find id cache.lru with
        (* If resurrecting or finalising, wait and try again *)
        | Some(`Resurrecting p) | Some (`Finalising p) -> 
          assert(dont_log || line __LINE__);
          Lru.promote id cache.lru;
          (With_lwt.yield () |> from_lwt) >>= fun () -> 
          p >>= fun _ -> get id cache

        (* If in the cache, return a new kref *)
        | Some(`Present x) -> 
          assert(dont_log || line __LINE__);
          incr x.count;
          Lru.promote id cache.lru;
          return (Kref.create ~counter:x.count x.obj)

        (* If not in the cache, resurrect (and mark resurrecting), then
           place in cache and return kref *)
        | None -> 
          assert(dont_log || line __LINE__);
          (* NOTE nasty bug where p was unguarded, ran immediately,
             returned immediately, then had `Present overridden by
             `Resurrecting! *)
          let (p,r) = Lwt.wait () in
          Lru.add id (`Resurrecting (p|>from_lwt)) cache.lru;
          (* resurrect the object, and replace entry in the cache; then try again *)
          resurrect id >>= fun obj ->           
          Lru.add id (`Present {count=ref 0;obj}) cache.lru;
          assert(Lru.find id cache.lru |> function | Some (`Present _) -> true | _ -> false);
          assert(dont_log || line __LINE__);
          (Lwt.wakeup_later r ());
          assert(dont_log || line __LINE__);
          get id cache

      (* let _ : id -> (lru,lwt) cache -> (kref, lwt) m = get *)

      let put kref = kref_ops.krelease kref

      let kref_to_obj = kref_ops.kref_to_obj
      
      let cache_ops : (id,a,kref,cache,t)cache_ops = {create;get;put;kref_to_obj}
    end)
    in
    cache_ops

  let _ = make_cache_ops

end


