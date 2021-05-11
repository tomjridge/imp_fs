(** An LRU with a slow lower layer, and batch eviction.    

Don't open - record field clashes, type clashes

 *)
open Tjr_monad


(** {2 Interfaces} *)

type ('k,'v) op = [
  | `Insert of 'k*'v
  | `Delete of 'k
]    

type 'v op' = [
  | `Insert of 'v
  | `Delete
]

(** Lowest level (bottom) operations, typically those supported by a
   persistent on-disk log. *)
module Bot = struct 
  type ('k,'v,'bot,'t) ops = {
    find_opt   : 'bot -> 'k -> ('v option, 't) m;
    (* insert     : 'bot -> 'k -> 'v -> (unit, 't) m; *)
    (* delete     : 'bot -> 'k -> (unit, 't) m; *)
    sync       : 'bot -> (unit,'t) m;
    exec       : 'bot -> sync:bool -> ('k,'v)op list -> (unit,'t)m;
  }
  (** We hope that the operations are executed linearly; it should be
     the case that operations (apart from find_opt) on the bottom
     level always need the "is_syncing" lock held. In this case, exec
     should presumably always be sync? Or do we allow some buffering
     here as well, in the case that sync flag is not set? And does an
     exec force other outstanding operations to sync, or just those in
     the list given? Or is the semantics that sync is called
     immediately afterwards, so everything is synced? Yes, this
     semantics seems simpler. FIXME rename exec to exec_sync and
     remove sync flag *)
end


(** Top-level operations. *)
module Top = struct

  type ('k,'v,'c,'bot,'t) ops = {
    initial_cache_state : 
      cap:int -> trim_delta:int -> bot:'bot -> bot_ops:('k,'v,'bot,'t)Bot.ops -> 'c;
    find_opt     : 'c -> 'k -> ('v option, 't) m;
    insert       : 'c -> 'k -> 'v -> (unit, 't) m;
    delete       : 'c -> 'k -> (unit, 't) m;
    unsafe_clean : 'c -> ('k,'v)op list;
    (** unsafe_clean removes all entries and returns them as a list;
       this assumes the caller has control over concurrency (ie that
       no-one is using the cache, and the cache is not syncing), and
       will sync to the lower layer themselves; it is used to avoid
       multiple syncs to the lower level when syncing eg a dir's dirty
       entries and a dir's meta, see v3_level2.ml *)


    sync       : 'c -> (unit,'t) m;
    (* sync_k    'c -> 'k -> (unit,'t)m; *)
    (* sync_ks   'c -> 'k list -> (unit,'t)m; *)
    exec       : 'c -> sync:bool -> ('k,'v)op list -> (unit,'t)m;

    remove_clean_entry : 'c -> 'k -> unit;    
    (** NOTE this assumes that the cache is locked, not syncing, and
       no other operations are pending *)
  }
  (* FIXME we may very well want to be able to sync a subset of keys *)
end


(** {2 Implementation} *)

type 'v entry' = 'v option * bool ref

type ('v,'t) entry = 
  | Plain of 'v entry' (** the ref is "dirty" flag *)
  | Finding of { time:int; promise: ('v option,'t)m }
  (** We use the time to distinguish between concurrent finds that
     return out of order (FIXME no longer used) *)

(* let is_dirty (e:_ entry') = !(snd e) *)

let lower_none () = (None, ref false)
let lower_some v = (Some v, ref false)
let insert v = (Some v, ref true)
let delete () = (None, ref true)


type ('k,'v,'lru,'bot,'t) cache_state = {
  bot: 'bot;
  bot_ops: ('k,'v,'bot,'t) Bot.ops;

  mutable is_syncing: bool;

  mutable syncing: (unit,'t)m;
  (** If we are syncing, then we service operations we can
     immediately, and other operations wait for the sync. This field
     is only meaningful if is_syncing is true. *)

  lru: 'lru; 
  (** Lru state is mutable *)

  trim_delta: int; 
  (** Number of entries we trim each time the cache gets too big;
     trim_delta should be much less than cap *)

  lru_capacity: int; (** LRU capacity *)
}


module type S = sig
  type t = lwt

  type k
  type v
  type lru
  val lru_ops : (k,(v,t)entry,lru) Tjr_lru.Mutable.lru_ops

  type bot
end

module type T = sig
  type t = lwt
  type k
  type v
  type lru
  type bot
  type cache_state'
  val ops: (k,v,cache_state',bot,t)Top.ops
  val to_cache_state: cache_state' -> (k,v,lru,bot,t)cache_state
  val of_cache_state: (k,v,lru,bot,t)cache_state -> cache_state'
end

(** Functor make *)
module Make(S:S) 
  : T with type k = S.k and type v = S.v and type lru = S.lru and type bot = S.bot 
= struct
  include S
  module Lru = (val lru_ops)
  type cache_state' = (k,v,lru,bot,t)cache_state
  let to_cache_state = fun x -> x
  let of_cache_state = fun x -> x

  open Tjr_monad.With_lwt

  (** Following generates new ids *)
  let get_time = 
    let clock = ref 0 in
    fun () -> 
      incr clock; 
      !clock


  let initial_cache_state ~cap ~trim_delta ~bot ~bot_ops = 
    assert(trim_delta > 0);
    assert(cap > 0);
    assert(trim_delta <= cap);
    {
      bot; 
      bot_ops;
      is_syncing   = false;
      syncing      = return ();
      lru          =  Lru.create cap;
      trim_delta;
      lru_capacity = cap;
    }

  exception Exit_early

  (** This is called when the capacity may potentially be exceeded; in
      this case, we may need to trim some entries by flushing them to
      the bottom layer. Finding entries are locked in the cache, and so
      potentially we won't be able to free any entries. *)
  let maybe_trim c = 
    let cap = c.lru_capacity in
    let sz = Lru.size c.lru in
    let no_trim = c.is_syncing || sz <= cap in (* sync automatically trims *)
    match no_trim with 
    | true -> return () 
    | false -> 
      (* we want to issue a call to the lower layer, and prevent any
         concurrent operation overtaking this one - effectively we
         want to serialize calls to the lower layer; the point is that
         just by calling the lower layer, we don't guarantee that the
         promise resolves immediately - it may be delayed and some
         other operation occurs in the meantime, which may use a newer
         kv, leading to the newer kv being overwritten by the older;
      *)
      (* NOTE: we might want to drop "finding" entries, but this
         introduces a lot of complications *)
      let to_trim = ref [] in
      let n_removed = ref 0 in (* number of removed entries *)
      let n_finding = ref 0 in (* number of finding entries *)
      let _ = 
        (* NOTE we modify the lru state as we iterate over it; this is
           safe with the current mutable lru implementation I believe
        *)
        try 
          c.lru |> Lru.iter (fun k v -> 
              match !n_removed >= c.trim_delta with
              | true -> raise Exit_early
              | false -> 
                let remove () = 
                  Lru.remove k c.lru;
                  incr n_removed
                in
                match v with 
                | Plain (Some v,dirty) -> (
                    match !dirty with
                    | true -> (
                        to_trim := `Insert(k,v)::!to_trim;
                        remove ())
                    | false -> remove())
                | Plain (None,dirty) -> (
                    match !dirty with
                    | true -> (
                        to_trim := `Delete k::!to_trim;
                        remove ())
                    | false -> remove())
                | Finding _ -> (incr n_finding)
                (* we do not drop a finding entry *))
        with Exit_early -> ()
      in
      (if !n_removed < c.trim_delta then 
         Printf.printf 
           "WARNING! Trimmed %d entries, but this is less than %d \
            (delta we aim to trim). Possibly too many finding entries? \
            (There are %d finding entries.)\n"
           (!n_removed)
           c.trim_delta
           (!n_finding)
      );
      (if Lru.size c.lru > c.lru_capacity then 
         Printf.printf 
           "WARNING! The LRU size %d exceeds the capacity %d and we \
            were unable to remove more entries\n" 
           (Lru.size c.lru) c.lru_capacity); 
      (* FIXME what is the reasonable thing to do in this situation?
         maybe timeout??? or block and wait for finds to complete? Or
         maybe mark cache as full, so that no new entries get
         added... possibly this is simplest *)
      (* we expect that sync should be true here - no point delaying
         writing to disk *)
      let p = c.bot_ops.exec c.bot ~sync:true !to_trim in
      c.is_syncing <- true;
      p >>= fun () -> 
      c.is_syncing <- false;
      return ()

  let unsafe_clean c =
    assert(not c.is_syncing);
    let dirties = ref [] in
    c.lru |> Lru.iter (fun k v -> 
        match v with 
        | Finding _ -> ()
        | Plain (v,dirty) -> 
          match !dirty with 
          | true -> (
              dirty:=false;
              let op = match v with
                | None -> `Delete k
                | Some v -> `Insert(k,v)
              in
              dirties:=op::!dirties)
          | false -> ());
    (* take the opportunity to trim some entries *)
    Lru.trim c.lru;
    !dirties

  let rec sync c = 
    match c.is_syncing with 
    | true -> 
      c.syncing >>= fun () -> 
      sync c (* try again, since we may have updated entries whilst syncing *)
    | false -> 
      (* construct promise *)
      let p () = 
        let dirties = unsafe_clean c in
        (* now process dirties as a batch *)
        c.bot_ops.exec c.bot ~sync:true dirties
      in
      c.is_syncing <- true;      
      c.syncing <- p ();
      c.syncing >>= fun () ->
      c.is_syncing <- false;
      return () 

  (* perform updates on lru; then maybe call sync, or else maybe_trim *)
  let exec c ~sync:sync' ops = 
    (* FIXME what is c.is_syncing? it's fine - we may call sync which
       will wait till current sync completes *)
    begin
      ops |> List.iter (function
          | `Insert (k,v) -> Lru.add k (Plain (insert v)) c.lru
          | `Delete k -> Lru.add k (Plain (delete ())) c.lru)
    end;
    match sync' with
    | false -> maybe_trim c
    | true -> sync c

  let rec find_opt c k = 
    (* try to find in cache *)
    Lru.find k c.lru |> function
    | None -> begin
        (* not in cache, so check whether we are syncing *)
        match c.is_syncing with
        | true -> 
          c.syncing >>= fun () -> 
          find_opt c k (* try again after sync completes *)
        | false -> 
          (* create the promise *)
          let now = get_time () in
          let promise = 
            c.bot_ops.find_opt c.bot k >>= fun v -> 
            (* CAREFUL! In the meantime the cache entry may have been
               updated, either by another find on the same key (which
               should result in the same value if no intervening
               updates) or by the user calling insert/delete... but
               suppose the key was updated, flushed, then removed from
               cache... in this case there would be no entry in the
               cache, and it would then be updated with the old value
               *)
            begin
              match Lru.find k c.lru with 
              | Some (Finding {time; _}) -> (
                  assert(time=now); (* finding entries are locked *)
                  match v with 
                  | None -> (
                      Lru.add k (Plain (lower_none ())) c.lru;
                      maybe_trim c >>= fun () -> 
                      return v)
                  | Some v -> ( 
                      Lru.add k (Plain (lower_some v)) c.lru;
                      maybe_trim c >>= fun () -> 
                      return (Some v)))
              | Some _ | None -> return v
            end
          in
          Lru.add k (Finding {time=now;promise}) c.lru;
          promise
      end
    | Some (Plain (v,_)) -> return v
    | Some (Finding {promise;_}) -> promise

  let insert c k v = 
    (* NOTE this can replace a finding *)
    Lru.add k (Plain (insert v)) c.lru;
    maybe_trim c

  let delete c k = 
    Lru.add k (Plain (delete ())) c.lru;
    maybe_trim c

  (** NOTE this requires not c.is_syncing *)
  let remove_clean_entry c k =
    assert(not c.is_syncing);
    Lru.find k c.lru |> function
    | None -> ()
    | Some v -> 
      match v with
      | Plain (_,dirty) -> 
        assert(not !dirty);
        Lru.remove k c.lru;
        ()
      | Finding _ -> 
        (* FIXME convince myself that removing a finding when some
           outer lock is held is actually OK *)
        Lru.remove k c.lru;
        ()

  let ops = Top.{ 
      initial_cache_state; find_opt; insert; delete; unsafe_clean; sync; exec;
      remove_clean_entry }
              
  let _ : (k, v, cache_state', bot, t) Top.ops = ops
  
end

type ('k,'v,'bot) lru_module = (module T with type k = 'k and type v = 'v and type bot='bot)

(** make as a function.

    NOTE: This uses Stdlib pervasive equality and hashing on values of
   type k; this provides a simpler interface where the Lru
   functionality is constructed for the user *)
let make (type k v bot) () = 
  let open (struct
    type t = lwt

    (* construct lru ops using Tjr_lru.Mutable *)
    module S = struct
      type nonrec k = k
      type nonrec v = (v,t)entry
    end
    module Lru = Tjr_lru.Mutable.Make_with_pervasives(S)
    type lru = Lru.t
    let lru_ops = Lru.lru_ops

    (* now call Make *)
    module S2 = struct
      type nonrec t = lwt
      type nonrec k = k
      type nonrec v = v
      type nonrec lru = lru
      let lru_ops = lru_ops

      type nonrec bot = bot
    end        

    module M = Make(S2)
  end)
  in
  (module M : T with type k = k and type v = v and type bot=bot)

let _ : 
unit -> 
('k,'v,'bot)lru_module
 = make


module Test() = struct
  (* FIXME best way to test? *)
  open Tjr_monad.With_lwt

  module Map = Map.Make(struct type t = int let compare = Stdlib.compare end)

  type map = int Map.t

  module Model = struct
    (* There are various possible models, some more detailed than others. *)

    (* The bottom level is modelled just as a map; we don't
       distinguish between unsynced and synced entries *)

    type bot = map ref

    let find_opt=(fun m k -> Map.find_opt k (!m) |> return)
    let insert=(fun m k v -> m:= Map.add k v !m; return ())
    let delete=(fun m k -> m:=Map.remove k !m; return ())
    let sync=(fun _m -> return ())
    let exec=(fun m ~sync ops -> 
        assert(sync);
        ops |> List.iter (function
            | `Insert(k,v) -> m:= Map.add k v !m
            | `Delete k -> m:=Map.remove k !m);
        return ()
      )

    let bot_model : _ Bot.ops = { find_opt; (* insert; delete; *) sync; exec }

    (* The model of the overall system is just a map (we ignore sync for the time being) *)
    let sys_model : _ Bot.ops = { find_opt; (* insert; delete; *) sync; exec }
    
  end

  open Model[@@warning "-33"]

  module Map_set = Set.Make(struct type t = map let compare = Map.compare Stdlib.compare end)

  type map_set = Map_set.t

  (* done - states we have fully explored; todo - states we need to transition from *)
  type test_state = { done_: map_set; todo: map_set }

  (* We will not be checking for concurrent interference... we assume
     that if c.is_syncing is true, then things are fine; note that
     find_opt uses bind (ie possibly yields) regardless of is_syncing;
     also insert and delete update the cache regardless of is_syncing

     In fact, find_opt looks problematic/buggy. There may be multiple
     find's in operation, and they may be reordered, resulting in an
     old value of k being updated in the cache (even overwriting a
     very new update!). This happens because we allow finding entries
     to be dropped from the Lru. Perhaps we can get round this by only
     updating the cache if the entry is still None.

  *)

  module S = struct
    type t = lwt
    type k = int
    type v = int
    module Lru = Tjr_lru.Mutable.Make_with_pervasives(struct type k = int type v = int end)
    type lru = Lru.t
    let lru_ops = Lru.lru_ops
  end

end
