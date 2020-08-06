(** Implement a concurrent-safe freelist using the persistent list.

There are two versions (controlled by a "version" value in the input
   sig). The first is for arbitrary elements. The second is for
   blk_ids (ie, a standard freelist).

This module implements the operations without consideration for the origin. At the end of this file, we wrap the operations with a function that updates the origin

*)

open Plist_intf
open Freelist_intf

(** Input sig *)
module type S = sig
  type blk_id
  type blk
  type buf

  type elt (** elements stored in the free list *)

  type t

  val monad_ops     : (t monad_ops)
  val event_ops     : t event_ops
  val async         : ((unit -> (unit, t) m) -> (unit, t) m) 
  val sync          : (unit -> (unit,t)m)
  val plist_ops     : (elt, buf, blk_id, t) plist_ops 
  val with_freelist : (elt freelist_im, t) with_state 
  val min_free_alloc: elt -> int -> elt list * elt
  val version       : (elt,blk_id,t)version 
  val params        : Freelist_intf.params
end

(** Result sig for Make_v2 *)
module type T = sig
  module S : S
  open S      
  val freelist_ops : (elt,blk_id,t) freelist_ops
end



(** Make with full sig *)
module Make_v1(S:S) = struct

  module S = S
  open S

  type nonrec version = (elt,blk_id,t)version

  (* try to keep the (tr)ansient list size between these two *)
  let tr_upper = params#tr_upper
  let tr_lower = params#tr_lower
  let min_free_alloc_size = params#min_free_alloc_size


  let ( >>= ) = monad_ops.bind 

  let return = monad_ops.return

  let { ev_create; ev_wait; ev_signal } = event_ops 

  let Plist_ops.{add; _ } = plist_ops 

  (* redefine add to use alloc if available *)
  let version = match version with
    | For_blkids {e2b;b2e} -> `For_blkids(e2b,b2e)
    | For_arbitrary_elts {alloc;free} -> 
      let add elt = 
        alloc () >>= fun blk_id ->
        add ~nxt:blk_id ~elt >>= function 
        | None -> return ()
        | Some blk_id -> free blk_id
      in
      `For_arbitrary_elts (alloc,free,add)

  let _ = version

  (** The disk_thread asynchronously attempts to add elements to the
     transient list by reading from the head of the on-disk list and
     returning new elts. *)

  (** The disk_thread gets triggered when we fall below tr_lower bound. 
      The disk_thread then repeatedly goes to disk and uses the
      resulting frees to satisfy the waiting threads; the disk_thread
      only finishes doing this if the number of transients AFTER
      satisfying the waiting threads is at least tr_lower.
      
      $(CONSIDER()) we may need to throttle if the number of waiters keeps
      growing, or at least never shrinks to 0; perhaps the disk thread
      exponentially increases the number of blocks it scans each time

      NOTE that when we advance the hd, the old head becomes free
      
      We need to:
      - (1) read elts from hd, and advance hd
      - (2) sync the change to hd to the origin block
      - (3) (arbitrary elts) free old head or (freelist) add hd to list of free elts
      - (4) for each waiting thread, satisfy the alloc request with
        one of the new free elts; if we run out of elts, we need to go
        back to disk (so we need to loop until all waiting threads
        have been satisfied)      
  *)
  let disk_thread () = 
    let step () = 
      (* (1) ---------- *)
      plist_ops.adv_hd () >>= function
      | Error () -> return `End_of_plist 
      | Ok {old_hd; old_elts; _ } -> (
          (* (2) ---------- *) 
          (* $(FIXME("""shouldn't this be sync hd? origin changed""")) *)
          plist_ops.sync_tl () >>= fun () ->
          
          (* (3) ---------- *)
          (match version with
           | `For_blkids (_e2b,b2e) -> return (Some(b2e old_hd))
           | `For_arbitrary_elts (_alloc,free,_add) -> 
             free old_hd >>= fun () -> return None) >>= fun e_opt ->

          let elts = match e_opt with None -> old_elts | Some e -> e::old_elts in

          (* (4) ---------- *)
          with_freelist.with_state (fun ~state:s ~set_state ->
              assert(s.disk_thread_active);
              (elts,s.waiting) |> iter_k (fun ~k (elts,waiting) -> 
                  match elts,waiting with
                  | [],_ -> return {s with waiting}
                  | _,[] -> return {s with waiting=[]; transient=elts@s.transient}
                  | e::elts,w::waiting -> (
                      ev_signal w e >>= fun () -> 
                      k (elts,waiting)))
              >>= fun s -> 
              let continue_ = s.waiting!=[] || List.length s.transient < tr_lower in
              (* NOTE if we know we are not going to continue, we
                 take this opportunity to unset the
                 disk_thread_active flag; we have to do this while
                 we have the state *)
              Printf.printf "%s: %d\n%!" __FILE__ __LINE__;
              set_state { s with disk_thread_active=(not continue_) } >>= fun () ->
              return (`Ok continue_)) )
    in
    let rec loop () = step () >>= function 
      | `Ok true -> loop ()
      | `Ok false -> return `Finished
      | `End_of_plist -> 
        (* at this point, we have exhausted the frees stored on disk *)
        return `Unfinished
    in
    loop ()

  
  (** The [alloc] function. We usually satisfy alloc requests from the
     in-memory list of transient frees. If this becomes low, we go to
     disk.

      Steps involved: 
      - (1) Check if we need to go to disk
      - (2) Maybe launch an asynchronous disk thread if we are getting
        low on transient frees
      - (2.1) If there are no more frees on disk, we can finally
        resort to using the "min_free_elt" if there is one.
      - (2.2) We add "min_free_alloc_size"+|waiting| elts to the
        transient elts
      - (2.3) And satisfy any waiting requests
      - (3) Finally, try to get a free element; if there are no
        transient frees, we add ourselves to the wait list 
      
      $(FIXME("not clear that our request will ever be satisfied?"))

  *)
  let alloc () =
    (* (1) ---------- *)
    with_freelist.with_state (fun ~state:s ~set_state -> 
        match List.length s.transient < tr_lower && not s.disk_thread_active with
        | false -> return false
        | true -> 
          (* $(FIXME("""we must ensure that the freelist state is
             protected eg with a mutex""")) *)
          assert(not s.disk_thread_active);
          Printf.printf "%s: %d\n%!" __FILE__ __LINE__;
          set_state {s with disk_thread_active=true} >>= fun () -> return true)

    (* (2) ---------- *)
    >>= (function
        | false -> return ()
        | true -> 
          let t () = 
            disk_thread () >>= fun x -> begin
              match x with 
              | `Finished -> return ()
              | `Unfinished -> 
                (* (2.1) ---------- *)
                Printf.printf "%s: Disk thread did not allocate; \
                               attempting to use min_free\n%!" __FILE__;
                with_freelist.with_state (fun ~state ~set_state -> 
                    match state.min_free with
                    | None -> 
                      (* $(FIXME("Need to quit gracefully if no frees")) *)
                      failwith "At this point there are no free \
                                elements and no min_free"
                    | Some elt -> 
                      (* (2.2) ---------- *)
                      let num_to_alloc = min_free_alloc_size+List.length state.waiting in
                      Printf.printf "%s: num_to_alloc=%d\n%!" __FILE__ num_to_alloc;
                      min_free_alloc elt num_to_alloc |> fun (elts,elt) -> 
                      (* $(FIXME("""As well as set_state, we should
                         record the new min_free elt on disk
                         synchronously otherwise there is a danger
                         that we crash and then reallocate some elts
                         that have already been allocated""")) *)
                      Printf.printf "%s: post disk: adding new \
                                     transients from min_free\n%!" __FILE__; 
                      (* (2.3) ---------- *)
                      (* remember to wake up any waiting; also
                         remember to use state.transient first (which
                         may be nonempty) *)
                      (state.transient@elts,state.waiting) |> iter_k (fun ~k (elts,waiting) ->
                          match elts,waiting with
                          | _,[] -> return (elts,[])
                          | [],_ -> 
                            (* $(FIXME("""handle this case""")) *)
                            failwith "we need to go back and allocate even more! FIXME"
                          | e::elts,w::waiting -> 
                            ev_signal w e >>= fun () ->
                            k (elts,waiting)) >>= fun (elts,waiting) -> 
                      Printf.printf "%s: post disk_thread: setting state\n%!" __FILE__;
                      Printf.printf "%s: |transient| = %d\n%!" __FILE__ (List.length elts);
                      Printf.printf "%s: %d\n%!" __FILE__ __LINE__;
                      set_state { transient=elts; 
                                  (* FIXME transient=[]? no, just < tr_lower *)
                                  waiting;
                                  disk_thread_active=false;
                                  (* $(FIXME("""make it clearer why this
                                     has to be here - the disk_thread
                                     finished long ago""")) *)
                                  min_free=Some(elt) }
                  )
            end
          in
          async t) >>= fun () ->

    (* (3) ---------- *)
    with_freelist.with_state (fun ~state:s ~set_state -> 
        match s.transient with
        | [] -> (
            (* $(FIXME("""are we guaranteed that the disk thread is active?
               not necessarily? and so we may not ever get our request
               satisfied?""")) *)
            (* assert(s.disk_thread_active=true); *)
            (* we need to have an invariant: after the disk thread
               runs and frees all the waiting threads, then the
               remaining transient elts are at least tr_lower *)
            Printf.printf "%s: thread waits for transient free elt\n%!" __FILE__;
            ev_create () >>= fun (ev: elt event) ->
            Printf.printf "%s: %d\n%!" __FILE__ __LINE__;
            set_state {s with waiting=ev::s.waiting } >>= fun () -> 
            (* NOTE do we have to release the state before waiting?
               yes; and we assume that if an event is fulfilled before
               waiting on it, the wait succeeds ie events are one-shot *)
            return (`Ev ev))
        | elt::transient -> 
          Printf.printf "%s: %d\n%!" __FILE__ __LINE__;
          set_state {s with transient} >>= fun () ->
          return (`Elt elt)) >>= fun x ->

    (match x with
     | `Ev ev -> ev_wait ev
     | `Elt elt -> return elt)


  let alloc_many _n = failwith "FIXME" 

  (** Free a list of elements, by appending to the on-disk list.

      NOTE the following code is rather tricky; if we have an alloc
      provided, we use this to extend the list; otherwise we are
      working with elts as blk_ids, so we can use one of the elts as
      a backing block for the list itself when we need to extend the
      list 
      
      $(CONSIDER("""This is somewhat inefficient because we invoke plist.add
      multiple times (rather than appending a whole blk of blk_ids)"""))
  *)
  let free_elts xs =
    match version with
    | `For_arbitrary_elts (_,_,add) -> (
        xs |> iter_k (fun ~k xs ->
            match xs with 
            | [] -> return ()
            | x::xs -> 
              add x >>= fun () ->
              k xs))
    | `For_blkids (e2b,b2e) -> (
        xs |> iter_k (fun ~k xs -> 
            match xs with
            | [] -> return ()
            | [e] -> (
                plist_ops.add_if_room e >>= function
                | true -> return ()
                | false -> (
                    (* so use this e as the nxt blk *)
                    plist_ops.adv_tl (e2b e)))
            | x1::x2::xs -> (
                plist_ops.add ~nxt:(e2b x2) ~elt:x1 >>= function
                | None -> k xs
                | Some x2 -> k ((b2e x2)::xs)) ))

  (** Free a single elt. This simply adds to the list of transient
     elements. If the list is then too big, we split it and append half the elements to
     the on-disk list. *)
  let free blk_id = 
    with_freelist.with_state (fun ~state:s ~set_state -> 
        let transient = blk_id::s.transient in
        match List.length transient >= tr_upper with
        | true -> (
            (* flush some to disk *)
            transient |> List_.split_at ((tr_upper+tr_lower) / 2) |> fun (xs,ys) ->
            free_elts xs >>= fun () ->
            Printf.printf "%s: %d\n%!" __FILE__ __LINE__;
            set_state {s with transient=ys })                
        | false -> (
            Printf.printf "%s: %d\n%!" __FILE__ __LINE__;            
            set_state {s with transient}))


  (* $(FIXME("Need to implement the many versions of alloc and free")) *)
  let free_many _pl = failwith "FIXME" 

  (* $(FIXME("""this doesn't sync the origin, but perhaps it
     should... or is this handled by autosync?""")) *)
  let sync () = 
    plist_ops.sync_tl () >>= fun () ->
    sync ()

  let get_origin () = 
    plist_ops.get_origin () >>= fun pl ->
    with_freelist.with_state (fun ~state ~set_state:_ ->
        return state.min_free) >>= fun min_free -> 
    let Pl_origin.{hd;tl;blk_len} = pl in
    return Fl_origin.{hd;tl;blk_len;min_free}
                                    

  let freelist_ops = 
    { alloc; alloc_many; free; free_many; sync; get_origin }

end


(** Make with restricted sig *)
module Make_v2(S:S) : (T with module S = S) = Make_v1(S)


(** Make without functor *)
let make (type blk_id blk buf elt t) x : (elt,blk_id,t)freelist_ops = 
  let module S = struct
    type nonrec blk_id = blk_id
    type nonrec blk = blk
    type nonrec buf = buf
    type nonrec elt = elt
    type nonrec t = t
    let monad_ops=x#monad_ops
    let event_ops=x#event_ops
    let async=x#async
    (* let blk_dev_ops=x#blk_dev_ops *)
    (* let barrier=x#barrier *)
    let sync=x#sync
    let plist_ops=x#plist_ops
    let with_freelist=x#with_freelist
    (* let read_origin=x#read_origin *)
    (* let write_origin=x#write_origin *)
    (* let origin=x#origin *)
    let version=x#version
    let params=x#params
    let min_free_alloc=x#min_free_alloc
  end
  in
  let module M = Make_v2(S) in
  M.freelist_ops


let _ = make

(* $(FIXME("""need to think a bit more about how the freelist is supposed
   to take care of the syncs (and similarly for plist)""")) *)
let wrap_with_origin_updates ~monad_ops ~(freelist_ops:_ freelist_ops) ~write_origin =
  let ( >>=) = monad_ops.bind in 
  (* let return = monad_ops.return in *)
  let { sync; _ } = freelist_ops in
  let sync () = sync () >>= fun () ->
    freelist_ops.get_origin () >>= fun fl -> 
    write_origin ~fl_origin:fl
  in
  {freelist_ops with sync}

