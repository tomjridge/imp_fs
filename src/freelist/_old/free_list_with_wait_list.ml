(** Free list

This supersedes the free list in fs_shared.

*)

(**

At the highest level, a free list could be just a set (finite or
   infinite) of blk_ids.

We probably want to avoid representing each blk_id, so we have a
   blk_id beyond which all blks are free.

Each object (file, directory...) has an associated list of blks in
   use. When a file is freed, we want to return this list to the free
   list in the most efficient way possible.



A free list contains the following:

- an (optional) open range of the form  \[n,infinity), indicating that
   blocks n upwards are free (there may additionally be a limit on the
   total number of blocks) - a list of free-blk-lists - or do we want
   to mutate the blks so we just have a huge list of free blks?  - a
   transient (in-memory) list of free blks that can be persisted with
   a sync operation


Normally we allocate from the transient list. If this is geting small,
   we spawn a thread to free up some more. If we run out of
   transients, then threads that are trying to allocate have to wait
   till transients becomes non-empty. Presumably we would like the
   waiting threads to be unblocked in the order that they were
   blocked. So just store a list of (blk_id event)s, and reverse and
   signal them in turn.

We might have the problem that in the time it takes to do this, we
   have so many waiting threads that we have to go back to the
   disk. So we need some back-pressure, to reduce the rate of requests
   to the free list.

For the time being, we parameterize by the number of blkids to free,
   and assume this is "big enough".



*)

type blk_id  = int (* a blk_id corresponding to a blk storing data of type 'a *)


(** Notion of a blk_id stored on disk: either a direct blk_id, or an "indirect" blk_id, i.e., a pointer to a blk which contains ... *)

type block = 
   | Blk1 of blks 
   | Blk2 of blks

and blks = { blks: blk_id list; prev: blk_id option }


(** In the freelist, a list formed from Blk1 blks is associated with
   each file. The freelist itself contains a list of Blk2s, where each
   pointed-to blk is the head of a blk1 list. *)


module Free_list = struct

(*
  (** A singly-linked on-disk list of blks; conceptually this is just a list (or set) of blk_ids *)
  type on_disk_blk = {
    blks         : blks;
    backing_blk  : blk_id;
  }
*)

  (* another type of on-disk blk *)
  type root_blk = {
    blk2_list : blk_id; (* blk_id of the head of the blk2 list *)
    free_from : blk_id option;
  }

  type 't free_list = {
    root_blk           : blk_id;  (* address of root blk *)
    blk2_list          : blk_id;
    free_from          : blk_id option;
    transient          : blk_id list;  (* not part of the on-disk freelist *)
    disk_thread        : blk_id Event.event option;
    (* if transient is empty we spawn an async thread to fiddle with
       the disk and free some blkids *)

    (* waiting threads: threads wait directly on disk_thread; if this
       contains any blocked threads then we expect the disk_thread to
       be active; since the whole state is protected by a mutex, we
       don't have to explicitly lock the wait list; this is presumably
       the "sheep pen" pattern, where threads accumulate waiting for
       some particular event to occur *)

  }

end
open Free_list



module Store = struct

  module M = Map.Make(struct type t = blk_id let compare: t -> t -> int = Pervasives.compare end)
  include M
  type store = block M.t
end  


module Example_store = struct
  (* store from blk 0 *)

  (* blk0 is the root of the freelist *)
  let blk0 = Blk2 { blks=[10;20]; prev=None; }
  let blk10 = Blk1 { blks=[11;12;13]; prev=None }
  let blk20 = Blk1 { blks=[21;23]; prev=None } 

  let store = 
    [(0,blk0);
     (10,blk10);
     (* 11, 12 and 13 are free *)
     (20,blk20)
     (* 21,23 are free *)
    ]
    |> List.to_seq
    |> Store.of_seq

  let dest_blk1 = function Blk1 blk1 -> blk1 | _ -> failwith "Expecting a Blk1"
  let dest_blk2 = function Blk2 blk2 -> blk2 | _ -> failwith "Expecting a Blk2"

  (* we return the explicitly free blkids, not including the blks that
     makeup the freelist itself *)
  let store_to_frees ~store ~root = 
    (* root is a ptr to a blk2 *)
    let fl : blk_id list ref = ref [] in
    let store_find id store = 
      fl:=id::!fl;
      Store.find id store
    in
    let acc : blk_id list list ref = ref [] in
    let rec f = function
      | Blk1{blks;prev} -> f1 {blks;prev}
      | Blk2{blks;prev} -> f2 {blks;prev}
    and f1 {blks;prev} = 
      acc:=blks::!acc;
      match prev with 
      | None -> ()
      | Some blk_id -> 
        store_find blk_id store |> dest_blk1 |> f1
    and f2 {blks;prev} = 
      (* blks contains ids which point to Blk1s *)
      blks |> List.iter (fun blk_id -> 
          store_find blk_id store |> dest_blk1 |> f1);
      (* and recurse on the prev *)
      match prev with
      | None -> ()
      | Some blk_id -> (
          store_find blk_id store |> dest_blk2 |> f2)
    in
    Store.find root store |> f;
    root::!fl,!acc

  let _ = store_to_frees ~store ~root:0
  (* the first component are the blks of the free list itself; the second are the data blocks *)
  (* - : blk_id list * blk_id list list = ([0; 20; 10], [[21; 23]; [11; 12; 13]]) *)
end


(** Now we need to implement the interfaces *)

(* blk_dev_ops type; these are assumed UNBUFFERED SYNCHRONOUS, ie a
   write really writes *)
type nonrec 't blk_dev_ops = (blk_id,block,'t) blk_dev_ops

type 't fl_ops = {
  alloc_acquire           : unit -> (blk_id,'t)m;
  free_release            : blk_id -> (unit,'t)m;
  release_list            : transients:blk_id list -> root:blk_id -> (unit,'t)m;  (* root points to a blk1 list *)
  sync_root               : unit -> (unit,'t)m;
  sync_root_and_transient : unit -> (unit,'t)m; 
}

(* In some sense, this is a custom version of the pcache (which is a
   in-mem/on-disk split datastructure). Is it simpler just to use the
   generic version, and log everything? But using this approach we can
   free up all blks used by a file in a single on-disk ptr
   operation. 

   Probably it is worth separating out the component that deals with the list of lists.
*)


[@@@warning "-26-27"] (* FIXME *)


let alloc_acquire ~monad_ops ~event_ops ~(blk_dev_ops:'t blk_dev_ops) ~with_state = 
  let open Event in
  let ( >>= ) = monad_ops.bind in
  let return = monad_ops.return in
  let {ev_create;ev_wait;ev_signal} = event_ops in
  let {read;write;_} = blk_dev_ops in
  let disk_thread ~ev = 
    (* do some potentially-lengthy disk ops *)
    return () >>= fun () -> 
    (* get a list of transient free blks *)
    let trans = [] (* FIXME *) in
    (* get the state, wake the waiters, remove the disk_thread *)
    with_state.with_state (fun ~state:fl ~set_state -> 
      (* iterate over fl.wait_list, allocating new trans blkids *)
      (* then put the remaining in the fl state *)
      set_state { fl with transient=trans; disk_thread=None } >>= fun () ->
      (* and terminate *)
      return ())
  in
  with_state.with_state (fun ~state:fl ~set_state ->
    match fl.transient with 
    | x::xs -> (
        set_state {fl with transient=xs} >>= fun () ->
        return x)
    | [] -> (
        (* is the disk_thread already active? *)
        match fl.disk_thread with 
        | None -> (
            (* add ourselves to the wait list, and spawn the disk thread *)
            ev_create () >>= fun (ev:blk_id event) -> 
            let async x = x in
            async(disk_thread ~ev) >>= fun () -> 
            let fl = { fl with disk_thread=Some ev } in
            set_state fl >>= fun () -> 
            print_endline "This line should be followed immediately...";
            (* FIXME we need to be sure of the conc model here: this
               should finish executing to allow the state lock to be
               freed for the disk_thread *)
            ev_wait ev >>= fun blk_id -> 
            print_endline "...by another.";
            return blk_id
          )  
        | Some ev -> (
            (* just wait *)
            ev_wait ev)
      ))
