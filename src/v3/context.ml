(** File system calls correspond to an lwt thread execution. The
   context is per-thread, and keeps track of eg resources (such as
   locks that have been acquired). *)

module type T = sig
  type t

  type 'a loc (* like the locations in operational semantics, used to model reference names *)

  val loc_create: 'a -> ('a loc,t)m (* with a name? *)
  val loc_get: 'a loc -> ('a,t)m
  val loc_set: 'a loc -> 'a -> (unit,t)m
  val loc_delete: 'a loc -> (unit,t)m
  val loc_stats: unit -> (int,t)m (* how many are active *)


  type ctxt (* = { id:int; resources:res set } *)

  type ctxt_id

  (* global operations; there is a single global pool of ctxts *)
  val ctxt_create : unit -> (ctxt_id,t) m (* like loc_create, but starts off with an empty ctxt *)
  val get_ctxt: ctxt_id -> (ctxt,t)m
  val set_ctxt: ctx_id:ctxt_id -> ctxt:ctxt -> (unit,t)m

  val ctxt_delete : ctxt_id -> (unit,t)m
  (** checks that there are no resources associated with the ctxt, then removes it from the system *)

  type res = Fs_origin | Lock of int

  (* available to a thread at runtime *)
  type ctxt_ops = {
    (* get_ctxt: unit -> (ctxt,t)m; *)
    (* set_ctxt: ctxt -> (unit,t)m; *)
    acquire: res -> (unit,t)m;
    release: res -> (unit,t)m;
    assert_no_resources: unit -> unit
  }
  (* acquire obtains the lock on a resource, and updates the context
     to record this *)


end
