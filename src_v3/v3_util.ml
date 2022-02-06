(** {2 Implementation types} *)

(* FIXME merlin gets confused if there is a util.ml in this dir and in subdir; so rename to v3_util... *)

(* FIXME probably better to avoid reusing OCaml ref syntax; just access record fields
   directly *)
module Refs_with_dirty_flags = struct
  type 'a ref = {
    mutable value: 'a;
    mutable dirty: bool
  }

  (** values start off clean *)
  let ref value = {value;dirty=false}

  let is_dirty x = x.dirty

  let clean x = x.dirty <- false

  (** assigning sets the dirty flag *)
  let ( := ) r value = r.value <-value; r.dirty <- true

  let (!) r = r.value
end
open struct module R = Refs_with_dirty_flags end

(*
(** For this version, we implement a file using an underlying
   filesystem. NOTE we also store sz in the database; we should check
   that the on-disk version agrees with the DB when resurrecting (and
   maybe times too). *)
type per_file = {
  filename       : string;
  file_descr     : Lwt_unix.file_descr;
  lock           : Lwt_mutex.t;  
  times          : times R.ref;
  sz             : int R.ref;
}
*)




(* config: dont_log, for debugging *)

(** Logging is by default disabled; to enable, explicitly set envvar DONT_LOG=false *)
let dont_log_envvar = 
  Sys.getenv_opt "DONT_LOG" |> function 
  | None -> true
  | Some "false" -> false
  | Some _ -> true

let dont_log : bool ref = ref dont_log_envvar

let convert_pread_pwrite_to_ba_buf ~pread ~pwrite = 
  let pread ~fd ~foff ~len ~buf:ba_buf ~boff = 
    pread ~fd ~foff ~len ~buf:(Shared_ctxt.{ba_buf;is_valid=true}) ~boff
  in
  let pwrite ~fd ~foff ~len ~buf:ba_buf ~boff = 
    let buf = Shared_ctxt.{ba_buf;is_valid=true} in
    pwrite ~fd ~foff ~len ~buf ~boff
  in
  (pread,pwrite)





