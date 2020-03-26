(** Interface to the block device; currently backed by single fd *)

open Imp_pervasives
open Tjr_btree.Disk_on_fd
open Tjr_btree.Disk_ops
open Tjr_btree.Base_types

open Imp_state

let with_world = Tjr_monad.State_passing_instance.with_world

let fd_ops = {
  get=(fun () -> with_world (fun t -> t.fd,t));
  set=(fun fd -> failwith "Don't use!");
}

(* FIXME do we want to specialize the monad? from the above it looks
   like we have already specialized to state passing monad *)
let disk_ops : 't block_device = 
  Tjr_btree.Disk_on_fd.make_disk ~monad_ops:imp_monad_ops ~blk_sz ~fd_ops

