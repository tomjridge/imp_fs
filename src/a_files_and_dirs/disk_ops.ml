(** Interface to the block device; currently backed by single fd *)

open Imp_pervasives
open X.Disk_on_fd
open X.Disk_ops
open X.Base_types

open Imp_state

let fd_ops = {
  get=(fun () -> fun t -> (t,Ok t.fd));
  set=(fun fd -> failwith "Don't use!");
}

let disk_ops = X.Disk_on_fd.make_disk blk_sz fd_ops

