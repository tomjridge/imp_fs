(* basic interface to the block device *)

open Imp_pervasives
open X.Disk_on_fd
open X.Disk_ops
open X.Monad

open Imp_state

let fd_ops = {
  get=(fun () -> fun t -> (t,Ok t.fd));
  set=(fun fd -> failwith "Don't use!");
}

let disk_ops : [< `Disk_ops of 'a ] = X.Disk_on_fd.make_disk blk_sz fd_ops

