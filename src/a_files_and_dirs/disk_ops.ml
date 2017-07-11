(* basic interface to the block device *)

open Tjr_btree
open Btree_api
open Monad
open Imp_pervasives
open Imp_state

let fd_ops = {
  get=(fun () -> fun t -> (t,Ok t.fd));
  set=(fun fd -> failwith "Don't use!");
}


let disk_ops : imp_state disk_ops = 
  Disk_on_fd.make_disk blk_sz fd_ops

