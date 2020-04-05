(** Run the v1 example *)

open Tjr_monad.With_lwt
open Std_types
open Tjr_impfs_v1
open V1

let fn = "/tmp/v1.store"

let main () =
  Blk_dev_factory.make_9 fn >>= fun bd ->
  let blk_dev_ops = bd#blk_dev_ops in
  blk_dev_ref := Some(blk_dev_ops);
  let min_free_blk = ref (B.of_int 2) in (* 0 is root blk; 1 is empty leaf for btree *)
  blk_alloc_ref := Some(make_blk_allocator min_free_blk);
  blk_dev_ops.write ~blk_id:(B.of_int 1) ~blk:(gom_empty_leaf_as_blk ()) >>= fun () ->
  let root_ref = ref (B.of_int 1) in
  root_ops_ref := Some(with_ref root_ref);
  min_free_id_ref := 1 (* 0 is for root dir *);
  let module With_gom = With_gom () in
  let module The_filesystem = With_gom.The_filesystem in
  (* let open The_filesystem in *)
  let ops : (_,_,_) Minifs_intf.ops = The_filesystem.{
      root;
      unlink;
      mkdir;
      opendir;
      readdir;
      closedir;
      create;
      open_;
      pread;
      pwrite;
      close;
      rename;
      truncate;
      stat;
      symlink;
      readlink;
      reset
    }
  in
  (* use with fuse; create a new thread *)
  let fuse_ops = Fuse_.mk_fuse_ops ~monad_ops ~ops ~co_eta:Tjr_minifs.Lwt_util.co_eta in
  let _fuse_main = Thread.create 
      (fun () ->
         Printf.printf "%s: FUSE main thread starts\n%!" __FILE__;
         Fuse.main Sys.argv fuse_ops) 
      ()
  in
  (* and an lwt thread that just keeps going... *)
  let rec t () = Lwt.Infix.(
      Lwt_unix.sleep 1.0 >>= fun () ->
      Printf.printf "%s: lwt thread wakes\n%!" __FILE__;
      t ())
  in
  Lwt_main.run (t ())
      
  
