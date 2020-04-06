(** Run the v1 example *)

open Tjr_monad.With_lwt
open Std_types
open Tjr_impfs_v1
open V1

let fn = "/tmp/v1.store"

(* set lwt running *)

let () = 
  (* an lwt thread that just keeps going... *)
  let rec t () = Lwt.Infix.(
      Lwt_unix.sleep 1.0 >>= fun () ->
      Printf.printf "%s: lwt thread wakes\n%!" __FILE__;
      t ())
  in
  (* run lwt in another thread *)
  let _t = Thread.create 
    (fun () ->
       Printf.printf "%s: Lwt main thread starts\n%!" __FILE__;
       Lwt_main.run (t()))
      ()
  in
  ()

(* main thread deals with fuse *)

let ops : (_,_,_) Minifs_intf.ops = 
  Printf.printf "%s: initializing ops\n%!" __FILE__;
  Lwt_preemptive.run_in_main (fun () -> to_lwt (
      Blk_dev_factory.make_9 fn >>= fun bd ->
      let blk_dev_ops = bd#blk_dev_ops in
      blk_dev_ref := Some(blk_dev_ops);
      let min_free_blk = ref (B.of_int !V1.min_free_blk) in 
      blk_alloc_ref := Some(make_blk_allocator min_free_blk);
      let gom_map_root = B.of_int V1.b1_gom_map_root in
      blk_dev_ops.write 
        ~blk_id:gom_map_root
        ~blk:(gom_empty_leaf_as_blk ()) >>= fun () ->
      let root_ref = ref gom_map_root in
      root_ops_ref := Some(with_ref root_ref);
      let module With_gom = With_gom () in
      let module The_filesystem = With_gom.The_filesystem in
      (* let open The_filesystem in *)
      V1.mk_stat_times () >>= fun times -> 
      Printf.printf "%s: about to create root dir\n%!" __FILE__;
      With_gom.create_dir_ ~is_root:true ~parent:root_did ~name:(Str_256.make "") ~times >>= fun () ->
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
      return ops))

let () =
  Printf.printf "%s: running FUSE main loop\n%!" __FILE__;
  let fuse_ops = Fuse_.mk_fuse_ops ~monad_ops ~ops ~co_eta:Tjr_minifs.Lwt_util.co_eta in
  Fuse.main Sys.argv fuse_ops
    
      
