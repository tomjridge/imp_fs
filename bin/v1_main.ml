(** Run the v1 example, using a (somewhat hairy) combination of FUSE
   and Lwt *)

open Tjr_monad.With_lwt
open Shared_ctxt
open Tjr_impfs_v1
open V1

(* FIXME move to config *)
let fn = "./tmp/v1.store"

(* debug line reached *)
let line s = Printf.printf "Reached %s\n%!" s

(** Set lwt running, but NOT in the main thread (we need that for
   FUSE) *)
let () = 
  (* an lwt thread that just keeps going... *)
  let rec t () = Lwt.Infix.(
      Lwt_unix.sleep 1.0 >>= fun () ->
      Printf.printf "%s: lwt thread wakes\n%!" __FILE__;
      t ())
  in
  (* run lwt in another thread *)
  let _t = 
    Thread.create 
      (fun () ->
         Printf.printf "%s: Lwt main thread starts\n%!" __FILE__;
         Lwt_main.run (t()))
      ()
  in
  ()

(** NOTE main thread deals with fuse *)

(** Initialize filesystem operations *)
let ops : (_,_,_) Minifs_intf.ops = 
  Printf.printf "%s: initializing ops\n%!" __FILE__;
  Lwt_preemptive.run_in_main (fun () -> to_lwt (

      (* Block device *)
      Blk_dev_factory.make_9 fn >>= fun bd ->
      let blk_dev_ops = bd#blk_dev_ops in
      blk_dev_ref := Some(blk_dev_ops);

      (* Block allocator *)
      Printf.printf "%s: min_free_blk is %d\n%!" __FILE__ (!V1.min_free_blk);
      let min_free_blk = ref (B.of_int !V1.min_free_blk) in 
      blk_alloc_ref := Some(make_blk_allocator min_free_blk);

      (* GOM root *)
      let gom_map_root = B.of_int V1.b1_gom_map_root in
      line __LOC__;
      blk_dev_ops.write 
        ~blk_id:gom_map_root
        ~blk:((Lazy.force V1.gom_factory)#empty_leaf_as_blk) >>= fun () ->
      line __LOC__;
      let root_ref = ref gom_map_root in
      root_ops_ref := Some(with_ref root_ref);
      line __LOC__;
      (* FIXME we also need to sync the root to disk *)
      let module With_gom = With_gom () in
      let module The_filesystem = With_gom.The_filesystem in

      (* Create root directory *)
      V1.mk_stat_times () >>= fun times -> 
      Printf.printf "%s: about to create root dir\n%!" __FILE__;
      With_gom.create_root_dir ~times >>= fun () ->
      let ops : (_,_,_) Minifs_intf.ops = The_filesystem.ops in
      return ops))

(** Run the FUSE main loop, with Lwt running in a separate
   (non-Lwt) thread *)
let () =
  Printf.printf "%s: running FUSE main loop\n%!" __FILE__;
  let fuse_ops = 
    Fuse_.mk_fuse_ops 
      ~monad_ops ~ops 
      ~co_eta:Tjr_minifs.Lwt_util.co_eta 
  in
  Fuse.main Sys.argv fuse_ops
    
      
