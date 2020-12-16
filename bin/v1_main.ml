(** Run the v1 example, using a (somewhat hairy) combination of FUSE
   and Lwt *)

(* FIXME needs create, restore etc *)

open Tjr_monad.With_lwt
open Shared_ctxt
(* open V1 *)

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

let sys_root_blk = B.of_int 0
let gom_root_blk = B.of_int 1
let min_free_blk = B.of_int 2 

(** Initialize filesystem operations *)
let ops : (_,_,_) Minifs_intf.ops = 
  Printf.printf "%s: initializing ops\n%!" __FILE__;
  Lwt_preemptive.run_in_main (fun () -> to_lwt (

      (* Block device *)
      blk_devs#lwt_open_file ~fn ~create:true ~trunc:true >>= fun bd ->
      let blk_dev_ops = bd#blk_dev_ops in

      (* Block allocator *)
      Printf.printf "%s: min_free_blk is %d\n%!" __FILE__ (min_free_blk|>B.to_int);
      (* 0 blk is system root blk; 1 is gom map root *)
      let blk_alloc = make_blk_allocator (ref min_free_blk) in
      
      (* GOM root *)
      line __LOC__;
      V1.gom_factory#write_empty_leaf ~blk_dev_ops ~blk_id:gom_root_blk >>= fun () ->
      line __LOC__;
      (* FIXME we also need to sync the root to disk *)
      let module With_gom = V1.Stage_1(struct
        let blk_dev_ops = blk_dev_ops
        let blk_alloc = blk_alloc
        let min_free_id = 1 (* 0 is root dir *)
        let gom_root = gom_root_blk
      end) 
      in
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
    
      
