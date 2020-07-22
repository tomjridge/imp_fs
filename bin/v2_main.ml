(** Run the v2 example, using a (somewhat hairy) combination of FUSE
   and Lwt *)

open Tjr_monad.With_lwt
open Shared_ctxt

let argv = Sys.argv |> Array.to_list |> List.tl

(* FIXME move to config *)
let fn = "./tmp/v2.store"

(* debug line reached *)
let line s = Printf.printf "Reached %s\n%!" s


(** Create the filesystem *)
module Create() = struct  

  let m = 
    (* Block device *)    
    blk_devs#lwt_open_file ~fn ~create:true ~trunc:true >>= fun bd ->
    (* Test_blk_dev.make () |> fun bd ->  *)
    let blk_dev_ops = bd#blk_dev_ops in
    let barrier = fun () -> return () in
    let sync = fun () -> return () in
    let with_ = V2_fs_impl.example#with_ ~blk_dev_ops ~barrier ~sync in
    with_#create () >>= fun _ops ->
    bd#close ()

  let _ = Lwt_main.run (to_lwt m)
end

(** Restore, but don't mount *)
module Restore() = struct

  let _ = Printf.printf "%s: restoring\n%!" __FILE__

  let _ = 
    Lwt_main.run (to_lwt (
        (* Block device *)
        blk_devs#lwt_open_file ~fn ~create:false ~trunc:false >>= fun bd ->
        (* Test_blk_dev.restore () |> fun bd ->  *)
        let blk_dev_ops = bd#blk_dev_ops in
        let barrier = fun () -> return () in
        let sync = fun () -> return () in
        let with_ = V2_fs_impl.example#with_ ~blk_dev_ops ~barrier ~sync in
        with_#restore () >>= fun ops -> 
        return ops))

end


(** Mount the filesystem via FUSE *)
module Run() = struct

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
        blk_devs#lwt_open_file ~fn ~create:false ~trunc:false >>= fun bd ->
        let blk_dev_ops = bd#blk_dev_ops in
        let barrier = fun () -> return () in
        let sync = fun () -> return () in
        let with_ = V2_fs_impl.example#with_ ~blk_dev_ops ~barrier ~sync in
        with_#restore () >>= fun ops -> 
        return ops))

  (** Run the FUSE main loop, with Lwt running in a separate
      (non-Lwt) thread *)
  let () =
    Printf.printf "%s: running FUSE main loop\n%!" __FILE__;
(* don't run for the time being
    let fuse_ops = 
      Fuse_.mk_fuse_ops 
        ~monad_ops ~ops 
        ~co_eta:Tjr_minifs.Lwt_util.co_eta 
    in
    Fuse.main Sys.argv fuse_ops
*)
    ()

end (* Run *)

let _ = 
  match argv with
  | ["create"] -> 
    let module X = Create() in
    ()
  | ["restore"] -> 
    let module X = Restore() in
    ()
  | _ -> 
    (* default: run the filesystem *)
    let module X = Run() in
    ()


