(** Freelist example 2.

This example uses the factory interface.
*)

(* open Tjr_freelist *)
open Freelist_intf

open Shared_ctxt
open Tjr_monad.With_lwt

(** Run_example: We open a freelist on a file, allocate some blk_ids,
   close, open, verify the state is consistent.  *)
let run_example ~params () = 
  let module A = struct

    let fn = params#filename

    (* let file_ops = lwt_file_ops *)
      
    let run_a () = 
      Printf.printf "%s: initializing file\n%!" __FILE__;
      blk_devs#with_ba_buf#from_filename ~fn ~create:true ~init:true >>= fun bd ->

      let module B = struct
        
        let blk_dev_ops = bd#blk_dev_ops

        let fact = fl_examples#for_r
                            
        (* let sync_blk_dev = bd#sync *)

        (* b0 holds the fl origin *)
        let b0 = B.of_int 0

        (* b1 is the start of the plist *)
        let b1 = B.of_int 1        
        
        (* b2 is first free block *)
        let b2 = B.of_int 2

        (* in-memory state *)
        let empty_freelist = fact#empty_freelist ~min_free:(Some b2)

        let barrier = fun () -> 
          Printf.printf "barrier called: %s\n%!" __FILE__;
          return ()

        let sync = fun () -> 
          Printf.printf "sync called: %s\n%!" __FILE__;
          return ()

        (* let origin_ops = fact#origin_ops 
         *     ~blk_dev_ops ~blk_id:b0 ~barrier ~sync *)

        let fact' = fact#with_
            ~blk_dev_ops ~barrier ~sync ~params:(params :> Freelist_intf.params)

        let write_origin = fact#write_origin ~blk_dev_ops ~blk_id:b0
        
        let run_b () = 
          (* we need to initialize b0 *)
          Printf.printf "%s: initializing b0\n%!" __FILE__;
          let origin = Fl_origin.{hd=b1;tl=b1;blk_len=1;min_free=Some b2} in
          write_origin ~origin >>= fun () -> 

          (* and b1 *)
          Printf.printf "%s: initializing b1\n%!" __FILE__;
          pl_examples#for_blk_id#with_blk_dev_ops ~blk_dev_ops ~barrier |> fun x ->
          x#init#create b1 >>= fun plist ->

          (* and get the freelist ops *)
          Printf.printf "%s: getting freelist ops\n%!" __FILE__;
          x#with_ref plist |> fun x2 ->
          x#with_state (x2#with_plist) |> fun plist_ops ->
          fact'#with_plist_ops plist_ops |> fun x3 -> 
          x3#with_locked_ref empty_freelist |> fun x4 -> 
          x4#freelist_ops |> fun freelist_ops -> 
          
          (* then run some operations *)
          Printf.printf "%s: running operations\n%!" __FILE__;
          let last = ref 0 in
          0 |> iter_k (fun ~k n ->
              match n >= 10_000 with
              | true -> return ()
              | false -> 
                Printf.printf "%s: Calling freelist alloc\n%!" __FILE__;
                freelist_ops.alloc () >>= fun i ->
                let i = B.to_int i in
                Printf.printf "%s: Allocated blk_id: %d\n%!" __FILE__ i;
                last:=i;
                k (n+1))
          >>= fun () ->
          Printf.printf "%s: closing blk dev\n%!" __FILE__;
          bd#close () >>= fun () ->
          Printf.printf "%s: NOTE that the first allocated blk_id is 2, so \
                         after 10k allocs, we expect to be at blkid \
                         10_001\n%!" __FILE__;
          assert(!last = 10_001);
          Printf.printf "%s: Finished\n%!" __FILE__;
          return ()
      end
      in
      B.run_b ()
  end
  in 
  A.run_a ()
        
