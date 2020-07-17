(** Freelist example.

Various examples (fixed elt type). Also some code to exercise the
   functionality.

*)
(* open Tjr_freelist *)
open Freelist_intf

open Shared_ctxt



(** Run_example: We open a freelist on a file, allocate some blk_ids,
   close, open, verify the state is consistent.  *)
let run_example ~params () = 
  let module A = struct

    open Tjr_monad.With_lwt

    (** This is used by the freelist example; we also need an
        int_freelist_factory example FIXME *)
    let int_factory = Tjr_plist.pl_examples#for_int

    let fn = params#filename

    let run_a () = 
      blk_devs#with_ba_buf#from_filename ~fn ~create:true ~init:true >>= fun bd ->
      let module B = struct

        let blk_dev_ops = bd#blk_dev_ops

        (* plist *)                           
        let sync = fun () -> 
          Printf.printf "sync called: %s\n%!" __FILE__;
          return ()

        let barrier = fun () -> 
          Printf.printf "barrier called: %s\n%!" __FILE__;
          return ()

        let plist_fact = int_factory#with_blk_dev_ops ~blk_dev_ops ~barrier

        let b1 = B.of_int 1

        (* NOTE create_plist writes the empty list to blk *)
        let run_b () = plist_fact#init#create b1 >>= fun pl ->          
          let module C = struct
            let pl_ref = ref pl 
            let with_state = with_ref pl_ref
            let plist_ops = plist_fact#with_state with_state

            (* freelist *)
                (*
            let origin_ops = Fl_origin.{
                read=(fun () -> failwith "FIXME");
                write=(fun _x -> 
                    (* FIXME should really write into blk 0 *)
                    Printf.printf "call to write_freelist_roots\n%!";
                    return ());
                (*sync=(fun () -> 
                    Printf.printf "call to sync\n%!";
                    return ())*)
              }
                   *)
                
            (* NOTE this actually uses ints rather than blk_ids, since we are
               using int_plist_ops FIXME use blkid_plist_ops *)
            let version = For_blkids { 
                e2b=(fun x -> B.of_int x); 
                b2e=(fun x -> B.to_int x) } 

            (* start off with some free elts in transient *)
            let min_free_alloc elt n = (List_.from_upto elt (elt+n), elt+n)
            let min_free = Some 6
            let fl_ref = ref { 
                transient=[2;3;4;5]; min_free; 
                waiting=[]; disk_thread_active=false } 

            (* for the freelist, we need to ensure that it is actually locked *)
            let with_freelist = with_locked_ref fl_ref 
            let freelist_ops = Fl_make_1.make (object
                method async=async
                method event_ops=event_ops
                method min_free_alloc=min_free_alloc
                method monad_ops=monad_ops
                method sync = sync
                method plist_ops=plist_ops 
                method version=version
                method with_freelist=with_freelist
                method params=object
                  method tr_lower=2; method tr_upper=4; method min_free_alloc_size=1000 end
              end)

            (* at last, can actually start allocating *)

            let last = ref 0 
                
            let run_c () = 
              begin
                0 |> iter_k (fun ~k n ->
                    match n >= 10_000 with
                    | true -> return ()
                    | false -> 
                      freelist_ops.alloc () >>= fun i ->
                      Printf.printf "Allocated blk_id: %d\n%!" i;
                      last:=i;
                      k (n+1))
                >>= fun () ->
                bd#close () >>= fun () ->
                Printf.printf "NOTE that the first allocated blk_id is \
                               2, so after 10k allocs, we expect to be \
                               at blkid 10_001\n%!";
                assert(!last = 10_001);
                Printf.printf "Finished\n%!";
                return ()
              end
          end (* C *)
          in
          C.run_c ()
      end (* B *)
      in
      B.run_b ()
  end (* A *)
  in
  A.run_a ()

let _ = run_example
