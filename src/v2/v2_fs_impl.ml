(** Implementation of the on-disk filesystem, which is essentially
   on-disk pointers to the freelist and the GOM *)
open V2_intf

type 'blk_id fs_origin = 'blk_id Fs_origin_block.t


type ('blk_id,'blk,'t,'fid,'dh) filesystem_factory = <

  read_origin: 
    blk_dev_ops : ('blk_id,'blk,'t) blk_dev_ops -> 
    blk_id : 'blk_id -> 
    ('blk_id fs_origin,'t)m;

  write_origin:
    blk_dev_ops : ('blk_id,'blk,'t) blk_dev_ops -> 
    blk_id : 'blk_id -> 
    origin: 'blk_id fs_origin -> 
    (unit,'t)m;

  with_:
    blk_dev_ops : ('blk_id,'blk,'t) blk_dev_ops -> 
    barrier     : (unit -> (unit,'t)m) -> 
    sync        : (unit -> (unit,'t)m) -> 
    <
      (* create: unit -> (('fid,'dh,'t)Minifs_intf.ops,'t)m; *)
      create: unit -> (unit,'t)m;
      (** Create a new filesystem, using block 0 for the origin *)
          
      restore: unit -> (('fid,'dh,'t)Minifs_intf.ops,'t)m;
      (** Restore an existing filesystem from block 0 *)
    >
>


open Shared_ctxt

module Example = struct

  module Bp = struct
    type t = Shared_ctxt.r Fs_origin_block.t[@@deriving bin_io]
    let max_sz = 3*9 (* FIXME check *)
  end

  let bp_mshlr : _ bp_mshlr = (module Bp)

  let ba_mshlr = bp_mshlrs#ba_mshlr ~mshlr:bp_mshlr ~buf_sz:(Shared_ctxt.blk_sz |> Blk_sz.to_int)

  module Ba = (val ba_mshlr)


  let read_origin ~blk_dev_ops ~blk_id = 
    blk_dev_ops.read ~blk_id >>= fun blk -> 
    Ba.unmarshal blk |> fun origin -> 
    Printf.printf "%s: read_origin: %s\n%!" __FILE__ 
      (origin |> Fs_origin_block.sexp_of_t' |> Sexplib.Sexp.to_string_hum);
    return origin

  let write_origin ~blk_dev_ops ~blk_id ~origin = 
    Printf.printf "%s: write_origin: %s\n%!" __FILE__ 
      (origin |> Fs_origin_block.sexp_of_t' |> Sexplib.Sexp.to_string_hum);
    origin |> Ba.marshal |> fun blk -> 
    blk_dev_ops.write ~blk_id ~blk  

  
  module Fl = Tjr_freelist

  let fl_factory = Fl.fl_examples#for_r

  open V2_counter
  let counter_factory : (r,_,_) counter_factory = V2_counter.example

  let gom_factory = V2_gom.gom_example

  let add_logging_to_blk_dev = V2.add_logging_to_blk_dev

  let with_ ~blk_dev_ops ~barrier ~sync =    
    let fl_factory' = 
      let read_msg blk_id = Printf.printf "freelist: read from %d\n%!" (B.to_int blk_id) in
      let write_msg blk_id = Printf.printf "freelist: write to %d\n%!" (B.to_int blk_id) in
      let blk_dev_ops = add_logging_to_blk_dev ~read_msg ~write_msg ~blk_dev_ops in
      fl_factory#with_ ~blk_dev_ops ~barrier ~sync ~params:Fl.fl_examples#fl_params_1 
    in
    let counter_factory' = 
      let read_msg blk_id = Printf.printf "counter: read from %d\n%!" (B.to_int blk_id) in
      let write_msg blk_id = Printf.printf "counter: write to %d\n%!" (B.to_int blk_id) in
      let blk_dev_ops = add_logging_to_blk_dev ~read_msg ~write_msg ~blk_dev_ops in
      counter_factory#with_ ~blk_dev_ops ~sync in
    let b0 = B.of_int 0 in
    let create () = 
      (* create the three components, write the origin at blk0, and
         return the Minifs ops *)

      Printf.printf "%s: about to create freelist\n%!" __FILE__;

      (* Freelist, with origin at block 1, empty list at blk 2, and
         frees from blk 3 *)
      let fl_origin = B.of_int 1 in
      let origin = fl_origin in
      fl_factory'#initialize ~origin 
        ~free_blk:(B.of_int 2) 
        ~min_free:(Some (B.of_int 3)) >>= fun () ->
      fl_factory'#restore ~autosync:true ~origin >>= fun x -> 
      return x#freelist_ops >>= fun fl_ops ->
      let fl_ops = Util.add_tracing_to_freelist ~freelist_ops:fl_ops in
      
      (* Counter *)
      Printf.printf "%s: about to create counter\n%!" __FILE__;
      fl_ops.alloc () >>= fun counter_origin -> 
      assert(counter_origin = B.of_int 3);
      counter_factory'#create ~blk_id:counter_origin ~min_free:1 >>= fun counter_ops -> 
      
      (* GOM *)
      Printf.printf "%s: about to create GOM\n%!" __FILE__;
      let gom_factory' = 
        let read_msg blk_id = Printf.printf "gom: read from %d\n%!" (B.to_int blk_id) in
        let write_msg blk_id = Printf.printf "gom: write to %d\n%!" (B.to_int blk_id) in
        let blk_dev_ops = add_logging_to_blk_dev ~read_msg ~write_msg ~blk_dev_ops in
        gom_factory#with_ ~blk_dev_ops ~barrier ~sync ~freelist_ops:fl_ops in
      gom_factory'#create () >>= fun x -> 
      Printf.printf "%s: GOM created\n%!" __FILE__;
      x#gom_ops |> fun gom_ops -> 
      let gom_origin = x#origin in

      (* Create root directory *)
      let root_did = (* Dir_impl.Dir_entry.Did *) 0 in
      let dir_factory = Dir_impl.dir_example in
      let dir_factory' = 
        let read_msg blk_id = Printf.printf "dir_factory: read from %d\n%!" (B.to_int blk_id) in
        let write_msg blk_id = Printf.printf "dir_factory: write to %d\n%!" (B.to_int blk_id) in
        let blk_dev_ops = add_logging_to_blk_dev ~read_msg ~write_msg ~blk_dev_ops in
        dir_factory#with_
          ~blk_dev_ops
          ~barrier
          ~sync
          ~freelist_ops:fl_ops
      in
      V1.mk_stat_times () >>= fun _times -> 
      (* For debugging, we want the root dir to have the same stat each time *)
      let times = Times.{atim=0.0;mtim=0.0} in
      Printf.printf "%s: about to create root dir...\n%!" __FILE__;
      dir_factory'#create_root_dir ~root_did ~times >>= fun blk_id ->
      Printf.printf "%s: ...created...\n%!" __FILE__;
      gom_ops.V2_gom.Gom_ops.insert (Dir_impl.Dir_entry.Did root_did) blk_id >>= fun () -> 
      Printf.printf "%s: ...inserted into GOM\n%!" __FILE__;

      (* Make sure everything is flushed *)
      gom_ops.sync () >>= fun _ -> 
      counter_ops.sync () >>= fun _ ->
      (* Wait for the freelist to finish going to disk *)
      With_lwt.(sleep 1.0 |> from_lwt) >>= fun () ->
      fl_ops.sync () >>= fun _ -> 
      

      (* Origin *)
      let origin : _ fs_origin = Fs_origin_block.{ fl_origin; gom_origin; counter_origin } in
      Printf.printf "%s: writing fs_origin\n%!" __FILE__;
      let read_msg blk_id = Printf.printf "fs_origin: read from %d\n%!" (B.to_int blk_id) in
      let write_msg blk_id = Printf.printf "fs_origin: write to %d\n%!" (B.to_int blk_id) in
      let blk_dev_ops = add_logging_to_blk_dev ~read_msg ~write_msg ~blk_dev_ops in
      write_origin ~blk_dev_ops ~blk_id:b0 ~origin >>= fun () -> 
      
(*      
      (* FIXME do we need the following? *)
      Printf.printf "%s: calling V2.make\n%!" __FILE__;
      V2.make ~blk_dev_ops ~fs_origin:origin ~fl_params:Fl.fl_examples#fl_params_1 
*)
      return ()
    in

    let restore () = 
      read_origin ~blk_dev_ops ~blk_id:b0 >>= fun origin -> 
      V2.make ~blk_dev_ops ~fs_origin:origin ~fl_params:Fl.fl_examples#fl_params_1 
    in

    object
      method create=create
      method restore=restore
    end
end

open Example

let example : _ filesystem_factory = 
  object
    method read_origin=read_origin
    method write_origin=write_origin
    method with_=with_
  end
  
  

