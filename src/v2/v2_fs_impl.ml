(** Implementation of the on-disk filesystem, which is essentially
   on-disk pointers to the freelist and the GOM *)


(** The origin block for the whole system (typically stored in block 0) *)
module Fs_origin_block = struct
  (* open Bin_prot.Std *)

  type 'blk_id t = {
    fl_origin      : 'blk_id;
    gom_origin     : 'blk_id;    
    counter_origin : 'blk_id;
  }[@@deriving bin_io]
(** freelist origin; root of GOM; object id counter (numbers >=
    counter are free to be used as object identifiers) *)
end

type 'blk_id origin = 'blk_id Fs_origin_block.t

type ('blk_id,'blk,'t,'fid,'dh) filesystem_factory = <

  read_origin: 
    blk_dev_ops : ('blk_id,'blk,'t) blk_dev_ops -> 
    blk_id : 'blk_id -> 
    ('blk_id origin,'t)m;

  write_origin:
    blk_dev_ops : ('blk_id,'blk,'t) blk_dev_ops -> 
    blk_id : 'blk_id -> 
    origin: 'blk_id origin -> 
    (unit,'t)m;

  with_:
    blk_dev_ops : ('blk_id,'blk,'t) blk_dev_ops -> 
    barrier     : (unit -> (unit,'t)m) -> 
    sync        : (unit -> (unit,'t)m) -> 
    <
      create: unit -> (('fid,'dh,'t)Minifs_intf.ops,'t)m;
      (** Create a new filesystem, using block 0 for the origin *)
          
      restore: unit -> (('fid,'dh,'t)Minifs_intf.ops,'t)m;
      (** Restore an existing filesystem from block 0 *)
    >
>


(*
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
    Ba.unmarshal blk |> return

  let write_origin ~blk_dev_ops ~blk_id ~origin = 
    origin |> Ba.marshal |> fun blk -> 
    blk_dev_ops.write ~blk_id ~blk  

  
  module Fl = Tjr_plist_freelist

  let fl_factory = Fl.fl_examples#for_r

  open V2_counter
  let counter_factory : (r,_,_) counter_factory = V2_counter.example

  let gom_factory = V2_gom.gom_example

  let with_ ~blk_dev_ops ~barrier ~sync =    
    let fl_factory' = fl_factory#with_ ~blk_dev_ops ~barrier ~sync ~params:Fl.fl_examples#fl_params_1 in
    let create () = 
      (* create the three components, write the origin at blk0, and
         return the Minifs ops *)

      (* Freelist, with origin at block fl_origin, empty list at blk
         fl_origin+1, and frees from fl_origin+2 *)
      let fl_origin = B.of_int 1 in
      let origin = fl_origin in
      fl_factory'#initialize ~origin 
        ~free_blk:(B.of_int 2) 
        ~min_free:(Some (B.of_int 3)) >>= fun () ->
      fl_factory'#restore ~autosync:true ~origin >>= fun x -> 
      return x#freelist_ops >>= fun fl_ops ->
      
      (* Counter at fl_origin -1 *)
      let counter_factory' = counter_factory#with_ ~blk_dev_ops ~sync in
      fl_ops.alloc () >>= fun counter_origin -> 
      counter_factory'#create ~blk_id:counter_origin ~min_free:1 >>= fun counter_ops -> 
      
      (* GOM *)
      let gom_factory' = gom_factory#with_ ~blk_dev_ops ~barrier ~sync ~freelist_ops:fl_ops in
      gom_factory'#create () >>= fun x -> 
      x#gom_ops |> fun gom_ops -> 
      let gom_origin = x#origin in
      
        
      
    
        
      
      
    
    

end

  

end

let example : _ filesystem_factory = 


*)
