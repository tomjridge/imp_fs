(** V2 - compared to V1, this version implements files (via file_impl_v2).

We try to reuse V1_generic. Some of the following copied from v1.ml *)

[@@@warning "-33"]

open Tjr_monad.With_lwt
open Shared_ctxt
open Bin_prot.Std
open V2_intf
module G = V2_generic


(** {2 Setup the generic instance} *)

(** Base types *)
module S0 = struct
  type t = lwt
  let monad_ops = monad_ops
  type fid = int[@@deriving bin_io]
  type did = int[@@deriving bin_io]
  type sid = int[@@deriving bin_io]
end
open S0

module Make = G.Make(S0)

(** derived types *)
module S1 = Make.S1
open S1

(** functor argument type holder *)
module S2 = Make.S2

(** what we need to provide to get a filesystem *)
module type T2 = S2.T2

(** functor to construct a filesystem (takes a module of type T2); the
   implementation is {!T2_impl}, towards the end of this file *)
module Make_2 = Make.Make_2


(** {2 Some simple defns we can complete here} *)

let root_did = 0

let mk_stat_times () = 
  Unix.time () |> fun t -> (* 1s resolution? FIXME *)
  return Times.{ atim=t; mtim=t }

type dir_entry = S1.dir_entry


(** {2 Various marshallers} *)

let shared_mshlrs = Tjr_fs_shared.bp_mshlrs

let dir_entry_mshlr : dir_entry bp_mshlr = 
  (module 
    (struct 
      type t = dir_entry[@@deriving bin_io] 
      let max_sz = 10 (* FIXME check 9+1 *)
    end))

[@@@warning "-32"]

module Stage_1(S1:sig
    val blk_dev_ops : (r,blk,t)blk_dev_ops
    val fs_origin : r Fs_origin_block.t
    (* FIXME replace with fl_examples#fl_params_1 *)
    val fl_params : Tjr_plist_freelist.Freelist_intf.params
  end) = struct
  open S1
  
  let barrier () = return ()
  let sync () = return ()

  (** {2 New identifiers} *)

  (** We use mutually exclusive subsets of int for identifiers; NOTE 0
      is reserved for the root directory *)
  let min_free_id_ref = ref fs_origin.min_free_object_id 
  (* FIXME make sure to flush *)

  let new_id () = 
    let r = !min_free_id_ref in
    incr min_free_id_ref;
    return r

  let new_did () = new_id () 

  let new_fid () = new_id () 

  let new_sid () = new_id () 

  let id_to_int = function
    | Did did -> did
    | Fid fid -> fid
    | Sid sid -> sid


  (** {2 The freelist } *)
  
  (** We need to read the freelist origin block, and resurrect the freelist *)
  
  let fl_params = object
    method tr_upper=2000
    method tr_lower=1000
    method min_free_alloc_size=500
  end
    
  (* FIXME we need to initialize the freelist at some point of course;
     an init functor? *)
  let freelist = 
    let fact = fl_examples#for_r in
    let with_ = 
      fact#with_
        ~blk_dev_ops
        ~barrier
        ~sync
        ~params:fl_params
    in
    with_#from_origin_with_autosync fs_origin.fl_origin

  let fl_ops = freelist >>= fun x -> 
    return x#freelist_ops
    

  module Stage_2(S2:sig val fl_ops: (r,r,t)Freelist_intf.freelist_ops end) = struct
    open S2


    (** {2 Blk allocator using freelist} *)

    let blk_alloc : _ blk_allocator_ops =     
      {
        blk_alloc=S2.fl_ops.alloc;
        blk_free=S2.fl_ops.free
      }

      
    (** {2 The global object map (GOM) } *)

    (** NOTE FIXME this should really track the blocks it uses, as
       files and directories do; so the GOM has its own origin block,
       with a pointer to the root and a pointer to the used list *)

    let gom = V2_gom.uncached
        ~blk_dev_ops
        ~blk_alloc
        ~init_btree_root:fs_origin.gom_root
        
    let _ = gom

    let gom_ops = gom#map_ops_with_ls

    (* FIXME promote Map_ops_with_ls module to top of Tjr_btree *)
    let Tjr_btree.Btree_intf.Map_ops_with_ls.{ find=gom_find; _ } = gom_ops
      
    (* NOTE we also have to make sure we flush the GOM root... *)


    (** {2 Dirs} *)

    let gom_find_opt = gom_ops.find

    let gom_find k = gom_find_opt ~k >>= function
      | Some r -> return r
      | None -> 
        Printf.printf "Error: gom key %d not found\n%!" (id_to_int k);
        failwith "gom: id did not map to an entry"

    let gom_insert = gom_ops.insert

    let gom_delete = gom_ops.delete

    let dirs : dirs_ops = {
      find = (fun did -> 
          gom_find (Did did) >>= fun blk_id ->
          Dir.read_rb ~blk_id >>= fun rb ->
        let rb = ref rb in
        let write_rb () = Dir.write_rb ~blk_id (!rb) in
        let root_ops = 
          let with_state f = 
            f ~state:(!rb.map_root) ~set_state:(fun map_root ->
                rb:={!rb with map_root};
                write_rb())
          in
          {with_state}
        in
        let dir_map_ops = Dir.dir_map_ops ~root_ops in
        let set_parent did = 
          rb:={!rb with parent=did};
          write_rb()
        in
        let get_parent () = (!rb).parent |> return in
        let set_times times =
          rb:={!rb with times};
          write_rb()
        in
        let get_times () = (!rb).times |> return in          
        let dir_ops = {
          find=(fun k -> dir_map_ops.find ~k);
          insert=(fun k v -> dir_map_ops.insert ~k ~v);
          delete=(fun k -> dir_map_ops.delete ~k);
          ls_create=
            Tjr_btree.Btree_intf.ls2object 
              ~monad_ops 
              ~leaf_stream_ops:dir_map_ops.leaf_stream_ops
              ~get_r:(fun () -> return !rb.map_root);
          set_parent;
          get_parent;
          set_times;
          get_times;
        }
        in
        return dir_ops);                           
    delete = (fun did -> gom_delete ~k:(Did did))

    }
    

  end
    
end

             
