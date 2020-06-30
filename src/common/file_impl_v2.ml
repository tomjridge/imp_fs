(** A simple file implementation; supersedes v1. 

Terminology: (file) block-index map: the map from blk index to blk_id

We implement a file as a map from blk_index to blk. The map is
typically implemented by a B-tree or similar.

New blocks are allocated as needed (by default the map is empty, and a
block is only allocated when that block is written to; so files are
"sparse").

The "iterated block read/write" component has been abstracted out
because it appears in a lot of other places.

Concurrency: The backing B-tree is itself a persistent object.

GOM interaction: We treat a file as a separate object; the GOM maps file id to origin block. 


TODO:
- b-tree to implement delete_after (or some alternative approach)
- validate against "normal" file semantics
- ensure the tests are run by some top-level test executable
- implement freeing of blocks after truncate
- maybe worth distinguishing the blk_id used by the map from the blk_id used to store data
*)


open Int_like
open Buffers_from_btree
open Fv2_types

module Iter_block_blit = Fv2_iter_block_blit
type idx = Iter_block_blit.idx
let idx_mshlr = Iter_block_blit.idx_mshlr 

(* $(PIPE2SH("""sed -n '/Construct[ ]file operations/,/^>/p' >GEN.file_factory.ml_""")) *)
(** Construct file operations.

Sequence of steps:

- (1) Read the file origin block; this contains: file size; the
  blk-idx map root; the usedlist roots
- (2) Construct the usedlist
- (2.1) Get the usedlist origin information
- (2.2) And this provides the usedlist operations
- (2.3) And allocation should indirect via the usedlist (to record
  which blk_ids have been allocated)
- (3) Construct the blk-idx map
- (3.1) Construct the ops from the blk-idx root
- (4) Finally construct the file operations

*)
type ('buf,'blk,'blk_id,'t) file_factory = <

  (* (1) *)
  read_origin: 
    blk_dev_ops : ('blk_id,'blk,'t) blk_dev_ops -> 
    blk_id : 'blk_id -> 
    ('blk_id File_origin_block.t,'t)m;

  write_origin:
    blk_dev_ops : ('blk_id,'blk,'t) blk_dev_ops -> 
    blk_id : 'blk_id -> 
    origin: 'blk_id File_origin_block.t -> 
    (unit,'t)m;

  origin_to_fim: 'blk_id File_origin_block.t -> 'blk_id File_im.t;

  usedlist_origin : 'blk_id File_origin_block.t -> 'blk_id Usedlist.origin;
  (** (2.1) *)

  with_: 
    blk_dev_ops  : ('blk_id,'blk,'t) blk_dev_ops -> 
    barrier      : (unit -> (unit,'t)m) -> 
    sync         : (unit -> (unit,'t)m) -> 
    freelist_ops : ('blk_id,'t) Freelist.ops -> 
    <    
      usedlist_ops : 'blk_id Usedlist.origin -> (('blk_id,'t) Usedlist.ops,'t)m;
      (** (2.2) *)

      alloc_via_usedlist : 
        ('blk_id,'t) Usedlist.ops ->         
        < alloc_via_usedlist: unit -> ('blk_id,'t)m>;
      (** (2.3) Allocate and record in usedlist *)

      blk_index_map_ops : 'blk_id -> (idx,'blk_id,'blk_id,'t)Btree_ops.t;
      (** (3.1) Argument is the B-tree on-disk root *)
      
      file_ops: 
        usedlist_ops       : ('blk_id,'t) Usedlist.ops -> 
        alloc_via_usedlist : (unit -> ('blk_id,'t)m) ->         
        blk_index_map_ops  : (int,'blk_id,'blk_id,'t)Btree_ops.t -> 
        with_fim           : ('blk_id File_im.t,'t) with_state -> 
        ('buf,'t)file_ops;
      (** (4) *)
    >
>


(** Check the arguments to pread *)
let pread_check = File_impl_v1.pread_check

(** Check arguments to pwrite *)
let pwrite_check = File_impl_v1.pwrite_check



module type S = sig
  type blk = ba_buf
  type buf = ba_buf
  type blk_id = Shared_ctxt.r
  type r = Shared_ctxt.r
  type t
  val monad_ops   : t monad_ops
  val buf_ops     : buf buf_ops  (* FIXME create_zeroed? *)

  (** For the freelist *)
  type a = blk_id
  val plist_factory : (a,blk_id,blk,buf,t) Plist_intf.plist_factory


  (* we don't use leaf-streams for idx maps *)
  type _ls

  (** NOTE this is the type for btree_factory#uncached *)
  val uncached : 
    blk_dev_ops     : (r, blk, t) blk_dev_ops -> 
    blk_alloc       : (r, t) blk_allocator_ops -> 
    init_btree_root : r -> 
    <
      get_btree_root: unit -> (r,t) m;
      map_ops_with_ls : (idx,r,r,_ls,t) Tjr_btree.Btree_intf.map_ops_with_ls
    >

  val file_origin_mshlr: blk_id File_origin_block.t ba_mshlr
end


module type T = sig
  module S : S
  open S
  val file_factory : (buf,blk,blk_id,t) file_factory
end


[@@@warning "-32"]

(** Version with full sig *)
module Make_v1(S:S) (* : T with module S = S*) = struct
  module S = S
  open S

  let ( >>= ) = monad_ops.bind
  let return = monad_ops.return 

  module File_origin_mshlr = (val file_origin_mshlr)

  let read_origin ~blk_dev_ops ~blk_id =
    blk_dev_ops.read ~blk_id >>= fun buf -> 
    File_origin_mshlr.unmarshal buf |> return

  let write_origin ~blk_dev_ops ~blk_id ~origin =
    origin |> File_origin_mshlr.marshal |> fun buf -> 
    assert( (buf_ops.buf_size buf).size = (blk_dev_ops.blk_sz|>Blk_sz.to_int)); 
    blk_dev_ops.write ~blk_id ~blk:buf

  let origin_to_fim (fo: _ File_origin_block.t) : (_ File_im.t) = 
    File_im.{
      origin_info=fo;
      origin_dirty=false;
    }
    
  let usedlist_origin (fo: _ Fo.t) = fo.usedlist_origin


  module With_(S2:sig
      val blk_dev_ops  : (blk_id,blk,t) blk_dev_ops
      val barrier      : (unit -> (unit,t)m)
      val sync         : (unit -> (unit,t)m)
      val freelist_ops : (blk_id,t) Freelist.ops
    end) 
  = 
  struct
    open S2

    (* NOTE unlike a buffer, a blk typically has a fixed size eg 4096 bytes *)        
    let blk_sz = blk_dev_ops.blk_sz |> Blk_sz.to_int

    let usedlist_ops (uo:_ Usedlist.origin) =
      plist_factory#with_blk_dev_ops ~blk_dev_ops ~barrier |> fun x -> 
      x#init#from_endpts uo >>= fun pl -> 
      x#with_ref pl |> fun y -> 
      x#with_state y#with_plist  |> fun plist_ops -> 
      return plist_ops
      
    let alloc_via_usedlist (ul_ops: _ Usedlist.ops) =
      let alloc () = 
        freelist_ops.alloc () >>= fun blk_id -> 
        ul_ops.add blk_id >>= fun () -> 
        barrier() >>= fun () ->
        return blk_id
      in
      object method alloc_via_usedlist=alloc end

    module Blk_idx = struct
      (* FIXME may be better to just reuse int r map *)
      let blk_sz = Shared_ctxt.blk_sz
      module S = struct
        type k = idx
        type v = Shared_ctxt.r
        type r = Shared_ctxt.r
        type t = Shared_ctxt.t
        let k_cmp: k -> k -> int = Stdlib.compare
        let monad_ops = Shared_ctxt.monad_ops
        let k_mshlr = idx_mshlr
        let v_mshlr = bp_mshlrs#r_mshlr
        let r_mshlr = bp_mshlrs#r_mshlr

        let k_size = let module X = (val k_mshlr) in X.max_sz
        let v_size = let module X = (val v_mshlr) in X.max_sz
        let cs = Tjr_btree.Bin_prot_marshalling.make_constants ~blk_sz ~k_size ~v_size
        let r_cmp = Shared_ctxt.r_cmp
      end
      
      module M = Tjr_btree.Make_6.Make_v2(S)
      let btree_factory = M.btree_factory
    end (* Blk_idx *)


    let blk_alloc = failwith "FIXME"

    let blk_index_map_ops r : (idx,_,_,_)Btree_ops.t = 
      let uncached = S.uncached
          ~blk_dev_ops
          ~blk_alloc
          ~init_btree_root:r
      in
      let ops (* map_ops_with_ls *) = uncached#map_ops_with_ls in
      let get_root = uncached#get_btree_root in
      let find = ops.find in
      let insert = ops.insert in
      let delete = ops.delete in
      Btree_ops.{
        find         =(fun k -> find ~k);
        insert       =(fun k v -> insert ~k ~v);
        delete       =(fun k -> delete ~k);
        delete_after =(fun _k -> failwith "FIXME");
        flush        =(fun () -> return ()); (* NOTE we are using uncached B-tree currently *)
        get_root
      }

    [@@@warning "-27"]

    module File_ops(S3:sig
        val usedlist_ops       : (blk_id,t) Usedlist.ops
        val alloc_via_usedlist : (unit -> (blk_id,t)m)
        val blk_index_map_ops  : (r -> (idx,blk_id,blk_id,t)Btree_ops.t)
        val with_fim           : (blk_id File_im.t,t) with_state
      end) 
    = 
    struct

          
(*
      open S3

      let empty_blk () = buf_ops.of_string (String.make blk_sz (Char.chr 0))
      (* assumes functional blk impl ?; FIXME blk_ops pads string automatically? *)

      let bind = blk_index_map_ops
      let dev = blk_dev_ops

      let with_fim = with_fim.with_state

      let size () = 
        with_fim (fun ~state ~set_state:_ -> 
            return state.origin_info.file_size)
  
      let blk_ops = Shared_ctxt.blk_ops
        
      let truncate_blk ~blk ~blk_off = 
        blk |> blk_ops.to_string |> fun blk -> String.sub blk 0 blk_off |> blk_ops.of_string
  
      let truncate ~(size:size) = 
        (* FIXME the following code is rather hacky, to say the least *)
        with_fim (fun ~state:fim ~set_state -> 
            match size.Int_like.size >= fim.origin_info.file_size.size with
            | true -> 
              (* no need to drop blocks *)
              FIXME we need to unify all the size params, and add binprot marshalling
              set_state{fim with origin_info={fim.origin_info with file_size=size}}
            | false -> 
              (* FIXME check the maths of this - if size is 0 we may want to have no entries in blk_index *)
              (* drop all blocks after size/blk_sz FIXME would be nice to have
           "delete from"; perhaps we list the contents of the index_map
           and remove the relevant keys? or a monadic fold or iteration? *)
        (* also need to zero out the bytes in the final block beyond size.size *)
        let i,blk_off = size.size / blk_sz, size.size mod blk_sz in
        let r = inode.blk_index_map_root in
        bind.delete_after ~r ~k:i >>= fun r ->
        (
          match blk_off > 0 with
          | true -> (
              (* read blk and zero out suffix, then rewrite *)
              bind.find ~r ~k:i >>= function
              | None -> return ()
              | Some blk_id -> 
                dev.read ~blk_id >>= fun blk ->
                truncate_blk ~blk ~blk_off |> fun blk ->
                (* FIXME we need a rewrite here *)
                dev.write ~blk_id ~blk)
          | false -> return ()
        ) >>= fun () -> 
        set_state {file_size=size; blk_index_map_root=r})
  in
  let read_blk {index=(i:int)} =
    with_state (fun ~state:inode ~set_state -> 
      bind.find ~r:inode.blk_index_map_root ~k:i >>= function
      | None -> (
          alloc () >>= fun r -> 
          let blk = empty_blk () in
          (* this ensures that every blk_id corresponds to a blk that has been written *)
          dev.write ~blk_id:r ~blk >>= fun () ->
          (* NOTE need to add to index map *)
          bind.insert ~r:inode.blk_index_map_root ~k:i ~v:r >>= fun ropt ->
          match ropt with
          | None -> return (r,blk)
          | Some blk_index_map_root -> 
            set_state {inode with blk_index_map_root} >>= fun () ->
            return (r,blk))
      | Some blk_id -> 
        dev.read ~blk_id >>= fun blk ->
        return (blk_id,blk))
  in
  let _ = read_blk in
  let alloc_and_write_blk {index=i} blk = 
    with_state (fun ~state:inode ~set_state ->       
      alloc () >>= fun blk_id -> 
      dev.write ~blk_id ~blk >>= fun () ->
      bind.insert ~r:inode.blk_index_map_root ~k:i ~v:blk_id >>= fun opt ->
      match opt with
      | None -> return ()
      | Some blk_index_map_root -> 
        set_state {inode with blk_index_map_root} >>= fun () -> 
        return ())
  in
  let buf_ops = bytes_buf_ops in
  let rewrite_blk_or_alloc {index=_i} (blk_id,blk) = 
    (* FIXME at the moment, this always succeeds FIXME note we don't
       attempt to reinsert into B-tree (concurrent modification by
       another thread may result in unusual behaviour) FIXME maybe
       need another layer about blk_dev, which allows rewrite *)
    (* FIXME if we have not already inserted index -> blk_id into the map, we probably should *)
    (* let blk_id = i |> Blk_id_as_int.of_int in *)
    dev.write ~blk_id ~blk >>= fun () -> return ()
  in
  let int_index_iso = {a_to_b=(fun x -> x); b_to_a=(fun x -> x) } in
  let Iter_block_blit.{ pread;pwrite } = 
    Iter_block_blit.make ~monad_ops ~buf_ops ~blk_ops ~int_index_iso ~read_blk 
      ~alloc_and_write_blk ~rewrite_blk_or_alloc
  in
  let pread ~off ~len =     
    with_state (fun ~state:inode ~set_state:_ ->
      let size = inode.file_size in
      (* don't attempt to read beyond end of file *)
      let len = {len=min len.len (size.size - off.off)} in
      pread_check ~size ~off ~len |> function 
      | Ok () -> pread ~off ~len >>= fun x -> return (Ok x)
      | Error s -> return (Error (Pread_error s)))
  in
  let pwrite ~src ~src_off ~src_len ~dst_off =
    with_state (fun ~state:inode ~set_state ->
      let size = inode.file_size in
      pwrite_check ~buf_ops ~src ~src_off ~src_len ~dst_off |> function
      | Error s -> return (Error (Pwrite_error s))
      | Ok () -> pwrite ~src ~src_off ~src_len ~dst_off >>= fun x -> 
        (* now may need to adjust the size of the file *)
        begin
          let size' = dst_off.off+src_len.len in
          match size' > size.size with
          | true -> set_state {inode with file_size={size=size'}}
          | false -> return ()
        end >>= fun () ->
        return (Ok x)
    )
  in
  { size; truncate; pread; pwrite }
*)
      
    end


    let file_ops
        (usedlist_ops       : (blk_id,t) Usedlist.ops)
        (alloc_via_usedlist : (unit -> (blk_id,t)m))
        (blk_index_map_ops  : (r -> (idx,blk_id,blk_id,t)Btree_ops.t))
        (with_fim           : (blk_id File_im.t,t) with_state)
      : (_,_)file_ops 
      =
      failwith ""

  end (* With_ *)
  
  let file_factory = failwith ""
end

