(** A simple file implementation; supersedes v1. 

Terminology: (file) block-index map: the map from blk index to blk_id

We implement a file as a map from blk index to blk. The map is
typically implemented by a B-tree or similar.

New blocks are allocated as needed (by default the map is empty, and a
block is only allocated when that block is written to; so files are
"sparse").

The "iterated block read/write" component has been abstracted out
because it appears in a lot of other places.

Concurrency: The backing B-tree is itself a persistent object.

GOM interaction: We treat a file as a separate object; the GOM maps
file id to origin block. For a file impl that uses in-place mutation,
we don't need to modify the GOM if the file changes.


TODO:
- b-tree to implement delete_after (or some alternative approach)
- validate against "normal" file semantics
- ensure the tests are run by some top-level test executable
- implement freeing of blocks after truncate

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

  (* origin_to_fim: 'blk_id File_origin_block.t -> 'blk_id File_im.t; *)

  usedlist_origin : 'blk_id File_origin_block.t -> 'blk_id Usedlist.origin;
  (** (2.1) *)


  with_: 
    blk_dev_ops  : ('blk_id,'blk,'t) blk_dev_ops -> 
    barrier      : (unit -> (unit,'t)m) -> 
    sync         : (unit -> (unit,'t)m) -> 
    freelist_ops : ('blk_id,'t) blk_allocator_ops -> 
    <    
      usedlist_ops : 'blk_id Usedlist.origin -> (('blk_id,'t) Usedlist.ops,'t)m;
      (** (2.2) *)

      alloc_via_usedlist : 
        ('blk_id,'t) Usedlist.ops ->         
        ('blk_id,'t)blk_allocator_ops;
      (** (2.3) Allocate and record in usedlist *)
      
      mk_blk_idx_map  : 
        usedlist: ('blk_id,'t) Usedlist.ops ->        
        btree_root:'blk_id -> 
        (int,'blk_id,'blk_id,'t)Btree_ops.t;
      (** (3.1) *)
      
      file_ops: 
        usedlist           : ('blk_id,'t) Usedlist.ops -> 
        alloc_via_usedlist : (unit -> ('blk_id,'t)m) ->         
        blk_idx_map        : (int,'blk_id,'blk_id,'t)Btree_ops.t -> 
        file_origin        : 'blk_id ->         
        file_size          : int -> 
        ('buf,'t)file_ops;
      (** (4) *)

      (* Convenience *)

      file_from_origin : 
        ('blk_id * 'blk_id File_origin_block.t) -> (('buf,'t)file_ops,'t)m;
      
      file_from_origin_blk : 'blk_id -> (('buf,'t)file_ops,'t)m;
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


  (** For the usedlist *)
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
      get_btree_root  : unit -> (r,t) m;
      map_ops_with_ls : (int,r,r,_ls,t) Tjr_btree.Btree_intf.map_ops_with_ls
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
    
  let usedlist_origin (fo: _ Fo.t) = fo.usedlist_origin

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


  module With_(S2:sig
      val blk_dev_ops  : (blk_id,blk,t) blk_dev_ops
      val barrier      : (unit -> (unit,t)m)
      val sync         : (unit -> (unit,t)m)
      val freelist_ops : (blk_id,t) blk_allocator_ops
    end) 
  = 
  struct
    open S2

    (* NOTE unlike a buffer, a blk typically has a fixed size eg 4096 bytes *)        
    let blk_sz = blk_dev_ops.blk_sz |> Blk_sz.to_int


    (** Implement the usedlist, using the plist and the global
       freelist. NOTE For the usedlist, we may hold a free block
       temporarily; if it isn't used we can return it immediately to
       the global freelist. *)
    let usedlist_ops (uo:_ Usedlist.origin) : ((r,t)Usedlist.ops,t)m  =
      plist_factory#with_blk_dev_ops ~blk_dev_ops ~barrier |> fun x -> 
      x#init#from_endpts uo >>= fun pl -> 
      x#with_ref pl |> fun y -> 
      x#with_state y#with_plist  |> fun (plist_ops:(_,_,_,_)Plist_intf.plist_ops) -> 
      let add r = 
        freelist_ops.blk_alloc () >>= fun nxt -> 
        plist_ops.add ~nxt ~elt:r >>= fun ropt ->
        match ropt with
        | None -> return ()
        | Some nxt -> freelist_ops.blk_free nxt
      in      
      let get_origin () = plist_ops.get_origin () in
      let flush () = return () in
      (* $(FIXME("usedlist flush is a no-op, since plist is uncached ATM")) *)
      return Usedlist.{
          add=add;
          get_origin;
          flush
        }
      
    let alloc_via_usedlist (ul_ops: _ Usedlist.ops) =
      let blk_alloc () = 
        freelist_ops.blk_alloc () >>= fun blk_id -> 
        ul_ops.add blk_id >>= fun () -> 
        barrier() >>= fun () ->
        return blk_id
      in
      let blk_free _r = 
        Printf.printf "Free called on usedlist; currently this is a \
                       no-op; at some point we should reclaim blks \
                       from live objects (at the moment, we reclaim \
                       only when an object such as a file is \
                       completely deleted)";
        return ()
      in
      { blk_alloc; blk_free }

    let mk_blk_idx_map 
        ~(usedlist:_ Usedlist.ops) 
        ~(btree_root: r)
      : (int,r,r,t)Btree_ops.t = 
      (* NOTE we need to alloc via the used list *)
      let blk_alloc = alloc_via_usedlist usedlist in
      let uncached = S.uncached
          ~blk_dev_ops
          ~blk_alloc
          ~init_btree_root:btree_root
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
        delete_after =(fun _k -> failwith "FIXME delete_after"); 
        (* $(FIXME("bt delete_after")) *)
        flush        =(fun () -> return ()); 
        (* NOTE we are using uncached B-tree currently, so nothing to
           flush *)
        get_root
      }

    let _ = mk_blk_idx_map

    [@@@warning "-27"]

    module File_ops(S3:sig
        val usedlist           : (blk_id,t) Usedlist.ops
        val alloc_via_usedlist : (unit -> (blk_id,t)m)
        val blk_idx_map        : (int,blk_id,blk_id,t)Btree_ops.t
        val file_origin        : blk_id
        val file_size          : int
      end) 
    = 
    struct

      open S3

      let empty_blk () = buf_ops.of_string (String.make blk_sz (Char.chr 0))
      (* assumes functional blk impl ?; FIXME blk_ops pads string automatically? *)

      let bind = blk_idx_map
      let dev = blk_dev_ops

      let alloc = alloc_via_usedlist

      let fim = File_im.{ file_size }
      let fim_ref = ref fim
      let with_fim = with_imperative_ref ~monad_ops fim_ref
      let with_fim = with_fim.with_state

      let size () = 
        with_fim (fun ~state ~set_state:_ -> 
            return state.file_size)
  
      let blk_ops = Shared_ctxt.blk_ops
        
      let truncate_blk ~blk ~blk_off = 
        blk |> blk_ops.to_string |> fun blk -> String.sub blk 0 blk_off |> blk_ops.of_string
  
      let truncate ~(size:int) = 
        (* FIXME the following code is rather hacky, to say the least *)
        with_fim (fun ~state:fim ~set_state -> 
            match size >= fim.file_size with
            | true -> 
              (* no need to drop blocks *)              
              set_state {file_size=size}
            | false -> 
              (* FIXME check the maths of this - if size is 0 we may want to have no entries in blk_index *)
              (* drop all blocks after size/blk_sz FIXME would be nice to have
                 "delete from"; perhaps we list the contents of the index_map
                 and remove the relevant keys? or a monadic fold or iteration? *)
              (* also need to zero out the bytes in the final block beyond size.size *)
              let i,blk_off = size / blk_sz, size mod blk_sz in
              bind.delete_after i >>= fun () ->
              (
                match blk_off > 0 with
                | true -> (
                    (* read blk and zero out suffix, then rewrite *)
                    bind.find i >>= function
                    | None -> return ()
                    | Some blk_id -> 
                      dev.read ~blk_id >>= fun blk ->
                      truncate_blk ~blk ~blk_off |> fun blk ->
                      (* FIXME we need a rewrite here *)
                      dev.write ~blk_id ~blk)
                | false -> return ()
              ) >>= fun () -> 
              set_state {file_size })

      (* NOTE the following are setting up the Iter_block_blit module *)
      
      let read_blk (i:int) =
        bind.find i >>= function
        | None -> (
            alloc () >>= fun r -> 
            let blk : blk = empty_blk () in
            (* this ensures that every blk_id corresponds to a blk
               that has been written... which is assumed by the
               testing code? FIXME why assume this? *)
            dev.write ~blk_id:r ~blk >>= fun () ->
            (* NOTE need to add to index map *)
            bind.insert i r >>= fun () ->
            return (r,blk))
        | Some blk_id -> 
          dev.read ~blk_id >>= fun blk ->
          return (blk_id,blk)

      let _ = read_blk

      let alloc_and_write_blk i blk = 
        alloc () >>= fun blk_id -> 
        dev.write ~blk_id ~blk >>= fun () ->
        bind.insert i blk_id 

      (* NOTE at the moment, we do not CoW the blk_dev, so this always
         succeeds in rewriting the block *)
      let rewrite_blk_or_alloc (i:int) (blk_id,blk) = 
        (* FIXME if we have not already inserted i -> blk_id into
           the map, we probably should; but isn't this guaranteed? *)
        dev.write ~blk_id ~blk

      let Fv2_iter_block_blit.{ pread;pwrite } = 
        Fv2_iter_block_blit.make ~monad_ops ~buf_ops ~blk_ops 
          ~read_blk:(fun idx -> read_blk idx.idx)
          ~alloc_and_write_blk:(fun idx blk -> alloc_and_write_blk idx.idx blk)
          ~rewrite_blk_or_alloc:(fun idx x -> rewrite_blk_or_alloc idx.idx x)

      let pread ~off ~len =     
        with_fim (fun ~state ~set_state:_ ->
            let size = state.file_size in
            (* don't attempt to read beyond end of file *)
            let len = {len=min len.len (size - off.off)} in
            pread_check ~size:{size} ~off ~len |> function 
            | Ok () -> pread ~off ~len >>= fun x -> return (Ok x)
            | Error s -> return (Error (Pread_error s)))
      
      let pwrite ~src ~src_off ~src_len ~dst_off =
        with_fim (fun ~state ~set_state ->
            let size = fim.file_size in
            pwrite_check ~buf_ops ~src ~src_off ~src_len ~dst_off |> function
            | Error s -> return (Error (Pwrite_error s))
            | Ok () -> pwrite ~src ~src_off ~src_len ~dst_off >>= fun x -> 
              (* now may need to adjust the size of the file *)
              begin
                let size' = dst_off.off+src_len.len in
                match size' > size with
                | true -> set_state {file_size=size'}
                | false -> return ()
              end >>= fun () ->
              return (Ok x.size) )

      (* $(TERMINOLOGY("""flush doesn't force to disk, it just clears
         the caches down to the blk dev and issues a blk dev
         barrier""")) *)
      (** NOTE this doesn't force to disk, it just clears caches down
         to blk dev and issues a blk_dev barrier *)
      let flush () = 
        (* Flush the usedlist and the blk_idx_map *)
        usedlist.flush() >>= fun () ->
        blk_idx_map.flush () >>= fun () ->
        S2.barrier() >>= fun () -> 

        (* NOTE now update root block *)
        size () >>= fun file_size -> 
        usedlist.get_origin () >>= fun usedlist_origin -> 
        blk_idx_map.get_root () >>= fun blk_idx_map_root -> 
        let origin = File_origin_block.{
            file_size;
            blk_idx_map_root;
            usedlist_origin }
        in
        write_origin ~blk_dev_ops ~blk_id:file_origin ~origin >>= fun () ->
        S2.barrier()
        
      let sync () = 
        flush () >>= fun () ->
        S2.sync()
          
      let file_ops = { size; truncate; pread; pwrite; flush; sync; }

    end

    let file_ops
        ~(usedlist           : (blk_id,t) Usedlist.ops)
        ~(alloc_via_usedlist : (unit -> (blk_id,t)m))
        ~(blk_idx_map        : (int,blk_id,blk_id,t)Btree_ops.t)
        ~(file_origin        : blk_id)
        ~(file_size          : int)
      : (_,_)file_ops 
      =
      let module S = struct
        let usedlist, alloc_via_usedlist, blk_idx_map, file_origin, file_size = 
          usedlist, alloc_via_usedlist, blk_idx_map, file_origin, file_size
      end
      in
      let module X = File_ops(S) in
      X.file_ops

    let file_from_origin (file_origin, origin) = 
      let File_origin_block.{ file_size; blk_idx_map_root; usedlist_origin } = origin in
      usedlist_ops usedlist_origin >>= fun usedlist ->
      let alloc_via_usedlist = alloc_via_usedlist usedlist in
      let blk_idx_map = mk_blk_idx_map ~usedlist ~btree_root:blk_idx_map_root in
      let file_ops = 
        file_ops 
          ~usedlist 
          ~alloc_via_usedlist:alloc_via_usedlist.blk_alloc
          ~blk_idx_map
          ~file_origin
          ~file_size
      in
      return file_ops

    let file_from_origin_blk blk_id = 
      read_origin ~blk_dev_ops ~blk_id >>= fun origin ->
      file_from_origin (blk_id,origin)

    let export = object 
      method usedlist_ops = usedlist_ops
      method alloc_via_usedlist = alloc_via_usedlist
      method mk_blk_idx_map = mk_blk_idx_map
      method file_ops = file_ops
      method file_from_origin = file_from_origin
      method file_from_origin_blk = file_from_origin_blk
    end

  end (* With_ *)

  let with_ ~blk_dev_ops ~barrier ~sync ~freelist_ops =
    let module X = struct
      let blk_dev_ops = blk_dev_ops
      let barrier = barrier
      let sync = sync
      let freelist_ops = freelist_ops
    end
    in
    let module W = With_(X) in
    W.export

  let _ = with_
  
  let file_factory : (_,_,_,_) file_factory = object
    method read_origin = read_origin
    method write_origin = write_origin
    method usedlist_origin = usedlist_origin
    method with_ = with_
  end
end

