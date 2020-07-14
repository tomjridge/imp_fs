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


(** {2 File types} *)

open Int_like

module Usedlist = Usedlist_impl.Usedlist

module Times = Minifs_intf.Times
type stat_times = Times.times[@@deriving bin_io]

(* NOTE now() is currently not modelled monadically *)
let now () = Unix.gettimeofday ()
let update_atim = Times.update_atim (now())
let update_mtim = Times.update_mtim (now())
let new_times () = now() |> fun tim -> Times.{atim=tim;mtim=tim}

(** We reconstruct the file from the contents of the file's origin
   blk; this includes the size, the [blk->index] map root, and the
   pointers for the used list (which is used to implement alloc in
   conjunction with the freelist) *)
module File_origin_block = struct

  open Bin_prot.Std

  (* NOTE Tjr_fs_shared has size but no deriving bin_io; FIXME perhaps it should? *)
  (* type size = {size:int} [@@deriving bin_io] *)

  (* $(PIPE2SH("""sed -n '/type[ ].*file_origin_block =/,/}/p' >GEN.file_origin_block.ml_""")) *)
  type 'blk_id file_origin_block = {
    file_size        : int; (* in bytes of course *)
    times            : stat_times;
    blk_idx_map_root : 'blk_id;
    usedlist_origin  : 'blk_id Usedlist.origin;
  }[@@deriving bin_io]

  type 'blk_id t = 'blk_id file_origin_block[@@deriving bin_io]
  

end
module Fo = File_origin_block


(** State we hold in memory for a particular file *)
module File_im = struct

  type t = {
    file_size: int;
    times : stat_times
  }
  (** The usedlist and blk-idx map are fixed for the lifetime of the
     file, so should be parameters on creation. Other than that, we
     just have a field for file size and a field for the location of
     the origin blk. *)

end


(* type pread_error = Pread_error of string *)
type pread_error = Call_specific_errors.pread_err
(* type pwrite_error = Pwrite_error of string *)
type pwrite_error = Call_specific_errors.pwrite_err


(* $(FIXME("for pwrite, perhaps return unit")) *)

(* $(FIXME("""check that file operations obey the origin/flush/sync behaviour""")) *)

(* $(PIPE2SH("""sed -n '/Standard[ ]file operations/,/^}/p' >GEN.file_ops.ml_""")) *)
(** Standard file operations, pwrite, pread, size and truncate.

NOTE we expect buf to be string for the functional version; for
   mutable buffers we may want to pass the buffer in as a parameter?

NOTE for pwrite, we always return src_len since all bytes are written
   (unless there is an error of course).

For pread, we always return a buffer of length len (assuming off+len
   <= file size).

NOTE origin/flush/sync behaviour: flush forces changes (including
   origin) to disk and issues a barrier; sync ditto, and issues a
   sync; otherwise, changes that affect the origin should be
   accompanied by a barrier -> origin-write -> barrier posthook FIXME
   are they?

*)
type ('blk_id,'buf,'t) file_ops = {
  size     : unit -> (int,'t)m;
  pwrite   : src:'buf -> src_off:offset -> src_len:len -> 
    dst_off:offset -> ((int (*n_written*),pwrite_error)result,'t)m;
  pread    : off:offset -> len:len -> (('buf,pread_error)result,'t)m;
  truncate : size:int -> (unit,'t)m;
  flush    : unit -> (unit,'t)m;
  sync     : unit -> (unit,'t)m;

  (* NOTE get_origin is the only function that exposes the blk_id type *)
  get_origin: unit -> ('blk_id File_origin_block.t,'t)m;
  get_times: unit -> (times,'t)m;
  set_times: times -> (unit,'t)m;
}


open Buffers_from_btree (* FIXME combine with the other buf_ops *)
open Usedlist_impl
open Fv2_types

module Iter_block_blit = Fv2_iter_block_blit
type idx = Iter_block_blit.idx
let idx_mshlr = Iter_block_blit.idx_mshlr 

(* type stat_times = Minifs_intf.times *)

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
    freelist_ops : ('blk_id,'blk_id,'t) Freelist_intf.freelist_ops -> 
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
        usedlist           : ('blk_id,'t) Usedlist.ops -> (* used for? why not just use alloc_via_usedlist? *)
        alloc_via_usedlist : (unit -> ('blk_id,'t)m) ->         
        blk_idx_map        : (int,'blk_id,'blk_id,'t)Btree_ops.t -> 
        file_origin        : 'blk_id ->         
        init_file_size     : int -> 
        init_times         : stat_times ->
        ('blk_id,'buf,'t)file_ops;
      (** (4) *)

      (* NOTE this has nothing to do with the GOM, so not added to
         parent etc; FIXME perhaps we should return file_ops? *)
      create_file : stat_times -> ('blk_id, 't)m;

      (* Convenience *)

      file_from_origin_blk : 
        ('blk_id * 'blk_id File_origin_block.t) -> (('blk_id,'buf,'t)file_ops,'t)m;
      
      file_from_origin : 'blk_id -> (('blk_id,'buf,'t)file_ops,'t)m;
    >
>


(** Check the arguments to pread *)
let pread_check = File_impl_v1.pread_check

(** Check arguments to pwrite *)
let pwrite_check = File_impl_v1.pwrite_check



module type S = sig
  type blk = ba_buf
  type buf = ba_buf
  val blk_sz : blk_sz (** assumed to match buf size *)

  type blk_id = Shared_ctxt.r
  type r = Shared_ctxt.r
  type t
  val monad_ops   : t monad_ops
  val buf_ops     : buf buf_ops  (* FIXME create_zeroed? *)

  
  val usedlist_factory : (blk_id,blk,t) usedlist_factory

  (** For the usedlist *)
  (* type a = blk_id *)
  (* val plist_factory : (a,blk_id,blk,buf,t) Plist_intf.plist_factory *)

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

  (* NOTE from btree_factory *)
  val write_empty_leaf:
    blk_dev_ops : (r, blk, t) blk_dev_ops -> 
    blk_id : r -> 
    (unit,t)m


  val file_origin_mshlr: blk_id File_origin_block.t ba_mshlr
end


module type T = sig
  module S : S
  open S
  val file_factory : (buf,blk,blk_id,t) file_factory
end


[@@@warning "-32"]

(** Version with full sig; NOTE that this is quite specialized to Shared_ctxt *)
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

  (* module U = Usedlist_impl.Make_v2(S) *)
  (* let usedlist_factory = U.usedlist_factory *)


  module Blk_idx = struct
    (* FIXME may be better to just reuse int r map *)
    let blk_sz = S.blk_sz
    module S = struct
      type k = idx
      type v = S.r
      type r = S.r
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
      val freelist_ops : (blk_id,blk_id,t) Freelist_intf.freelist_ops
    end) 
  = 
  struct
    open S2

    (* NOTE unlike a buffer, a blk typically has a fixed size eg 4096 bytes *)        
    let blk_sz = blk_dev_ops.blk_sz |> Blk_sz.to_int

    let usedlist_factory' = usedlist_factory#with_ 
        ~blk_dev_ops
        ~barrier
        ~freelist_ops

    (** Implement the usedlist, using the plist and the global
       freelist. NOTE For the usedlist, we may hold a free block
       temporarily; if it isn't used we can return it immediately to
       the global freelist. *)
    let usedlist_ops  : (_ Usedlist.origin) -> ((r,t)Usedlist.ops,t)m =
      usedlist_factory'#usedlist_ops
        
    let alloc_via_usedlist = usedlist_factory'#alloc_via_usedlist

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
        val init_file_size     : int
        val init_times         : stat_times
      end) 
    = 
    struct

      open S3

      let empty_blk () = buf_ops.of_string (String.make blk_sz (Char.chr 0))
      (* assumes functional blk impl ?; FIXME blk_ops pads string automatically? *)

      let bind = blk_idx_map
      let dev = blk_dev_ops

      let alloc = alloc_via_usedlist

      module Pvt = struct
        let fim = File_im.{ file_size=init_file_size; times=init_times }
        let fim_ref = ref fim
        let with_fim = with_imperative_ref ~monad_ops fim_ref
        let with_fim = with_fim.with_state
      end


      let with_fim = Pvt.with_fim

      let size () = 
        with_fim (fun ~state ~set_state:_ -> 
            return state.file_size)

      (* FIXME wrap this up in a functor, with a private type in result *)
      let make_ba_blk_ops ~blk_sz = { 
        blk_sz;
        of_string=(fun s ->
            assert(String.length s <= Blk_sz.to_int blk_sz);        
            let buf = Bigstring.create (Blk_sz.to_int blk_sz) in
            Bigstring.blit_of_string s 0 buf 0 (String.length s);
            buf);
        to_string=(fun ba -> Bigstring.to_string ba);
        of_bytes=(fun bs ->
            assert(Bytes.length bs = Blk_sz.to_int blk_sz);
            Bigstring.of_bytes bs);
        to_bytes=(fun ba -> 
            Bigstring.to_bytes ba)
      }

      let blk_ops = make_ba_blk_ops ~blk_sz:blk_dev_ops.blk_sz

      let _ = assert (
        let b = blk_ops.blk_sz = blk_dev_ops.blk_sz in
        if b then true else
          (Printf.printf "%s: blk_dev_ops and blk_ops disagree over size: %d %d\n%!"
            __FILE__ (blk_dev_ops.blk_sz |> Blk_sz.to_int) (blk_ops.blk_sz |> Blk_sz.to_int);
           false))
        
      let truncate_blk ~blk ~blk_off = 
        blk |> blk_ops.to_string |> fun blk -> String.sub blk 0 blk_off |> blk_ops.of_string
  
      let truncate ~(size:int) : (unit,'t)m = 
        (* FIXME the following code is rather hacky, to say the least *)
        with_fim (fun ~state ~set_state -> 
            match size >= state.file_size with
            | true -> 
              (* no need to drop blocks *)              
              set_state {file_size=size;times=new_times()} (* FIXME check time updates are correct *)
            | false -> 
              (* FIXME check the maths of this - if size is 0 we may
                 want to have no entries in blk_index *)
              (* drop all blocks after size/blk_sz FIXME would be nice
                 to have "delete from"; perhaps we list the contents
                 of the index_map and remove the relevant keys? or a
                 monadic fold or iteration? *)
              (* also need to zero out the bytes in the final block
                 beyond size.size *)
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
              set_state {file_size=size;times=new_times () })

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
            | Error s -> return (Error `Error_other)) (* FIXME really EINVAL *)
      
      let pwrite ~src ~src_off ~src_len ~dst_off =
        with_fim (fun ~state ~set_state ->
            let size = state.file_size in
            pwrite_check ~buf_ops ~src ~src_off ~src_len ~dst_off |> function
            | Error s -> return (Error `Error_other) (* FIXME really EINVAL *)
            | Ok () -> pwrite ~src ~src_off ~src_len ~dst_off >>= fun x -> 
              (* now may need to adjust the size of the file *)
              begin
                let size' = dst_off.off+src_len.len in
                match size' > size with
                | true -> set_state {file_size=size';times=new_times()}
                | false -> return ()
              end >>= fun () ->
              return (Ok x.size) )

          
      let get_times () =
        with_fim (fun ~state ~set_state:_ -> 
            return state.times)

      let set_times times =
        with_fim (fun ~state ~set_state -> 
            set_state {state with times})

      let get_origin () =
        with_fim (fun ~state ~set_state:_ ->
            usedlist.get_origin () >>= fun usedlist_origin -> 
            blk_idx_map.get_root () >>= fun blk_idx_map_root -> 
            let origin = File_origin_block.{
                file_size=state.file_size;
                times=state.times;
                blk_idx_map_root;
                usedlist_origin }
            in
            return origin)        

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
        get_origin () >>= fun origin ->
        write_origin ~blk_dev_ops ~blk_id:file_origin ~origin >>= fun () ->
        S2.barrier()
        
      let sync () = 
        flush () >>= fun () ->
        S2.sync()

      let file_ops = { size; truncate; pread; pwrite; flush; sync;
                       get_times; set_times; get_origin}
                     
    end (* File_ops *)

    let file_ops
        ~(usedlist           : (blk_id,t) Usedlist.ops)
        ~(alloc_via_usedlist : (unit -> (blk_id,t)m))
        ~(blk_idx_map        : (int,blk_id,blk_id,t)Btree_ops.t)
        ~(file_origin        : blk_id)
        ~(init_file_size     : int)
        ~(init_times         : Times.times)
      : (_,_,_)file_ops 
      =
      let module S = struct
        let usedlist, alloc_via_usedlist, blk_idx_map, file_origin, init_file_size, init_times = 
          usedlist, alloc_via_usedlist, blk_idx_map, file_origin, init_file_size, init_times
      end
      in
      let module X = File_ops(S) in
      X.file_ops

    let create_file times =
      (* initialize usedlist and btree, then origin *)
      usedlist_factory'#create () >>= fun ul_ops ->
      let alloc_via_usedlist' = alloc_via_usedlist ul_ops in
      alloc_via_usedlist'.blk_alloc () >>= fun blk_id ->
      S.write_empty_leaf ~blk_dev_ops ~blk_id >>= fun () ->
      alloc_via_usedlist'.blk_alloc () >>= fun origin_blk_id ->      
      ul_ops.get_origin() >>= fun usedlist_origin -> 
      let origin = File_origin_block.{
          file_size=0;
          times;
          blk_idx_map_root=blk_id;
          usedlist_origin
        }
      in
      write_origin ~blk_dev_ops ~blk_id:origin_blk_id ~origin >>= fun () ->
      return origin_blk_id

    let file_from_origin_blk (file_origin, origin) = 
      let File_origin_block.{ file_size; times; blk_idx_map_root; usedlist_origin } = origin in
      usedlist_ops usedlist_origin >>= fun usedlist ->
      let alloc_via_usedlist = alloc_via_usedlist usedlist in
      let blk_idx_map = mk_blk_idx_map ~usedlist ~btree_root:blk_idx_map_root in
      let file_ops = 
        file_ops 
          ~usedlist 
          ~alloc_via_usedlist:alloc_via_usedlist.blk_alloc
          ~blk_idx_map
          ~file_origin
          ~init_file_size:file_size
          ~init_times:times
      in
      return file_ops

    let file_from_origin blk_id = 
      read_origin ~blk_dev_ops ~blk_id >>= fun origin ->
      file_from_origin_blk (blk_id,origin)

    let export = object 
      method usedlist_ops = usedlist_ops
      method alloc_via_usedlist = alloc_via_usedlist
      method mk_blk_idx_map = mk_blk_idx_map
      method file_ops = file_ops
      method create_file = create_file
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


(** Version with restricted sig *)
module Make_v2(S:S) : T with module S = S = struct
  include Make_v1(S)
end

let file_examples = 
  let mk_example ~blk_sz = 
    let module S = struct
      type blk = ba_buf
      type buf = ba_buf
      let blk_sz = blk_sz
      type blk_id = Shared_ctxt.r
      type r = Shared_ctxt.r
      type t = Shared_ctxt.t
      let monad_ops = Shared_ctxt.monad_ops

      let buf_ops : _ Buffers_from_btree.buf_ops = 
        Buffers_from_btree.Unsafe__ba_buf.buf_ops

      let usedlist_factory = Usedlist_impl.usedlist_example

      let btree_factory = Tjr_btree.Make_6.Examples.int_r_factory

      type _ls = Tjr_btree.Make_6.Examples.Int_r.ls

      let uncached : 
        blk_dev_ops     : (r, blk, t) blk_dev_ops -> 
        blk_alloc       : (r, t) blk_allocator_ops -> 
        init_btree_root : r -> 
        <
          get_btree_root  : unit -> (r,t) m;
          map_ops_with_ls : (int,r,r,_ls,t) Tjr_btree.Btree_intf.map_ops_with_ls
        >
        = btree_factory#uncached

      let write_empty_leaf = btree_factory#write_empty_leaf

      module File_origin_mshlr = struct
        open Blk_id_as_int
        type t = blk_id File_origin_block.t[@@deriving bin_io]
        let max_sz = 256 (* FIXME check *)
      end
      let fom_mshlr : _ bp_mshlr = (module File_origin_mshlr)

      let file_origin_mshlr : blk_id File_origin_block.t ba_mshlr = 
        bp_mshlrs#ba_mshlr ~mshlr:fom_mshlr ~buf_sz:(Shared_ctxt.blk_sz |> Blk_sz.to_int)
        (* FIXME add buf_sz to Shared_ctxt *)

    end
    in
    let module X = Make_v2(S) in
    X.file_factory
  in
  let example_1 = mk_example ~blk_sz:blk_sz_4096 in
  let example_2 = mk_example ~blk_sz:(Blk_sz.of_int 2) in
  object
    method example_1 = example_1
    method example_2 = example_2
  end
(** 
- example_1: (ba_buf, ba_buf, Shared_ctxt.r, Shared_ctxt.t) file_factory (blk_sz 4096)
- example_2: (ba_buf, ba_buf, Shared_ctxt.r, Shared_ctxt.t) file_factory (blk_sz 2, for testing)
*)





(** Test the file_example with 
- monad is Shared_ctxt.t
- blk_dev is in-memory, map from blk_id to blk; barrier and sync are no-ops
- blk_id is blk_id_as_int
- freelist_ops is a dummy counter
- usedlist is a dummy; alloc_via_usedlist uses the usedlist
- blk_idx_map is a ref (to a map from int to blk_id)
- file_origin is a dummy blk_id -1
- file_size is 0 initially

*)
module Test() = struct

    let monad_ops = Shared_ctxt.monad_ops

    let ( >>= ) = monad_ops.bind

    let return = monad_ops.return

    (* we want to use example_2#with_...#file_ops *)

    let example = file_examples#example_2

    let blk_sz = Blk_sz.of_int 2

    type blk_id = Shared_ctxt.blk_id

    type blk = ba_buf
    
    (* FIXME put in-memory blkdev in fs_shared *)
    module Blk_dev_im = struct
      module M = Map.Make(struct
          type t = blk_id
          let compare = Stdlib.compare
        end)
      type t = blk M.t
      let with_blk_dev = Tjr_monad.with_imperative_ref ~monad_ops (ref M.empty)
      let blk_dev_ops ~blk_sz : _ blk_dev_ops = 
        let write = fun ~blk_id ~blk -> 
          with_blk_dev.with_state (fun ~state ~set_state -> 
              set_state (M.add blk_id blk state))
        in
        { blk_sz; write;
          read=(fun ~blk_id -> 
              with_blk_dev.with_state (fun ~state ~set_state:_ -> 
                  return (M.find blk_id state)));
          write_many=(fun xs -> 
              xs |> iter_k (fun ~k xs -> 
                  match xs with
                  | [] -> return ()
                  | (blk_id,blk)::xs -> 
                    write ~blk_id ~blk >>= fun () ->
                    k xs))
        }
    end

    let blk_dev_ops = Blk_dev_im.blk_dev_ops ~blk_sz


    let barrier () = return ()
    let sync () = return ()

    (* FIXME move this example to shared *)
    module Freelist_im = struct
      let freelist_ops : _ Freelist_intf.freelist_ops =
        let min_free = ref 1 in 
        let blk_alloc () = 
          let x = !min_free in 
          incr min_free; return (Blk_id_as_int.of_int x)
        in
        let blk_free _x = return () in
        { alloc=blk_alloc; 
          alloc_many=(fun _ -> failwith __LOC__); 
          free=blk_free; 
          free_many=(fun _ -> failwith __LOC__); 
          get_origin=(fun _ -> failwith __LOC__); 
          sync=(fun _ -> failwith __LOC__); 
        }       
    end
    let freelist_ops = Freelist_im.freelist_ops
    
    let with_ = 
      example#with_
        ~blk_dev_ops
        ~barrier
        ~sync 
        ~freelist_ops

    let file_ops = with_#file_ops

    let _ = file_ops

    (* now need to provide impls of:
        usedlist           : ('blk_id,'t) Usedlist.ops -> (* used for? why not just use alloc_via_usedlist? *)
        alloc_via_usedlist : (unit -> ('blk_id,'t)m) ->         
        blk_idx_map        : (int,'blk_id,'blk_id,'t)Btree_ops.t -> 
        file_origin        : 'blk_id ->         
        file_size          : int -> 
    *)
      

    (* FIXME move this example somewhere *)
    (* usedlist is just a mutable list of blk_id; get_origin returns a dummy or fails *)
    let usedlist () = 
      let xs : 'blk_id list ref = ref [] in
      let add x = xs:=x::!xs; return () in
      let get_origin () = failwith __LOC__ in
      let flush () = return ()
      in
      object 
        method ref_ = xs
        method ops = Usedlist_impl.Usedlist.{add; get_origin; flush }
      end

    let usedlist = (usedlist ())#ops
                     
    let alloc_via_usedlist () = 
      freelist_ops.alloc () >>= fun blk_id -> 
      usedlist.add blk_id >>= fun () -> 
      return blk_id

    module Blk_idx_map = struct
      module M = Map.Make(struct type t = int let compare = Stdlib.compare end)
      type t = blk_id M.t
         
      let ops ~(with_state:(t,lwt) with_state) = 
        let find k = 
          with_state.with_state (fun ~state ~set_state:_ ->
              return (M.find_opt k state))
        in
        let insert k v = 
          with_state.with_state (fun ~state ~set_state ->
              set_state (M.add k v state))
        in
        let delete k = 
          with_state.with_state (fun ~state ~set_state ->
              set_state (M.remove k state))
        in
        (* FIXME add this to stlib map overlay Map_; perhaps consider
           a version that is actually reasonably efficient *)
        let delete_after k = 
          with_state.with_state (fun ~state ~set_state ->
              state |> M.split k |> fun (_,_,gt) -> 
              gt |> M.bindings |> fun xs -> 
              (xs,state) |> iter_k (fun ~k:kont (xs,s) -> 
                  match xs with
                  | [] -> set_state s
                  | (k,_)::xs -> 
                    M.remove k state |> fun s ->
                    kont (xs,s)))
        in
        let _ = delete_after in
        let flush () = return () in
        let get_root () = return (Blk_id_as_int.of_int (-1)) in
        Btree_ops.{ find;insert;delete;delete_after;flush;get_root }

      let empty = M.empty
    end

    let blk_idx_map_ref = ref Blk_idx_map.empty
    let with_state = with_imperative_ref ~monad_ops blk_idx_map_ref
    let blk_idx_map = Blk_idx_map.ops ~with_state
    
    let file_origin = Blk_id_as_int.of_int (-2)

    let init_file_size = 0

    let init_times = new_times()

    let file_ops = file_ops
      ~usedlist
      ~alloc_via_usedlist
      ~blk_idx_map
      ~file_origin
      ~init_file_size
      ~init_times

    let { pread; pwrite; _ } = file_ops

    let buf_ops = Buffers_from_btree.Unsafe__ba_buf.buf_ops

    let run () = 
      Printf.printf "%s: tests starting...\n" __MODULE__;
      (* read from empty file *)
      pread ~off:{off=0} ~len:{len=100} >>= fun (Ok buf) ->
      assert(buf_ops.to_string buf = "");  (* file initially empty *)

      (* write "tom" *)
      let src = buf_ops.of_string "tom" in
      pwrite ~src ~src_off:{off=0} ~src_len:{len=3} ~dst_off:{off=0} >>= fun (Ok _nwritten) ->
      (* Printf.printf "%d\n" size; *)
      pread ~off:{off=0} ~len:{len=3} >>= fun (Ok buf) ->
      assert(
        buf_ops.to_string buf = "tom" || (
          Printf.printf "|%s|\n" (buf_ops.to_string buf); false));

      (* try to read 10 bytes from file *)
      pread ~off:{off=0} ~len:{len=10} >>= fun (Ok buf) ->
      assert(buf_ops.to_string buf = "tom");

      (* make file contain 20xNULL *)
      let src = buf_ops.of_string (String.make 20 '\x00') in
      pwrite ~src ~src_off:{off=0} ~src_len:{len=20} ~dst_off:{off=0} >>= fun (Ok nwritten) ->
      assert(nwritten=20);

      (* write "homas" at off 1 *)
      let src = buf_ops.of_string "thomas" in
      pwrite ~src ~src_off:{off=1} ~src_len:{len=5} ~dst_off:{off=1} >>= fun (Ok _nwritten) ->

      (* read 10 bytes *)
      pread ~off:{off=0} ~len:{len=10} >>= fun (Ok buf) ->
      assert(
        let b = buf_ops.to_string buf = "\x00homas\x00\x00\x00\x00" in
        if b then true else (
          Printf.printf "%s: string comparison failure: %s\n%!" __FILE__ (buf_ops.to_string buf |> String.escaped);
          false));
      Printf.printf "%s: tests end!\n" __MODULE__;
      return ()
      [@@warning "-8"]

end
