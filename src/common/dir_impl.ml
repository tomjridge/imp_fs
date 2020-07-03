(** A simple directory implementation.

Directories are one of the main objects we deal with.

We implement a directory as a map from name (string, max length 256
   bytes) to entry (file, directory or symlink). Of course, this uses
   an underlying B-tree.

Any used blocks are recorded in a per-directory-object used
   list. Currently, blocks are only reclaimed when the directory is
   deleted. Later, we plan to implement cleaning of directories that
   are still live, to free up blocks that are no longer used.

We reuse the usedlist implementation from {!File_impl_v2}.

A directory is really a thin layer over a B-tree, interfacing with the
   used list, and handling the origin block.

*)

module Usedlist = Fv2_types.Usedlist
open Usedlist_impl

module Dir_origin = struct
  (* $(PIPE2SH("""sed -n '/type[ ].*dir_origin =/,/}/p' >GEN.dir_origin.ml_""")) *)
  type 'blk_id dir_origin = {
    dir_map_root: 'blk_id;
    usedlist_origin: 'blk_id Usedlist.origin;
  }[@@deriving bin_io]

  type 'blk_id t = 'blk_id dir_origin[@@deriving bin_io]
end

module Dir = struct
  (* $(PIPE2SH("""sed -n '/type[ ].*dir_ops =/,/}/p' >GEN.dir_ops.ml_""")) *)
  type ('k,'v,'r,'t) dir_ops = {
    find     : 'k -> ('v option,'t) m;
    insert   : 'k -> 'v -> (unit,'t) m;
    delete   : 'k -> (unit,'t) m;
    flush    : unit -> (unit,'t)m;
    sync     : unit -> (unit,'t)m;
  }
  type ('k,'v,'r,'t) t = ('k,'v,'r,'t) dir_ops
end



(* $(PIPE2SH("""sed -n '/NOTE[ ].de stands for/,/^>/p' >GEN.dir_factory.ml_""")) *)
(** NOTE 'de stands for dir_entry *)
type ('blk_id,'blk,'de,'t) dir_factory = <
  read_origin: 
    blk_dev_ops : ('blk_id,'blk,'t) blk_dev_ops -> 
    blk_id : 'blk_id -> 
    ('blk_id Dir_origin.t,'t)m;

  write_origin:
    blk_dev_ops : ('blk_id,'blk,'t) blk_dev_ops -> 
    blk_id : 'blk_id -> 
    origin: 'blk_id Dir_origin.t -> 
    (unit,'t)m;
    
  with_: 
    blk_dev_ops  : ('blk_id,'blk,'t) blk_dev_ops -> 
    barrier      : (unit -> (unit,'t)m) -> 
    sync         : (unit -> (unit,'t)m) -> 
    freelist_ops : ('blk_id,'t) blk_allocator_ops -> 
    <    
      usedlist_ops : 'blk_id Usedlist.origin -> (('blk_id,'t) Usedlist.ops,'t)m;

      alloc_via_usedlist : 
        ('blk_id,'t) Usedlist.ops ->         
        ('blk_id,'t)blk_allocator_ops;
      
      mk_dir : 
        usedlist     : ('blk_id,'t) Usedlist.ops ->        
        dir_map_root : 'blk_id -> 
        origin       : 'blk_id ->
        (str_256,'de,'blk_id,'t)Dir.t;

      (* Convenience *)
      
      dir_from_origin_blk : 
        ('blk_id*'blk_id Dir_origin.t) -> ((str_256,'de,'blk_id,'t)Dir.t,'t)m;

      dir_from_origin: 'blk_id -> ((str_256,'de,'blk_id,'t)Dir.t,'t)m;
    >;  
>


module type S = sig
  type blk = ba_buf
  type blk_id = Shared_ctxt.r
  type r = Shared_ctxt.r
  type t
  val monad_ops : t monad_ops
  (* FIXME two different versions of buf_ops *)

  val dir_origin_mshlr: blk_id Dir_origin.t ba_mshlr

  type dir_entry
  val de_mshlr: dir_entry bp_mshlr

  val usedlist_factory : (blk_id,blk,t) usedlist_factory

  type ls

  (** NOTE this is the type for btree_factory#uncached *)
  val uncached : 
    blk_dev_ops     : (r, blk, t) blk_dev_ops -> 
    blk_alloc       : (r, t) blk_allocator_ops -> 
    init_btree_root : r -> 
    <
      get_btree_root  : unit -> (r,t) m;
      map_ops_with_ls : (str_256,dir_entry,r,ls,t) Tjr_btree.Btree_intf.map_ops_with_ls
    >
end

module type T = sig
  module S : S
  open S
  val dir_factory : (blk_id,blk,dir_entry,t) dir_factory
end


(** Make with full sig *)
module Make_v1(S:S) = struct
  module S = S
  open S

  let ( >>= ) = monad_ops.bind
  let return = monad_ops.return 

  let buf_ops = ba_buf_ops

  module Dir_origin_mshlr = (val dir_origin_mshlr)

  
  let read_origin ~blk_dev_ops ~blk_id =
    blk_dev_ops.read ~blk_id >>= fun buf -> 
    Dir_origin_mshlr.unmarshal buf |> return
  
  let write_origin ~blk_dev_ops ~blk_id ~origin =
    origin |> Dir_origin_mshlr.marshal |> fun buf -> 
    assert( (buf_ops.len buf) = (blk_dev_ops.blk_sz|>Blk_sz.to_int)); 
    blk_dev_ops.write ~blk_id ~blk:buf

  module With(S2:sig
      val blk_dev_ops  : (blk_id,blk,t) blk_dev_ops
      val barrier      : (unit -> (unit,t)m)
      val sync         : (unit -> (unit,t)m)
      val freelist_ops : (blk_id,t) blk_allocator_ops      
    end) 
  = 
  struct
    open S2
    let usedlist_factory' = usedlist_factory#with_ 
        ~blk_dev_ops
        ~barrier
        ~freelist_ops

    let usedlist_ops  : (_ Usedlist.origin) -> ((r,t)Usedlist.ops,t)m =
      usedlist_factory'#usedlist_ops
        
    let alloc_via_usedlist = usedlist_factory'#alloc_via_usedlist

    let mk_dir 
        ~(usedlist     : ('blk_id,'t) Usedlist.ops)        
        ~(dir_map_root : 'blk_id)
        ~(origin       : 'blk_id)
      : _ Dir.t
      =
      (* NOTE we need to alloc via the used list *)
      let blk_alloc = alloc_via_usedlist usedlist in
      let uncached = S.uncached
          ~blk_dev_ops
          ~blk_alloc
          ~init_btree_root:dir_map_root
      in
      let get_btree_root = uncached#get_btree_root in
      let ops (* map_ops_with_ls *) = uncached#map_ops_with_ls in
      let find k = ops.find ~k in
      let insert k v = ops.insert ~k ~v in
      let delete k = ops.delete ~k in
      let flush () = 
        (* Flush the usedlist *)
        usedlist.flush() >>= fun () ->
        (* NOTE the dir_map is uncached currently *)
        S2.barrier() >>= fun () ->
        usedlist.get_origin () >>= fun usedlist_origin ->
        get_btree_root () >>= fun dir_map_root ->
        write_origin ~blk_dev_ops ~blk_id:origin 
          ~origin:Dir_origin.{usedlist_origin;dir_map_root} >>= fun () ->
        S2.barrier()
          
      in
      let sync () = 
        flush () >>= fun () ->
        S2.sync()
      in
      Dir.{ find; insert; delete; flush; sync }
     
    let dir_from_origin_blk (blk_id,(origin: _ Dir_origin.t)) = 
      let usedlist_origin = origin.usedlist_origin in
      usedlist_ops usedlist_origin >>= fun usedlist ->
      let dir_map_root = origin.dir_map_root in
      let origin = blk_id in
      return (mk_dir ~usedlist ~dir_map_root ~origin)

    let dir_from_origin blk_id =
      read_origin ~blk_dev_ops ~blk_id >>= fun origin -> 
      dir_from_origin_blk (blk_id,origin)

    let export = object
      method usedlist_ops = usedlist_ops
      method alloc_via_usedlist = alloc_via_usedlist
      method mk_dir = mk_dir
      method dir_from_origin_blk = dir_from_origin_blk
      method dir_from_origin = dir_from_origin
    end
  end (* With *)

  let with_ ~blk_dev_ops ~barrier ~sync ~freelist_ops =
    let module X = struct
      let blk_dev_ops = blk_dev_ops
      let barrier = barrier
      let sync = sync
      let freelist_ops = freelist_ops
    end
    in 
    let module W = With(X) in
    W.export

  let _ = with_

  let dir_factory : _ dir_factory = object
    method read_origin=read_origin
    method write_origin=write_origin
    method with_=with_
  end
  
end

(** Make with restricted sig *)
module Make_v2(S:S) : T with module S=S = struct
  include Make_v1(S)
end

module Dir_entry = struct
  open Bin_prot.Std
  type fid = int[@@deriving bin_io]
  type did = int[@@deriving bin_io]
  type sid = int[@@deriving bin_io] 

  type dir_entry = Fid of fid | Did of did | Sid of sid[@@deriving bin_io]                  
  type t = dir_entry[@@deriving bin_io]
end

let dir_example = 
  let module S = struct
    type blk = ba_buf
    type blk_id = Shared_ctxt.r
    type r = Shared_ctxt.r[@@deriving bin_io]
    type t = Shared_ctxt.t
    let monad_ops = Shared_ctxt.monad_ops

    let dir_origin_mshlr : blk_id Dir_origin.t ba_mshlr = (
        let module X = struct
          open Blk_id_as_int
          type t = blk_id Dir_origin.t[@@deriving bin_io]
          let max_sz = 256 (* FIXME check this*)
        end
        in
        let bp_mshlr : _ bp_mshlr = (module X) in
        bp_mshlrs#ba_mshlr ~mshlr:bp_mshlr ~buf_sz:(Shared_ctxt.blk_sz |> Blk_sz.to_int))

    type dir_entry = Dir_entry.t[@@deriving bin_io]
    
    let de_mshlr : dir_entry bp_mshlr = (
        let module X = struct
          type t = dir_entry[@@deriving bin_io]
          let max_sz = 10
        end
        in
        (module X))

    let usedlist_factory : (blk_id,blk,t) usedlist_factory = 
      Usedlist_impl.usedlist_example

    (* now use the B-tree factory to get hold of the ls type *)

    module S256_de = Tjr_btree.Make_6.Make_v2(struct
        type k = str_256
        type v = dir_entry
        type r = Shared_ctxt.r
        type t = Shared_ctxt.t
        let k_cmp: k -> k -> int = 
          (* FIXME add compare to Str_256 module *)
          fun x y -> Stdlib.compare (x :> string) (y :> string)
        let monad_ops = Shared_ctxt.monad_ops
        let k_mshlr = bp_mshlrs#s256_mshlr
        let v_mshlr = de_mshlr
        let r_mshlr = bp_mshlrs#r_mshlr

        let blk_sz = Shared_ctxt.blk_sz

        let k_size = let module X = (val k_mshlr) in X.max_sz
        let v_size = let module X = (val v_mshlr) in X.max_sz
        let cs = Tjr_btree.Bin_prot_marshalling.make_constants ~blk_sz ~k_size ~v_size
        let r_cmp = Shared_ctxt.r_cmp
      end)

    type ls = S256_de.ls

    let s256_de_factory = S256_de.btree_factory
                            
    let uncached = s256_de_factory#uncached
                     
  end
  in
  let module X = Make_v2(S) in
  X.dir_factory

let _ = dir_example
