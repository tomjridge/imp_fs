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

type stat_times = Minifs_intf.times[@@deriving bin_io]

module Dir_origin = struct
  (* $(PIPE2SH("""sed -n '/type[ ].*dir_origin =/,/}/p' >GEN.dir_origin.ml_""")) *)
  type ('blk_id,'did) dir_origin = {
    dir_map_root    : 'blk_id;
    usedlist_origin : 'blk_id Usedlist.origin;
    parent          : 'did;
    stat_times      : stat_times;
  }[@@deriving bin_io]

  type ('blk_id,'did) t = ('blk_id,'did) dir_origin[@@deriving bin_io]
end

module Dir_ops = struct
  (* $(PIPE2SH("""sed -n '/type[ ].*dir_ops =/,/}/p' >GEN.dir_ops.ml_""")) *)
  type ('k,'v,'r,'t,'did) dir_ops = {
    find     : 'k -> ('v option,'t) m;
    insert   : 'k -> 'v -> (unit,'t) m;
    delete   : 'k -> (unit,'t) m;

    ls_create: unit -> (('k,'v,'t)Tjr_btree.Btree_intf.ls,'t)m;
    set_parent: 'did -> (unit,'t)m;
    get_parent: unit -> ('did,'t)m;
    set_times : stat_times -> (unit,'t)m;
    get_times : unit -> (stat_times,'t)m;    

    flush    : unit -> (unit,'t)m;
    sync     : unit -> (unit,'t)m;    

    get_origin: unit -> (('r,'did)Dir_origin.t,'t)m;
  }
  type ('k,'v,'r,'t,'did) t = ('k,'v,'r,'t,'did) dir_ops
end

(** What we keep in memory to represent a dir (in addition to the used
   list and the dir map) *)
module Dir_im = struct
  type 'did dir_im = {
    parent : 'did;
    stat_times : stat_times;
  }
end
open Dir_im

(* $(PIPE2SH("""sed -n '/NOTE[ ].de stands for/,/^>/p' >GEN.dir_factory.ml_""")) *)
(** NOTE 'de stands for dir_entry *)
type ('blk_id,'blk,'de,'t,'did) dir_factory = <
  read_origin: 
    blk_dev_ops : ('blk_id,'blk,'t) blk_dev_ops -> 
    blk_id : 'blk_id -> 
    (('blk_id,'did) Dir_origin.t,'t)m;

  write_origin:
    blk_dev_ops : ('blk_id,'blk,'t) blk_dev_ops -> 
    blk_id : 'blk_id -> 
    origin: ('blk_id,'did) Dir_origin.t -> 
    (unit,'t)m;
    
  with_: 
    blk_dev_ops  : ('blk_id,'blk,'t) blk_dev_ops -> 
    barrier      : (unit -> (unit,'t)m) -> 
    sync         : (unit -> (unit,'t)m) -> 
    freelist_ops : ('blk_id,'blk_id,'t) Freelist_intf.freelist_ops -> 
    <    
      usedlist_ops : 'blk_id Usedlist.origin -> (('blk_id,'t) Usedlist.ops,'t)m;

      alloc_via_usedlist : 
        ('blk_id,'t) Usedlist.ops ->         
        ('blk_id,'t) blk_allocator_ops;
      
      (* NOTE this doesn't update the origin when things change *)
      mk_dir : 
        usedlist   : ('blk_id,'t) Usedlist.ops ->
        btree_root : 'blk_id ->
        with_dir   : ('did dir_im,'t)with_state -> 
        (str_256,'de,'blk_id,'t,'did)Dir_ops.t;


      (* NOTE this has nothing to do with the GOM, so no name, not
         added to parent etc *)
      create_dir : 
        parent:'did -> times:stat_times -> ('blk_id,'t)m;
      (** Returns the 'blk_id of the origin blk; NOTE this has nothing
         to do with the GOM, and does not add the entry to the parent
         directory (no way to access the parent directory here) *)


      (* Convenience *)
       
      dir_add_autosync : 
        'blk_id -> 
        (str_256,'de,'blk_id,'t,'did)Dir_ops.t -> 
        (str_256,'de,'blk_id,'t,'did)Dir_ops.t;
      (** Wrap the ops with code that automatically updates the origin *)
      
      (* NOTE the following functions include autosync *)

      dir_from_origin_blk : 
        ('blk_id*('blk_id,'did) Dir_origin.t) -> ((str_256,'de,'blk_id,'t,'did)Dir_ops.t,'t)m;
      
      dir_from_origin: 'blk_id -> ((str_256,'de,'blk_id,'t,'did)Dir_ops.t,'t)m;
    >;  
>


module type S = sig
  type blk = ba_buf
  type blk_id = Shared_ctxt.r
  type r = Shared_ctxt.r
  type did
  type t
  val monad_ops : t monad_ops
  (* FIXME two different versions of buf_ops *)

  val dir_origin_mshlr: (blk_id,did) Dir_origin.t ba_mshlr

  type dir_entry
  val de_mshlr: dir_entry bp_mshlr

  val usedlist_factory : (blk_id,blk,t) usedlist_factory

  type ls

  (** NOTE this is the type for btree_factory#uncached; FIXME could
     just assume btree_factory *)
  val uncached : 
    blk_dev_ops     : (r, blk, t) blk_dev_ops -> 
    blk_alloc       : (r, t) blk_allocator_ops -> 
    init_btree_root : r -> 
    <
      get_btree_root  : unit -> (r,t) m;
      map_ops_with_ls : (str_256,dir_entry,r,ls,t) Tjr_btree.Btree_intf.map_ops_with_ls
    >
    
  (** NOTE from btree_factory *)
  val write_empty_leaf:    
    blk_dev_ops : (r, blk, t) blk_dev_ops -> 
    blk_id : r -> 
    (unit,t)m

end

module type T = sig
  module S : S
  open S
  val dir_factory : (blk_id,blk,dir_entry,t,did) dir_factory
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
      val freelist_ops : (blk_id,blk_id,t) Freelist_intf.freelist_ops
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
        ~(btree_root : 'blk_id)
        ~(with_dir   : ('did dir_im,'t)with_state) 
      : _ Dir_ops.t
      =
      (* NOTE we need to alloc via the used list *)
      let blk_alloc = alloc_via_usedlist usedlist in
      let uncached = S.uncached
          ~blk_dev_ops
          ~blk_alloc
          ~init_btree_root:btree_root
      in
      (* let get_btree_root = uncached#get_btree_root in *)
      let ops (* map_ops_with_ls *) = uncached#map_ops_with_ls in
      let find k = ops.find ~k in
      let insert k v = ops.insert ~k ~v in
      let delete k = ops.delete ~k in
      
      let ls_create 
        : unit -> (('k,'v,'t)Tjr_btree.Btree_intf.ls,'t)m
        =
        fun () ->
          Tjr_btree.Btree_intf.ls2object ~monad_ops ~leaf_stream_ops:ops.leaf_stream_ops
            ~get_r:uncached#get_btree_root ()          
      in

      let get_parent () = 
        with_dir.with_state (fun ~state ~set_state:_ ->
            return state.parent)
      in

      let set_parent parent = 
        with_dir.with_state (fun ~state ~set_state ->
            set_state {state with parent})
      in

      let get_times () = 
        with_dir.with_state (fun ~state ~set_state:_ ->
            return state.stat_times)
      in

      let set_times stat_times =
        with_dir.with_state (fun ~state ~set_state ->
            set_state {state with stat_times})
      in

      let get_origin () = 
        uncached#get_btree_root () >>= fun dir_map_root -> 
        usedlist.get_origin () >>= fun usedlist_origin ->
        with_dir.with_state (fun ~state ~set_state:_ ->
            let { parent; stat_times } = state in
            return Dir_origin.{dir_map_root;usedlist_origin;parent;stat_times}
          )
      in

      let flush () = 
        (* Flush the usedlist *)
        usedlist.flush() >>= fun () ->
        (* NOTE the dir_map is uncached currently *)
        S2.barrier() >>= fun () ->
        (* $(FIXME("""the B-tree should probably also have a flush and
           sync (for when we use the cached version)""")) *)
        return ()
      in

      (* NOTE this should be overridden when wrapping using autosync *)
      let sync =
        let msg = lazy(Printf.printf "%s: WARNING! attempt to use \
                                      no-op sync; likely something is \
                                      wrong" __FILE__) in
        fun () -> Lazy.force msg; return ()
      in
      Dir_ops.{ 
        find; insert; delete; flush; sync; ls_create;
        get_parent; set_parent; get_times; set_times;
        get_origin
      }
     

    let create_dir ~parent ~times =
      (* create a usedlist and a B-tree, and then an origin blk; NOTE
         that all blks involved are stored in the usedlist, except for
         the blocks that constitute the usedlist itself *)

      (* usedlist *)
      usedlist_factory'#create () >>= fun ul_ops ->
      let alloc_via_usedlist = alloc_via_usedlist ul_ops in

      (* B-tree *)
      (* NOTE we allocate from the system freelist rather than via usedlist *)
      alloc_via_usedlist.blk_alloc () >>= fun btree_root -> 
      S.write_empty_leaf ~blk_dev_ops ~blk_id:btree_root >>= fun () ->

      (* Origin *)
      alloc_via_usedlist.blk_alloc () >>= fun blk_id -> 

      ul_ops.get_origin () >>= fun usedlist_origin -> 

      let origin = Dir_origin.{
          dir_map_root=btree_root;
          usedlist_origin;
          parent;
          stat_times=times
        }
      in
      write_origin ~blk_dev_ops ~blk_id ~origin >>= fun () ->
      return blk_id
      

    let wrap (type a) ~sync_origin ~(dir_ops:_ Dir_ops.t) (f:unit -> (a,t)m) = 
      let Dir_ops.{ get_origin; _ } = dir_ops in
      get_origin () >>= fun o1 -> 
      f () >>= fun r -> 
      get_origin () >>= fun o2 -> 
      (match o1=o2 with
       | true -> return ()
       | false -> sync_origin o2) >>= fun () -> 
      return r

    let dir_add_autosync blk_id (dir_ops:_ Dir_ops.t) =
      let sync_origin o = write_origin ~blk_dev_ops ~blk_id ~origin:o in
      let wrap x = wrap ~sync_origin ~dir_ops x in
      let Dir_ops.{ 
          find; insert; delete; flush; sync=_; ls_create;
          get_parent; set_parent; get_times; set_times;
          get_origin } = dir_ops 
      in
      Dir_ops.{
        find=(fun k -> wrap (fun () -> find k)); 
        insert=(fun k v -> wrap (fun () -> insert k v)); 
        delete=(fun k -> wrap (fun () -> delete k)); 
        flush=(fun () -> wrap (fun () -> flush ()));
        sync=(fun () -> flush () >>= fun () -> 
               (* $(FIXME("""need sync for btree and usedlist""")) *)
               get_origin () >>= fun o -> 
               sync_origin o >>= fun () ->
               (* FIXME S2.barrier? or S2.sync? *)
               S2.sync ());
       ls_create;
       get_parent;
       set_parent=(fun p -> wrap (fun () -> set_parent p));
       get_times;
       set_times=(fun t -> wrap (fun () -> set_times t));
       get_origin}
        

    let dir_from_origin_blk (blk_id,(origin: _ Dir_origin.t)) = 
      let Dir_origin.{dir_map_root;usedlist_origin;parent;stat_times} = origin in
      usedlist_ops usedlist_origin >>= fun usedlist ->
      let dir_im = Dir_im.{parent;stat_times} in
      let with_dir = Tjr_monad.with_imperative_ref ~monad_ops (ref dir_im) in
      let dir_ops = mk_dir ~usedlist ~btree_root:dir_map_root ~with_dir in
      let dir_ops' = dir_add_autosync blk_id dir_ops in
      return dir_ops'
                  
    let dir_from_origin blk_id =
      read_origin ~blk_dev_ops ~blk_id >>= fun origin -> 
      dir_from_origin_blk (blk_id,origin)        

    let export = object
      method usedlist_ops = usedlist_ops
      method alloc_via_usedlist = alloc_via_usedlist
      method mk_dir = mk_dir
      method create_dir = create_dir
      method dir_add_autosync = dir_add_autosync
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

  type ('fid,'did,'sid) dir_entry' = 
      Fid of 'fid | Did of 'did | Sid of 'sid[@@deriving bin_io]
  type dir_entry = (fid,did,sid)dir_entry'[@@deriving bin_io]                  
  type t = dir_entry[@@deriving bin_io]

  let dir_entry_to_int = function
    | Did did -> did
    | Fid fid -> fid
    | Sid sid -> sid

end

let dir_example = 
  let module S = struct
    type blk = ba_buf
    type blk_id = Shared_ctxt.r
    type r = Shared_ctxt.r[@@deriving bin_io]
    type t = Shared_ctxt.t
    type did = Dir_entry.did[@@deriving bin_io]

    let monad_ops = Shared_ctxt.monad_ops

    let dir_origin_mshlr : (blk_id,did) Dir_origin.t ba_mshlr = (
        let module X = struct
          open Blk_id_as_int
          type t = (blk_id,did) Dir_origin.t[@@deriving bin_io]
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

    let write_empty_leaf = s256_de_factory#write_empty_leaf
                     
  end
  in
  let module X = Make_v2(S) in
  X.dir_factory

let _ = dir_example


