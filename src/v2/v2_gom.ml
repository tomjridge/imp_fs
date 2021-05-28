(** The "global object map" GOM

This is just an instance of Tjr_btree, mapping object id (which we
   identify with dir_entry) to blk_id 

NOTE this is responsible for handling its own synchronization

FIXME this should use allocation via a usedlist

*)

open V2_intf
open Imp_util

let dont_log = false

module Usedlist = V2_usedlist_impl.Usedlist

module Gom_origin_block = struct
  (* open Bin_prot.Std *)

  type 'blk_id t = {
    gom_root        : 'blk_id;
    usedlist_origin : 'blk_id Usedlist.origin;
  }[@@deriving bin_io]

end

(* Local abbrev; not exposed *)
open (struct
  type 'blk_id origin = 'blk_id Gom_origin_block.t
end)


module Gom_ops = struct
  type ('k,'v,'r,'t) t = {
    find     : 'k -> ('v option,'t) m;
    insert   : 'k -> 'v -> (unit,'t) m;
    delete   : 'k -> (unit,'t) m;
    get_root : unit -> ('r,'t)m;
    sync     : unit -> (unit,'t)m;
  }
  (** NOTE sync is just for tidy shutdown *)
end

open (struct
  type ('k,'v,'r,'t) gom_ops = ('k,'v,'r,'t) Gom_ops.t
end)

type ('blk_id,'blk,'t,'de,'gom_ops) gom_factory = <
  (* NOTE 'gom_ops is just an abbrev; don't actually call this function *)
  note_type_abbrev: 'gom_ops -> ('de,'blk_id,'blk_id,'t) gom_ops -> unit;

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
    barrier      : (unit -> (unit,'t)m) -> 
    sync         : (unit -> (unit,'t)m) -> 
    freelist_ops : ('blk_id,'blk_id,'t) Freelist_intf.freelist_ops -> 
    <
      usedlist_ops : 'blk_id Usedlist.origin -> (('blk_id,'t) Usedlist.ops,'t)m;

      alloc_via_usedlist : 
        ('blk_id,'t) Usedlist.ops ->         
        ('blk_id,'t) blk_allocator_ops;
            
      create           : unit -> 
        (<
          gom_ops:'gom_ops;
          origin:'blk_id > ,'t)m;
      (** Construct new gom_ops *)

      init_from_origin : 'blk_id -> 'blk_id origin -> ('gom_ops,'t)m;      
      (** Assuming blk_id holds origin, construct gom_ops *)

      init_from_disk   : 'blk_id -> ('gom_ops,'t)m;
      (** Assuming blk_id holds a gom origin, construct gom_ops *)
    >
>

(** Functor, reasonably abstract, but assumes blk=ba_buf, t=lwt *)
module Make(S:sig
    type blk_id 
    type r = blk_id
    type blk = Shared_ctxt.blk
    type t = lwt
    type dir_entry
    val dir_entry_deriving_sexp : dir_entry deriving_sexp

    type nonrec gom_ops = (dir_entry,blk_id,blk_id,t)gom_ops

    val origin_mshlr : blk_id origin ba_mshlr

    type _leaf 
    type _node
    type _dnode
    type _ls 
    type _wbc
    val btree_factory : 
      (dir_entry,blk_id,blk_id,t,_leaf,_node,_dnode,_ls,blk,_wbc) Tjr_btree.Make_6.btree_factory                        

    val usedlist_factory : (blk_id,blk,t) V2_usedlist_impl.usedlist_factory
    
  end) = struct
  open S

  open Tjr_monad.With_lwt

  
  let entry_to_string  = 
    let module A = (val dir_entry_deriving_sexp) in
    fun (de:dir_entry) ->
      de |> A.sexp_of_t |> Sexplib.Sexp.to_string_hum

  let note_type_abbrev: gom_ops -> (dir_entry,r,r,t)Gom_ops.t -> unit = 
    fun x y -> 
    (* just note that x and y have the same type *)
    let r = ref x in r:=y; ()

  module Origin_mshlr = (val origin_mshlr)

  let read_origin ~(blk_dev_ops:(_,blk,_) blk_dev_ops) ~blk_id = 
    blk_dev_ops.read ~blk_id >>= fun blk -> 
    Origin_mshlr.unmarshal blk.Shared_ctxt.ba_buf |> return
    
  let _ = read_origin

  let write_origin ~(blk_dev_ops:_ blk_dev_ops) ~blk_id ~origin = 
    Origin_mshlr.marshal origin |> fun ba_buf ->
    blk_dev_ops.write ~blk_id ~blk:Shared_ctxt.{ba_buf}

  module With_(W:sig
      val blk_dev_ops  : (blk_id,blk,t) blk_dev_ops
      val barrier      : (unit -> (unit,t)m)
      val sync         : (unit -> (unit,t)m)
      (* val freelist_ops : (blk_id,t) blk_allocator_ops  *)
      val freelist_ops : (blk_id,blk_id,t) Freelist_intf.freelist_ops 
      (** NOTE this should be the global freelist, which we used for the usedlist *)
    end) 
  = 
  struct
    open W

    (* $(FIXME("currently the GOM doesn't free blks")) *)
    let freelist_ops = {freelist_ops with free=(fun _ -> return ()) }
    
    let blk_alloctor = Freelist_intf.freelist_to_blk_allocator freelist_ops

    let usedlist_factory' = usedlist_factory#with_ 
        ~blk_dev_ops
        ~barrier
        ~freelist_ops

    let usedlist_ops  : (_ Usedlist.origin) -> ((r,t)Usedlist.ops,t)m =
      usedlist_factory'#usedlist_ops
        
    let alloc_via_usedlist = usedlist_factory'#alloc_via_usedlist

    let wrap ~get_origin ~write_origin ~sync_blk_dev f () =
      get_origin () >>= fun o1 -> 
      f () >>= fun r -> 
      get_origin () >>= fun o2 -> 
      match o1=o2 with
      | false -> 
        write_origin o2 >>= fun () ->
        sync_blk_dev () >>= fun () ->
        return r
      | true -> 
        return r      
          
    (** Wrap gom_ops with auto-sync. NOTE all exposed gom_ops include
       autosync *)
    let wrap ~get_origin ~write_origin ~sync_blk_dev ~(gom_ops:gom_ops) = 
      let Gom_ops.{ find; insert; delete; get_root; sync } = gom_ops in
      let wrap f = wrap ~get_origin ~write_origin ~sync_blk_dev f () in
      let find k = wrap (fun () -> find k) in
      let insert k v = wrap (fun () -> insert k v) in
      let delete k = wrap (fun () -> delete k) in
      Gom_ops.{ find; insert; delete; get_root; sync }

    (* Assume that blk_id holds origin; construct B-tree using usedlist *)
    let init_from_origin blk_id (origin:_ origin) = 
      usedlist_ops origin.usedlist_origin >>= fun usedlist_ops ->
      let blk_alloc = alloc_via_usedlist usedlist_ops in
      let btree = 
        btree_factory#uncached ~blk_dev_ops ~blk_alloc ~btree_root:(`A origin.gom_root) in
      let Tjr_btree.Btree_intf.Map_ops_with_ls.{ find; insert; delete; _ } = 
        btree#map_ops_with_ls in
      let get_origin () =
        btree#get_btree_root () >>= fun gom_root -> 
        usedlist_ops.get_origin () >>= fun usedlist_origin -> 
        let origin = Gom_origin_block.{ gom_root; usedlist_origin } in
        return origin
      in
      let sync () = 
        get_origin () >>= fun origin -> 
        write_origin ~blk_dev_ops ~blk_id ~origin
      in
      let gom_ops = Gom_ops.{
          find=(fun k -> 
              assert(dont_log || (Printf.printf "GOM find %s\n%!" (entry_to_string k); true));
              find ~k); 
          insert=(fun k v -> insert ~k ~v); 
          delete=(fun k -> delete ~k); 
          get_root=btree#get_btree_root;
          sync;
        }
      in
      (* NOTE we add autosync to gom_ops *)
      let gom_ops' = 
        wrap 
          ~get_origin 
          ~write_origin:(fun origin -> write_origin ~blk_dev_ops ~blk_id ~origin) 
          ~sync_blk_dev:W.sync 
          ~gom_ops
      in 
      return gom_ops'
          
    let create () = 
      (* usedlist *)
      usedlist_factory'#create () >>= fun usedlist_ops ->
      let alloc_via_usedlist = alloc_via_usedlist usedlist_ops in
      
      (* B-tree *)
      alloc_via_usedlist.blk_alloc () >>= fun gom_root -> 
      btree_factory#write_empty_leaf ~blk_dev_ops ~blk_id:gom_root >>= fun () ->
            
      (* Origin *) 
      alloc_via_usedlist.blk_alloc () >>= fun blk_id -> 

      (* FIXME is it better to include the origin blk in the usedlist,
         or not? *)

      usedlist_ops.flush () >>= fun () ->
      usedlist_ops.get_origin () >>= fun usedlist_origin -> 
      let origin = Gom_origin_block.{gom_root; usedlist_origin} in
      (* NOTE the origin block IS included in the usedlist *)
      write_origin ~blk_dev_ops ~blk_id ~origin >>= fun () -> 
      init_from_origin blk_id origin >>= fun gom_ops ->
      return (object
        method gom_ops=gom_ops
        method origin=blk_id
      end)
        
    let init_from_disk blk_id = 
      read_origin ~blk_dev_ops ~blk_id >>= fun origin ->
      init_from_origin blk_id origin
      

    let export = object
      method usedlist_ops = usedlist_ops
      method alloc_via_usedlist = alloc_via_usedlist
      method create = create
      method init_from_origin = init_from_origin
      method init_from_disk = init_from_disk
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


  let gom_factory : _ gom_factory = object
    method note_type_abbrev = note_type_abbrev
    method read_origin = read_origin
    method write_origin = write_origin
    method with_ = with_
  end

end
  
  
module Dir_entry = struct
  open Bin_prot.Std
  open Sexplib.Std
  type t = (int,int,int) dir_entry'[@@deriving bin_io, sexp]
  let max_sz = 9
end


module Pvt = struct
  open Shared_ctxt

  (** We try to fulfil the signature S *)
  module S = struct
    type nonrec blk_id = blk_id
    type r = blk_id
    type blk = Shared_ctxt.blk
    type nonrec t = t
    type dir_entry = Dir_entry.t
    let dir_entry_deriving_sexp : Dir_entry.t deriving_sexp = (module Dir_entry)

    type nonrec gom_ops = (dir_entry,blk_id,blk_id,t)gom_ops

    module Bp = struct
      type t = Shared_ctxt.r Gom_origin_block.t[@@deriving bin_io]
      let max_sz = 9
    end

    let origin_mshlr : blk_id origin ba_mshlr = 
      let open (struct

        let bp_mshlr : _ bp_mshlr = (module Bp)

        let ba_mshlr = bp_mshlrs#ba_mshlr ~mshlr:bp_mshlr ~buf_sz:(blk_sz |> Blk_sz.to_int)

        let origin_mshlr = ba_mshlr
      end)
      in
      origin_mshlr
        

    (* btree *)

    module S = struct
      let k_mshlr : _ bp_mshlr = (module Dir_entry)
      type k = Dir_entry.t[@@deriving bin_io]
      type v = Shared_ctxt.r
      type r = Shared_ctxt.r
      type t = Shared_ctxt.t
      let k_cmp: k -> k -> int = Stdlib.compare
      let monad_ops = Shared_ctxt.monad_ops
      (* let k_mshlr = dir_entry_mshlr *)
      let v_mshlr = bp_mshlrs#r_mshlr
      let r_mshlr = bp_mshlrs#r_mshlr

      let k_size = let module X = (val k_mshlr) in X.max_sz
      let v_size = let module X = (val v_mshlr) in X.max_sz
      let cs = Tjr_btree.Bin_prot_marshalling.make_constants ~blk_sz ~k_size ~v_size
      let r_cmp = Shared_ctxt.r_cmp
    end

    module T = Tjr_btree.Make_6.Make_v2(S)
    (* open T *)
    type _leaf = T.leaf
    type _node = T.node
    type _dnode = (_node,_leaf)Isa_btree.dnode
    type _ls = T.ls
    type _wbc = T.wbc
    
    let btree_factory = T.btree_factory

    let usedlist_factory = V2_usedlist_impl.usedlist_example
  end

  module M = Make(S)

  let gom_factory = M.gom_factory

end

open Shared_ctxt
let gom_example 
  : (blk_id, blk, t, Dir_entry.t, Pvt.S.gom_ops) gom_factory
  = Pvt.gom_factory

(*

  module Ba = (val ba_mshlr)

  let read_origin ~blk_dev_ops ~blk_id = 
    blk_dev_ops.read ~blk_id >>= fun blk -> 
    return (Ba.unmarshal blk)

  let write_origin ~blk_dev_ops ~blk_id ~origin = 
    Ba.marshal origin |> fun blk -> 
    blk_dev_ops.write ~blk_id ~blk

module S = struct
  let k_mshlr : _ bp_mshlr = (module Dir_entry)
  type k = Dir_entry.t[@@deriving bin_io]
  type v = Shared_ctxt.r
  type r = Shared_ctxt.r
  type t = Shared_ctxt.t
  let k_cmp: k -> k -> int = Stdlib.compare
  let monad_ops = Shared_ctxt.monad_ops
  (* let k_mshlr = dir_entry_mshlr *)
  let v_mshlr = bp_mshlrs#r_mshlr
  let r_mshlr = bp_mshlrs#r_mshlr

  let k_size = let module X = (val k_mshlr) in X.max_sz
  let v_size = let module X = (val v_mshlr) in X.max_sz
  let cs = Tjr_btree.Bin_prot_marshalling.make_constants ~blk_sz ~k_size ~v_size
  let r_cmp = Shared_ctxt.r_cmp
end

module T = Tjr_btree.Make_6.Make_v2(S)

(** Use the uncached method from the following to implement the GOM *)
let gom_factory : (Dir_entry.t, S.v, S.r, S.t, T.leaf, T.node,
 (T.node, T.leaf) Isa_btree.dnode, T.ls, T.blk, T.wbc)
Tjr_btree.Make_6.btree_factory
= T.btree_factory

let write_empty_leaf = gom_factory#write_empty_leaf

(** Currently we used the uncached B-tree *)
let uncached = gom_factory#uncached

*)
