(** A persistent list that keeps track of blocks that are allocated to
   a particular object. *)

(** NOTE the in-memory state of the usedlist is opaque to us; after
   operations, we check the origin to see if it has changed and then
   possibly flush/barrier/sync *)
module Usedlist = struct

  type 'blk_id origin = 'blk_id Plist_intf.Pl_origin.pl_origin[@@deriving bin_io]

  (* $(PIPE2SH("""sed -n '/usedlist_ops:[ ]/,/}/p' >GEN.usedlist_ops.ml_""")) *)
  (** usedlist_ops: The operations provided by the usedlist; in
     addition we need to integrate the freelist with the usedlist:
     alloc_via_usedlist

      NOTE a sync is just a flush followed by a sync of the underlying
     blkdev, since we assume all object operations are routed to the
     same blkdev *)      
  type ('blk_id,'t) usedlist_ops = {
    add        : 'blk_id -> (unit,'t)m;    
    get_origin : unit -> ('blk_id origin,'t)m;
    flush      : unit -> (unit,'t)m;
  }

  type ('blk_id,'t) ops = ('blk_id,'t) usedlist_ops
end


(* $(PIPE2SH("""sed -n '/^type[ ].*usedlist_factory/,/^>/p' >GEN.usedlist_factory.ml_""")) *)
type ('blk_id,'blk,'t) usedlist_factory = <
  
  (* NOTE we don't implement origin read/write because usedlists are
     always used in conjunction with some other object, which will
     take care of persisting the origin information; this object is
     also in charge of issuing sync calls to the blk dev when
     necessary *)
  
  with_: 
    blk_dev_ops  : ('blk_id,'blk,'t) blk_dev_ops -> 
    barrier      : (unit -> (unit,'t)m) -> 
    freelist_ops : ('blk_id,'t) blk_allocator_ops -> 
    <
      usedlist_ops : 'blk_id Usedlist.origin -> (('blk_id,'t) Usedlist.ops,'t)m;
      
      alloc_via_usedlist : 
        ('blk_id,'t) Usedlist.ops ->         
        ('blk_id,'t)blk_allocator_ops;
            
    >;

>

module type S = sig
  type blk = ba_buf
  type buf = ba_buf
  type blk_id = Shared_ctxt.r
  type r = Shared_ctxt.r
  type t
  val monad_ops   : t monad_ops

  (** For the usedlist *)
  type a = blk_id
  val plist_factory : (a,blk_id,blk,buf,t) Plist_intf.plist_factory

end

module type T = sig
  module S : S
  open S
  val usedlist_factory : (blk_id,blk,t) usedlist_factory
end


(** Version with full sig *)
module Make_v1(S:S) = struct
  
  module S = S
  open S

  let ( >>= ) = monad_ops.bind
  let return = monad_ops.return 


  module With_(S2:sig
      val blk_dev_ops  : (blk_id,blk,t) blk_dev_ops
      val barrier      : (unit -> (unit,t)m)
      (* val sync         : (unit -> (unit,t)m) *)
      val freelist_ops : (blk_id,t) blk_allocator_ops
    end) 
  = 
  struct
    open S2

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

    let export = object
      method usedlist_ops = usedlist_ops
      method alloc_via_usedlist = alloc_via_usedlist
    end
  end  

  let with_ ~blk_dev_ops ~barrier ~freelist_ops =
    let module X = struct
      let blk_dev_ops = blk_dev_ops
      let barrier = barrier
      (* let sync = sync *)
      let freelist_ops = freelist_ops
    end
    in
    let module W = With_(X) in
    W.export

  let usedlist_factory : _ usedlist_factory = object
    method with_ = with_
  end

end


(** With restricted sig *)
module Make_v2(S:S) : T with module S=S = struct
  include Make_v1(S)
end


let usedlist_example = 
  let module S = struct
    type blk = Shared_ctxt.blk      
    type buf = Shared_ctxt.buf
    type blk_id = Shared_ctxt.r
    type r = Shared_ctxt.r
    type t = Shared_ctxt.t
    let monad_ops = Shared_ctxt.monad_ops

    type a = blk_id
    let plist_factory : (a,blk_id,blk,buf,t) Plist_intf.plist_factory =
      Tjr_plist.pl_examples#for_blk_id
  end
  in
  let module X = Make_v2(S) in
  X.usedlist_factory

let _ = usedlist_example
