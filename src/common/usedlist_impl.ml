(** A persistent list that keeps track of blocks that are allocated to
   a particular object. *)

module Usedlist = Fv2_types.Usedlist

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
