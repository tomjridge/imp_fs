(** A layer over fl_make_1, to target the factory interface.

FIXME fl_make_1 should target the interface without the updates to
   origin blk; then this layer can add that in

*)


open Plist_intf
open Shared_ctxt
open Freelist_intf

module Pvt_debug = struct
  open Sexplib.Std
  type t = {
    transient: r list;
    min_free: r option;
    disk_thread_active:bool
  }[@@deriving sexp]

  let to_string (x:_ freelist_im) = 
    let Freelist_intf.{transient;min_free;disk_thread_active;_} = x in
    (* only store the beginning and end of the transients... *)
    let transient = 
      if List.length transient > 10 then 
        (List_.take 5 transient)@(transient |> List.rev |> List_.take 5  |> List.rev)
      else
        transient
    in
    {transient;min_free;disk_thread_active} |> sexp_of_t |> Sexplib.Sexp.to_string_hum
                                                              
end


(** Example for blk_ids *)
module Fl_example_1 = struct
  let version = { e2b=(fun x -> x); b2e=(fun x->x) }

  let empty_freelist ~min_free = Freelist_intf.empty_freelist ~min_free

  module M = struct
    (* open Fl_origin *)
    type t = (r,r) Fl_origin.t[@@deriving bin_io, sexp]
    let max_sz = 9*3 + 10 
    (* assume hd,tl,blk_len and min_free option can be marshalled in this many bytes *)

    let origin_to_string (o:t) = 
      o |> sexp_of_t |> Sexplib.Sexp.to_string_hum
  end

  let origin_mshlr = 
    bp_mshlrs#ba_mshlr 
      ~mshlr:(module M) 
      ~buf_sz:(Blk_sz.to_int blk_sz_4096) 

  module Origin_mshlr = (val origin_mshlr) 

  let read_origin ~(blk_dev_ops:(r,blk,_)blk_dev_ops) ~blk_id =
    blk_dev_ops.read ~blk_id >>= fun blk ->       
    Origin_mshlr.unmarshal blk.ba_buf |> fun o -> 
    Printf.printf "%s: read origin %d %s\n%!" __FILE__ (blk_id|>B.to_int) (o|>M.origin_to_string);
    return o

  let write_origin ~(blk_dev_ops:(r,blk,_)blk_dev_ops) ~blk_id ~origin =
    Printf.printf "%s: write origin %d %s\n%!" __FILE__ (blk_id|>B.to_int) (origin|>M.origin_to_string);
    Origin_mshlr.marshal origin |> fun ba_buf ->
    blk_dev_ops.write ~blk_id ~blk:{ba_buf}


  (* util; we read the fl_origin then need to instantiate the plist *)
  let fl_origin_to_pl Fl_origin.{hd;tl;blk_len;_} = Pl_origin.{hd;tl;blk_len}

  module With_(S:sig
      val blk_dev_ops : (r,blk,t)blk_dev_ops
      val barrier : unit -> (unit,t)m
      val sync : unit -> (unit,t)m
      val params : params
    end) = struct
    open S

    let pl_factory = pl_examples#for_blk_id

    let _ : r Pl_type_abbrevs.plist_factory = pl_factory

    let pl_with_ = pl_factory#with_blk_dev_ops ~blk_dev_ops ~barrier

    let plist_ops pl_origin = 
      pl_origin |> fun Pl_origin.{hd;tl;blk_len} -> 
      let open (struct
        let x = pl_with_

        let plist_ops = 
          x#init#from_endpts Plist_intf.Pl_origin.{hd; tl; blk_len} >>= fun pl -> 
          x#with_ref pl |> fun y -> 
          return (x#with_state y#with_plist)
      end)
      in
      plist_ops

    let min_free_alloc =
      fun (r:r) n -> 
      let start = B.to_int r in
      let end_ = start+n in
      (List_.map_range ~f:B.of_int start end_,B.of_int end_)



    let with_plist_ops (plist_ops:(_,_,_,_)plist_ops) = 
      let with_state = fun with_state -> 
        Fl_make_1.make (object
          method monad_ops=monad_ops
          method event_ops=event_ops
          method async=async
          method sync=sync
          method plist_ops=plist_ops
          method with_freelist=with_state
          method version=For_blkids version
          method params=params
          method min_free_alloc=min_free_alloc
        end)
      in
      let with_locked_ref = fun fl ->
        let freelist_ref = ref fl in
        let with_state' = With_lwt.with_locked_ref freelist_ref in
        (* FIXME the following is broken wrt to locking
        (* Add some debugging *)
        let with_state'' = 
          let with_state = with_state'.with_state in
          { with_state=(fun f -> 
                with_state (fun ~state ~set_state -> return (state,set_state)) >>= fun (state,set_state) ->
                f ~state ~set_state:(fun (s:_ freelist_im) -> 
                    Printf.printf "%s: freelist set_state called: %s\n" __FILE__ (Pvt_debug.to_string s);
                    set_state s)) }
        in
        *)
        let freelist_ops = with_state with_state' in
        object
          method freelist_ops=freelist_ops
          method freelist_ref=freelist_ref
        end
      in
      object
        method with_state=with_state
        method with_locked_ref=with_locked_ref
      end

    (** This wraps the freelist operations in an inefficient piece of
       code that checks whether the origin has changed, and if it has,
       it syncs the origin *)
    (* $(FIXME("""consider improving the freelist_ops wrapping; FIXME
       also this is not concurrent safe""")) *)
    let wrap (type a b) ~sync_origin ~(freelist_ops:_ freelist_ops) (f:a -> (b,t)m) = 
      let { get_origin; _ } = freelist_ops in
      fun a -> 
        get_origin () >>= fun o1 -> 
        f a >>= fun r -> 
        get_origin () >>= fun o2 -> 
        (match o1=o2 with
         | true -> return ()
         | false -> sync_origin o2) >>= fun () -> 
        return r
     
    let _ = wrap

    let add_origin_autosync ~origin_blk_id:blk_id freelist_ops =
      let sync_origin o = write_origin ~blk_dev_ops ~blk_id ~origin:o in
      let wrap () = wrap ~sync_origin ~freelist_ops in
      let _ = wrap in
      let { alloc; alloc_many; free; free_many; get_origin; sync } = freelist_ops in
      { alloc=wrap () alloc; alloc_many=wrap () alloc_many; 
        free=wrap () free; free_many=wrap () free_many; get_origin; sync }


    let initialize ~origin:blk_id ~free_blk ~min_free =
      (* initialize free_blk as an empty plist *)
      pl_with_#init#create free_blk >>= fun plist -> 
      let Plist_intf.{hd;tl;blk_len;_} = plist in
      let origin = Fl_origin.{hd;tl;blk_len;min_free } in
      write_origin ~blk_dev_ops ~blk_id ~origin

    type fl_origin' = (r,r) Fl_origin.t[@@deriving sexp]

    (* NOTE this doesn't sync the origin *)
    let restore' blk_id = 
      read_origin ~blk_dev_ops ~blk_id >>= fun origin ->
      Printf.printf "%s: freelist restored: %s\n%!" __FILE__ 
        (origin |> sexp_of_fl_origin' |> Sexplib.Sexp.to_string_hum);
      let fl = empty_freelist ~min_free:origin.min_free in
      let pl_origin = fl_origin_to_pl origin in
      plist_ops pl_origin >>= fun plist_ops ->
      with_plist_ops plist_ops |> fun x -> 
      x#with_locked_ref fl |> return


    let restore ~autosync ~origin:blk_id = 
      Printf.printf "%s: restoring freelist from %d\n%!" __FILE__ (B.to_int blk_id);
      restore' blk_id >>= fun obj -> 
      let freelist_ops = obj#freelist_ops in
      let freelist_ops' = 
        if autosync then 
            add_origin_autosync ~origin_blk_id:blk_id freelist_ops 
        else
          freelist_ops
      in
      return (object
        method freelist_ops=freelist_ops'
        method freelist_ref=obj#freelist_ref
      end)
      

  end (* With_ *)

  let with_ ~blk_dev_ops ~barrier ~sync ~params =
    let module S = struct
      let blk_dev_ops=blk_dev_ops
      let barrier=barrier
      let sync=sync
      let params=params
    end
    in
    let module X = With_(S) in      
    object
      method plist_ops=X.plist_ops
      method with_plist_ops=X.with_plist_ops
      method add_origin_autosync=X.add_origin_autosync
      method initialize=X.initialize
      method restore=X.restore
    end

  let factory : _ freelist_factory = object
    method version=version
    method empty_freelist=empty_freelist
    method read_origin=read_origin
    method write_origin=write_origin
    method fl_origin_to_pl=fl_origin_to_pl
    method with_=with_
  end

end


(* FIXME also add an example for ints *)
let fl_examples = object
  method fl_params_1=object
    method tr_upper=2000
    method tr_lower=1000
    method min_free_alloc_size=500
  end  
  method for_r=Fl_example_1.factory
  method for_int=failwith "FIXME"
end  
