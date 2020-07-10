(** An on-disk counter that manages its own persistence *)


type 't counter_ops = {
  alloc : unit -> (int,'t)m;
  sync  : unit -> (unit,'t)m;
}
(** sync is not necessary because the counter takes care of its own
   persistence; sync is for clean shutdown *)


(** On-disk state *)
module Counter_origin = struct
  open Bin_prot.Std
  type counter_origin = {
    min_free: int
  }[@@deriving bin_io]

  module Bp = struct
    type t = counter_origin[@@deriving bin_io]
    let max_sz = 9
  end

  let bp_mshlr : _ bp_mshlr = (module Bp)
  let ba_mshlr = 
    bp_mshlrs#ba_mshlr 
      ~mshlr:bp_mshlr 
      ~buf_sz:(Shared_ctxt.blk_sz|>Blk_sz.to_int)

  include (val ba_mshlr)  
end
type counter_origin = Counter_origin.t
type origin = counter_origin


(** In-memory state; as well as min_free, we retain the
   "last_persisted_min_free", which is actually min_free+delta; we
   only update the on-disk state when min_free exceeds this *)
module Counter_im = struct
  open Bin_prot.Std
  type counter_im = {
    min_free:int;
    last_persisted_min_free:int;
  }[@@deriving bin_io]

  module Bp = struct
    type t = counter_im[@@deriving bin_io]
    let max_sz = 9
  end

  let bp_mshlr : _ bp_mshlr = (module Bp)
  let ba_mshlr = 
    bp_mshlrs#ba_mshlr 
      ~mshlr:bp_mshlr 
      ~buf_sz:(Shared_ctxt.blk_sz|>Blk_sz.to_int)

  include (val ba_mshlr)  
end
type counter_im = Counter_im.t
type counter_state = counter_im

type ('blk_id,'blk,'t) counter_factory = <
  read_origin: 
    blk_dev_ops : ('blk_id,'blk,'t) blk_dev_ops -> 
    blk_id : 'blk_id -> 
    (origin,'t)m;

  write_origin:
    blk_dev_ops : ('blk_id,'blk,'t) blk_dev_ops -> 
    blk_id : 'blk_id -> 
    origin: origin -> 
    (unit,'t)m;
  
  with_:
    blk_dev_ops : ('blk_id,'blk,'t) blk_dev_ops -> 
    sync        : (unit -> (unit,'t)m) ->
    <
      create : blk_id:'blk_id -> min_free:int -> ('t counter_ops,'t)m;
      init_from_disk : 'blk_id -> ('t counter_ops,'t)m;
    >   
>

open Shared_ctxt
    
let example : _ counter_factory = 
  (* We persist only occasionally; to implement this, we record on
     disk that some ids are used when they are actually not; if we
     crash, then these ids are lost *)
  let delta = 1000 in
  let read_origin ~blk_dev_ops ~blk_id =
    blk_dev_ops.read ~blk_id >>= fun blk -> 
    Counter_origin.unmarshal blk|>return    
  in
  let write_origin ~blk_dev_ops ~blk_id ~origin =
    Counter_origin.marshal origin |> fun blk -> 
    blk_dev_ops.write ~blk_id ~blk
  in
  let with_ 
      ~blk_dev_ops 
      ~sync
    =
    let init ~blk_id ~min_free = 
      let last_persisted_min_free = min_free+delta in
      let origin = Counter_origin.{min_free=last_persisted_min_free} in
      write_origin ~blk_dev_ops ~blk_id ~origin >>= fun () ->
      let counter_state : counter_state = {min_free;last_persisted_min_free} in
      let ref_ = ref counter_state in
      let with_state = with_imperative_ref ~monad_ops ref_ in
      let alloc () = 
        with_state.with_state (fun ~state ~set_state ->            
            let Counter_im.{min_free;last_persisted_min_free} = state in
            match min_free >= last_persisted_min_free with
            | true -> 
              assert(min_free=last_persisted_min_free);
              (* update the last persisted *)
              let last_persisted_min_free = last_persisted_min_free+delta in
              write_origin ~blk_dev_ops ~blk_id ~origin:{min_free=last_persisted_min_free} >>= fun () ->
              set_state {min_free=min_free+1;last_persisted_min_free} >>= fun () ->
              return min_free
            | false -> 
              set_state {state with min_free=state.min_free+1} >>= fun () -> 
              return state.min_free)
      in
      let sync () = 
        with_state.with_state (fun ~state ~set_state:_ ->
            write_origin ~blk_dev_ops ~blk_id ~origin:{min_free=state.min_free} >>= fun () ->
            sync ())
      in
      return { alloc; sync }
    in
    let create ~blk_id ~min_free = init ~blk_id ~min_free in
    let init_from_disk blk_id = 
      read_origin ~blk_dev_ops ~blk_id >>= fun o -> 
      create ~blk_id ~min_free:o.min_free
    in
    object 
      method create=create
      method init_from_disk=init_from_disk
    end
  in
  object
    method read_origin=read_origin
    method write_origin=write_origin
    method with_=with_
  end
      
