(** This is a cache-aware implementation of files, for v3.

We model a file as a partial map from blk_off to blk, together with a
   size in bytes, and file times.

Don't open - too many types with names duplicated elsewhere. *)

(* See also: 

tjr_fs_shared/.../write_back_cache_v3.ml

*)



module Flag = struct
  let dirty = true
  let clean = false
end

type 'a maybe_dirty = 'a * bool
let is_dirty (_v,b) = (b=Flag.dirty)

type ('k,'v,'cache) cache_ops = ('k,'v,'cache) wbc_ops
  
type blk_off = { blk_off:int }

(** Per-file cache entries *)
type ('blk,'cache) f_cache = {         
  times : Times.times maybe_dirty;
  size  : int maybe_dirty;
  data  : 'cache; 
}

(* based file_impl_v2; read and write blocks at block offsets *)
type ('blk_id,'blk,'t) pre_file_ops = {
  size           : unit -> (int,'t)m; (* size in bytes, unrelated to the data cache *)
  truncate       : size:int -> (unit,'t)m;

  get_times      : unit -> (times,'t)m;
  set_times      : times -> (unit,'t)m;

  write_file_blk : blk_off -> 'blk -> (unit,'t)m;
  read_file_blk  : blk_off -> ('blk option,'t)m;

  flush          : unit -> (unit,'t)m;
  sync           : unit -> (unit,'t)m;

}

(** We need a way to write to the lower non-cached file implementation *)
type ('blk,'t) lower_ops = {
  set_size       : int -> (unit,'t)m;
  set_times      : times -> (unit,'t)m;
  write_file_blk : blk_off -> 'blk -> (unit,'t)m;
  read_file_blk  : blk_off -> ('blk option,'t)m; (* NOTE always returns a blk *)
  (* flush - needed? *)
  sync           : unit -> (unit,'t)m;
}

module Make(S: sig
    type t
    val monad_ops: t monad_ops

    type blk
    type cache

    (* if there is no entry, it means we don't know anything; if there
       is a None, we know that the lower map has no entry; otherwise
       we have some actual block in the cache *)
    val cache_ops : (blk_off,blk option,cache) cache_ops

    val with_cache: ((blk,cache) f_cache,t) with_state    

    val lower_ops: (blk,t) lower_ops
  end) = struct

  open S

  let ( >>= ) = monad_ops.bind
  let return = monad_ops.return

  let size () = 
    with_cache.with_state (fun ~state ~set_state:_ -> 
        state.size |> fst |> return)

  let truncate ~size = 
    with_cache.with_state (fun ~state ~set_state -> 
        set_state {state with size=(size,Flag.dirty) })

  let get_times () = 
    with_cache.with_state (fun ~state ~set_state:_ -> 
        state.times |> fst |> return)

  let set_times times = 
    with_cache.with_state (fun ~state ~set_state -> 
        set_state {state with times=(times,Flag.dirty) })

  let lower_write blks = 
    blks |> iter_k (fun ~k xs -> 
        match xs with 
        | [] -> return ()
        | (blk_off,blk)::rest -> 
          assert(blk <> None); (* can't have a dirty None in cache *)
          let blk = dest_Some blk in
          lower_ops.write_file_blk blk_off blk >>= fun () -> 
          k rest)

  let write_file_blk blk_off blk =
    with_cache.with_state (fun ~state ~set_state -> 
        cache_ops.insert blk_off (Some blk,Flag.dirty) state.data |> fun data -> 
        (* may need trim *)
        begin
          match cache_ops.needs_trim data with
          | false -> return data
          | true -> 
            (* may want to just put this on a queue to lower so we can
               return quicker? on the other hand, we don't explicitly
               flush or sync here *)
            cache_ops.trim data |> fun (xs,data) -> 
            lower_write xs >>= fun () -> 
            return data
        end
        >>= fun data ->         
        set_state {state with data})

  let read_file_blk blk_off = 
    (* what if blk_off > size? we don't handle that at this level -
       higher up we would likely set the size field to something *)
    with_cache.with_state (fun ~state ~set_state -> 
        cache_ops.find blk_off state.data |> fun (vopt,data) -> 
        match vopt with
        | None -> 
          (* not in cache; go to lower *)
          begin
            lower_ops.read_file_blk blk_off >>= fun blk -> 
            match blk with 
            | None -> 
              let data = cache_ops.insert blk_off (None,Flag.clean) data in
              set_state {state with data} >>= fun () -> 
              return None
            | Some blk -> 
              let data = cache_ops.insert blk_off (Some blk,Flag.clean) data in
              set_state {state with data} >>= fun () -> 
              return None              
          end
        | Some (v,_flag) -> 
          (* in cache *)
          match v with 
          | None -> 
            (* not present in lower; cached as None *)
            return None
          | Some blk -> 
            return (Some blk))
      
  let flush () = 
    with_cache.with_state (fun ~state ~set_state -> 
        (* do size and times first *)        
        let { times; size; data } = state in
        (* size *)
        begin
          let sz = state.size |> fst in
          let dirty = state.size |> snd = Flag.dirty in
          if dirty then 
            lower_ops.set_size sz
          else return ()
        end >>= fun () -> 
        (* times *)
        begin 
          let t = state.times |> fst in
          let dirty = state.times |> snd = Flag.dirty in
          if dirty then 
            lower_ops.set_times t else return ()
        end >>= fun () -> 
        (* data *)
        (* FIXME current wbc.clean removes all entries :( *)        
        cache_ops.clean data |> fun (dirty_blocks,data) -> 
        lower_write dirty_blocks >>= fun () -> 
        set_state { data; times=(times|>fst,Flag.clean); size=(size|>fst,Flag.clean)})

  let sync () = 
    flush () >>= fun () -> 
    lower_ops.sync ()       
    

  let pre_file_ops : _ pre_file_ops = {
    size; truncate; get_times; set_times; write_file_blk; read_file_blk; flush; sync }

end


(* FIXME should probably do some testing *)
