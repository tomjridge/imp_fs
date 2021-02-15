(** This is a cache-aware implementation of files, for v3.

We model a file as a partial map from blk_off to blk, together with a
   size in bytes, and file times.

Layers (high to low): 

- pread,pwrite
- aligned block read/write ("pre_file_ops")
- cache
- raw aligned block read/write
- (underlying B-tree implementation or other)


Don't open - too many types with names duplicated elsewhere. *)

(* See also: 

tjr_fs_shared/.../write_back_cache_v3.ml

*)

let dont_log = false


(** {2 Types} *)

type blk_off = int

module Flag = struct
  let dirty = true
  let clean = false
end

module Maybe_dirty = struct
  type 'a maybe_dirty = 'a * bool
  let is_dirty (_v,b) = (b=Flag.dirty)
end
open Maybe_dirty

type ('k,'v,'cache) cache_ops = ('k,'v,'cache) wbc_ops
  
(** Per-file cache entries *)
type ('blk,'cache) f_cache = {         
  times : Times.times maybe_dirty;
  size  : int maybe_dirty;
  data  : 'cache; 
}

(** We need a way to write to the lower non-cached file implementation *)
type ('blk,'t) lower_ops = {
  set_size       : int -> (unit,'t)m;
  set_times      : times -> (unit,'t)m;
  write_file_blk : blk_off:int -> 'blk -> (unit,'t)m;
  read_file_blk  : blk_off:int -> ('blk option,'t)m; (* NOTE always returns a blk *)
  (* flush - needed? *)
  sync           : unit -> (unit,'t)m;
}

(* FIXME maybe we want a better name than pre-file *)

(* based file_impl_v2; read and write blocks at block offsets *)
type ('blk,'t) pre_file_ops = {
  size           : unit -> (int,'t)m; (* size in bytes, unrelated to the data cache *)
  truncate       : size:int -> (unit,'t)m;

  get_times      : unit -> (times,'t)m;
  set_times      : times -> (unit,'t)m;

  write_file_blk : blk_off:int -> 'blk -> (unit,'t)m;
  read_file_blk  : blk_off:int -> ('blk option,'t)m;

  flush          : unit -> (unit,'t)m;
  sync           : unit -> (unit,'t)m;

}


(* based file_impl_v2 *)
type pread_error = Call_specific_errors.pread_err
type pwrite_error = Call_specific_errors.pwrite_err

type ('buf,'t) file_ops = {
  size           : unit -> (int,'t)m; (* size in bytes, unrelated to the data cache *)
  truncate       : size:int -> (unit,'t)m;

  get_times      : unit -> (times,'t)m;
  set_times      : times -> (unit,'t)m;

  pwrite   : src:'buf -> src_off:int -> src_len:int -> 
    dst_off:int -> ((int (*n_written*),pwrite_error)result,'t)m;
  pread    : off:int -> len:int -> (('buf,pread_error)result,'t)m;

  flush          : unit -> (unit,'t)m;
  sync           : unit -> (unit,'t)m;

}


(** {2 pread/pwrite argument checks} 

copied from File_impl_v1 *)

(** Check the arguments to pread *)
let pread_check = 
  let module A = struct

    let pread_check_1 ~size ~off ~len = 
      assert(off >=0);
      assert(len >=0);  
      match () with
      | _ when off < 0                 -> `Off_neg
      | _ when len < 0                 -> `Len_neg
      | _ when off > size && len=0     -> `Off_gt_sz_and_len_0 (* not an error *)
      | _ when off > size              -> `Off_gt_sz_and_pos_len
      | _ when off+len > size && len=0 -> `Offlen_gt_sz_and_len_0
      | _ when off+len > size          -> `Offlen_gt_sz_and_pos_len
      | _ -> `Ok

    let pread_check_2 ~size ~off ~len = 
      pread_check_1 ~size ~off ~len |> function
      | `Off_neg | `Len_neg -> `Error `Off_or_len
      | `Off_gt_sz_and_len_0 | `Offlen_gt_sz_and_len_0 -> `Ok `Gt_sz_len_0
      | `Off_gt_sz_and_pos_len | `Offlen_gt_sz_and_pos_len -> `Error `Gt_sz_pos_len
      | `Ok -> `Ok `Unit

    let pread_check ~size ~off ~len = 
      pread_check_2 ~size ~off ~len |> function
      | `Ok _ -> Ok ()
      | `Error `Off_or_len -> Error "off<0 or len<0"
      | `Error `Gt_sz_pos_len -> Error "off+len>size and len>0"
  end
  in A.pread_check


(** Check arguments to pwrite *)
let pwrite_check ~(buf_ops: _ buf_ops) =
  let module A = struct
    let pwrite_check_1 : src:'buf -> src_off:int -> src_len:int -> dst_off:int -> _ = 
      fun ~src ~src_off ~src_len ~dst_off ->
        match () with
        | _ when src_off < 0 -> `Err `Src_off_neg
        | _ when dst_off < 0 -> `Err `Dst_off_neg
        | _ when src_off+src_len > buf_ops.buf_length src -> `Err `Einval
        | _ -> `Ok

    let pwrite_check ~src ~src_off ~src_len ~dst_off = 
      pwrite_check_1 ~src ~src_off ~src_len ~dst_off |> function
      | `Err `Src_off_neg -> Error "src_off<0"
      | `Err `Dst_off_neg -> Error "dst_off<0"
      | `Err `Einval -> Error "src_off+src_len>src.size"
      | `Ok -> Ok ()
  end
  in A.pwrite_check



(** {2 Make_v1 - user lower_ops; implement cached pre_file_ops} *)

module Make_v1(S: sig
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
          lower_ops.write_file_blk ~blk_off blk >>= fun () -> 
          k rest)

  let write_file_blk ~blk_off blk =
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

  let read_file_blk ~blk_off = 
    (* what if blk_off > size? we don't handle that at this level -
       higher up we would likely set the size field to something *)
    with_cache.with_state (fun ~state ~set_state -> 
        cache_ops.find blk_off state.data |> fun (vopt,data) -> 
        match vopt with
        | None -> 
          (* not in cache; go to lower *)
          begin
            lower_ops.read_file_blk ~blk_off >>= fun blk -> 
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


(** {2 Make_v2 - use pre_file_ops; implement pread/pwrite using iterated block read/write} *)

module Make_v2(S: sig
    type t
    val monad_ops: t monad_ops

    type blk
    type buf
    val blk_ops: (blk,buf)blk_ops
    val buf_ops: buf buf_ops

    val pre_file_ops : (blk,t) pre_file_ops
  end) = struct

  open S

  let ( >>= ) = monad_ops.bind
  let return = monad_ops.return

  let blk_sz = blk_ops.blk_sz |> Blk_sz.to_int 

  let _ = assert(
    Printf.printf "%s: blk_sz is %d\n%!" __MODULE__ blk_sz;
    true)

  let { read_file_blk; write_file_blk; size; truncate=set_size; set_times; _ } = pre_file_ops

  let pread ~off:off0 ~len:len0 : (buf,_)m =
    let rec loop ~buf ~buf_off ~blk_n ~blk_off ~remain =
      assert(blk_off < blk_sz);
      assert(blk_off >= 0);

      let blit_blk_to_buf ~(blk:blk) ~blk_off ~len =
        assert(blk_off + len <= blk_sz);
        blk |> blk_ops.blk_to_buf |> fun src -> 
        (* FIXME change buf_ops: we don't need labels AND records *)
        buf_ops.blit ~src ~src_off:{off=blk_off}
          ~src_len:{len} ~dst:buf ~dst_off:{off=buf_off} |> fun buf ->
        buf
      in

      match remain with 
      | 0 -> return buf
      | _ -> 
        (* try to read the blk, add to buf, cont if len is > 0 *)
        read_file_blk ~blk_off:blk_n >>= fun blk ->
        (* If a sparse file, just create an empty block here *)
        let blk = match blk with 
          | None -> buf_ops.buf_create blk_sz |> blk_ops.buf_to_blk
          | Some blk -> blk
        in
        let n_can_write = blk_sz - blk_off in
        (* Printf.printf "can_write: %d\n" can_write; *)
        assert(n_can_write <= blk_sz);
        (* don't write more than we need to *)
        let len = min remain n_can_write in
        blit_blk_to_buf ~blk ~blk_off ~len |> fun buf ->
        let remain' = remain - len in
        match () with 
        | _ when remain' = 0 -> return buf
        | _ when blk_off+len=blk_sz -> 
          loop ~buf ~buf_off:(buf_off+len) ~blk_n:(1+blk_n) ~blk_off:0 ~remain:remain'
        | _ -> assert false
    in

    let buf = buf_ops.buf_create len0 in
    loop ~buf ~buf_off:0 ~blk_n:(off0 / blk_sz) ~blk_off:(off0 mod blk_sz) ~remain:len0

  let _ = pread

  (** adjust pread: check arguments; don't read beyond end of file *)
  let pread ~off ~len =     
    size () >>= fun size -> 
    (* don't attempt to read beyond end of file *)
    let len = min len (size - off) in
    pread_check ~size ~off ~len |> function 
    | Ok () -> pread ~off ~len >>= fun x -> return (Ok x)
    | Error _s -> return (Error `Error_other) (* FIXME really EINVAL *)

  
  (* NOTE this is slightly different, because for full blocks we
     don't need to read then write *)
  let pwrite ~src ~src_off ~src_len:src_len0 ~dst_off = 
    let rec loop ~src_off ~remain ~(blk_n:int) ~blk_off = 
      (* Printf.printf "pwrite: src_off=%d remain=%d blk_n=%d
         blk_off=%d\n" src_off remain (i_to_int blk_n) blk_off;
      *)
      match remain with 
      | 0 -> return (*nwritten*)src_len0
      | _ -> 
        match blk_off = 0 && remain >= blk_sz with
        | true -> (
            (* block aligned; write and continue; FIXME we may need to adjust size? *)
            let len = blk_sz in
            let blk = buf_ops.buf_sub ~buf:src ~off:src_off ~len |> blk_ops.buf_to_blk in
            write_file_blk ~blk_off:blk_n blk >>= fun () -> 
            loop ~src_off:(src_off+len) ~remain:(remain-len) ~blk_n:(1+blk_n) ~blk_off:0)
        | false -> (
            (* have to read blk then update *)
            read_file_blk ~blk_off:blk_n >>= fun blk ->
            let blk = match blk with 
              | None -> buf_ops.buf_create blk_sz |> blk_ops.buf_to_blk
              | Some blk -> blk
            in
            blk |> blk_ops.blk_to_buf |> fun blk ->
            let len = min (blk_sz - blk_off) remain in
            buf_ops.blit ~src ~src_off:{off=src_off} ~src_len:{len} 
              ~dst:blk ~dst_off:{off=blk_off} |> fun blk ->
            write_file_blk ~blk_off:blk_n (blk |> blk_ops.buf_to_blk) 
            >>= fun () (* _ropt *) ->
            let remain' = remain - len in
            match remain' = 0 with
            | true -> return (*nwritten*)src_len0
            | false -> 
              assert(len=blk_sz-blk_off);
              loop ~src_off:(src_off+len) ~remain:remain' ~blk_n:(1+blk_n) ~blk_off:0)
    in
    let blk_n = dst_off / blk_sz in
    let blk_off = dst_off mod blk_sz in
    loop ~src_off ~remain:src_len0 ~blk_n ~blk_off


  let new_times = File_impl_v2.new_times


  (** adjust pwrite: check arguments; adjust size etc *)
  let pwrite ~src ~src_off ~src_len ~dst_off =
    assert(dont_log || (Printf.printf "pwrite called\n%!"; true));
    size () >>= fun size -> 
    pwrite_check ~buf_ops ~src ~src_off ~src_len ~dst_off |> function
    | Error _s -> return (Error `Error_other) (* FIXME really EINVAL *)
    | Ok () -> 
      pwrite ~src ~src_off ~src_len ~dst_off >>= fun x -> 
      assert(dont_log || (Printf.printf "pwrite wrote %d\n%!" x; true));
      (* now may need to adjust the size of the file *)
      begin
        let size' = dst_off+src_len in
        assert(dont_log || (Printf.printf "size' is %d\n%!" size'; true));
        (* set times *)
        set_times (new_times()) >>= fun () -> 
        (* maybe adjust size *)
        begin match size' > size with 
          | true -> (
              assert(dont_log || (
                  Printf.printf "updating file size from %d to is %d\n%!" size size'; true));
              set_size ~size:size')
          | false -> return ()
        end
        >>= fun () -> 
        return (Ok src_len)
      end


end


(** {2 Lwt instance} *)

module With_lwt = struct
  open Shared_ctxt
      
  (* $(FIXME("""functional cache - probably prefer imperative lru2gen for real impl""")) *)

  module K = struct type t = int let compare = Int_.compare end
  module V = struct type t = blk option end
  module Wbc = Write_back_cache.Make(K)(V)
  include Wbc

  (* $(CONFIG("fv3 wbc_params")) *)
  let wbc_params = object method cap=100 method delta=10 end
  let cap,delta = wbc_params#cap, wbc_params#delta
  let wbc_ops = wbc_factory#make_wbc ~cap ~delta

  type cache = Wbc.wbc
  let cache_ops = wbc_ops#ops

  let empty_cache = wbc_ops#empty

  let make_file_cache ~times ~size : (blk,cache) f_cache = { times; size; data=empty_cache } 

  let file_factory = 
    object
      (* method empty_cache = empty_cache *)
      (* method cache_ops = cache_ops *)
      (* method _wbc_ops = wbc_ops *)
      method make_file_cache = make_file_cache
      method with_ ~lower_ops ~with_cache = 
        let module A = struct
          module S1 = struct
            include Shared_ctxt
            type nonrec cache = cache
            let cache_ops = cache_ops
            let with_cache = with_cache
            let lower_ops = lower_ops
          end
          module V1 = Make_v1(S1)
          include V1
          module S2 = struct
            include S1
            let pre_file_ops = V1.pre_file_ops
          end
          module V2 = Make_v2(S2)
          include V2
          let file_ops = {
            size; truncate; get_times; set_times; pwrite; pread; flush; sync }
        end
        in
        A.file_ops
    end

  type file_factory = 
    < make_file_cache : 
        times:times maybe_dirty ->
        size:blk_off maybe_dirty -> 
        (blk, cache) f_cache;
      with_ : 
        lower_ops:(blk, lwt) lower_ops ->
        with_cache:((blk, cache) f_cache, lwt) with_state ->
        (blk, lwt) file_ops >

  let file_factory : file_factory = (file_factory :> file_factory)
      
end

let file_factory = With_lwt.file_factory

