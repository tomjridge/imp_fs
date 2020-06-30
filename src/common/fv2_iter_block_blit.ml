(** {2 Iterated block blit, for file_impl_v2}

Abstract model of blitting from blks to buffer.
 *)

open Int_like
open Buffers_from_btree


(* FIXME at the moment, this assumes that we can rewrite file blocks as we wish *)

(** NOTE The pread,pwrite functions that result will lock the file
    whilst executing, in order to udpate the B-tree root *)


(** Abstract model of blitting from blks to buffer.

    Spec: a seq of blits equiv to a single blit, but at most 1 blit is not
    block aligned, and at most 2 blits are not blk sized; also, minimal
    number of blits (no overlapping blits). 

    This version assumes that the
    file is backed by a map from blk_index -> blk_id (although for the
    test code, we ignore blk_id and just work directly with bytes.)

    - read_blk: given a block index, we look up the actual blk_id in
      the map, and read that block; return empty blk if no blk; ignore size

    - alloc_and_write_blk: for the situation where we
      definitely want to allocate a new blk; we have a blk_index, a blk,
      and we allocate a blk_id, write the blk, insert the (index,blk_id)
      into the map, then return the blk_id

    - rewrite_blk_or_alloc: given a blk_index and a blk_id (of the previous
      version of the blk) and the new blk, we attempt to rewrite that blk
      directly; if for some reason we cannot mutate the old blk, we
      allocate a new blk_id, write the new blk, insert the
      (index,blk_id) into the map

    - truncate n: drop the blk-index map entries for all blocks after byte index n

    For the usage with file_impl, we assume the inode is locked (so we can update the inode using set_state).
*)

(* These versions assume that the arguments are all "ok" *)
type ('buf,'t) ops = {
  pwrite   : src:'buf -> src_off:offset -> src_len:len -> dst_off:offset -> (size,'t)m;
  pread    : off:offset -> len:len -> ('buf,'t)m;
}

(** indexes, unboxed *)
module Idx = struct
  open Bin_prot.Std
  type t = { idx:int } [@@unboxed] [@@deriving bin_io]
  let max_sz = 9
end
open Idx

let idx_mshlr : _ bp_mshlr = (module Idx)

type idx = Idx.t

module type S = sig
  type buf
  type blk_id
  type blk
  type t 

  val monad_ops            : t monad_ops
  val buf_ops              : buf buf_ops
  val blk_ops              : blk blk_ops

  (** Following operations provided by the B-tree + blk-dev *)

  (** Read a blk, given the index, and some map from idx to blk-id;
      returns the underlying blk-id, in case we need to rewrite

      NOTE blk_id is used only when rewriting
  *)
  val read_blk             : idx -> (blk_id*blk,t)m 

  (** If there is no existing blk for a given index, we need to
      allocate one and write it (and update the B-tree) *)
  val alloc_and_write_blk  : idx -> blk -> (unit,t)m

  (** Try to rewrite a blk; if for some reason (eg snapshot) we
      can't rewrite, then just allocate another blkid and use that
      instead (updating the B-tree of course). *)
  val rewrite_blk_or_alloc : idx -> blk_id*blk -> (unit,t)m (* FIXME return bool? unit? *)
end

module type T = sig
  module S : S
  open S
  val ops : (buf,t)ops
end

(** Make with full sig *)
module Make_v1(S:S) = struct
  module S = S
  open S

  (** NOTE we want [make] to be (as far as possible) independent
      of the updates to the blk-index map. NOTE that blk_id is
      essentially completely abstract here, and only used when rewriting. *)

  let ( >>= ) = monad_ops.bind
  let return = monad_ops.return 

  let blk_sz = blk_ops.blk_sz |> Blk_sz.to_int 

  (** This version assumes that the arguments pass the
      pread_check. We create a buffer of size len. *)
  (* $(FIXME("""perhaps set some max size to read""")) *)
  (* FIXME we may want to launch threads for each block 
        read, then wait on all these threads to finish?  *)
  (* FIXME this block-aligned blitting should probably be factored out and tested *)
  let pread ~off:{off=off0} ~len:{len=len0} : (buf,_)m =

    let rec loop ~buf ~buf_off ~(blk_n:idx) ~blk_off ~len_remain =
      assert(blk_off < blk_sz);
      assert(blk_off >= 0);

      let blit_blk_to_buf ~(blk:blk) ~blk_off ~len =
        assert(blk_off.off + len.len <= blk_sz);
        blk |> blk_ops.to_string |> fun src -> 
        buf_ops.blit_string_to_buf ~src ~src_off:blk_off
          ~src_len:len ~dst:buf ~dst_off:{off=buf_off} |> fun buf ->
        buf
      in

      match len_remain with 
      | 0 -> return buf
      | _ -> (
          (* try to read the blk, add to buf, cont if len is > 0 *)
          read_blk blk_n >>= fun (_,blk) ->
          let n_can_write = blk_sz - blk_off in
          (* Printf.printf "can_write: %d\n" can_write; *)
          assert(n_can_write <= blk_sz);
          (* don't write more than we need to *)
          let len = min len_remain n_can_write in
          blit_blk_to_buf ~blk ~blk_off:{off=blk_off} ~len:{len} |> fun buf ->
          let len_remain' = len_remain - len in
          match () with 
          | _ when len_remain' = 0 -> return buf
          | _ when blk_off+len=blk_sz -> 
            loop ~buf ~buf_off:(buf_off+len) ~blk_n:({idx=1+blk_n.idx}) ~blk_off:0 ~len_remain:len_remain'
          | _ -> assert false)
    in

    let buf = buf_ops.buf_create len0 in
    loop ~buf ~buf_off:0 ~blk_n:{idx=(off0 / blk_sz)} ~blk_off:(off0 mod blk_sz) ~len_remain:len0

  let _ = pread

  (* NOTE this is slightly different, because for full blocks we
     don't need to read then write *)
  (* $(FIXME("""conversions from blk/buf to string is awful""")) *)
  let pwrite ~(src:buf) ~src_off:{off=src_off} ~src_len:{len=src_len0} ~dst_off:{off=dst_off} = 
    let src : string = buf_ops.to_string src in

    let rec loop ~src_off ~len_remain ~(blk_n:idx) ~blk_off = 
      (* Printf.printf "pwrite: src_off=%d len_remain=%d blk_n=%d
         blk_off=%d\n" src_off len_remain (i_to_int blk_n) blk_off;
      *)
      match len_remain with 
      | 0 -> return {size=src_len0}
      | _ -> 
        match blk_off = 0 && len_remain >= blk_sz with
        | true -> (
            (* block aligned; write and continue; FIXME we may need to adjust size? *)
            let len = blk_sz in
            let blk = blk_ops.of_string (StringLabels.sub src ~pos:src_off ~len) in
            alloc_and_write_blk blk_n blk >>= fun _r -> 
            (* FIXME don't need to record the blkid? *)
            loop ~src_off:(src_off+len) ~len_remain:(len_remain-len) ~blk_n:{idx=1+blk_n.idx} ~blk_off:0)
        | false -> (
            (* have to read blk then update *)
            read_blk blk_n >>= fun (rr,blk) ->
            blk |> blk_ops.to_string |> buf_ops.of_string |> fun blk ->
            let len = min (blk_sz - blk_off) len_remain in
            buf_ops.blit_string_to_buf ~src ~src_off:{off=src_off} ~src_len:{len} 
              ~dst:blk ~dst_off:{off=blk_off} |> fun blk ->
            rewrite_blk_or_alloc blk_n (rr,blk |> buf_ops.to_string |> blk_ops.of_string) 
            >>= fun () (* _ropt *) ->
            let len_remain' = len_remain - len in
            match len_remain' = 0 with
            | true -> return {size=src_len0}
            | false -> 
              assert(len=blk_sz-blk_off);
              loop ~src_off:(src_off+len) ~len_remain:len_remain' ~blk_n:{idx=1+blk_n.idx} ~blk_off:0)
    in

    let blk_n = dst_off / blk_sz in
    let blk_off = dst_off mod blk_sz in
    loop ~src_off ~len_remain:src_len0 ~blk_n:{idx=blk_n} ~blk_off

  let ops = { pread; pwrite }    
end

(** Restricted sig *)
module Make_v2(S:S) : T with module S=S = struct
  include Make_v1(S)
end

(** As a function, not a functor *)
let make (type buf blk_id blk t)
    ~monad_ops
    ~buf_ops
    ~blk_ops
    ~read_blk
    ~alloc_and_write_blk
    ~rewrite_blk_or_alloc
  =
  let module S = struct
    type nonrec buf=buf
    type nonrec blk_id=blk_id
    type nonrec blk=blk
    type nonrec t=t
    let monad_ops = monad_ops
    let buf_ops = buf_ops
    let blk_ops = blk_ops
    let read_blk = read_blk
    let alloc_and_write_blk = alloc_and_write_blk
    let rewrite_blk_or_alloc = rewrite_blk_or_alloc
  end
  in
  let module M = Make_v1(S) in
  M.ops

let _ = make

let test () =
  Printf.printf "Iter_block_blit: tests start...\n";
  let monad_ops = imperative_monad_ops in
  let ( >>= ) = monad_ops.bind in
  let return = monad_ops.return in
  let to_m,of_m = Imperative.(to_m,of_m) in
  let buf_ops = bytes_buf_ops in
  let blk_ops = Blk_factory.Internal.String_.make ~blk_sz:(Blk_sz.of_int 2) in 
  (* FIXME this needs to have blk_sz 2; FIXME perhaps make it
     clearer that Common_blk_ops.string_blk_ops has size 4096 *)
  let blk_sz = blk_ops.blk_sz |> Blk_sz.to_int in
  let with_bytes buf = 
    let buf_size = (buf_ops.buf_size buf).size in
    assert(buf_size mod blk_sz = 0);
    let buf = ref buf in
    let read_blk {idx=i} = to_m (
        buf_ops.buf_to_string ~src:!buf ~off:{off=(i*blk_sz)} ~len:{len=blk_sz} |> fun s ->
        (-1,s)) in
    let rewrite_blk_or_alloc {idx=i} (_blk_id,blk) = to_m (
        buf_ops.blit_string_to_buf ~src:blk ~src_off:{off=0} ~src_len:{len=blk_sz}
          ~dst:!buf ~dst_off:{off=i*blk_sz} |> fun buf' ->
        buf := buf'; ())
    in
    let alloc_and_write_blk i blk = 
      rewrite_blk_or_alloc i (-1,blk) >>= fun _ -> 
      return ()
    in
    let { pread; pwrite; _ } = 
      make ~monad_ops ~buf_ops ~blk_ops ~read_blk 
        ~alloc_and_write_blk ~rewrite_blk_or_alloc
    in
    pread,pwrite,buf
  in
  of_m (
    with_bytes (buf_ops.of_string "abcdef") |> fun (pread,pwrite,buf) -> 
    pread ~off:{off=0} ~len:{len=0} >>= fun b -> 
    assert(buf_ops.to_string b = "");
    pread ~off:{off=0} ~len:{len=1} >>= fun b -> 
    assert(buf_ops.to_string b = "a");
    pread ~off:{off=0} ~len:{len=2} >>= fun b -> 
    assert(buf_ops.to_string b = "ab");
    pread ~off:{off=0} ~len:{len=3} >>= fun b -> 
    assert(buf_ops.to_string b = "abc");
    pread ~off:{off=0} ~len:{len=6} >>= fun b -> 
    assert(buf_ops.to_string b = "abcdef");
    let src = (buf_ops.of_string "xyz") in
    pwrite ~src ~src_off:{off=0} ~src_len:{len=1} ~dst_off:{off=0} >>= fun _sz -> 
    assert(buf_ops.to_string (!buf) = "xbcdef");
    pwrite ~src ~src_off:{off=0} ~src_len:{len=2} ~dst_off:{off=0} >>= fun _sz -> 
    assert(buf_ops.to_string (!buf) = "xycdef");
    pwrite ~src ~src_off:{off=0} ~src_len:{len=3} ~dst_off:{off=0} >>= fun _sz -> 
    assert(buf_ops.to_string (!buf) = "xyzdef");
    buf:=buf_ops.of_string "abcdef";
    pwrite ~src ~src_off:{off=0} ~src_len:{len=1} ~dst_off:{off=0} >>= fun _sz -> 
    assert(buf_ops.to_string (!buf) = "xbcdef");
    buf:=buf_ops.of_string "abcdef";
    pwrite ~src ~src_off:{off=1} ~src_len:{len=1} ~dst_off:{off=1} >>= fun _sz -> 
    assert(buf_ops.to_string (!buf) = "aycdef" |> fun b -> 
           b || (Printf.printf "buf: %s\n" (buf_ops.to_string (!buf)); false));      
    buf:=buf_ops.of_string "abcdef";
    pwrite ~src ~src_off:{off=0} ~src_len:{len=3} ~dst_off:{off=1} >>= fun _sz -> 
    assert(buf_ops.to_string (!buf) = "axyzef");
    return ());
  Printf.printf "Iter_block_blit: tests end!\n"
[@@ocaml.warning "-8"]

