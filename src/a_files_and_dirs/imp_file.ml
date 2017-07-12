open Tjr_btree
open Btree_api
(* store_ops for file *)
open Small_string
open Bin_prot_util
open Monad

open Imp_pervasives
open Disk_ops
open Imp_state
open Free_ops

(* int -> blk_id map *)
module Map_int_blk_id = struct
  let ps = Map_int_int.ps' ~blk_sz
  let store_ops = Disk_to_store.disk_to_store ~ps ~disk_ops ~free_ops
  let map_ops ~page_ref_ops = Store_to_map.store_ops_to_map_ops ~ps 
      ~store_ops  ~page_ref_ops ~kk:(fun ~map_ops ~find_leaf -> map_ops)
end
include Map_int_blk_id

(* write_blk and read_blk based on disk_ops *)
let imp_write_block blk = (
  free_ops.get () |> bind (fun blk_id -> 
    disk_ops.write blk_id blk |> bind (fun _ -> 
      free_ops.set (blk_id+1) |> bind (fun () -> 
        return blk_id))))

let imp_read_block blk_id = (
  disk_ops.read blk_id |> bind (fun blk ->
    return (Some blk)))

(* files are implemented using the (index -> blk) map *)
let mk_map_int_blk_ops ~page_ref_ops = 
  let map_ops = map_ops ~page_ref_ops in
  Map_int_blk.mk_int_blk_map ~write_blk:imp_write_block 
    ~read_blk:imp_read_block ~map_ops



(* file ops --------------------------------------------------------- *)

(* we have to implement pread and pwrite on top of omap, since size is
   stored in omap *)

(* NOTE buffer matches Fuse.buffer and Core.Bigstring *)
type buffer = 
  (char, Bigarray.int8_unsigned_elt, Bigarray.c_layout) Bigarray.Array1.t

let buffer_length = Bigarray.Array1.dim

let blit_string_buffer = Core.Bigstring.From_string.blit

let blit_buffer_string = Core.Bigstring.To_string.blit

(* requires dst_pos + src_len <= blk_sz; FIXME inefficient *)
let buf_block_blit ~blk_sz ~src ~src_pos ~len ~dst ~dst_pos = (
  assert (dst_pos + len <= blk_sz);
  dst |> Block.to_string |> fun dst -> 
  blit_buffer_string ~src ~src_pos ~len ~dst ~dst_pos : unit)

let block_buf_blit ~blk_sz ~src ~src_pos ~len ~dst ~dst_pos = (
  assert (dst_pos + len <= buffer_length dst);
  assert (src_pos + len <= blk_sz);
  src |> Block.to_string |> fun src ->
  blit_string_buffer ~src ~src_pos ~len ~dst ~dst_pos : unit)


open Block

open Monad

(* calculate blk_index, offset within block and len st. offset+len
   <= blk_sz *)
(*
let calc ~pos ~len kk = (
  (* which block are we interested in? *)
  let blk_index = pos / blk_sz in

  (* what offset within the block? *)
  let offset = pos - (blk_index * blk_sz) in

  (* how many bytes should we read/write within the block? *)
  let len = 
    if offset+len > blk_sz then blk_sz - offset else len in
  let _ =  assert (offset + len <= blk_sz) in

  kk ~blk_index ~offset ~len
)
*)

let split_at i xs = Core.List.split_n xs i


(* convert assoc list to map; assumes assoc list is sorted *)
let rec assoc_list_to_bst kvs = 
  match kvs with
  | [] -> fun k -> None
  | [(k,v)] -> fun k' -> if k=k' then Some v else None
  | _ -> 
    List.length kvs 
    |> fun n -> 
    kvs |> split_at (n/2) 
    |> fun (xs,(k,v)::ys) -> 
    let f1 = assoc_list_to_bst xs in
    let f2 = assoc_list_to_bst ((k,v)::ys) in
    fun k' -> if k' < k then f1 k' else f2 k'
[@@warning "-8"]

type ('k,'r)rstk = ('k,'r) Tjr_btree.Small_step.rstk

let stack_to_lu_of_child = Tjr_btree.Isa_export.Tree_stack.stack_to_lu_of_child

(* t is the root block of the idx_map; TODO this should make sure
   not to read beyond the end of file *)

(*

* pread specification

- dst is modified from dst_pos
- no more than len' <= len bytes are modified
- the contents of dst from dst_pos matches src from src_pos (which
  implies that src_pos+len' <= src_length)
- len' is returned in the monad
- len > 0 --> len' > 0

This involves being able to reconstruct src from the idx->blk_id map
and read_block.


*)


let pread'
    ~read_block 
    ~(find_leaf: 'k -> 'r -> ('r*('k*'v)list*('k,'r)rstk,'t) m)
    ~r
    ~src_length ~src_pos ~len ~dst ~dst_pos 
  = 
  assert (src_pos+len <= src_length);
  assert (dst_pos + len <= buffer_length dst);
  (* read the relevant leaf *)
  let blk_i = src_pos / blk_sz in
  find_leaf blk_i r |> bind @@ fun (_,kvs,rstk) ->
  (* kvs is the map from idx -> block_id; rstk tells us the
     maximum block we can try to read; restrict len so that we
     do not try to read past u; NOTE that u is strictly
     greater than blk_i *)
  let (_,u) = stack_to_lu_of_child rstk in
  (* do not attempt to read block u or higher; assume no file anywhere
     near max_int blocks TODO *)
  let limit_blk = u |> option_case ~_None:max_int ~_Some:(fun i -> i) in
  (* convert kvs to map for ease of use *)
  let map = assoc_list_to_bst kvs in
  let empty_blk = lazy (Block.of_string blk_sz "") in
  let rec loop ~src_pos ~len ~dst_pos ~n_read = 
    let blk_i = src_pos / blk_sz in
    match len=0 || blk_i >= limit_blk with
    | true -> return n_read
    | false -> 
      let blk_offset = src_pos mod blk_sz in
      (* which block to read? *)
      map blk_i 
      |> option_case 
        ~_None:(return (Lazy.force empty_blk))
        ~_Some:(fun i -> i)
      |> bind @@ fun blk -> 
      (* NOTE len > 0 and 0 <= blk_offset < blk_sz *)
      let len' = min len (blk_sz - blk_offset) in
      block_buf_blit 
        ~blk_sz ~src:blk ~src_pos:blk_offset ~len:len' ~dst ~dst_pos;
      loop 
        ~src_pos:(src_pos+len') ~len:(len-len') 
        ~dst_pos:(dst_pos+len') ~n_read:(n_read +len')
  in
  loop ~src_pos ~len ~dst_pos ~n_read:0

let _ = pread'


(* pwrite is similar to pread; we aim for insert_many-like
   behaviour; we don't want to allocate all blocks up front; so
   insert_many needs to take a function that can be stepped and can
   produce a block or indicate that it is finished; for partial
   blocks, this needs access to read_block  *)

(*

* pwrite specification

- dst is modified from dst_pos
- no more than len' <= len bytes are modified
- dst@dst_pos, len' bytes matches src@src_pos len' bytes (obviously
  src is unaltered; dst file size may be increased)
- (the abstraction from the btree to the file contents is wellformed;
  the root ref is updated; btree invariants preserved)


*)

(* let insert_many ~(k:'k) ~(v:'v) ~(ks:'k list) ~(vs:'k -> blk) : ('k list,imp_state) m = failwith "TODO"  *)

let pwrite'
    ~(kvs_insert: 'k -> 'v -> ('k*'v)list -> ('k*'v)list)
    ~max_leaf_keys
    ~read_block
    ~write_block
    ~(insert_many_up: ('k*'v) list -> ('k,'r)rstk -> ('r,'t) m)  (* TODO upwards phase of insert_many; may split initial leaf *)
    ~(find_leaf: 'k -> 'r -> ('r*('k*'v)list*('k,'r)rstk,'t) m)  (* TODO locate initial leaf holding the blk_i of the blk we first modify *)
    ~src ~src_pos ~len 
    ~r ~dst_pos ~dst_size  (* for dst size if writing beyond end *)
    ~kk
  = 
  assert (src_pos + len <= buffer_length src);
  (* for dst, file size can grow *)
  let blk_i = dst_pos / blk_sz in
  find_leaf blk_i r |> bind @@ fun (_,kvs,rstk) ->
  (* kvs is the map from idx -> block_id *)
  let (_,u) = stack_to_lu_of_child rstk in
  (* do not attempt to write block u or higher *)
  let limit_blk = u |> option_case ~_None:max_int ~_Some:(fun i -> i) in
  (* convert kvs to map for ease of use *)
  let map = assoc_list_to_bst kvs in
  let rec loop ~src_pos ~len ~dst_pos ~n_wrote ~kvs = 
    let blk_i = src_pos / blk_sz in
    (* either: nothing left to write; can't insert beyond the context
       bound; can't have a leaf too large. *) 
    match len=0 || blk_i >= limit_blk || List.length kvs >= 2*max_leaf_keys with
    | true -> 
      (* Now execute the up stage of insert_many to get a new page_ref *)
      insert_many_up kvs rstk |> bind @@ fun r ->
      kk ~r ~sz:(max dst_size (dst_pos+n_wrote)) ~n_wrote
    | _ -> 
      let blk_offset = src_pos mod blk_sz in
      (* we may not be writing a full block; NOTE len > 0 and 0 <=
         blk_offset < blk_sz and len' <= blk_sz *)
      let len' = min len (blk_sz - blk_offset) in
      begin match blk_offset > 0 || len' < blk_sz with
        | true -> 
          (* a partial block write; read the original block *)
          map blk_i 
          |> option_case
            ~_None:(return (Block.of_string blk_sz ""))
            ~_Some:(fun i -> read_block i)
          |> bind @@ fun orig_blk -> 
          buf_block_blit 
            ~blk_sz ~src ~src_pos ~len:len' ~dst:orig_blk ~dst_pos:blk_offset; 
          return orig_blk
          (* update; NOTE this mutates orig_blk *)
          (* FIXME or mutate in place? *)
        | false ->
          (* NOTE blk_offset = 0 && len' >= blk_sz ie len'=blk_sz *)
          assert (blk_offset = 0 && len' = blk_sz);
          Block.of_string blk_sz "" |> fun dst -> 
          buf_block_blit ~blk_sz ~src ~src_pos ~len:blk_sz ~dst ~dst_pos:0;
          return dst
          (* FIXME inefficient create of initial block? *)
      end
      |> bind @@ fun blk ->
      (* now write the blk and get a new blk_id to insert into kvs *)
      write_block blk |> bind @@ fun r ->
      let kvs = kvs_insert blk_i r kvs in
      loop 
        ~src_pos:(src_pos+len') ~len:(len-len') ~dst_pos:(dst_pos+len') 
        ~n_wrote:(n_wrote+len') ~kvs
  in
  loop ~src_pos ~len ~dst_pos ~n_wrote:0 ~kvs


let _ = pwrite'


