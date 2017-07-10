(* global object map ------------------------------------------------ *)

(** We maintain a single global object map, from oid (int) to entry. *)

open Tjr_btree
open Btree_api
open Block
open Page_ref_int
open Bin_prot_util
open Omap_entry
open Imp_pervasives
open Imp_state

(* invariant: fid maps to file, did maps to dir; package as two ops? *)
type omap_ops = (oid,omap_entry,imp_state) map_ops

type size = int

type omap_fid_ops = (fid,blk_id*size,imp_state) map_ops

let omap_fid_ops = failwith "TODO"

type omap_did_ops = (did,blk_id,imp_state) map_ops

let omap_did_ops = failwith "TODO"



(* object map ------------------------------------------------------- *)

module Omap = struct
  
  open Bin_prot_util
  open Omap_entry
  open Disk_ops
  type k = oid
  type v = omap_entry
  let v_size = bin_size_omap_entry_

  let ps = 
    Binprot_marshalling.mk_ps ~blk_sz
      ~cmp:Int_.compare ~k_size:bin_size_int ~v_size
      ~read_k:bin_reader_int ~write_k:bin_writer_int
      ~read_v:bin_reader_omap_entry ~write_v:bin_writer_omap_entry

  let store_ops = Disk_to_store.disk_to_store ~ps ~disk_ops ~free_ops

  let page_ref_ops = failwith "TODO"

  let map_ops = Store_to_map.store_ops_to_map_ops ~ps ~store_ops 
      ~page_ref_ops ~kk:(fun ~map_ops ~find_leaf -> map_ops)

  open Monad

  (* return get/set for a particular oid *)
  let did_to_page_ref_ops ~did = 
    did |> dest_did |> fun oid ->
    { 
      get=(fun () -> map_ops.find oid |> bind @@ fun ent_opt ->
        match ent_opt with
        | None -> failwith __LOC__ (* TODO impossible? oid has been deleted? *)
        | Some ent -> 
          let open Omap_entry in 
          match ent with
          | F_blkid_sz(r,sz) -> failwith __LOC__ (* TODO invariant did is a dir *)
          | D_blkid(r) -> return r);
      set=(fun r -> map_ops.insert oid (D_blkid(r)))
    }

  let lookup_did ~did =     
    did |> dest_did |> fun oid ->
    map_ops.find oid |> bind @@ fun ent_opt ->
    match ent_opt with
    | None -> failwith __LOC__  (* TODO old reference no longer valid? *)
    | Some ent -> 
      match ent with
      | F_blkid_sz (r,sz) -> failwith __LOC__
      | D_blkid r -> return r
                         
  let did_to_map_ops ~did = 
    lookup_did ~did |> bind @@ fun r ->
    let page_ref_ops = did_to_page_ref_ops ~did in
    return (Dir.map_ops ~page_ref_ops)
      (* TODO caching? *)

  let did_to_ls_ops ~did = 
    lookup_did ~did |> bind @@ fun r ->
    let page_ref_ops = did_to_page_ref_ops ~did in
    return (Dir.ls_ops ~page_ref_ops)

end




(* file ops --------------------------------------------------------- *)

(* we have to implement pread and pwrite on top of omap, since size is
   stored in omap *)

(* requires dst_pos + src_len <= blk_sz; FIXME inefficient *)
let buf_block_blit ~blk_sz ~src ~src_pos ~len ~dst ~dst_pos = (
    assert (dst_pos + len <= blk_sz);
    dst |> Block.to_string |> Bytes.of_string
    |> (fun dst -> StdLabels.Bytes.blit ~src ~src_pos ~dst ~dst_pos ~len; dst)
    |> Bytes.to_string |> Block.of_string blk_sz
)

let block_buf_blit ~blk_sz ~src ~src_pos ~len ~dst ~dst_pos = (
  assert (dst_pos + len <= Bytes.length dst);
  assert (src_pos + len <= blk_sz);
  let src = Block.to_string src in
  let src = Bytes.unsafe_of_string src in
  StdLabels.Bytes.blit ~src ~src_pos ~dst ~dst_pos ~len)


module File_ops = struct

  open Monad

  (* calculate blk_index, offset within block and len st. offset+len
     <= blk_sz *)
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
      ~page_ref_ops
      ~src_length ~src_pos ~len ~dst ~dst_pos 
    = (
      assert (src_pos+len <= src_length);
      assert (dst_pos + len <= Bytes.length dst);
      (* read the relevant leaf *)
      let blk_i = src_pos / blk_sz in
      page_ref_ops.get () |> bind (fun r ->
        find_leaf blk_i r |> bind (fun (_,kvs,rstk) ->
          (* kvs is the map from idx -> block_id; rstk tells us the
             maximum block we can try to read; restrict len so that we
             do not try to read past u; NOTE that u is strictly
             greater than blk_i *)
          let (_,u) = stack_to_lu_of_child rstk in
          (* do not attempt to read block u or higher *)
          let limit_blk = match u with 
            | None -> max_int  (* assume no file anywhere near max_int blocks *)
            | Some i -> i
          in          
          (* convert kvs to map for ease of use *)
          let map = assoc_list_to_bst kvs in
          let empty_blk = lazy (Block.of_string blk_sz "") in
          let rec loop ~src_pos ~len ~dst_pos ~n_read = (
            let blk_i = src_pos / blk_sz in
            match () with
            | _ when len=0 || blk_i >= limit_blk -> return n_read
            | _ -> 
              let blk_offset = src_pos mod blk_sz in
              let get_blk = 
                (* which block to read? *)
                match map blk_i with
                | None -> return (Lazy.force empty_blk)
                | Some i -> read_block i
              in
              get_blk |> bind (fun blk -> 
                (* NOTE len > 0 and 0 <= blk_offset < blk_sz *)
                let len' = min len (blk_sz - blk_offset) in
                block_buf_blit ~blk_sz ~src:blk ~src_pos:blk_offset 
                  ~len:len' ~dst ~dst_pos;
                loop ~src_pos:(src_pos+len') ~len:(len-len') 
                  ~dst_pos:(dst_pos+len') ~n_read:(n_read +len')))
          in
          loop ~src_pos ~len ~dst_pos ~n_read:0)))

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

  let insert_many ~(k:'k) ~(v:'v) ~(ks:'k list) ~(vs:'k -> blk) : ('k list,omap_state) m = failwith "TODO" 
    
  let pwrite'
      ~(kvs_insert: 'k -> 'v -> ('k*'v)list -> ('k*'v)list)
      ~max_leaf_keys
      ~read_block
      ~write_block
      ~page_ref_ops  (* for B-tree root *)
      ~dst_size_ops  (* for dst size if writing beyond end *)
      ~(insert_many_up: ('k*'v) list -> ('k,'r)rstk -> ('r,'t) m)  (* upwards phase of insert_many; may split initial leaf *)
      ~(find_leaf: 'k -> 'r -> ('r*('k*'v)list*('k,'r)rstk,'t) m)  (* locate initial leaf holding the blk_i of the blk we first modify *)
      ~src ~src_pos ~len ~dst_pos : (int,omap_state) m 
    = (
      assert (src_pos + len <= Bytes.length src);
      (* for dst, file size can grow *)
      begin
        let blk_i = dst_pos / blk_sz in
        page_ref_ops.get () |> bind (fun r ->
          find_leaf blk_i r |> bind (fun (_,kvs,rstk) ->
            (* kvs is the map from idx -> block_id; rstk tells us the
               maximum block we can try to write *)
            let (_,u) = stack_to_lu_of_child rstk in
            (* do not attempt to write block u or higher *)
            let limit_blk = match u with 
              | None -> max_int
              | Some i -> i
            in
            (* convert kvs to map for ease of use *)
            let map = assoc_list_to_bst kvs in
            let rec loop ~src_pos ~len ~dst_pos ~n_wrote ~kvs = (
              let blk_i = src_pos / blk_sz in
              match () with
              | _ when 
                  len=0 || (* nothing left to write *)
                  blk_i >= limit_blk || (* can't insert beyond the context bound *)
                  List.length kvs >= 2*max_leaf_keys (* can't have a leaf too large *)
                -> (  
                    (* now execute the up stage of insert_many to get a new page_ref *)
                    insert_many_up kvs rstk |> bind (fun r ->
                      page_ref_ops.set r |> bind (fun () ->
                        (* and update the size *)
                        dst_size_ops.get () |> bind (fun dst_size -> 
                          let new_size = max dst_size (dst_pos+n_wrote) in
                          (match new_size > dst_size with
                           | true -> dst_size_ops.set new_size | false -> return ())
                          |> bind (fun () -> return n_wrote)))))
              | _ -> 
                let blk_offset = src_pos mod blk_sz in
                (* we may not be writing a full block; NOTE len > 0
                   and 0 <= blk_offset < blk_sz and len' <= blk_sz *)
                let len' = min len (blk_sz - blk_offset) in
                let blk = 
                  match blk_offset > 0 || len' < blk_sz with
                  | true -> (
                      (* a partial block write; read the original
                         block *)
                      (match map blk_i with
                       | None -> return (Block.of_string blk_sz "")
                       | Some i -> read_block i)
                      |> bind (fun orig_blk -> 
                        (* update *)
                        let blk = buf_block_blit ~blk_sz ~src ~src_pos ~len:len' ~dst:orig_blk 
                            ~dst_pos:blk_offset 
                        in (* FIXME or mutate in place? *)
                        return blk ))
                  | false -> (
                      Block.of_string blk_sz "" 
                      |> fun dst -> 
                      (* NOTE blk_offset = 0 and len' >= blk_sz ie len' = blk_sz *)
                      assert (blk_offset = 0 && len' = blk_sz);
                      buf_block_blit ~blk_sz ~src ~src_pos ~len:blk_sz ~dst ~dst_pos:0 
                      (* FIXME inefficient create of initial block *)
                      |> return)
                in
                blk |> bind (fun blk ->
                  (* now write the blk and get a new blk_id to insert into kvs *)
                  write_block blk |> bind (fun r ->
                    let kvs = kvs_insert blk_i r kvs in
                    loop ~src_pos:(src_pos+len') ~len:(len-len') ~dst_pos:(dst_pos+len') 
                      ~n_wrote:(n_wrote+len') ~kvs)))
            in
            loop ~src_pos ~len ~dst_pos ~n_wrote:0 ~kvs))
      end)

  let _ = pwrite'
  
end


let omap_ops : omap_ops = failwith ""


(* TODO instantiate the omap with cache *)

