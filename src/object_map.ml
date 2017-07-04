(* global object map ------------------------------------------------ *)

open Tjr_btree
open Btree_api
open Block
open Page_ref_int
open Bin_prot_util

let blk_sz = 4096

(* object ids -------------------------------------------------------- *)

type object_id = int 
type oid = object_id


(* directory entries ------------------------------------------------- *)

type f_size = int  (* 32 bits suffices? *)

type f_ent = blk_id * f_size 

type d_ent = blk_id 

(* CHOICE do we want to have two maps (one for files, one for dirs) or
   just one (to sum type)? for simplicity have one for the time being *)

module Entries = struct
  include 
    struct
      open Bin_prot.Std
      (* these types should match f_ent and d_ent; don't want derivings
         everywhere *)
      type entry = F of int * int | D of int [@@deriving bin_io]  
    end
  type t = entry
  type omap_entry = entry

  let bin_size_entry = 1 (* tag*) + 2*bin_size_int
end
module E = Entries


(* global state ----------------------------------------------------- *)

(* "omap" is the "global" object map from oid to Ent.t *)

module S = struct
  type t = {

    (* We always allocate new blocks. *)
    free: page_ref;

    (* The omap is represented by a pointer to the B-tree *)
    omap_root: blk_id;

    (* The omap is cached. *)
    omap_cache: unit; (* TODO *)

    (* This is the "global transaction log", which records synced
       objects without requiring a sync of the object map. Represented
       using a B-tree or (better?) a persistent on-disk list. *)
    omap_additional_object_roots: blk_id; 

    (* The B-tree backing each file also has a cache. *)
    file_caches: oid -> unit;

    (* Ditto directories *)
    dir_caches: oid -> unit;

    (* TODO other layers of caching *)
  }
end

type omap_ops = (oid,E.t,S.t) map_ops


(* disk ------------------------------------------------------------- *)
module Disk = struct
  open Btree_api
  let disk_ops : S.t disk_ops = failwith "TODO"
end
include Disk


(* free space ------------------------------------------------------- *)
(* all stores share the same free space map *)
module Free = struct
  open S
  open Monad
  let free_ops : (blk_id,S.t) mref = {
    get=(fun () -> (fun t -> (t,Ok t.free)));
    set=(fun free -> fun t -> ({t with free}, Ok ()));
  }
end
include Free




(* directories ------------------------------------------------------ *)

(* store ops are specific to type: file_store_ops, dir_store_ops,
   omap_store_ops; *)
module Dir = struct

  open Small_string
  open Bin_prot_util
  type k = SS.t
  type v = oid
  
  let ps = 
    Binprot_marshalling.mk_ps ~blk_sz 
      ~cmp:SS.compare ~k_size:bin_size_ss ~v_size:bin_size_int
      ~read_k:bin_reader_ss ~write_k:bin_writer_ss
      ~read_v:bin_reader_int ~write_v:bin_writer_int

  let dir_store_ops : (k,v,page_ref,S.t) store_ops = 
    Disk_to_store.disk_to_store ~ps ~disk_ops ~free_ops

  let map_ops = Store_to_map.store_ops_to_map_ops ~ps ~store_ops:dir_store_ops
      ~kk:(fun ~map_ops ~find_leaf -> map_ops)
end




(* files ------------------------------------------------------------ *)

module File = struct
  (* store_ops for file *)
  open Small_string
  open Bin_prot_util
  open Monad

  (* int -> blk_id map *)
  module Map_int_blk_id = struct
    let ps = Map_int_int.ps' ~blk_sz
    let store_ops = Disk_to_store.disk_to_store ~ps ~disk_ops ~free_ops
    let map_ops ~page_ref_ops = Store_to_map.store_ops_to_map_ops ~ps 
        ~store_ops  ~page_ref_ops ~kk:(fun ~map_ops ~find_leaf -> map_ops)
  end
  include Map_int_blk_id

  (* write_blk and read_blk based on disk_ops *)
  let write_blk blk = (
    free_ops.get () |> bind (fun blk_id -> 
        disk_ops.write blk_id blk |> bind (fun _ -> 
            free_ops.set (blk_id+1) |> bind (fun () -> 
                return blk_id))))

  let read_blk blk_id = (
    disk_ops.read blk_id |> bind (fun blk ->
        return (Some blk)))
      
  (* files are implemented using the (index -> blk) map *)
  let mk_map_int_blk_ops ~page_ref_ops = 
    let map_ops = map_ops ~page_ref_ops in
    Map_int_blk.mk_int_blk_map ~write_blk ~read_blk ~map_ops
  
end



(* object map ------------------------------------------------------- *)

module Omap = struct
  
  open Bin_prot_util
  open E
  type k = oid
  type v = entry
  let v_size = E.bin_size_entry

  let ps = 
    Binprot_marshalling.mk_ps ~blk_sz
      ~cmp:Int_.compare ~k_size:bin_size_int ~v_size
      ~read_k:bin_reader_int ~write_k:bin_writer_int
      ~read_v:bin_reader_entry ~write_v:bin_writer_entry

  let store_ops = Disk_to_store.disk_to_store ~ps ~disk_ops ~free_ops

  let map_ops ~page_ref_ops = Store_to_map.store_ops_to_map_ops ~ps ~store_ops 
        ~page_ref_ops

end




(* file ops --------------------------------------------------------- *)

(* we have to implement pread and pwrite on top of omap, since size is
   stored in omap *)

(* requires dst_pos + src_len <= blk_sz; FIXME inefficient *)
let buf_block_blit ~blk_sz ~src ~src_pos ~src_len ~dst ~dst_pos = (
    assert (dst_pos + src_len <= blk_sz);
    dst |> Block.to_string |> Bytes.of_string
    |> (fun dst -> StdLabels.Bytes.blit ~src ~src_pos ~dst ~dst_pos ~len:src_len; dst)
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


  type ('k,'r)rstk = ('k,'r) Tjr_btree.Small_step.rstk

  let stack_to_lu_of_child = Tjr_btree.Isa_export.Tree_stack.stack_to_lu_of_child

  (* t is the root block of the idx_map *)
  let pread 
      ~read_block 
      ~(find_leaf: 'k -> 'r -> ('r*('k*'v)list*('k,'r)rstk,'t) m)
      ~page_ref_ops
      ~src_pos ~len ~dst ~dst_pos 
    = (
      assert (dst_pos + len <= Bytes.length dst);
      (* read the relevant leaf *)
      let blk_i = src_pos / blk_sz in
      page_ref_ops.get () |> bind (fun r ->
        find_leaf blk_i r |> bind (fun (_,kvs,rstk) ->
          (* kvs is the map from idx -> block_id; rstk tells us the
             maximum block we can try to read *)
          (* restrict len so that we do not try to read past u *)
          let (_,u) = stack_to_lu_of_child rstk in
          (* do not attempt to read block u or higher *)
          let limit_blk = match u with 
            | None -> max_int
            | Some i -> i
          in
          (* convert kvs to map for ease of use *)
          let map = assoc_list_to_bst kvs in
          let rec loop ~src_pos ~len ~dst_pos ~n_read = (
            let blk = src_pos / blk_sz in
            match () with
            | _ when len=0 || blk >= limit_blk -> return n_read
            | _ -> 
              let blk_offset = src_pos mod blk_sz in
              let get_blk = 
                (* which block to read? *)
                match map blk with
                | None -> return (Block.of_string blk_sz "")
                | Some i -> read_block i
              in
              get_blk |> bind (fun blk -> 
                let len' = min len (blk_sz - blk_offset) in
                block_buf_blit ~blk_sz ~src:blk ~src_pos:blk_offset 
                  ~len:len' ~dst ~dst_pos;
                loop ~src_pos:(src_pos+len') ~len:(len-len') 
                  ~dst_pos:(dst_pos+len') ~n_read:(n_read +len')))
          in
          loop ~src_pos ~len ~dst_pos ~n_read:0)))

  let pwrite 
        ~size_ops ~(map_ops:(int,blk,S.t)map_ops) 
        ~src ~src_pos ~src_len ~dst_pos : (int,S.t) m 
    = (
      assert (src_pos + src_len <= Bytes.length src);    
      begin
        let more_than_one_block = src_len >= blk_sz in
        match (dst_pos mod blk_sz = 0 && more_than_one_block) with
        | _ when src_len = 0 -> (return 0)
        | true -> (
          (* 
            * Case 1: writing a set of blocks at a block index 
  
            In this case, do the writes.

            NOTE: src_len is not necessarily a multiple of blk_sz 
  
            FIXME given the insert_many interface, we seem to have
            to create new blocks; read-only slices of underlying array
            would be useful so change Block impl; may want to use
            unsafe_to_string, with string slices
           *)
          let dst_blk_origin = dst_pos / blk_sz in
          let n_blks = src_len / blk_sz in  
          let blks = ref [] in
          let _ = 
            for i = n_blks-1 downto 0 do 
              let blk = Bytes.sub_string src (src_pos+(i*blk_sz)) blk_sz in
              blks:=(dst_blk_origin+i,Block.of_string blk_sz blk)::!blks
            done
          in
          match !blks with
          | [] -> failwith "impossible"
          | (k,v)::kvs -> (
            map_ops.insert_many k v kvs 
            |> bind (fun kvs' -> assert (kvs' = []); return (n_blks * blk_sz)))
        )
        | false -> (
        (* either less than one block, or non-block-aligned write (or
           both); write out a new, possibly partial, block; let the
           user be responsible for calling pwrite again *)
          (* we must read a block *)
          calc ~pos:dst_pos ~len:src_len (fun ~blk_index ~offset ~len -> 
                 let dst_blk = blk_index in
                 let dst_pos = offset in
                 (* read block *)
                 map_ops.find dst_blk 
                 |> bind (fun blk ->
                        let blk = 
                          match blk with
                          | None -> (Block.of_string blk_sz "")
                          | Some blk -> blk 
                        in
                        (* now update block: copy len bytes from
                           src_pos into blk at offset *)
                        let blk' = 
                          buf_block_blit ~blk_sz ~src ~src_pos ~src_len:len ~dst:blk ~dst_pos
                        in
                        (* write block back *)
                        map_ops.insert dst_blk blk'
                        |> bind (fun () -> return len))
               )
        )
      end
      (* now maybe adjust the size *)
      |> bind (fun n -> 
             size_ops.get () 
             |> bind (fun z -> if dst_pos+n > z then size_ops.set (dst_pos+n) else return ())
             |> bind (fun () -> return n))
    )    

end


let omap_ops : omap_ops = failwith ""


(* TODO instantiate the omap with cache *)


(*
    calc ~pos:dst_pos ~len:src_len (fun ~blk_index ~offset ~len:src_len -> 

        (* we can only write individual blocks to the lower layer... *)
        (* we may have to read a block if offset <> 0 or src_len < blk_sz *)
        let blk' = (
          match (offset<>0 || src_len < blk_sz) with
          | true -> (
              map_ops.find blk_index |> bind (fun x -> 
                  match x with 
                  | None -> return (BlkN.of_string blk_sz "")
                  | Some blk -> return blk))
          | false -> (return (BlkN.of_string blk_sz "")))
        in
        blk' |> bind (fun blk' -> 
            let blk' = blk_to_buffer blk' in

            (* blk' is the block that we are preparing; at this point, if
               needed it contains the data that was already present at
               blk_index; now we blit the new bytes *)

            (* FIXME we probably want to ensure that blks are
               immutable, but at the same time minimize copying *)

            blit ~src:src ~src_pos:src_pos ~dst:blk' ~dst_pos:offset ~len:src_len;
            let blk' = buffer_to_blk blk' in
            map_ops.insert blk_index blk' |> bind (fun _ -> 

                (* we may have to adjust the size of the bfile; but
                   clearly we want to avoid reading and writing the
                   meta block if we can; *)
                failwith "FIXME" |> bind (fun _ -> return src_len)))))

 *)
