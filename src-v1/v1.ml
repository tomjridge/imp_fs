(** V1 - implement directories and file metadata, with file data
   passed through to an underlying filesystem.

We have a single "global object map", which maps ids (ints) to either
   dir-roots, or file-meta, or symlinks.

NOTE much of this is based on {!Tjr_minifs.In_mem}, but with on-disk
   maps, rather than in-memory. *)

open Tjr_monad.With_lwt
open Std_types

open Bin_prot.Std


module F_meta = struct
  (* FIXME don't we want to store file size in the meta as well?
     what's the point of storing just this stuff? we are trying to
     make stat, ls etc very fast *)

  (** NOTE we store the file data in a file "n", where n is the id of the file. *)
  open Tjr_minifs.Minifs_intf.Stat_record
  type f_meta = stat_record [@@deriving bin_io]

  let time = Unix.time (* 1s resolution *)

  let make ~sz : f_meta = 
    time () |> fun t ->
    ({ sz; meta={ atim=t;ctim=();mtim=t }; kind=`File } : f_meta)

end
open F_meta

type obj_root = blk_id  [@@deriving bin_io]

module Dir_entry = struct
  (* open Str_256 *)
  type dir_entry = 
    | F of obj_root
    | D of obj_root
    | S of obj_root
  [@@deriving bin_io]
end
open Dir_entry

type fid=int type did=int

type id = 
  | Fid of int
  | Did of int
  | Sid of int
[@@deriving bin_io]

let root_dir_id = 0  (** the root directory *)


let consistent_entry id e = 
  match id,e with
  | Fid _,F _  | Did _,D _ | Sid _, S _ -> true
  | _ -> false
  

(** {2 Various marshallers} *)

module Ms = struct
  open Tjr_btree.Make_3
         
  let id_mshlr : id bin_mshlr = 
    (module 
      (struct 
        type t = id[@@deriving bin_io] 
        let max_sz = 10 (* FIXME check 9+1 *)
      end))

  let dir_entry_mshlr : dir_entry bin_mshlr = 
    (module 
      (struct 
        type t = dir_entry[@@deriving bin_io] 
        let max_sz = 10 (* FIXME check 9+1 *)
      end))

  open Str_256
  let s256_mshlr : str_256 bin_mshlr =
    (module 
      (struct 
        type t = str_256[@@deriving bin_io] 
        let max_sz = 259 (* FIXME check 256+3 *)
      end))
end



(** {2 The global object map (GOM) } *)


module Gom = struct
  open Tjr_btree.Make_3
  open Ms
       
  type k = id

  let k_mshlr : k bin_mshlr = id_mshlr

  type v = dir_entry

  let v_mshlr : v bin_mshlr = dir_entry_mshlr

  let gom_args = object
    method k_cmp: id -> id -> int = Stdlib.compare
    method k_mshlr = k_mshlr
    method v_mshlr = v_mshlr
  end  
end
open Gom


(** {2 Various runtime components} *)

let blk_dev_ref = ref None
let blk_dev_ops : (_,_,_) blk_dev_ops Lazy.t = lazy (
  match !blk_dev_ref with
  | None -> failwith __LOC__
  | Some x -> x)


let blk_alloc_ref = ref None  
let blk_alloc : (_,_)blk_allocator_ops Lazy.t = lazy (
  match !blk_alloc_ref with 
  | None -> failwith __LOC__
  | Some x -> x)


let root_ops_ref = ref None
let root_ops : (_,_)with_state Lazy.t = lazy (
  match !root_ops_ref with
  | None -> failwith __LOC__
  | Some x -> x)

type ('k,'v,'t) uncached_btree = ('k,'v,'t)Tjr_btree.Make_3.uncached_btree

let gom_btree : (k,v,t) uncached_btree Lazy.t = lazy (
  Tjr_btree.Make_4.make
    ~args:gom_args
    ~blk_dev_ops:(Lazy.force blk_dev_ops)
    ~blk_alloc:(Lazy.force blk_alloc)
    ~root_ops:(Lazy.force root_ops))

let _ = gom_btree

let min_free_id_ref = ref 0
let new_id () = 
  let r = !min_free_id_ref in
  incr min_free_id_ref;
  return r

let new_did = new_id

let new_fid = new_id



(** Instantiate this after gom_btree has been initialized *)
module With_gom() = struct
  let blk_dev_ops = Lazy.force blk_dev_ops 
  let blk_alloc = Lazy.force blk_alloc
  let gom_btree = Lazy.force gom_btree 

  let gom_find = gom_btree#find

  let gom_insert = gom_btree#insert

  (** NOTE this checks that the entries are consistent, and drop the option type *)
  let gom_find : id -> (dir_entry,t) m = fun i -> 
    gom_find i >>= function 
    | None -> failwith (Printf.sprintf "%s: attempt to deref unknown identifier" __FILE__)
    | Some e -> 
      assert(consistent_entry i e);
      return e
        

  (** In order to incorporate path resolution, we need to be able to
     lookup an entry in a directory *)
  module Dir = struct
    open Ms
    open Tjr_btree.Make_3
    open Str_256
    type k = str_256
    type v = id
      
    let k_mshlr = s256_mshlr
    let v_mshlr = id_mshlr
    
    let dir_args = object
      method k_cmp: str_256 -> str_256 -> int = Stdlib.compare
      method k_mshlr = k_mshlr
      method v_mshlr = v_mshlr
    end

    let dir_ops ~root_ops = 
      Tjr_btree.Make_4.make 
        ~args:dir_args
        ~blk_dev_ops
        ~blk_alloc
        ~root_ops

    let _ : root_ops:_ -> (k,v,t) uncached_btree = dir_ops

    open Tjr_path_resolution.Intf
    let resolve_comp: did -> comp_ -> ((fid,did)resolved_comp,t)m = 
      fun did comp ->
      assert(String.length comp <= 256);
      let comp = Str_256.make comp in
      gom_find (Did did) >>= fun (D r) -> 
      let r = ref r in
      let root_ops = with_ref r in
      let dir_ops = dir_ops ~root_ops in
      dir_ops#find comp >>= fun vopt ->
      match vopt with
      | None -> return RC_missing
      | Some x -> 
        match x with
        | Fid i -> RC_file i |> return
        | Did i -> RC_dir i |> return
        | Sid _i -> 
          (* FIXME perhaps we also need to identify a symlink by id
             during path res? *)
          failwith "FIXME need to lookup the contents of the sid"[@@warning "-8"]
                      
    let fs_ops = {
      root=root_dir_id;
      resolve_comp
    }

    (* NOTE for fuse we always resolve absolute paths *)
    let resolve = Tjr_path_resolution.resolve ~monad_ops ~fs_ops ~cwd:root_dir_id

    let _ :
follow_last_symlink:follow_last_symlink ->
string ->
((fid, did) resolved_path_or_err,t)m 
      = resolve

    (* The following gives reasonable behaviour *)      
    let resolve = resolve ~follow_last_symlink:`If_trailing_slash
    
    let _ : string -> ((fid, did) resolved_path_or_err,t)m = resolve

    
  end
  let resolve = Dir.resolve


  let lookup_file: int -> (f_meta,t)m = fun i ->
    gom_find (Fid i) >>= function
    | F r -> 
      blk_dev_ops.read ~blk_id:r >>= fun blk ->
      F_meta.bin_read_f_meta blk ~pos_ref:(ref 0) |> fun f_meta ->
      return f_meta
    | _ -> failwith "impossible"

  let lookup_dir: int -> ( (Dir.k,Dir.v,t)uncached_btree,t )m = fun i ->
    gom_find (Did i) >>= function
    | D r0 ->
      (* r0 is a blk that contains the blk_id which is the root of the B-tree *)
      blk_dev_ops.read ~blk_id:r0 >>= fun blk ->
      blk |> blk_to_buf |> fun buf -> 
      bin_read_r buf ~pos_ref:(ref 0) |> fun r ->
      let r = ref r in
      (* if we update the btree, we also have to update r0 *)
      let root_ops = 
        let with_state = fun f -> 
          f ~state:(!r)
            ~set_state:(fun r' -> 
                r:=r'; 
                let buf = buf_ops.create (Blk_sz.to_int blk_sz) in
                bin_write_r buf ~pos:0 r' |> fun _ ->
                let blk = buf_to_blk buf in
                blk_dev_ops.write ~blk_id:r0 ~blk >>= fun () ->
                return ())
        in
        {with_state}
      in
      let dir_ops = Dir.dir_ops ~root_ops in
      return dir_ops      
    | _ -> failwith "impossible"
    

  let ops: (_,_,_) Tjr_minifs.Minifs_intf.ops = 
    let open (struct
      let unlink path = 
        resolve path >>= fun x ->
        match x with
        | Ok r ->
          (* FIXME this also deals with the missign case, which is
             presumably an error FIXME *)
          let p = r.parent_id in
          let c = r.comp |> Str_256.make in
          (* remove c from p; obviously we should also remove objects
             which are no longer referenced FIXME *)
          (* lookup p *)
          lookup_dir p >>= fun dir_ops ->
          dir_ops#delete c >>= fun () ->
          return (Ok ())
        | Error _e -> (* FIXME this case *)
          return (Error `Error_other)

      let mkdir path =
        resolve path >>= fun x -> 
        match x with
        | Ok { result=Missing; _ } -> 
          new_did () >>= fun did ->
          (* new blk to store the B-tree root *)
          blk_alloc.blk_alloc () >>= fun r0 ->
          (* new blk to store the empty leaf *)
          blk_alloc.blk_alloc () >>= fun r1 ->
          (* write an empty leaf to r1 *)
          blk_dev_ops.write ~blk_id:r1 ~blk:(failwith "FIXME") >>= fun () ->
          (* write the blk_id to r0 *)
          blk_dev_ops.write ~blk_id:r0 ~blk:(failwith "FIXME") >>= fun () ->
          (* update the gom *)            
          gom_insert (Did did) (D r0) >>= fun () ->
          return (Ok ())

        | _ -> return (Error `Error_exists) (* FIXME this case *)            

      type dh = (Dir.k,Dir.v,t) Tjr_btree.Make_3.ls 

      let opendir path = 
        resolve path >>= fun x ->
        match x with
        | Ok { result=Dir did; _ } -> 
          (* lookup did to get root blk *)
          lookup_dir did >>= fun dir_ops ->
          dir_ops#ls_create () >>= fun ls ->
          return (Ok ls)
        
        | _ -> 
          (* FIXME this case *)
          return (Error `Error_not_directory)
        
      let readdir (ls:dh) = 
        ls#ls_kvs () |> fun kvs -> 
        (* FIXME add str_256 conversion to_string to str_256 intf; this is ugly *)
        let kvs = List.map (fun ( (k:str_256),_v) -> (k :> string) ) kvs in 
        ls#ls_step () >>= fun Tjr_btree.Make_3.{finished} ->
        return (Ok (kvs,Tjr_minifs.Minifs_intf.{finished})) (* FIXME move to fs_shared *)
      
      let closedir _ = return (Ok())
      (* FIXME should we record which rd are valid? ie not closed *)

      let create path = 
        resolve path >>= function
        | Ok { result=Missing; _ } ->
          new_fid () >>= fun fid ->
          let meta = F_meta.make ~sz:0 in
          (* create a new blk for the file root *)
          blk_alloc.blk_alloc () >>= fun r0 ->
          buf_ops.create (Blk_sz.to_int blk_sz) |> fun buf ->
          (* marshal the new meta to the file root blk *)
          F_meta.bin_write_f_meta buf ~pos:0 meta |> fun _ -> 
          buf |> buf_to_blk |> fun blk ->
          blk_dev_ops.write ~blk_id:r0 ~blk >>= fun () ->
          (* now update the gom *)
          gom_insert (Fid fid) (F r0) >>= fun () ->
          return (Ok ())
        | _ -> 
          return (Error `Error_exists) (* FIXME this case *)

      let open_ _path = failwith "FIXME"
      end)
    in
    {
      root="/";
      unlink;
      mkdir;
      opendir;
      readdir;
      closedir;
      create;
      open_;
    }
 
end

(* FIXME this is repeating a lot of stuff from in_mem; perhaps we should abstract? *)


