(* FIXME commented for the time being; use mini-fs version

(** FuSE integration *)

(* fuse integration ------------------------------------------------- *)

open Unix
open LargeFile
open Bigarray
open Fuse

open Imp_pervasives
open Tjr_fs_shared
open X.Base_types_pervasives
open X.Page_ref_int
open X.Params
open X.Block
module Blk=Blk4096
open Blk

open Imp_state
open Path_resolution
open Object_map

let default_stats = LargeFile.stat "."

let safely f = 
  try f () with e -> Printexc.to_string e |> print_endline; raise e


(* data ------------------------------------------------------------- *)

(* stores all versions of data blocks and id->id map, using underlying
   fd *)

module Data = struct

  (* where data is stored in the base filesystem; grows without limit! *)
  let fn = "./data_blocks"

  let fd = safely (
      fun () -> Unix.openfile fn [O_CREAT;O_RDWR] 0o640)

end


(* global mutable state --------------------------------------------- *)

let the_state = ref {  
  fd=Data.fd;
  free=0;
  omap_root=0;
  omap_cache=();
  omap_additional_object_roots=0;
  file_caches=(fun _ -> ());
  dir_caches=(fun _ -> ());
}

let run_ m = m |> Monad.run !the_state |> (fun (s',r) -> 
  the_state:=s';
  match r with 
  | Ok v -> v
  | Error e -> failwith e)


(* fuse ------------------------------------------------------------- *)


let root_did : did = (Obj.magic 0)

open Monad

let do_readdir path _ : string list = safely @@ fun () -> 
  begin 
    path 
    |> (resolve
        ~root_did 
        ~did_to_map_ops
        ~_Error:(fun e -> `Error e)  (* TODO *)
        ~_Missing:(fun ~parent ~c ~ends_with_slash -> `Error `ENOENT)
        ~_File:(fun ~parent ~fid -> `Error(`ENOTDIR))
        ~_Dir:(fun ~parent ~did -> `Dir(did))) 
    |> bind @@ function 
    | `Dir(did) -> 
      did_to_ls_ops ~did |> bind @@ fun ls_ops ->
      X.Leaf_stream_util.all_kvs ~ls_ops
    | `Error _ -> 
      failwith __LOC__ (* TODO *)
  end
  |> run_ 
  |> List.map (fun x -> x |> fst |> X.Small_string.to_string)


let do_fopen path flags = safely (fun () -> 
  begin
    path
    |> (resolve
        ~root_did 
        ~did_to_map_ops
        ~_Error:(fun e -> `Error e)
        ~_Missing:(fun ~parent ~c ~ends_with_slash -> `Error `ENOENT)
        ~_File:(fun ~parent ~fid -> `File)
        ~_Dir:(fun ~parent ~did -> `Dir)
    )        
    |> bind @@ function 
    | `Dir | `File -> return None
    | `Error _ -> raise (Unix_error (ENOENT,"open",path))
  end
  |> run_)


(* assume all reads are block-aligned; ofs is offset in file? *)
let do_read path buf ofs _ = safely @@ fun () -> 
  let open Imp_file in
  let len=buffer_length buf in
  begin 
    path
    |> (resolve
        ~root_did 
        ~did_to_map_ops
        ~_Error:(fun e -> `Error e)
        ~_Missing:(fun ~parent ~c ~ends_with_slash -> `Error `ENOENT)
        ~_File:(fun ~parent ~fid -> `File(fid))
        ~_Dir:(fun ~parent ~did -> `Error `EISDIR))
    |> bind @@ function 
    | `Error _ -> raise (Unix_error (ENOENT (* FIXME *),"read",path)) 
    | `File(fid) ->
      (* get page ref for fid *)
      lookup_fid ~fid |> bind @@ fun (r,sz) ->
      let find_leaf = failwith "TODO" in
      let read_block i = imp_read_block i |> bind (fun x -> return (dest_Some x)) in (* FIXME *)
      pread' ~read_block ~find_leaf ~r
        ~src_pos:ofs ~src_length:len ~len ~dst:buf ~dst_pos:0
  end
  |> run_


(* NOTE ofs is offset in file (pointer to by path) *)
let do_write path buf ofs _fd = safely @@ fun () -> 
  let open Imp_file in
  let len = buffer_length buf in
  

  (if (offset_within_block + buf_size > blk_sz) then
     Test.warn (__LOC__^": offset_within_block + buf_size > blk_sz"));
  let open Btree_api.Imperative_map_ops in
  match () with
  | _ when (not wf) -> (
      Test.warn (__LOC__^": not wf");
      raise (Unix_error (ENOENT,"write",path)))
  | _ -> (
      try
        let blk = 
          match offset_within_block = 0 with
          | true -> Bytes.create blk_sz
          | false -> (
              (* have to read existing block *)
              imap_ops.find blk_id |> function None -> Bytes.create blk_sz | Some blk -> blk|>Blk.to_string)
        in
        let n = min (blk_sz - offset_within_block) buf_size in  (* allow writing < blk_sz *)
        for j = 0 to n -1 do
          Bytes.set blk (offset_within_block+j) buf.{j}  (* FIXME use blit *)
        done;
        blk |> Bytes.to_string |> Blk.of_string |> fun blk ->
        imap_ops.insert blk_id blk |> (fun () -> n)
      with e -> (
          print_endline "do_write:!"; 
          msg ()|>print_endline;
          e|>Printexc.to_string|>print_endline;
          Printexc.get_backtrace() |> print_endline;
          ignore(exit 1);
          raise (Unix_error (ENOENT,"write",path))) )


(*

(* following apparently required for a block device backed by a file on a fuse fs ? *)
(* let do_fsync path ds hnd = () *)

(* let do_getxattr s1 s2 = "?" *)


let _ =
  main Sys.argv 
    { default_operations with 
      (* getattr = do_getattr; *)
      readdir = do_readdir;
      fopen = do_fopen;
      read = do_read;
      write = do_write;
      truncate = (fun _ _ -> print_endline "truncate"; ());
      (*      fsync = do_fsync;
              getxattr = do_getxattr; *)
    }


(* opendir = (fun path flags -> 
   (Unix.openfile path flags 0 |> Unix.close);
   None); (* FIXME from fusexmp; not sure needed *) *)

*)
*)
