(* fuse integration ------------------------------------------------- *)

open Unix
open LargeFile
open Bigarray
open Fuse
open Tjr_btree
open Btree_api
open Page_ref_int
open Params
open Block

module Blk=Blk4096
open Blk
open Imp_pervasives
open Imp_state
open Path_resolution
open Object_map
open Monad

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

let do_readdir path _ : string list = safely @@ fun () -> 
  string_to_components path @@ fun ~cs ~ends_with_slash ->
  let dir_map_ops did = did_to_map_ops ~did in
  let m = 
    (resolve' 
       ~root_did 
       ~dir_map_ops
       ~cs
       ~ends_with_slash
       ~_Error:(fun e -> `Error e)  (* TODO *)
       ~_Missing:(fun ~parent ~c ~ends_with_slash -> `Error `ENOENT)
       ~_File:(fun ~parent ~fid -> `Error(`ENOTDIR))
       ~_Dir:(fun ~parent ~did -> `Dir(did))) |> bind @@ function 
    | `Dir(did) -> 
      did_to_ls_ops ~did |> bind @@ fun ls_ops ->
      Btree_api.all_kvs ls_ops
    | `Error _ -> 
      failwith __LOC__ (* TODO *)
  in
  m |> run_ 
  |> List.map (fun x -> x |> fst |> Small_string.to_string)


let do_fopen path flags = safely (fun () -> 
  string_to_components path @@ fun ~cs ~ends_with_slash ->
  let dir_map_ops did = did_to_map_ops ~did in
  let m = 
    (resolve' 
       ~root_did 
       ~dir_map_ops
       ~cs
       ~ends_with_slash
       ~_Error:(fun e -> `Error e)
       ~_Missing:(fun ~parent ~c ~ends_with_slash -> `Error `ENOENT)
       ~_File:(fun ~parent ~fid -> `File)
       ~_Dir:(fun ~parent ~did -> `Dir)) |> bind @@ function 
    | `Dir | `File -> return None
    | `Error _ -> raise (Unix_error (ENOENT,"open",path))
  in
  m |> run_)


(* assume all reads are block-aligned *)
let do_read path buf ofs _ = safely @@ fun () -> 
  let open Imp_file in
  string_to_components path @@ fun ~cs ~ends_with_slash ->
  let dir_map_ops did = did_to_map_ops ~did in
  (resolve' 
     ~root_did 
     ~dir_map_ops
     ~cs
     ~ends_with_slash
     ~_Error:(fun e -> `Error e)
     ~_Missing:(fun ~parent ~c ~ends_with_slash -> `Error `ENOENT)
     ~_File:(fun ~parent ~fid -> `File(fid))
     ~_Dir:(fun ~parent ~did -> `Error `EISDIR))
  |> bind @@ function 
  | `Error _ -> raise (Unix_error (ENOENT (* FIXME *),"read",path)) 
  | `File(fid) -> 
   FIXME got here

  m |> run_)


  let buf_size = Bigarray.Array1.dim buf in
  let msg () = 
    `List[`String "read"; `String path; `Int (Int64.to_int ofs); `Int buf_size]
    |> Yojson.Safe.to_string 
  in
  Test.test(fun () -> msg () |> print_endline);


  let dir_map_ops did = did_to_map_ops ~did in
  resolve' 
    ~root_did
    ~dir_map_ops
  
        (* we want to return a single block FIXME to begin with *)
        let i = ((Int64.to_int ofs) / blk_sz) in
        let blk = imap_ops.find i in
        let blk = 
          match blk with
          | None -> Blk.of_string ""
          | Some blk -> blk
        in
        let blk = Blk.to_string blk in
        for j = 0 to blk_sz -1 do  (* copy to buf FIXME use blit *)
          buf.{j} <- String.get blk j 
        done;
        blk_sz
      with e -> (
          print_endline "do_read:!"; 
          msg ()|>print_endline; 
          e|>Printexc.to_string|>print_endline;
          Printexc.get_backtrace() |> print_endline;
          ignore(exit 1);  (* exit rather than allow fs to continue *)
          raise (Unix_error (ENOENT,"read",path))))


(* NOTE ofs is offset in file (pointer to by path); loopback writes
   may be non-block-aligned, and less than the block size *)
let do_write path (buf:('x,'y,'z)Bigarray.Array1.t) ofs _fd = safely (fun () -> 
    let buf_size = Bigarray.Array1.dim buf in
    let blk_id = ((Int64.to_int ofs) / blk_sz) in
    let offset_within_block = Int64.rem ofs (Int64.of_int blk_sz) |> Int64.to_int in
    let wf = 
      path = "/"^Img.fn &&
      buf_size > 0 &&  (* allow < blk_sz, but not 0 *)
      true (* offset_within_block = 0  FIXME may want to allow writing at an offset *)
    in 
    let msg () = 
      `List[`String "write"; `String path; `Int (Int64.to_int ofs); 
            `String "buf_size,blk_id,offset_within_block"; `Int buf_size; `Int blk_id; `Int offset_within_block]
      |> Yojson.Safe.to_string 
    in
    Test.test (fun () -> msg () |> print_endline);
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
            raise (Unix_error (ENOENT,"write",path))) ))


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

