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
open Omap_state

let default_stats = LargeFile.stat "."

let safely f = 
  try f () with e -> Printexc.to_string e |> print_endline; raise e


(* global state ----------------------------------------------------- *)

type t = {
  fd: Disk_on_fd.fd;
  omap_state:omap_state;
}
type fs_state = t

module Ops = struct
  open Monad
  let fd_ops = {
    get=(fun () -> fun t -> (t,Ok t.fd));
    set=(fun fd -> failwith "Don't use!");
  }

  let free_ops = {
    get=(fun () -> fun t -> (t,Ok t.free));
    set=(fun free -> fun t -> ({t with free}, Ok ()));
  }
  
  let page_ref_ops = {
    get=(fun () -> fun t -> (t,Ok t.omap_state.omap_root));
    set=(fun omap_root -> fun t -> (
        {t with omap_state={t.omap_state with omap_root}},Ok ()));
  }

end

open Ops


(* data ------------------------------------------------------------- *)

(* stores all versions of data blocks and id->id map, using underlying
   fd *)

module Data = struct

  (* where data is stored in the base filesystem; grows without limit! *)
  let fn = "./data_blocks"

  let fd = safely (
      fun () -> Unix.openfile fn [O_CREAT;O_RDWR] 0o640)

  let disk_ops : fs_state Btree_api.disk_ops = 
    Disk_on_fd.make_disk blk_sz fd_ops

end

(* fuse ------------------------------------------------------------- *)

open Path_resolution
open Object_map
open Omap_entry
open Monad

let root_oid = 0

let do_readdir path _ = safely @@ fun () -> 
  string_to_components path @@ fun ~cs ~ends_with_slash ->
  let dir_map_ops oid = Omap.get_dir_map_ops ~oid in
  let m = 
    resolve' 
      ~root_oid 
      ~dir_map_ops
      ~cs
      ~ends_with_slash
      ~_Dir:(fun ~parent ~oid -> `Dir(oid))
      ~_File:(fun ~parent ~oid ~sz -> `Error(`ENOTDIR)
      ~_Error:(fun e -> e)  (* TODO *)
    |> bind @@ fun resolved -> 
    match resolved with
    | `Dir(oid) -> Omap.get_dir_ls_ops ~oid |> bind @@ fun ls_ops ->
      Btree_api.all_kvs ls_ops
    | F(oid,sz) -> failwith __LOC__ (* TODO *)
  in
  m |> Monad.run !the_state |> fun (s',r) -> match r with

;;
  match resolved with
  | 
  Imp_fs.readdir !{ls_ops=}

let do_fopen path flags = safely (fun () -> 
    if path = "/"^Img.fn then None
    else raise (Unix_error (ENOENT,"open",path)))

(* assume all reads are block-aligned *)
let do_read path buf ofs _ = safely (fun () ->     
    let buf_size = Bigarray.Array1.dim buf in
    let wf = 
      path = "/"^Img.fn &&
      buf_size >= blk_sz &&  (* allow attempts to read more, but only read blk_sz *)
      Int64.rem ofs (Int64.of_int blk_sz) = Int64.zero       
    in 
    let msg () = 
      `List[`String "read"; `String path; `Int (Int64.to_int ofs);
            `Int buf_size]
      |> Yojson.Safe.to_string 
    in
    Test.test(fun () -> 
      msg () |> print_endline);
    (if buf_size > blk_sz then
       Test.warn (__LOC__^": buf_size > blk_sz"));
    let open Btree_api.Imperative_map_ops in
    match () with
    | _ when (not wf) -> (
        Test.warn (__LOC__^": not wf");
        raise (Unix_error (ENOENT,"read",path)))
    | _ -> (
        try
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
            raise (Unix_error (ENOENT,"read",path)))))

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
      getattr = do_getattr;
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

