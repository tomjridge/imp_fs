(** Run the V3 filesystem *)

(** Enable/disable logging *)
let _ = Util.dont_log := true

(** Use the env var IMP_ROOT to point to where impfs should store data *)
let _IMP_ROOT = Sys.getenv_opt "IMP_ROOT" |> function
  | None -> failwith "Environment variable IMP_ROOT not set; this should be a directory where impfs can store data"
  | Some x -> x

let file_data_path = _IMP_ROOT^"/v3_data"
let db_path = _IMP_ROOT^"/v3_database.db"

let do_init () = 
  Printf.printf "Initializing... IMP_ROOT=%s (contains database and file data)\n%!" _IMP_ROOT;
  let _rename_existing =
    file_exists db_path |> function
    | true -> 
      Unix.rename db_path (db_path ^ "." ^ (string_of_float @@ Unix.time()))
    | false -> ()
  in
  let open Sqlite3 in
  let open Sqlite_dir in
  let db = db_open db_path in
  create_tables db;
  add_root_directory db;
  Stdlib.exit 0

let _ = 
  Sys.argv |> Array.to_list |> List.tl |> fun argv -> 
  match argv with
  | ["init"] -> do_init ()
  | _ -> () (* lots of fuse args etc which are passed to the fuse lib *)


(** {2 Level 2} *)

open V3_intf
open V3_level2

module Level2_stage1 = struct

  let config = {
    entries_cache_capacity   = 1000;
    entries_cache_trim_delta = 20;
    live_dirs_capacity       = 1000;
    live_dirs_trim_delta     = 200;
    file_data_path;
    live_files_capacity      = 1000;
    live_files_trim_delta    = 20;
  }

  module V3_sqlite_dir = V3_sqlite_dir.Make()

  (* FIXME not sure why FULL is necessary if default sqlite3
     https://sqlite.org/threadsafe.html is serialized... perhaps
     ocaml-sqlite doesn't default to serialized? FIXME actually still
     saw errors even with full mutex, see log.15 *)
  let db = Sqlite3.(db_open ~mutex:`FULL db_path)

  let sql_dir_ops = V3_sqlite_dir.make_dir_ops ~db
end

module Level2_stage2 = V3_level2.Stage2(Level2_stage1)


(** {2 Level 1} *)

module V3_level1_1 = V3_level1.Make(V3_base_types)
module V3_level1_2 = V3_level1_1.Make_2(Level2_stage2)

let level1_ops : _ Level1_provides.ops = V3_level1_2.ops

module V3_level0 = V3_level0.Make()

let ops : _ Minifs_intf.Ops_type.ops = V3_level0.make ~level1_ops


(** {2 Threads: Lwt_main and FUSE} *)

(** Set lwt running, but NOT in the main thread (we need that for
   FUSE) *)
let () = 
  (* an lwt thread that just keeps going... *)
  let rec t () = Lwt.Infix.(
      Lwt_unix.sleep 1.0 >>= fun () ->
      Printf.printf "%s: lwt thread wakes\n%!" __FILE__;
      t ())
  in
  (* run lwt in another thread *)
  let _t = 
    Thread.create 
      (fun () ->
         Printf.printf "%s: Lwt main thread starts\n%!" __FILE__;
         Lwt_main.run (t()))
      ()
  in
  ()

open Tjr_monad.With_lwt

(** Run the FUSE main loop, with Lwt running in a separate (non-Lwt)
   thread *)
let () =
  Printf.printf "%s: running FUSE main loop\n%!" __FILE__;
  let fuse_ops = 
    Fuse_.mk_fuse_ops 
      ~monad_ops ~ops 
      ~co_eta:Tjr_minifs.Lwt_util.co_eta 
  in
  Fuse.main Sys.argv fuse_ops

