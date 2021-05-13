(** Run the V3 filesystem *)


(** {2 Level 2} *)

open V3_intf
open V3_level2

module Level2_stage1 = struct

  let config = {
    entries_cache_capacity   = 10;
    entries_cache_trim_delta = 5;
    live_dirs_capacity       = 10;
    live_dirs_trim_delta     = 5;
    file_data_path           = "tmp/v3_data";
    live_files_capacity      = 10;
    live_files_trim_delta    = 5;
  }

  module V3_sqlite_dir = V3_sqlite_dir.Make()

  let db = Sqlite3.(db_open "tmp/v3_database.db")

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

