let argv = Sys.argv |> Array.to_list |> List.tl

let _ = 
  match argv with
  | ["print_trace";dirname] -> 
    Test_blk_dev.print_trace ~dirname
  | ["test";"file_impl_v2"] -> 
    let module X = File_impl_v2.Test() in
    Lwt_main.run (X.run () |> With_lwt.to_lwt)
  | ["test";"sqlite_dir"] -> 
    let module X = Sqlite_dir.Test() in
    Lwt_main.run (X.test_program |> Tjr_monad.With_lwt.to_lwt)
(* sqlite_gom not used  | ["test";"sqlite_gom"] -> 
    let module X = Sqlite_gom.Test() in
    Lwt_main.run (X.test_program |> Tjr_monad.With_lwt.to_lwt) *)
  | _ -> failwith "Unrecognized argv"

