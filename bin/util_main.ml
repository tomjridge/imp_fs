let argv = Sys.argv |> Array.to_list |> List.tl

let _ = 
  match argv with
  | ["print_trace";dirname] -> 
    Test_blk_dev.print_trace ~dirname
  | ["test";"sqlite_dir"] -> 
    let module X = Sqlite_dir.Test() in
    Lwt_main.run (X.test_program |> Tjr_monad.With_lwt.to_lwt)
  | _ -> failwith "Unrecognized argv"

