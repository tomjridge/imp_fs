let argv = Sys.argv |> Array.to_list |> List.tl

let _ = 
  match argv with
  | ["print_trace";dirname] -> 
    Test_blk_dev.print_trace ~dirname
  | _ -> failwith "Unrecognized argv"

