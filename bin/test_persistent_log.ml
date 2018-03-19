open Tjr_log
open Imp_fs
    
let _ = 
  match Persistent_log.Test.main () with
  | exception e -> (
    Printf.printf "Exception occurred\nPrinting log:\n";
    Imp_pervasives.log_ops.print_last_n ();
    Printf.printf "End log\n";
    raise e;    
  )
  | _ -> ()


let _=  
  Printf.printf "Printing log:\n";
  Imp_pervasives.log_ops.print_last_n ();
  Printf.printf "End log\n";
