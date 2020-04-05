open Tjr_log
open Imp_fs

module Ip = Imp_pervasives
;;

Ip.log_ops.log_n := 100;;
    
let _ = 
  match Persistent_log.Test.test ~depth:6 with
  | exception e -> (
    Printf.printf "Exception occurred\nPrinting log:\n";
    Ip.log_ops.print_last_n ();
    Printf.printf "End log\n";
    raise e)
  | _ -> ()


(*
let _=  
  Printf.printf "Printing log:\n";
  Imp_pervasives.log_ops.print_last_n ();
  Printf.printf "End log\n";
*)
