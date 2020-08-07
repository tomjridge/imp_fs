(*
let _ = 
  File_impl_v1.Iter_block_blit.test ();
  File_impl_v1.test ()
*)

module X = File_impl_v2.Test()

let _ = Lwt_main.run (X.run () |> With_lwt.to_lwt)
